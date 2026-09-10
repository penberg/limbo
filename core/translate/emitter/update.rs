use super::gencol::compute_virtual_columns;
use super::TranslateCtx;
use crate::alloc::{TryClone, TursoIteratorExt};
use crate::schema::{Column, ColumnLayout, GeneratedType, Table};
use crate::translate::insert::halt_desc_and_on_error;
use crate::translate::plan::ColumnMask;
use crate::translate::stmt_journal::any_effective_replace;
use crate::vdbe::builder::SelfTableContext;
use crate::{
    ast, emit_explain,
    error::{SQLITE_CONSTRAINT_NOTNULL, SQLITE_CONSTRAINT_PRIMARYKEY, SQLITE_CONSTRAINT_UNIQUE},
    schema::{
        collect_column_dependencies_of_expr, BTreeTable, CheckConstraint, Index,
        EXPR_INDEX_SENTINEL, ROWID_SENTINEL,
    },
    sync::Arc,
    translate::{
        emitter::{
            check_expr_references_columns, delete::emit_fk_child_decrement_on_delete,
            emit_cdc_autocommit_commit, emit_cdc_full_record, emit_cdc_insns,
            emit_cdc_patch_record, emit_check_constraints, emit_index_column_value_new_image,
            emit_index_column_value_old_image, emit_make_record, emit_program_for_select,
            OperationMode, Resolver, UpdateRowSource,
        },
        eqp::eqp_detail_for_table_op,
        expr::{
            emit_dml_expr_index_value, emit_returning_results, emit_returning_scan_back,
            emit_table_column, restore_returning_row_image_in_cache,
            seed_returning_row_image_in_cache, translate_expr, translate_expr_no_constant_opt,
            NoConstantOptReason, ReturningBufferCtx,
        },
        fkeys::{
            emit_fk_child_update_counters, emit_fk_parent_deferred_new_key_probes,
            emit_fk_update_parent_actions, fire_fk_update_actions, stabilize_new_row_for_fk,
            ForeignKeyActions, ParentKeyNewProbeMode,
        },
        main_loop::{CloseLoop, InitLoop, OpenLoop},
        plan::{
            EvalAt, IterationDirection, JoinOrderMember, JoinedTable, NonFromClauseSubquery,
            Operation, OuterQueryReference, QueryDestination, ResultSetColumn, Scan, Search,
            SubqueryEvalPhase, TableReferences, UpdatePlan, UpdateSetClause,
        },
        planner::ROWID_STRS,
        subquery::{emit_non_from_clause_subqueries_for_eval_at, emit_non_from_clause_subquery},
        trigger_exec::{
            fire_trigger, get_triggers_including_temp, has_triggers_including_temp, TriggerContext,
        },
        ProgramBuilder,
    },
    util::normalize_ident,
    vdbe::{
        affinity::Affinity,
        builder::{CursorKey, CursorType, DmlColumnContext},
        insn::{to_u32, CmpInsFlags, IdxInsertFlags, InsertFlags, Insn, RegisterOrLiteral},
        BranchOffset,
    },
    CaptureDataChangesExt, Connection, HashSet, Result, MAIN_DB_ID,
};
use std::num::NonZeroUsize;
use tracing::{instrument, Level};
use turso_macros::{turso_assert, turso_assert_eq};
use turso_parser::ast::{ResolveType, TriggerEvent, TriggerTime};

/// Info about position of rowid alias in the table if present + whether the current UPDATE statement will update the rowid.
struct RowidUpdateInfo {
    rowid_alias_index: Option<usize>,
    updates_rowid: bool,
}

fn rowid_update_info(
    target_table: &JoinedTable,
    set_clauses: &[UpdateSetClause],
) -> RowidUpdateInfo {
    let has_direct_rowid_update = set_clauses
        .iter()
        .any(|set_clause| set_clause.column_index == ROWID_SENTINEL);
    let rowid_alias_index = target_table
        .table
        .columns()
        .iter()
        .position(|column| column.is_rowid_alias());
    let updates_rowid = has_direct_rowid_update
        || rowid_alias_index.is_some_and(|alias_idx| {
            set_clauses
                .iter()
                .any(|set_clause| set_clause.column_index == alias_idx)
        });

    RowidUpdateInfo {
        rowid_alias_index,
        updates_rowid,
    }
}

#[instrument(skip_all, level = Level::DEBUG)]
pub fn emit_program_for_update(
    connection: &Arc<Connection>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    mut plan: UpdatePlan,
    after: impl FnOnce(&mut ProgramBuilder),
) -> Result<()> {
    program.set_resolve_type(plan.or_conflict.unwrap_or(ResolveType::Abort));
    program
        .flags
        .set_has_statement_conflict(plan.or_conflict.is_some());

    // Open an ephemeral table for buffering RETURNING results.
    // All DML completes before any RETURNING rows are yielded to the caller.
    let returning_buffer = if plan.returning.as_ref().is_some_and(|r| !r.is_empty()) {
        let btree_table = plan
            .target_table
            .table
            .btree()
            .expect("UPDATE target must be a BTree table");
        let ret_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(btree_table));
        program.emit_insn(Insn::OpenEphemeral {
            cursor_id: ret_cursor_id,
            is_table: true,
        });
        Some(ReturningBufferCtx {
            cursor_id: ret_cursor_id,
            num_columns: plan.returning.as_ref().unwrap().len(),
        })
    } else {
        None
    };

    let target_table = Arc::new(plan.target_table.clone());
    let target_tables = TableReferences::new(
        vec![plan.target_table.clone()],
        plan.from_tables.outer_query_refs().to_vec(),
    );
    let write_set_plan = plan.write_set_plan.take();
    let temp_cursor_id = match write_set_plan.as_ref() {
        Some(plan) => {
            let QueryDestination::EphemeralTable { cursor_id, .. } = &plan.select.query_destination
            else {
                crate::bail_parse_error!("write set plan must use an ephemeral table destination");
            };
            Some(*cursor_id)
        }
        None => None,
    };
    // If an ephemeral scratch table is used (either for UPDATE...FROM or general Halloween protection)
    // then the write phase will just iterate and read from the scratch table.
    // The target table is kept as an outer query reference so that RETURNING can reference the target table
    // during the write phase.
    let uses_write_set = temp_cursor_id.is_some();
    let mut write_phase_tables = if let Some(write_set_plan) = write_set_plan {
        let QueryDestination::EphemeralTable {
            table: scratch_table,
            ..
        } = &write_set_plan.select.query_destination
        else {
            crate::bail_parse_error!("write set plan must use an ephemeral table destination");
        };
        let scratch_table = scratch_table.clone();
        let scratch_table_internal_id = write_set_plan.scratch_table_id;
        program.emit_insn(Insn::OpenEphemeral {
            cursor_id: temp_cursor_id.unwrap(),
            is_table: true,
        });
        program
            .nested(|program| emit_program_for_select(program, resolver, write_set_plan.select))?;
        let mut write_phase_tables = TableReferences::new(
            vec![JoinedTable {
                table: Table::BTree(scratch_table),
                identifier: "ephemeral_scratch".to_string(),
                internal_id: scratch_table_internal_id,
                op: Operation::Scan(Scan::BTreeTable {
                    iter_dir: IterationDirection::Forwards,
                    index: None,
                }),
                join_info: None,
                col_used_mask: Default::default(),
                column_use_counts: Vec::new(),
                expression_index_usages: Vec::new(),
                database_id: MAIN_DB_ID,
                indexed: None,
                plan_estimate: None,
            }],
            vec![],
        );
        write_phase_tables.add_outer_query_reference(OuterQueryReference {
            identifier: target_table.identifier.clone(),
            internal_id: target_table.internal_id,
            table: target_table.table.clone(),
            using_dedup_hidden_cols: ColumnMask::default(),
            col_used_mask: target_table.col_used_mask.try_clone()?,
            cte_select: None,
            cte_explicit_columns: vec![],
            cte_id: None,
            cte_definition_only: false,
            rowid_referenced: false,
            scope_depth: 0,
        });
        // CTEs are also visible during the write phase, so add them here.
        for outer_ref in plan.from_tables.outer_query_refs() {
            write_phase_tables.add_outer_query_reference(outer_ref.clone());
        }
        write_phase_tables
    } else {
        target_tables
    };

    let mut t_ctx = TranslateCtx::new(
        program,
        resolver.fork(),
        write_phase_tables.joined_tables().len(),
        connection.db.opts.unsafe_testing,
    );

    let after_main_loop_label = program.allocate_label();
    t_ctx.label_main_loop_end = Some(after_main_loop_label);

    // No rows will be read from source table loops if there is a constant false condition eg. WHERE 0
    if plan.contains_constant_false_condition {
        program.emit_insn(Insn::Goto {
            target_pc: after_main_loop_label,
        });
    }

    let mode = OperationMode::UPDATE(if uses_write_set {
        UpdateRowSource::PrebuiltEphemeralTable {
            ephemeral_table_cursor_id: temp_cursor_id.expect(
                "ephemeral table cursor id is always allocated when UPDATE uses a write set",
            ),
            target_table: target_table.clone(),
        }
    } else {
        UpdateRowSource::Normal
    });

    let join_order = write_phase_tables
        .joined_tables()
        .iter()
        .enumerate()
        .map(|(i, t)| JoinOrderMember {
            table_id: t.internal_id,
            original_idx: i,
            is_outer: false,
        })
        .collect::<Vec<_>>();

    // Evaluate uncorrelated subqueries as early as possible (only for the direct path).
    // For the ephemeral write-set path, WHERE clause subqueries are handled by emit_program_for_select
    // on the ephemeral SELECT. SET clause subqueries remain in the main plan and are emitted
    // inside the update loop (after open_loop) where the write cursor is correctly positioned.
    if !uses_write_set {
        emit_non_from_clause_subqueries_for_eval_at(
            program,
            &t_ctx.resolver,
            &mut plan.non_from_clause_subqueries,
            &join_order,
            Some(&write_phase_tables),
            EvalAt::BeforeLoop,
            |_| true,
        )?;
    }

    // Drain write-phase subqueries so init_loop/open_loop only handle WHERE-clause
    // subqueries. SET subqueries must run after NotExists positions the write cursor,
    // and RETURNING subqueries must run after the row has been written.
    // This applies to both the direct and ephemeral write-set UPDATE paths.
    let mut update_subqueries = Vec::new();
    {
        let mut i = 0;
        while i < plan.non_from_clause_subqueries.len() {
            let subquery = &plan.non_from_clause_subqueries[i];
            if subquery.eval_phase == SubqueryEvalPhase::BeforeLoop {
                i += 1;
                continue;
            }
            if matches!(
                subquery.eval_phase,
                SubqueryEvalPhase::PreWrite | SubqueryEvalPhase::PostWriteReturning
            ) {
                update_subqueries.push(plan.non_from_clause_subqueries.remove(i));
            } else {
                i += 1;
            }
        }
    }

    // Initialize the main loop
    InitLoop::emit(
        program,
        &mut t_ctx,
        &write_phase_tables,
        &mut [],
        &mode,
        &plan.where_clause,
        &join_order,
        &mut plan.non_from_clause_subqueries,
    )?;

    // Prepare index cursors
    let target_database_id = target_table.database_id;
    let mut index_cursors = Vec::with_capacity(plan.indexes_to_update.len());
    for index in &plan.indexes_to_update {
        let index_cursor = if let Some(cursor) = program.resolve_cursor_id_safe(&CursorKey::index(
            write_phase_tables
                .joined_tables()
                .first()
                .unwrap()
                .internal_id,
            index.clone(),
        )) {
            cursor
        } else {
            let cursor = program.alloc_cursor_index(None, index)?;
            program.emit_insn(Insn::OpenWrite {
                cursor_id: cursor,
                root_page: RegisterOrLiteral::Literal(index.root_page),
                db: target_database_id,
            });
            cursor
        };
        let record_reg = program.alloc_register();
        index_cursors.push((index_cursor, record_reg));
    }

    // Emit EXPLAIN QUERY PLAN annotation (only for the direct path;
    // write-set UPDATE already emits EQP via emit_program_for_select).
    if !uses_write_set {
        emit_explain!(
            program,
            true,
            eqp_detail_for_table_op(&plan.target_table, None, None)
        );
    }

    // Open the main loop
    OpenLoop::emit(
        program,
        &mut t_ctx,
        &write_phase_tables,
        &join_order,
        &plan.where_clause,
        temp_cursor_id,
        mode.clone(),
        &mut plan.non_from_clause_subqueries,
    )?;

    let target_table_cursor_id =
        program.resolve_cursor_id(&CursorKey::table(target_table.internal_id));
    let rowid_update = rowid_update_info(target_table.as_ref(), &plan.set_clauses);

    // When any conflict resolution path may use REPLACE, we need cursors on ALL
    // indexes — deleting a conflicting row requires removing its entries from every
    // index, not just the ones touched by SET clauses.
    //
    // REPLACE can come from the statement (UPDATE OR REPLACE), the PK DDL
    // (INTEGER PRIMARY KEY ON CONFLICT REPLACE), or a unique index DDL
    // (UNIQUE ON CONFLICT REPLACE). Only indexes whose columns are being
    // updated can trigger a conflict, so we only check indexes_to_update.
    // Only consider PK REPLACE when the UPDATE actually changes the rowid,
    // since PK REPLACE can only fire on rowid collisions.
    let rowid_alias_conflict = if rowid_update.updates_rowid {
        target_table
            .table
            .btree()
            .and_then(|bt| bt.rowid_alias_conflict_clause)
    } else {
        None
    };
    let any_replace = any_effective_replace(
        program.flags.has_statement_conflict(),
        program.resolve_type,
        rowid_alias_conflict,
        plan.indexes_to_update.iter().map(|idx| idx.on_conflict),
    );
    let all_index_cursors = if any_replace {
        let table_name = target_table.table.get_name();
        let all_indexes: Vec<_> = resolver.with_schema(target_database_id, |s| {
            s.get_indices(table_name).cloned().collect()
        });
        let source_table = write_phase_tables
            .joined_tables()
            .first()
            .expect("UPDATE must have a joined table");
        let internal_id = source_table.internal_id;

        // Determine which index (if any) is being used for iteration
        // We need to reuse that cursor to avoid corruption when deleting from it
        let iteration_index_name = match &source_table.op {
            Operation::Scan(Scan::BTreeTable { index, .. }) => index.as_ref().map(|i| &i.name),
            Operation::Search(Search::Seek {
                index: Some(index), ..
            }) => Some(&index.name),
            _ => None,
        };

        all_indexes
            .into_iter()
            .map(|index| {
                // Check if this index already has a cursor opened (from indexes_to_update)
                let existing_cursor = plan
                    .indexes_to_update
                    .iter()
                    .zip(&index_cursors)
                    .find(|(idx, _)| idx.name == index.name)
                    .map(|(_, (cursor_id, _))| *cursor_id);

                let cursor = if let Some(cursor) = existing_cursor {
                    cursor
                } else if iteration_index_name == Some(&index.name) {
                    // This index is being used for iteration - reuse that cursor
                    program.resolve_cursor_id(&CursorKey::index(internal_id, index.clone()))
                } else {
                    // This index is not in indexes_to_update and not used for iteration
                    // Open a new cursor
                    let cursor = program
                        .alloc_cursor_index(None, &index)
                        .expect("to allocate index cursor");
                    program.emit_insn(Insn::OpenWrite {
                        cursor_id: cursor,
                        root_page: RegisterOrLiteral::Literal(index.root_page),
                        db: target_database_id,
                    });
                    cursor
                };
                (index, cursor)
            })
            .collect::<Vec<(Arc<Index>, usize)>>()
    } else {
        Vec::new()
    };

    // Emit update instructions
    turso_assert!(
        plan.set_clauses.iter().all(|set_clause| {
            set_clause.update_from_result.as_ref().is_none_or(|result| {
                matches!(result.as_ref(), ast::Expr::Column { .. })
            })
        }),
        "materialized UPDATE set clauses must stay attached to their original clause and read from scratch-table columns"
    );
    emit_update_insns(
        connection,
        &mut write_phase_tables,
        &plan.set_clauses,
        plan.cdc_update_alter_statement.as_deref(),
        &plan.indexes_to_update,
        plan.returning.as_ref(),
        temp_cursor_id,
        &mut t_ctx,
        program,
        &index_cursors,
        &all_index_cursors,
        target_table_cursor_id,
        target_table,
        resolver,
        returning_buffer.as_ref(),
        &mut update_subqueries,
    )?;

    // Close the main loop
    CloseLoop::emit(
        program,
        &mut t_ctx,
        &write_phase_tables,
        &join_order,
        mode,
        None,
    )?;

    program.preassign_label_to_next_insn(after_main_loop_label);
    if let Some(cdc_cursor_id) = t_ctx.cdc_cursor_id {
        emit_cdc_autocommit_commit(program, resolver, cdc_cursor_id)?;
    }
    // Emit scan-back loop for buffered RETURNING results.
    // All DML is complete at this point; now yield the buffered rows to the caller.
    // FkCheck must come before the scan-back so that FK violations prevent
    // RETURNING rows from being emitted (matching SQLite behavior).
    if let Some(ref buf) = returning_buffer {
        program.emit_insn(Insn::FkCheck { deferred: false });
        emit_returning_scan_back(program, buf);
    }
    after(program);

    program.result_columns = plan.returning.unwrap_or_default();
    program.table_references.extend(write_phase_tables);
    Ok(())
}

/// Shared state threaded through the per-column emission loop of an UPDATE.
///
/// Bundles everything a column-level emitter needs to produce the NEW-image
/// register for a single target column: the target table and its cursor, the
/// register layout (`start` + `layout`) that maps column index → register,
/// rowid bookkeeping (including whether the UPDATE changes the rowid and
/// where the new value comes from), the optional partial-index cursor in use,
/// CDC tracking registers, and the mask of columns actually named in the SET
/// clause so untouched columns can be copied from the OLD image.
struct UpdateColumnCtx<'a> {
    cdc_update_alter_statement: Option<&'a str>,
    target_table: &'a Arc<JoinedTable>,
    target_table_cursor_id: usize,
    start: usize,
    rowid_reg: usize,
    updates_rowid: bool,
    rowid_set_clause_reg: Option<usize>,
    is_virtual_table: bool,
    index: &'a Option<(Arc<Index>, usize)>,
    cdc_updates_register: Option<usize>,
    layout: &'a ColumnLayout,
    affected_columns: &'a ColumnMask,
}

impl UpdateColumnCtx<'_> {
    fn col_len(&self) -> usize {
        self.target_table.table.columns().len()
    }

    fn table_name(&self) -> &str {
        self.target_table.table.get_name()
    }
}

/// Emit the VDBE instructions that enforce a `NOT NULL` constraint on the
/// value currently in `target_reg`, honoring the UPDATE's OR conflict clause:
///
/// - `OR IGNORE`: branch to `skip_row_label` when the value is NULL, leaving
///   the existing row untouched.
/// - `OR REPLACE`: if the column has a DEFAULT, evaluate it into `target_reg`;
///   otherwise halt with `SQLITE_CONSTRAINT_NOTNULL`.
/// - everything else (ABORT/ROLLBACK/FAIL, or the default): halt with
///   `SQLITE_CONSTRAINT_NOTNULL`.
#[allow(clippy::too_many_arguments)]
fn emit_notnull_constraint_check(
    program: &mut ProgramBuilder,
    table_references: &TableReferences,
    target_reg: usize,
    table_column: &Column,
    table_name: &str,
    or_conflict: ResolveType,
    skip_row_label: BranchOffset,
    resolver: &Resolver,
) -> crate::Result<()> {
    let description = || {
        format!(
            "{}.{}",
            table_name,
            table_column
                .name
                .as_ref()
                .expect("Column name must be present")
        )
    };
    match or_conflict {
        ResolveType::Ignore => {
            program.emit_insn(Insn::IsNull {
                reg: target_reg,
                target_pc: skip_row_label,
            });
        }
        ResolveType::Replace => {
            if let Some(default_expr) = table_column.default.as_ref() {
                let continue_label = program.allocate_label();
                program.emit_insn(Insn::NotNull {
                    reg: target_reg,
                    target_pc: continue_label,
                });
                translate_expr_no_constant_opt(
                    program,
                    Some(table_references),
                    default_expr,
                    target_reg,
                    resolver,
                    NoConstantOptReason::RegisterReuse,
                )?;
                program.preassign_label_to_next_insn(continue_label);
            }
            // The value must still satisfy NOT NULL, whether it is the original
            // (non-null) value or the substituted default: a column may declare
            // DEFAULT (NULL), so substituting the default does not resolve the
            // violation and SQLite aborts.
            program.emit_insn(Insn::HaltIfNull {
                target_reg,
                err_code: SQLITE_CONSTRAINT_NOTNULL,
                description: description(),
            });
        }
        _ => {
            program.emit_insn(Insn::HaltIfNull {
                target_reg,
                err_code: SQLITE_CONSTRAINT_NOTNULL,
                description: description(),
            });
        }
    }
    Ok(())
}

/// Build the `TriggerContext` used to fire BEFORE/AFTER row triggers for this
/// UPDATE. Carries the NEW and OLD register snapshots the trigger body will
/// read, and propagates a conflict-resolution override when one is in effect:
/// a `trigger_conflict_override` pushed by an enclosing UPSERT `DO UPDATE`
/// wins, otherwise the outer UPDATE's explicit `OR <conflict>` overrides the
/// trigger's own conflict resolution (except for the default `ABORT`, which
/// is left unset so the trigger's own resolution applies). `after` selects
/// between the BEFORE and AFTER constructors.
fn update_trigger_context(
    program: &ProgramBuilder,
    btree_table: &Arc<BTreeTable>,
    new_registers: Option<Vec<usize>>,
    old_registers: Option<Vec<usize>>,
    or_conflict: ResolveType,
    timing: TriggerTime,
) -> TriggerContext {
    let override_conflict = program
        .trigger_conflict_override
        .or_else(|| (!matches!(or_conflict, ResolveType::Abort)).then_some(or_conflict));

    let after = matches!(timing, TriggerTime::After);
    match (after, override_conflict) {
        (true, Some(override_conflict)) => TriggerContext::new_after_with_override_conflict(
            btree_table.clone(),
            new_registers,
            old_registers,
            override_conflict,
        ),
        (true, None) => {
            TriggerContext::new_after(btree_table.clone(), new_registers, old_registers)
        }
        (false, Some(override_conflict)) => TriggerContext::new_with_override_conflict(
            btree_table.clone(),
            new_registers,
            old_registers,
            override_conflict,
        ),
        (false, None) => TriggerContext::new(btree_table.clone(), new_registers, old_registers),
    }
}

/// Emit the delete half of `OR REPLACE` conflict resolution during an UPDATE.
///
/// When the new image of the row being updated would violate a UNIQUE or
/// PRIMARY KEY constraint and `OR REPLACE` is in effect, the existing
/// conflicting row (identified by `conflicting_rowid_reg`) must be removed
/// before the UPDATE can proceed. This function emits that delete:
/// it removes every index entry belonging to the conflicting row from
/// `all_index_cursors`, deletes the btree row itself, and — when foreign
/// keys are enabled — prepares and fires the appropriate FK actions and
/// child-FK counter decrements. `new_row_registers`, when provided, lets
/// FK action emission see the incoming NEW image so it can distinguish a
/// cascading REPLACE from an unrelated delete.
#[allow(clippy::too_many_arguments)]
fn emit_replace_delete<'a>(
    program: &mut ProgramBuilder,
    connection: &Arc<Connection>,
    table_references: &mut TableReferences,
    target_table: &Arc<JoinedTable>,
    target_table_cursor_id: usize,
    all_index_cursors: &[(Arc<Index>, usize)],
    conflicting_rowid_reg: usize,
    new_row_registers: Option<(usize, usize)>,
    update_database_id: usize,
    t_ctx: &mut TranslateCtx<'a>,
) -> crate::Result<()> {
    let table_name = target_table.table.get_name();
    let internal_id = target_table.internal_id;
    let prepared_fk_actions = if connection.foreign_keys_enabled() {
        let prepared = if t_ctx.resolver.with_schema(update_database_id, |s| {
            s.any_resolved_fks_referencing(table_name)
        }) {
            ForeignKeyActions::prepare_fk_delete_actions(
                program,
                &mut t_ctx.resolver,
                table_name,
                target_table_cursor_id,
                conflicting_rowid_reg,
                new_row_registers,
                update_database_id,
            )?
        } else {
            ForeignKeyActions::default()
        };
        if t_ctx
            .resolver
            .with_schema(update_database_id, |s| s.has_child_fks(table_name))
        {
            emit_fk_child_decrement_on_delete(
                program,
                &target_table
                    .table
                    .btree()
                    .expect("UPDATE target must be a BTree table"),
                table_name,
                target_table_cursor_id,
                conflicting_rowid_reg,
                update_database_id,
                &t_ctx.resolver,
            )?;
        }
        prepared
    } else {
        ForeignKeyActions::default()
    };

    for (other_index, other_idx_cursor_id) in all_index_cursors {
        let other_num_regs = other_index.columns.len() + 1;
        let other_start_reg = program.alloc_registers(other_num_regs);

        for (reg_offset, column_index) in other_index.columns.iter().enumerate() {
            emit_index_column_value_old_image(
                program,
                &t_ctx.resolver,
                table_references,
                target_table_cursor_id,
                internal_id,
                column_index,
                other_start_reg + reg_offset,
            )?;
        }

        program.emit_insn(Insn::Copy {
            src_reg: conflicting_rowid_reg,
            dst_reg: other_start_reg + other_num_regs - 1,
            extra_amount: 0,
        });

        program.emit_insn(Insn::IdxDelete {
            start_reg: other_start_reg,
            num_regs: other_num_regs,
            cursor_id: *other_idx_cursor_id,
            raise_error_if_no_matching_entry: other_index.where_clause.is_none(),
        });
    }

    program.emit_insn(Insn::Delete {
        cursor_id: target_table_cursor_id,
        table_name: table_name.to_string(),
        is_part_of_update: true,
    });

    prepared_fk_actions.fire_prepared_fk_delete_actions(
        program,
        &mut t_ctx.resolver,
        connection,
        update_database_id,
    )?;

    Ok(())
}

/// Helper function to evaluate SET expressions and read column values for UPDATE.
/// This is invoked once for every UPDATE, but will be invoked again if there are
/// any BEFORE UPDATE triggers that fired, because the triggers may have modified the row,
/// in which case the previously read values are stale.
#[allow(clippy::too_many_arguments)]
fn emit_update_column_values<'a>(
    program: &mut ProgramBuilder,
    table_references: &mut TableReferences,
    set_clauses: &[UpdateSetClause],
    column_ctx: &UpdateColumnCtx<'_>,
    t_ctx: &mut TranslateCtx<'a>,
    skip_set_clauses: bool,
    skip_row_label: BranchOffset,
    skip_notnull_checks: bool,
) -> crate::Result<()> {
    let or_conflict = program.resolve_type;
    if let Some(expr) = set_clauses
        .iter()
        .find(|set_clause| set_clause.column_index == ROWID_SENTINEL)
        .map(UpdateSetClause::emitted_expr)
    {
        if !skip_set_clauses {
            let rowid_set_clause_reg = column_ctx.rowid_set_clause_reg.unwrap();
            translate_expr(
                program,
                Some(table_references),
                expr,
                rowid_set_clause_reg,
                &t_ctx.resolver,
            )?;
            program.emit_insn(Insn::MustBeInt {
                reg: rowid_set_clause_reg,
                target_pc: None,
            });
        }
    }
    let target_table_columns = column_ctx.target_table.table.columns();

    for (idx, table_column) in target_table_columns.iter().enumerate() {
        let target_reg = column_ctx.layout.to_register(column_ctx.start, idx);

        // If the column needs to be updated, retrieve its column index, or its expression.
        // Such a column can be directly updated, in which case `expr` is the right-side of the SET
        // clause, or it can be an indirectly updated generated columns, in which case `expr` is the
        // column's expression.
        let update_expr = set_clauses
            .iter()
            .find(|set_clause| set_clause.column_index == idx)
            .map(UpdateSetClause::emitted_expr)
            .or_else(|| {
                if column_ctx.affected_columns.get(idx) {
                    table_column.generated_expr()
                } else {
                    None
                }
            });

        if let Some(expr) = update_expr {
            if !skip_set_clauses {
                // Skip if this is the sentinel value
                if idx == ROWID_SENTINEL {
                    continue;
                }
                if column_ctx.updates_rowid
                    && (table_column.primary_key() || table_column.is_rowid_alias())
                    && !column_ctx.is_virtual_table
                {
                    let rowid_set_clause_reg = column_ctx.rowid_set_clause_reg.unwrap();
                    translate_expr(
                        program,
                        Some(table_references),
                        expr,
                        rowid_set_clause_reg,
                        &t_ctx.resolver,
                    )?;

                    program.emit_insn(Insn::MustBeInt {
                        reg: rowid_set_clause_reg,
                        target_pc: None,
                    });

                    program.emit_null(target_reg, None);
                } else {
                    let self_table_context = match table_column.generated_type() {
                        GeneratedType::Virtual { .. } => Some(SelfTableContext::ForDML {
                            dml_ctx: DmlColumnContext::layout(
                                column_ctx.target_table.table.columns(),
                                column_ctx.start,
                                column_ctx.rowid_reg,
                                column_ctx.layout.clone(),
                            ),
                            table: column_ctx.target_table.table.require_btree()?,
                        }),
                        GeneratedType::NotGenerated => None,
                    };

                    t_ctx.resolver.with_self_table_context(
                        program,
                        self_table_context.as_ref(),
                        |program, _| {
                            // Save/restore target_union_type so union_value() resolves tags
                            // against this column's union type. See ProgramBuilder::target_union_type.
                            let union_td = t_ctx
                                .resolver
                                .schema
                                .get_type_def_unchecked(&table_column.ty_str)
                                .filter(|td| td.is_union())
                                .cloned();
                            let prev_union = program.target_union_type.take();
                            program.target_union_type = union_td;

                            // Columns with custom type encode must not have their
                            // SET expressions hoisted as constants. See the doc
                            // comment on NoConstantOptReason::CustomTypeEncode.
                            let has_custom_encode = {
                                let ty = &table_column.ty_str;
                                !ty.is_empty()
                                    && t_ctx
                                        .resolver
                                        .schema
                                        .get_type_def_unchecked(ty)
                                        .is_some_and(|td| td.encode().is_some())
                            };
                            let translate_result = if has_custom_encode {
                                translate_expr_no_constant_opt(
                                    program,
                                    Some(table_references),
                                    expr,
                                    target_reg,
                                    &t_ctx.resolver,
                                    NoConstantOptReason::CustomTypeEncode,
                                )
                            } else {
                                translate_expr(
                                    program,
                                    Some(table_references),
                                    expr,
                                    target_reg,
                                    &t_ctx.resolver,
                                )
                            };
                            program.target_union_type = prev_union;
                            translate_result?;
                            if table_column.notnull() && !skip_notnull_checks {
                                let notnull_conflict = if program.flags.has_statement_conflict() {
                                    or_conflict
                                } else {
                                    table_column
                                        .notnull_conflict_clause
                                        .unwrap_or(ResolveType::Abort)
                                };
                                emit_notnull_constraint_check(
                                    program,
                                    table_references,
                                    target_reg,
                                    table_column,
                                    column_ctx.table_name(),
                                    notnull_conflict,
                                    skip_row_label,
                                    &t_ctx.resolver,
                                )?;
                            }
                            Ok(())
                        },
                    )?;
                }

                if let Some(cdc_updates_register) = column_ctx.cdc_updates_register {
                    let change_reg = cdc_updates_register + idx;
                    let value_reg = cdc_updates_register + column_ctx.col_len() + idx;
                    program.emit_bool(true, change_reg);
                    program.mark_last_insn_constant();
                    let mut updated = false;
                    if let Some(ddl_query_for_cdc_update) = column_ctx.cdc_update_alter_statement {
                        if table_column.name.as_deref() == Some("sql") {
                            program.emit_string8(ddl_query_for_cdc_update.to_string(), value_reg);
                            updated = true;
                        }
                    }
                    if !updated {
                        program.emit_insn(Insn::Copy {
                            src_reg: target_reg,
                            dst_reg: value_reg,
                            extra_amount: 0,
                        });
                    }
                }
            }
        } else {
            // Column is not being updated, read it from the table
            match table_column.generated_type() {
                GeneratedType::NotGenerated => {
                    let column_idx_in_index = column_ctx.index.as_ref().and_then(|(idx, _)| {
                        idx.columns.iter().position(|c| {
                            table_column
                                .name
                                .as_ref()
                                .is_some_and(|tc_name| c.name.eq_ignore_ascii_case(tc_name))
                        })
                    });

                    // don't emit null for pkey of virtual tables. they require first two args
                    // before the 'record' to be explicitly non-null
                    if table_column.is_rowid_alias() && !column_ctx.is_virtual_table {
                        program.emit_null(target_reg, None);
                    } else if column_ctx.is_virtual_table {
                        program.emit_insn(Insn::VColumn {
                            cursor_id: column_ctx.target_table_cursor_id,
                            column: idx,
                            dest: target_reg,
                        });
                    } else {
                        let cursor_id = column_ctx
                            .index
                            .as_ref()
                            .filter(|_| column_idx_in_index.is_some())
                            .map(|(_, id)| *id)
                            .unwrap_or(column_ctx.target_table_cursor_id);
                        program.emit_column_or_rowid(
                            cursor_id,
                            column_idx_in_index.unwrap_or(idx),
                            target_reg,
                        );
                    }
                }
                GeneratedType::Virtual { .. } => {
                    // no-op
                }
            }

            if let Some(cdc_updates_register) = column_ctx.cdc_updates_register {
                let change_bit_reg = cdc_updates_register + idx;
                let value_reg = cdc_updates_register + column_ctx.col_len() + idx;
                program.emit_bool(false, change_bit_reg);
                program.mark_last_insn_constant();
                program.emit_null(value_reg, None);
                program.mark_last_insn_constant();
            }
        }
    }
    Ok(())
}

/// Emit NOT NULL constraint checks for SET clause columns after BEFORE triggers have fired.
/// This is deferred from the first `emit_update_column_values` call so that triggers
/// run before constraint checks, matching SQLite's behavior.
fn emit_deferred_notnull_checks<'a>(
    program: &mut ProgramBuilder,
    table_references: &mut TableReferences,
    target_table: &Arc<JoinedTable>,
    updated_column_indices: &ColumnMask,
    column_ctx: &UpdateColumnCtx<'_>,
    skip_row_label: BranchOffset,
    t_ctx: &mut TranslateCtx<'a>,
) -> crate::Result<()> {
    let or_conflict = program.resolve_type;
    for (idx, table_column) in target_table.table.columns().iter().enumerate() {
        if !table_column.notnull() {
            continue;
        }
        // Only check columns that are in SET clauses
        if !updated_column_indices.get(idx) {
            continue;
        }
        let target_reg = column_ctx.layout.to_register(column_ctx.start, idx);
        emit_notnull_constraint_check(
            program,
            table_references,
            target_reg,
            table_column,
            column_ctx.table_name(),
            or_conflict,
            skip_row_label,
            &t_ctx.resolver,
        )?;
    }
    Ok(())
}

#[instrument(skip_all, level = Level::DEBUG)]
#[allow(clippy::too_many_arguments)]
/// Emits the instructions for the UPDATE loop.
///
/// `temp_cursor_id` is the cursor id of the prebuilt write set, when UPDATE is using one.
///
/// `target_table_cursor_id` is the cursor id of the table that is being updated.
///
/// `target_table` is the table that is being updated.
///
/// `all_index_cursors` contains cursors for ALL indexes on the table (used for REPLACE to delete
/// conflicting rows from all indexes, not just those being updated).
fn emit_update_insns<'a>(
    connection: &Arc<Connection>,
    table_references: &mut TableReferences,
    set_clauses: &[UpdateSetClause],
    cdc_update_alter_statement: Option<&str>,
    indexes_to_update: &[Arc<Index>],
    returning: Option<&'a Vec<ResultSetColumn>>,
    temp_cursor_id: Option<usize>,
    t_ctx: &mut TranslateCtx<'a>,
    program: &mut ProgramBuilder,
    index_cursors: &[(usize, usize)],
    all_index_cursors: &[(Arc<Index>, usize)],
    target_table_cursor_id: usize,
    target_table: Arc<JoinedTable>,
    resolver: &Resolver,
    returning_buffer: Option<&ReturningBufferCtx>,
    non_from_clause_subqueries: &mut [NonFromClauseSubquery],
) -> crate::Result<()> {
    let uses_write_set = temp_cursor_id.is_some();
    let iteration_cursor_id = temp_cursor_id.unwrap_or(target_table_cursor_id);
    let or_conflict = program.resolve_type;
    let internal_id = target_table.internal_id;
    // Copy loop labels early to avoid borrow conflicts with mutable t_ctx borrow later
    let loop_labels = *t_ctx
        .labels_main_loop
        .first()
        .expect("loop labels to exist");
    // Label to skip to the next row on conflict (for IGNORE mode)
    let skip_row_label = loop_labels.next;
    let access_table = table_references
        .joined_tables()
        .first()
        .expect("UPDATE must have a source table");
    let (index, is_virtual_table) = match &access_table.op {
        Operation::Scan(Scan::BTreeTable { index, .. }) => (
            index.as_ref().map(|index| {
                (
                    index.clone(),
                    program.resolve_cursor_id(&CursorKey::index(internal_id, index.clone())),
                )
            }),
            false,
        ),
        Operation::Scan(_) => (None, target_table.virtual_table().is_some()),
        Operation::Search(search) => match search {
            &Search::RowidEq { .. }
            | Search::Seek { index: None, .. }
            | Search::InSeek { index: None, .. } => (None, false),
            Search::Seek {
                index: Some(index), ..
            }
            | Search::InSeek {
                index: Some(index), ..
            } => (
                Some((
                    index.clone(),
                    program.resolve_cursor_id(&CursorKey::index(internal_id, index.clone())),
                )),
                false,
            ),
        },
        Operation::IndexMethodQuery(_) => {
            // IndexMethodQuery indexes (e.g. FTS) don't store original column values
            // like B-tree indexes do, so we must read unchanged columns from the table cursor.
            (None, false)
        }
        Operation::HashJoin(_) => {
            crate::bail_parse_error!(
                "access through HashJoin is not supported for update operations"
            )
        }
        Operation::MultiIndexScan(_) => {
            crate::bail_parse_error!(
                "access through MultiIndexScan is not supported for update operations"
            )
        }
    };
    turso_assert!(
        !uses_write_set || index.is_none(),
        "prebuilt UPDATE write set must not scan an index directly"
    );

    let rowid_update = rowid_update_info(target_table.as_ref(), set_clauses);
    let rowid_alias_index = rowid_update.rowid_alias_index;
    let updates_rowid = rowid_update.updates_rowid;

    let beg = program.alloc_registers(
        target_table.table.columns().len()
            + if is_virtual_table {
                2 // two args before the relevant columns for VUpdate
            } else {
                1 // rowid reg
            },
    );
    program.emit_insn(Insn::RowId {
        cursor_id: iteration_cursor_id,
        dest: beg,
    });

    let rowid_set_clause_reg = if updates_rowid {
        Some(program.alloc_register())
    } else {
        None
    };
    let effective_rowid_reg = rowid_set_clause_reg.unwrap_or(beg);

    // Effective INTEGER PK conflict resolution: statement-level OR clause takes precedence;
    // otherwise use the constraint-level rowid_alias_conflict_clause from the table DDL.
    let constraint_rowid_alias_conflict = target_table
        .table
        .btree()
        .and_then(|bt| bt.rowid_alias_conflict_clause);
    let effective_rowid_alias_conflict = if program.flags.has_statement_conflict() {
        or_conflict
    } else {
        constraint_rowid_alias_conflict.unwrap_or(ResolveType::Abort)
    };

    let not_exists_check_required = updates_rowid || iteration_cursor_id != target_table_cursor_id;
    let update_database_id = target_table.database_id;
    let updated_column_indices: ColumnMask = set_clauses
        .iter()
        .map(|set_clause| set_clause.column_index)
        .try_collect()?;
    let has_any_update_triggers = if let Some(btree_table) = target_table.table.btree() {
        has_triggers_including_temp(
            &t_ctx.resolver,
            update_database_id,
            TriggerEvent::Update,
            Some(&updated_column_indices),
            &btree_table,
        )
    } else {
        false
    };

    let check_rowid_not_exists_label = if not_exists_check_required || has_any_update_triggers {
        Some(program.allocate_label())
    } else {
        None
    };

    // Label for RAISE(IGNORE) to skip the current row during UPDATE triggers
    let trigger_ignore_jump_label = program.allocate_label();

    if not_exists_check_required {
        program.emit_insn(Insn::NotExists {
            cursor: target_table_cursor_id,
            rowid_reg: beg,
            target_pc: check_rowid_not_exists_label.unwrap(),
        });
    } else {
        // if no rowid, we're done
        program.emit_insn(Insn::IsNull {
            reg: beg,
            target_pc: t_ctx.label_main_loop_end.unwrap(),
        });
    }

    // Emit any remaining SET-clause subqueries inside the loop, after the write cursor
    // is positioned via NotExists. This only applies to the non-FROM ephemeral path:
    // UPDATE ... FROM moves SET expressions into the ephemeral SELECT payload, but
    // trigger/REPLACE/FK-driven ephemeral updates still keep SET subqueries in the
    // main plan so correlated references resolve against the positioned write cursor.
    // RETURNING subqueries are skipped here and emitted after Insert so that
    // correlated column references read post-UPDATE values from the cursor.
    for subquery in non_from_clause_subqueries
        .iter_mut()
        .filter(|s| !s.has_been_evaluated() && !s.is_post_write_returning())
    {
        let subquery_plan = subquery.consume_plan(EvalAt::Loop(0));
        emit_non_from_clause_subquery(
            program,
            &t_ctx.resolver,
            *subquery_plan,
            &subquery.query_type,
            subquery.correlated,
            false,
        )?;
    }

    if is_virtual_table {
        program.emit_insn(Insn::Copy {
            src_reg: beg,
            dst_reg: beg + 1,
            extra_amount: 0,
        })
    }

    let col_len = target_table.table.columns().len();

    // we scan a column at a time, loading either the column's values, or the new value
    // from the Set expression, into registers so we can emit a MakeRecord and update the row.

    // we allocate 2C registers for "updates" as the structure of this column for CDC table is following:
    // [C boolean values where true set for changed columns] [C values with updates where NULL is set for not-changed columns]
    let cdc_updates_register = if program.capture_data_changes_info().has_updates() {
        Some(program.alloc_registers(2 * col_len))
    } else {
        None
    };
    let table_name = target_table.table.get_name();
    let start = if is_virtual_table { beg + 2 } else { beg + 1 };
    let layout = ColumnLayout::from_table(&target_table.as_ref().table)?;
    let affected_columns = match target_table.table.btree() {
        Some(btree) => btree.columns_affected_by_update(&updated_column_indices)?,
        None => updated_column_indices.clone(),
    };
    let column_ctx = UpdateColumnCtx {
        cdc_update_alter_statement,
        target_table: &target_table,
        target_table_cursor_id,
        start,
        rowid_reg: beg,
        updates_rowid,
        rowid_set_clause_reg,
        is_virtual_table,
        index: &index,
        cdc_updates_register,
        layout: &layout,
        affected_columns: &affected_columns,
    };
    let skip_set_clauses = false;

    // A BEFORE UPDATE trigger can change the row before constraint checks run,
    // so NOT NULL checks on SET-clause columns are deferred until after those
    // triggers fire (see emit_deferred_notnull_checks below). AFTER triggers
    // run once the row is already written and do not affect this ordering, so
    // they must not defer the checks. Skipping the inline check for any update
    // trigger, but only re-emitting it when a BEFORE trigger exists, would drop
    // the NOT NULL check entirely for a table that has only AFTER triggers.
    let relevant_before_update_triggers = match target_table.table.btree() {
        Some(btree_table) => get_triggers_including_temp(
            &t_ctx.resolver,
            update_database_id,
            TriggerEvent::Update,
            TriggerTime::Before,
            Some(updated_column_indices.clone()),
            &btree_table,
        ),
        None => Vec::new(),
    };
    let has_before_triggers = !relevant_before_update_triggers.is_empty();

    emit_update_column_values(
        program,
        table_references,
        set_clauses,
        &column_ctx,
        t_ctx,
        skip_set_clauses,
        skip_row_label,
        has_before_triggers,
    )?;

    // For non-STRICT tables, apply column affinity to the NEW values early.
    // This must happen before index operations and triggers so that all operations
    // use the converted values.
    if let Some(btree_table) = target_table.table.btree() {
        if !btree_table.is_strict {
            let affinity = btree_table
                .columns()
                .iter()
                .filter(|c| !c.is_virtual_generated())
                .map(|c| c.affinity());

            // Only emit Affinity if there's meaningful affinity to apply
            if affinity.clone().any(|a| a != Affinity::Blob) {
                if let Ok(count) = NonZeroUsize::try_from(layout.num_non_virtual_cols()) {
                    program.emit_insn(Insn::Affinity {
                        start_reg: start,
                        count,
                        affinities: affinity.map(|a| a.aff_mask()).collect(),
                    });
                }
            }
        }
    }

    // Fire BEFORE UPDATE triggers and preserve old_registers for AFTER triggers
    let mut has_after_triggers = false;
    let preserved_old_registers: Option<Vec<usize>> =
        if let Some(btree_table) = target_table.table.btree() {
            has_after_triggers = has_triggers_including_temp(
                &t_ctx.resolver,
                update_database_id,
                TriggerEvent::Update,
                Some(&updated_column_indices),
                &btree_table,
            );

            let has_fk_cascade = connection.foreign_keys_enabled()
                && t_ctx.resolver.with_schema(update_database_id, |s| {
                    s.any_resolved_fks_referencing(table_name)
                });

            let needs_old_registers = has_before_triggers || has_after_triggers || has_fk_cascade;

            // Only read OLD row values when triggers or FK cascades need them
            let columns = target_table.table.columns();
            let old_registers: Option<Vec<usize>> = if needs_old_registers {
                let mut regs = Vec::with_capacity(col_len + 1);
                for (i, column) in columns.iter().enumerate() {
                    let reg = program.alloc_register();
                    emit_table_column(
                        program,
                        target_table_cursor_id,
                        internal_id,
                        table_references,
                        column,
                        i,
                        reg,
                        &t_ctx.resolver,
                    )?;
                    regs.push(reg);
                }
                regs.push(beg);
                Some(regs)
            } else {
                None
            };

            if has_before_triggers {
                let old_registers =
                    old_registers.expect("old_registers allocated when has_before_triggers");
                // NEW row values are already in 'start' registers.
                // If the rowid is being updated (INTEGER PRIMARY KEY in SET clause),
                // use the new rowid register; otherwise use the current rowid (beg).
                let new_rowid_reg = effective_rowid_reg;

                // Compute virtual columns for NEW values
                //TODO only emit required virtual columns
                if let Table::BTree(ref btree) = target_table.table {
                    let new_ctx =
                        DmlColumnContext::layout(columns, start, new_rowid_reg, layout.clone());
                    compute_virtual_columns(
                        program,
                        &btree.columns_topo_sort()?,
                        &new_ctx,
                        &t_ctx.resolver,
                        btree,
                    )?;
                }

                let new_registers = (0..col_len)
                    .map(|i| layout.to_register(start, i))
                    .chain(std::iter::once(new_rowid_reg))
                    .collect();

                // Propagate conflict resolution to trigger context:
                // 1. UPSERT DO UPDATE override takes precedence
                // 2. Outer UPDATE's explicit ON CONFLICT overrides trigger body
                // 3. Otherwise, use trigger's own conflict resolution
                let trigger_ctx = update_trigger_context(
                    program,
                    &btree_table,
                    Some(new_registers),
                    Some(old_registers.clone()),
                    or_conflict,
                    TriggerTime::Before,
                );

                for trigger in relevant_before_update_triggers {
                    fire_trigger(
                        program,
                        &mut t_ctx.resolver,
                        trigger,
                        &trigger_ctx,
                        connection,
                        update_database_id,
                        trigger_ignore_jump_label,
                    )?;
                }

                // BEFORE UPDATE Triggers may have altered the btree so we need to seek again.
                program.emit_insn(Insn::NotExists {
                cursor: target_table_cursor_id,
                rowid_reg: beg,
                target_pc: check_rowid_not_exists_label.expect(
                    "check_rowid_not_exists_label must be set if there are BEFORE UPDATE triggers",
                ),
            });

                if has_after_triggers {
                    // Preserve pseudo-row 'OLD' for AFTER triggers by copying to new registers
                    // (since registers might be overwritten during trigger execution)
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
                    Some(old_registers)
                }
            } else {
                // No BEFORE triggers — pass through whatever old_registers we have
                old_registers
            }
        } else {
            None
        };

    // If BEFORE UPDATE triggers fired, they may have modified the row being updated.
    // According to the SQLite documentation, the behavior in these cases is undefined:
    // https://sqlite.org/lang_createtrigger.html
    // However, based on fuzz testing and observations, the logic seems to be:
    // The values that are NOT referred to in SET clauses will be evaluated again,
    // and values in SET clauses are evaluated using the old values.
    // sqlite> create table t(c0,c1,c2);
    // sqlite> create trigger tu before update on t begin update t set c1=666, c2=666; end;
    // sqlite> insert into t values (1,1,1);
    // sqlite> update t set c0 = c1+1;
    // sqlite> select * from t;
    // 2|666|666
    if target_table.table.btree().is_some() && has_before_triggers {
        let skip_set_clauses = true;
        // Re-read non-SET columns (triggers may have changed them).
        // NOT NULL checks are NOT skipped here — they cover non-SET columns.
        emit_update_column_values(
            program,
            table_references,
            set_clauses,
            &column_ctx,
            t_ctx,
            skip_set_clauses,
            skip_row_label,
            false,
        )?;

        // Now emit NOT NULL checks for SET clause columns that were deferred
        // from the first emit_update_column_values call. In SQLite, NOT NULL
        // constraint checks happen after BEFORE triggers fire.
        emit_deferred_notnull_checks(
            program,
            table_references,
            &target_table,
            &updated_column_indices,
            &column_ctx,
            skip_row_label,
            t_ctx,
        )?;
    }

    // Build a complete NEW row image for FK code now, while the old cursor is
    // still positioned. This is only data preparation. FK counters must not be
    // touched yet because later CHECK/PK/UNIQUE conflict handling may still
    // decide that `ON CONFLICT IGNORE` makes this row a no-op.
    if connection.foreign_keys_enabled() {
        let rowid_new_reg = effective_rowid_reg;
        if let Some(table_btree) = target_table.table.btree() {
            stabilize_new_row_for_fk(
                program,
                &table_btree,
                &updated_column_indices,
                target_table_cursor_id,
                start,
                rowid_new_reg,
            )?;
        }
    }

    // Populate register-to-affinity map for expression index evaluation.
    // When column references are rewritten to Expr::Register during UPDATE, comparison
    // operators need the original column affinity. This is set once here and cleared at
    // the end of the function.
    {
        let columns = target_table.table.columns();
        for (idx, col) in columns.iter().enumerate() {
            t_ctx
                .resolver
                .register_affinities
                .insert(layout.to_register(start, idx), col.affinity());
        }
        t_ctx
            .resolver
            .register_affinities
            .insert(effective_rowid_reg, Affinity::Integer);
    }

    let update_affects_virtual_columns = affected_columns.count() > updated_column_indices.count();
    let has_returning = returning.as_ref().is_some_and(|r| !r.is_empty());
    if let Table::BTree(ref btree) = target_table.table {
        let has_check_constraints = !btree.check_constraints.is_empty();
        let cols = btree.columns();
        let virtual_col_names: HashSet<String> = cols
            .iter()
            .filter(|c| c.is_virtual_generated())
            .filter_map(|c| c.name.as_ref().map(|n| normalize_ident(n)))
            .collect();
        let expr_references_virtual = |expr: &ast::Expr| {
            !virtual_col_names.is_empty()
                && !collect_column_dependencies_of_expr(expr, cols).is_disjoint(&virtual_col_names)
        };
        let index_references_virtual_column = indexes_to_update.iter().any(|idx| {
            idx.columns.iter().any(|col| {
                if col.pos_in_table != EXPR_INDEX_SENTINEL {
                    cols[col.pos_in_table].is_virtual_generated()
                } else {
                    col.expr.as_deref().is_some_and(expr_references_virtual)
                }
            }) || idx
                .where_clause
                .as_deref()
                .is_some_and(expr_references_virtual)
        });

        if update_affects_virtual_columns
            || has_before_triggers
            || has_after_triggers
            || has_returning
            || has_check_constraints
            || index_references_virtual_column
        {
            let columns = target_table.table.columns();

            //TODO don't emit all virtual columns
            let dml_ctx =
                DmlColumnContext::layout(columns, start, effective_rowid_reg, layout.clone());
            compute_virtual_columns(
                program,
                &btree.columns_topo_sort()?,
                &dml_ctx,
                &t_ctx.resolver,
                btree,
            )?;
        }
    }

    let target_is_strict = target_table
        .table
        .btree()
        .is_some_and(|btree| btree.is_strict);

    // Non-REPLACE PK constraint check. Must run BEFORE the index preflight so that
    // PK ABORT/FAIL/ROLLBACK fires before an index IGNORE can silently skip the row.
    // SQLite checks PK constraints before index constraints in the UPDATE path.
    if target_table.table.btree().is_some()
        && updates_rowid
        && !matches!(effective_rowid_alias_conflict, ResolveType::Replace)
    {
        let record_label = program.allocate_label();
        let target_reg = rowid_set_clause_reg.unwrap();

        // If the new rowid equals the old rowid, no conflict
        program.emit_insn(Insn::Eq {
            lhs: target_reg,
            rhs: beg,
            target_pc: record_label,
            flags: CmpInsFlags::default(),
            collation: program.curr_collation(),
        });

        // If a row with the new rowid doesn't exist, no conflict
        program.emit_insn(Insn::NotExists {
            cursor: target_table_cursor_id,
            rowid_reg: target_reg,
            target_pc: record_label,
        });

        // Handle conflict resolution for rowid/primary key conflict.
        // Replace is excluded by the outer guard; only Ignore/Abort/Fail/Rollback reach here.
        match effective_rowid_alias_conflict {
            ResolveType::Ignore => {
                // For IGNORE, skip this row's update but continue with other rows
                program.emit_insn(Insn::Goto {
                    target_pc: skip_row_label,
                });
            }
            _ => {
                // ABORT/FAIL/ROLLBACK behavior
                let raw_desc = if let Some(idx) = rowid_alias_index {
                    String::from(table_name)
                        + "."
                        + target_table
                            .table
                            .columns()
                            .get(idx)
                            .unwrap()
                            .name
                            .as_ref()
                            .map_or("", |v| v)
                } else {
                    String::from(table_name) + ".rowid"
                };
                let (description, on_error) = halt_desc_and_on_error(
                    &raw_desc,
                    effective_rowid_alias_conflict,
                    program.flags.has_statement_conflict(),
                );
                program.emit_insn(Insn::Halt {
                    err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
                    description,
                    on_error,
                    description_reg: None,
                });
            }
        }

        program.preassign_label_to_next_insn(record_label);
    }

    // After the PK check above, NotExists may have repositioned the cursor.
    // Re-seek to the row under update so old-image reads in Phase 2 are correct.
    if updates_rowid && !matches!(effective_rowid_alias_conflict, ResolveType::Replace) {
        if let Some(label) = check_rowid_not_exists_label {
            program.emit_insn(Insn::NotExists {
                cursor: target_table_cursor_id,
                rowid_reg: beg,
                target_pc: label,
            });
        }
    }

    // Evaluate STRICT type checks and CHECK constraints before any index mutations.
    // This ensures that if a constraint fails, indexes remain consistent.
    if let Some(btree_table) = target_table.table.btree() {
        if btree_table.is_strict {
            let set_col_indices: ColumnMask = set_clauses
                .iter()
                .map(|set_clause| set_clause.column_index)
                .try_collect()?;

            // Pre-encode TypeCheck: validate SET column input types.
            // Non-SET columns hold encoded values from disk, so skip them (ANY).
            program.emit_insn(Insn::TypeCheck {
                start_reg: start,
                count: layout.num_non_virtual_cols(),
                check_generated: true,
                table_reference: BTreeTable::input_type_check_table_ref(
                    &btree_table,
                    t_ctx.resolver.schema(),
                    Some(&set_col_indices),
                )?,
            });

            // Encode only SET clause columns. Non-SET columns were read from disk
            // and are already encoded; re-encoding them would corrupt data.
            crate::translate::expr::emit_custom_type_encode_columns(
                program,
                &t_ctx.resolver,
                btree_table.columns(),
                start,
                Some(&set_col_indices),
                table_name,
                &layout,
            )?;

            // Post-encode TypeCheck: validate encoded values match storage type.
            program.emit_insn(Insn::TypeCheck {
                start_reg: start,
                count: layout.num_non_virtual_cols(),
                check_generated: true,
                table_reference: BTreeTable::type_check_table_ref(
                    &btree_table,
                    t_ctx.resolver.schema(),
                ),
            });
        }

        if !btree_table.check_constraints.is_empty() {
            // SQLite only evaluates CHECK constraints that reference at least one
            // column in the SET clause. Build a set of updated column names to filter.
            let mut updated_col_names: HashSet<String> = btree_table
                .columns()
                .iter()
                .enumerate()
                .filter(|(idx, _)| affected_columns.get(*idx))
                .filter_map(|(_, col)| col.name.as_deref())
                .map(normalize_ident)
                .collect();

            // If the rowid is being updated (either directly via ROWID_SENTINEL or
            // through a rowid alias column), also include the rowid pseudo-column
            // names so that CHECK(rowid > 0) etc. are properly triggered.
            if updates_rowid {
                for name in ROWID_STRS {
                    updated_col_names.insert(name.to_string());
                }
            }

            let relevant_checks: Vec<CheckConstraint> = btree_table
                .check_constraints
                .iter()
                .filter(|cc| check_expr_references_columns(&cc.expr, &updated_col_names))
                .cloned()
                .collect();

            let check_constraint_tables =
                TableReferences::new(vec![target_table.as_ref().clone()], vec![]);
            emit_check_constraints(
                program,
                &relevant_checks,
                &mut t_ctx.resolver,
                &btree_table.name,
                effective_rowid_reg,
                btree_table
                    .columns()
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, col)| {
                        col.name.as_deref().map(|n| {
                            if col.is_rowid_alias() {
                                (n, effective_rowid_reg)
                            } else {
                                (n, layout.to_register(start, idx))
                            }
                        })
                    }),
                connection,
                or_conflict,
                skip_row_label,
                Some(&check_constraint_tables),
            )?;
        }
    }

    // =========================================================================
    // Three-phase index update — matches SQLite's separated architecture.
    // Phase 1: Evaluate partial WHERE predicates, build new index keys,
    //          and check unique constraints (with inline REPLACE deletion;
    //          REPLACE indexes are ordered last, after all other indexes,
    //          because they are the only mutative ones).
    // Phase 2: Delete old index entries from ALL indexes.
    // Phase 3: Insert new index entries into ALL indexes.
    // This ensures no index mutations happen until ALL constraint checks pass.
    // =========================================================================

    // Per-index context collected in Phase 1, consumed by Phases 2 and 3.
    struct IndexUpdatePhaseCtx {
        idx_cursor_id: usize,
        record_reg: usize,
        idx_start_reg: usize,
        num_cols: usize,
        old_satisfies_where: Option<usize>,
        new_satisfies_where: Option<usize>,
    }

    let mut idx_phase_ctxs: Vec<IndexUpdatePhaseCtx> = Vec::with_capacity(indexes_to_update.len());

    // ---- Phase 1: Constraint checks + new key build ----
    let mut seen_replace = false;
    for (index, (idx_cursor_id, record_reg)) in indexes_to_update.iter().zip(index_cursors) {
        let (old_satisfies_where, new_satisfies_where) = if index.where_clause.is_some() {
            // This means that we need to bind the column references to a copy of the index Expr,
            // so we can emit Insn::Column instructions and refer to the old values.
            let where_clause = index
                .bind_where_expr(Some(table_references), resolver)?
                .expect("index.where_clause was checked to be Some above");
            let old_satisfied_reg = program.alloc_register();
            translate_expr_no_constant_opt(
                program,
                Some(table_references),
                &where_clause,
                old_satisfied_reg,
                &t_ctx.resolver,
                NoConstantOptReason::RegisterReuse,
            )?;

            // Evaluate the partial index predicate against the NEW row image.
            // We use emit_dml_expr_index_value which properly sets up SelfTableContext::ForDML,
            // allowing resolve_union_from_column to find type definitions for custom type
            // functions like union_tag() in the WHERE clause.
            let new_where_expr = index
                .where_clause
                .as_ref()
                .expect("checked where clause to exist")
                .as_ref()
                .clone();
            let columns = target_table.table.columns();
            let mut column_regs: Vec<usize> = columns
                .iter()
                .enumerate()
                .map(|(i, col)| {
                    if col.is_rowid_alias() {
                        effective_rowid_reg
                    } else {
                        layout.to_register(start, i)
                    }
                })
                .collect();
            let new_satisfied_reg = program.alloc_register();
            let bt = target_table.table.require_btree()?;
            emit_dml_expr_index_value(
                program,
                &t_ctx.resolver,
                new_where_expr,
                columns,
                &mut column_regs,
                &bt,
                new_satisfied_reg,
            )?;

            // now we have two registers that tell us whether or not the old and new values satisfy
            // the partial index predicate, and we can use those to decide whether or not to
            // delete/insert a new index entry for this partial index.
            (Some(old_satisfied_reg), Some(new_satisfied_reg))
        } else {
            (None, None)
        };

        // Build new index key for constraint checking and later insertion (Phase 3).
        let num_cols = index.columns.len();
        let idx_start_reg = program.alloc_registers(num_cols + 1);
        let rowid_reg = effective_rowid_reg;

        for (i, col) in index.columns.iter().enumerate() {
            emit_index_column_value_new_image(
                program,
                &t_ctx.resolver,
                target_table.table.columns(),
                start,
                rowid_reg,
                col,
                idx_start_reg + i,
                &layout,
                &target_table
                    .table
                    .btree()
                    .expect("expected btree table: no indexes on virtual tables"),
            )?;
        }
        // last register is the rowid
        program.emit_insn(Insn::Copy {
            src_reg: rowid_reg,
            dst_reg: idx_start_reg + num_cols,
            extra_amount: 0,
        });

        // Apply affinity BEFORE MakeRecord so the index record has correctly converted values.
        // This is needed for all indexes (not just unique) because the index should store
        // values with proper affinity conversion.
        let aff = index
            .columns
            .iter()
            .map(|ic| {
                if ic.pos_in_table == EXPR_INDEX_SENTINEL {
                    Affinity::Blob.aff_mask()
                } else {
                    target_table.table.columns()[ic.pos_in_table]
                        .affinity_with_strict(target_is_strict)
                        .aff_mask()
                }
            })
            .collect::<String>();
        program.emit_insn(Insn::Affinity {
            start_reg: idx_start_reg,
            count: NonZeroUsize::new(num_cols).expect("nonzero col count"),
            affinities: aff,
        });

        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(idx_start_reg),
            count: to_u32(num_cols + 1),
            dest_reg: to_u32(*record_reg),
            index_name: Some(index.name.clone()),
            affinity_str: None,
        });

        // Handle unique constraint BEFORE IdxDelete (matches SQLite order).
        // If the constraint check fails (Halt/Ignore/Replace), the old index
        // entry is still intact — no statement journal needed for rollback.
        if index.unique {
            let idx_conflict = if program.flags.has_statement_conflict() {
                or_conflict
            } else {
                index.on_conflict.unwrap_or(ResolveType::Abort)
            };
            // REPLACE indexes must be sorted after all non-REPLACE indexes
            // (schema.rs:add_index ensures this). If a non-REPLACE index
            // appears after a REPLACE one, constraint check ordering is wrong.
            if idx_conflict == ResolveType::Replace {
                seen_replace = true;
            } else {
                turso_assert!(
                    !seen_replace,
                    "non-REPLACE index after REPLACE index — sort order invariant violated"
                );
            }

            let constraint_check = program.allocate_label();

            // For partial indexes, skip the constraint check if new values don't
            // satisfy the WHERE clause (no insert → no conflict possible).
            if let Some(new_satisfied) = new_satisfies_where {
                program.emit_insn(Insn::IfNot {
                    reg: new_satisfied,
                    target_pc: constraint_check,
                    jump_if_null: true,
                });
            }

            // check if the record already exists in the index for unique indexes and abort if so
            program.emit_insn(Insn::NoConflict {
                cursor_id: *idx_cursor_id,
                target_pc: constraint_check,
                record_reg: idx_start_reg,
                num_regs: num_cols,
            });

            let idx_rowid_reg = program.alloc_register();
            program.emit_insn(Insn::IdxRowId {
                cursor_id: *idx_cursor_id,
                dest: idx_rowid_reg,
            });

            // Skip over the UNIQUE constraint failure if the existing row is the one that we are currently changing
            program.emit_insn(Insn::Eq {
                lhs: beg,
                rhs: idx_rowid_reg,
                target_pc: constraint_check,
                flags: CmpInsFlags::default(),
                collation: program.curr_collation(),
            });
            match idx_conflict {
                ResolveType::Ignore => {
                    // For IGNORE, skip this row's update but continue with other rows
                    program.emit_insn(Insn::Goto {
                        target_pc: skip_row_label,
                    });
                }
                ResolveType::Replace => {
                    let original_rowid_reg = program.alloc_register();
                    program.emit_insn(Insn::Copy {
                        src_reg: beg,
                        dst_reg: original_rowid_reg,
                        extra_amount: 0,
                    });

                    // Seek to the conflicting row
                    let after_delete_label = program.allocate_label();
                    program.emit_insn(Insn::SeekRowid {
                        cursor_id: target_table_cursor_id,
                        src_reg: idx_rowid_reg,
                        target_pc: after_delete_label, // Skip if row doesn't exist
                    });

                    emit_replace_delete(
                        program,
                        connection,
                        table_references,
                        &target_table,
                        target_table_cursor_id,
                        all_index_cursors,
                        idx_rowid_reg,
                        Some((start, effective_rowid_reg)),
                        update_database_id,
                        t_ctx,
                    )?;

                    program.preassign_label_to_next_insn(after_delete_label);

                    // Seek back to the original row we're updating
                    let continue_label = program.allocate_label();
                    program.emit_insn(Insn::SeekRowid {
                        cursor_id: target_table_cursor_id,
                        src_reg: original_rowid_reg,
                        target_pc: continue_label, // Should always succeed
                    });
                    program.preassign_label_to_next_insn(continue_label);
                }
                _ => {
                    // ABORT/FAIL/ROLLBACK behavior
                    let column_names = index.columns.iter().enumerate().fold(
                        String::with_capacity(50),
                        |mut accum, (idx, col)| {
                            if idx > 0 {
                                accum.push_str(", ");
                            }
                            accum.push_str(table_name);
                            accum.push('.');
                            accum.push_str(&col.name);
                            accum
                        },
                    );
                    let (description, on_error) = halt_desc_and_on_error(
                        &column_names,
                        idx_conflict,
                        program.flags.has_statement_conflict(),
                    );
                    program.emit_insn(Insn::Halt {
                        err_code: SQLITE_CONSTRAINT_UNIQUE,
                        description,
                        on_error,
                        description_reg: None,
                    });
                }
            }

            program.preassign_label_to_next_insn(constraint_check);
        }

        idx_phase_ctxs.push(IndexUpdatePhaseCtx {
            idx_cursor_id: *idx_cursor_id,
            record_reg: *record_reg,
            idx_start_reg,
            num_cols,
            old_satisfies_where,
            new_satisfies_where,
        });
    }

    turso_assert_eq!(
        idx_phase_ctxs.len(),
        indexes_to_update.len(),
        "idx_phase_ctxs.len() != indexes_to_update.len()"
    );

    // PK REPLACE: when the new rowid conflicts with an existing row, delete it.
    // Runs AFTER Phase 1 (all index constraint checks) so that non-REPLACE index
    // constraints fire before this deletion, matching SQLite's ordering.
    if target_table.table.btree().is_some()
        && updates_rowid
        && matches!(effective_rowid_alias_conflict, ResolveType::Replace)
    {
        let target_reg = rowid_set_clause_reg.expect("rowid_set_clause_reg must be set");
        let no_rowid_conflict_label = program.allocate_label();
        let row_not_found_label = check_rowid_not_exists_label
            .expect("check_rowid_not_exists_label must be set when rowid is updated");

        // If the new rowid equals the old rowid, no conflict.
        program.emit_insn(Insn::Eq {
            lhs: target_reg,
            rhs: beg,
            target_pc: no_rowid_conflict_label,
            flags: CmpInsFlags::default(),
            collation: program.curr_collation(),
        });

        // If a row with the new rowid doesn't exist, no conflict.
        program.emit_insn(Insn::NotExists {
            cursor: target_table_cursor_id,
            rowid_reg: target_reg,
            target_pc: no_rowid_conflict_label,
        });

        emit_replace_delete(
            program,
            connection,
            table_references,
            &target_table,
            target_table_cursor_id,
            all_index_cursors,
            target_reg,
            Some((start, effective_rowid_reg)),
            update_database_id,
            t_ctx,
        )?;

        // Re-seek to the row under update so Phase 2's old-image reads are correct.
        program.preassign_label_to_next_insn(no_rowid_conflict_label);
        program.emit_insn(Insn::NotExists {
            cursor: target_table_cursor_id,
            rowid_reg: beg,
            target_pc: row_not_found_label,
        });
    }

    // This is the first point where FK counter changes are safe:
    //
    // * `ON CONFLICT IGNORE` no-op paths for CHECK/PK/UNIQUE have already
    //   jumped away. A skipped row must not increment or decrement the single
    //   deferred FK counter.
    // * No table or index entry has been rewritten yet. If an immediate FK
    //   check fails, the statement can stop without leaving partial mutations
    //   behind.
    //
    // Both parent-side and child-side FK checks below run against the
    // pre-rewrite table image. Self-referential rows therefore need explicit
    // handling: table scans see OLD values, while the NEW parent/child keys
    // live in registers until the rewrite.
    let mut deferred_new_key_plans = Vec::new();
    if connection.foreign_keys_enabled() {
        let rowid_new_reg = effective_rowid_reg;
        if let Some(table_btree) = target_table.table.btree() {
            // Parent-side checks scan child rows that point at the OLD parent
            // key. Each match means this update may create a new violation:
            // RESTRICT halts immediately, while NO ACTION increments the FK
            // counter so the statement/transaction can fail later if nothing
            // fixes it.
            if t_ctx.resolver.with_schema(update_database_id, |s| {
                s.any_resolved_fks_referencing(table_name)
            }) {
                let new_key_probe_mode = if any_effective_replace(
                    program.flags.has_statement_conflict(),
                    or_conflict,
                    table_btree.rowid_alias_conflict_clause,
                    indexes_to_update.iter().map(|idx| idx.on_conflict),
                ) {
                    ParentKeyNewProbeMode::AfterReplace
                } else {
                    ParentKeyNewProbeMode::BeforeWrite
                };
                deferred_new_key_plans = emit_fk_update_parent_actions(
                    program,
                    &table_btree,
                    indexes_to_update.iter(),
                    target_table_cursor_id,
                    beg,
                    start,
                    rowid_new_reg,
                    rowid_set_clause_reg,
                    &updated_column_indices,
                    new_key_probe_mode,
                    update_database_id,
                    &t_ctx.resolver,
                )?;
            }
        }
    }

    // Child-side checks answer the opposite question: does the row's NEW child
    // key have a parent? They also have to run after no-op conflict paths and
    // before the physical rewrite, for the same counter and rollback reasons as
    // the parent-side checks above.
    if connection.foreign_keys_enabled() {
        if let Some(table_btree) = target_table.table.btree() {
            if t_ctx.resolver.schema().has_child_fks(table_name) {
                emit_fk_child_update_counters(
                    program,
                    &table_btree,
                    table_name,
                    target_table_cursor_id,
                    start,
                    effective_rowid_reg,
                    &affected_columns,
                    update_database_id,
                    &t_ctx.resolver,
                    &layout,
                )?;
            }
        }
    }

    // ---- Phase 2: Delete old index entries ----
    // All constraint checks passed. Now safe to mutate indexes.
    for (index, ctx) in indexes_to_update.iter().zip(idx_phase_ctxs.iter()) {
        let mut skip_delete_label = None;

        if let Some(old_satisfied) = ctx.old_satisfies_where {
            skip_delete_label = Some(program.allocate_label());
            program.emit_insn(Insn::IfNot {
                reg: old_satisfied,
                target_pc: skip_delete_label.unwrap(),
                jump_if_null: true,
            });
        }

        let num_regs = index.columns.len() + 1;
        let delete_start_reg = program.alloc_registers(num_regs);
        for (reg_offset, column_index) in index.columns.iter().enumerate() {
            emit_index_column_value_old_image(
                program,
                &t_ctx.resolver,
                table_references,
                target_table_cursor_id,
                internal_id,
                column_index,
                delete_start_reg + reg_offset,
            )?;
        }
        program.emit_insn(Insn::RowId {
            cursor_id: target_table_cursor_id,
            dest: delete_start_reg + num_regs - 1,
        });
        program.emit_insn(Insn::IdxDelete {
            start_reg: delete_start_reg,
            num_regs,
            cursor_id: ctx.idx_cursor_id,
            raise_error_if_no_matching_entry: true,
        });

        if let Some(label) = skip_delete_label {
            program.preassign_label_to_next_insn(label);
        }
    }

    // ---- Phase 3: Insert new index entries ----
    for ctx in idx_phase_ctxs.iter() {
        let mut skip_insert_label = None;

        if let Some(new_satisfied) = ctx.new_satisfies_where {
            skip_insert_label = Some(program.allocate_label());
            program.emit_insn(Insn::IfNot {
                reg: new_satisfied,
                target_pc: skip_insert_label.unwrap(),
                jump_if_null: true,
            });
        }

        program.emit_insn(Insn::IdxInsert {
            cursor_id: ctx.idx_cursor_id,
            record_reg: ctx.record_reg,
            unpacked_start: Some(ctx.idx_start_reg),
            unpacked_count: Some((ctx.num_cols + 1) as u32),
            flags: IdxInsertFlags::new().nchange(true),
        });

        if let Some(label) = skip_insert_label {
            program.preassign_label_to_next_insn(label);
        }
    }

    match &target_table.table {
        Table::BTree(ref table) => {
            let record_reg = program.alloc_register();
            emit_make_record(
                program,
                target_table.table.columns().iter(),
                start,
                record_reg,
                table.is_strict,
            );

            if not_exists_check_required {
                program.emit_insn(Insn::NotExists {
                    cursor: target_table_cursor_id,
                    rowid_reg: beg,
                    target_pc: check_rowid_not_exists_label.unwrap(),
                });
            }

            // create alias for CDC rowid after the change (will differ from cdc_rowid_before_reg only in case of UPDATE with change in rowid alias)
            let cdc_rowid_after_reg = effective_rowid_reg;

            // create separate register with rowid before UPDATE for CDC
            let cdc_rowid_before_reg = if t_ctx.cdc_cursor_id.is_some() {
                let cdc_rowid_before_reg = program.alloc_register();
                if updates_rowid {
                    program.emit_insn(Insn::RowId {
                        cursor_id: target_table_cursor_id,
                        dest: cdc_rowid_before_reg,
                    });
                    Some(cdc_rowid_before_reg)
                } else {
                    Some(cdc_rowid_after_reg)
                }
            } else {
                None
            };

            // create full CDC record before update if necessary
            let cdc_before_reg = if program.capture_data_changes_info().has_before() {
                Some(emit_cdc_full_record(
                    program,
                    target_table.table.columns(),
                    target_table_cursor_id,
                    cdc_rowid_before_reg.expect("cdc_rowid_before_reg must be set"),
                    table.is_strict,
                ))
            } else {
                None
            };

            // If we are updating the rowid, we cannot rely on overwrite on the
            // Insert instruction to update the cell. We need to first delete the current cell
            // and later insert the updated record.
            // In MVCC mode, we also need DELETE+INSERT to properly version the row (Hekaton model).
            let needs_delete = not_exists_check_required
                || connection.mv_store_for_db(update_database_id).is_some();
            if needs_delete {
                program.emit_insn(Insn::Delete {
                    cursor_id: target_table_cursor_id,
                    table_name: table_name.to_string(),
                    is_part_of_update: true,
                });
            }

            program.emit_insn(Insn::Insert {
                cursor: target_table_cursor_id,
                key_reg: effective_rowid_reg,
                record_reg,
                flag: if not_exists_check_required {
                    // The previous Insn::NotExists and Insn::Delete seek to the old rowid,
                    // so to insert a new user-provided rowid, we need to seek to the correct place.
                    InsertFlags::new()
                        .require_seek()
                        .update_rowid_change()
                        .skip_last_rowid()
                } else {
                    InsertFlags::new().skip_last_rowid()
                },
                table_name: target_table.identifier.clone(),
            });

            // MVCC AUTOINCREMENT: an UPDATE that moves the rowid forward
            // must advance the implicit sequence past the new rowid, just
            // like an explicit-rowid INSERT does — otherwise the next
            // `INSERT … DEFAULT VALUES` would emit a value smaller than
            // the manually-set rowid and either collide with it or
            // produce a non-monotonic id.
            //
            // emit_disk_advance_past emits a direction-aware seek + a
            // value comparison + a conditional INSERT into the backing
            // table. The INSERT key is the value itself (rowid alias),
            // so two concurrent writers either pick distinct values
            // (safe) or collide on the watermark key (resolved by the
            // standard MVCC conflict path).
            //
            // WAL mode follows the SQLite contract: UPDATE does not
            // bump `sqlite_sequence`. The next AUTOINCREMENT INSERT
            // takes the max of `sqlite_sequence.seq` and `MAX(rowid)`
            // (see the AUTOINCREMENT allocator in insert.rs) and so
            // automatically skips past any manually-advanced rowid.
            if updates_rowid
                && table.has_autoincrement
                && connection.mv_store_for_db(update_database_id).is_some()
            {
                let seq_name = crate::schema::autoincrement_sequence_name(&target_table.identifier);
                let seq = t_ctx
                    .resolver
                    .with_schema(update_database_id, |s| s.get_sequence(&seq_name).cloned())
                    .ok_or_else(|| {
                        crate::LimboError::InternalError(format!(
                            "missing implicit sequence for AUTOINCREMENT table \"{}\"",
                            target_table.identifier
                        ))
                    })?;
                crate::translate::sequence::emit_disk_advance_past(
                    program,
                    &t_ctx.resolver,
                    update_database_id,
                    &seq_name,
                    &seq,
                    effective_rowid_reg,
                )?;
            }

            if connection.foreign_keys_enabled() {
                emit_fk_parent_deferred_new_key_probes(
                    program,
                    &deferred_new_key_plans,
                    update_database_id,
                    &t_ctx.resolver,
                )?;
            }

            // Fire FK CASCADE/SET NULL actions AFTER the parent row is updated
            // This ensures the new parent key exists when cascade actions update child rows
            if connection.foreign_keys_enabled()
                && t_ctx.resolver.with_schema(update_database_id, |s| {
                    s.any_resolved_fks_referencing(table_name)
                })
            {
                // OLD column values are stored in preserved_old_registers (contiguous registers)
                let old_values_start = preserved_old_registers
                    .as_ref()
                    .expect("FK check requires OLD values")[0];
                fire_fk_update_actions(
                    program,
                    &mut t_ctx.resolver,
                    table_name,
                    beg, // old_rowid_reg
                    old_values_start,
                    start, // new_values_start
                    effective_rowid_reg,
                    connection,
                    update_database_id,
                )?;
            }

            // RETURNING and CDC must capture the row as written by this UPDATE,
            // before any AFTER trigger runs. SQLite fires its RETURNING pseudo-
            // trigger ahead of user AFTER triggers, so an AFTER trigger that
            // rewrites or deletes the row does not change what RETURNING reports.
            // The AFTER trigger is therefore fired at the end of this block.
            let has_post_write_returning_subqueries = non_from_clause_subqueries
                .iter()
                .any(|s| !s.has_been_evaluated() && s.is_post_write_returning());
            if has_post_write_returning_subqueries {
                let cache_state = seed_returning_row_image_in_cache(
                    program,
                    table_references,
                    start,
                    effective_rowid_reg,
                    &mut t_ctx.resolver,
                    &layout,
                )?;
                let result: Result<()> = (|| {
                    // Emit RETURNING subqueries after Insert so correlated references
                    // resolve against the post-write row image, not the old cursor state.
                    for subquery in non_from_clause_subqueries
                        .iter_mut()
                        .filter(|s| !s.has_been_evaluated() && s.is_post_write_returning())
                    {
                        let rerun_for_target_scan = subquery
                            .reads_table(target_table.database_id, target_table.table.get_name());
                        let subquery_plan = subquery.consume_plan(EvalAt::Loop(0));
                        emit_non_from_clause_subquery(
                            program,
                            &t_ctx.resolver,
                            *subquery_plan,
                            &subquery.query_type,
                            subquery.correlated || rerun_for_target_scan,
                            true,
                        )?;
                    }
                    Ok(())
                })();
                restore_returning_row_image_in_cache(&mut t_ctx.resolver, cache_state);
                result?;
            }

            // Emit RETURNING results if specified
            if let Some(returning_columns) = &returning {
                if !returning_columns.is_empty() {
                    emit_returning_results(
                        program,
                        table_references,
                        returning_columns,
                        start,
                        effective_rowid_reg,
                        &mut t_ctx.resolver,
                        returning_buffer,
                        &layout,
                    )?;
                }
            }

            // create full CDC record after update if necessary
            let cdc_after_reg = if program.capture_data_changes_info().has_after() {
                Some(emit_cdc_patch_record(
                    program,
                    &target_table.table,
                    start,
                    record_reg,
                    cdc_rowid_after_reg,
                    &layout,
                ))
            } else {
                None
            };

            let cdc_updates_record = if let Some(cdc_updates_register) = cdc_updates_register {
                let record_reg = program.alloc_register();
                program.emit_insn(Insn::MakeRecord {
                    start_reg: to_u32(cdc_updates_register),
                    count: to_u32(2 * col_len),
                    dest_reg: to_u32(record_reg),
                    index_name: None,
                    affinity_str: None,
                });
                Some(record_reg)
            } else {
                None
            };

            // emit actual CDC instructions for write to the CDC table
            if let Some(cdc_cursor_id) = t_ctx.cdc_cursor_id {
                let cdc_rowid_before_reg =
                    cdc_rowid_before_reg.expect("cdc_rowid_before_reg must be set");
                if updates_rowid {
                    emit_cdc_insns(
                        program,
                        &t_ctx.resolver,
                        OperationMode::DELETE,
                        cdc_cursor_id,
                        cdc_rowid_before_reg,
                        cdc_before_reg,
                        None,
                        None,
                        table_name,
                    )?;
                    emit_cdc_insns(
                        program,
                        &t_ctx.resolver,
                        OperationMode::INSERT,
                        cdc_cursor_id,
                        cdc_rowid_after_reg,
                        cdc_after_reg,
                        None,
                        None,
                        table_name,
                    )?;
                } else {
                    emit_cdc_insns(
                        program,
                        &t_ctx.resolver,
                        OperationMode::UPDATE(if uses_write_set {
                            UpdateRowSource::PrebuiltEphemeralTable {
                                ephemeral_table_cursor_id: iteration_cursor_id,
                                target_table: target_table.clone(),
                            }
                        } else {
                            UpdateRowSource::Normal
                        }),
                        cdc_cursor_id,
                        cdc_rowid_before_reg,
                        cdc_before_reg,
                        cdc_after_reg,
                        cdc_updates_record,
                        table_name,
                    )?;
                }
            }

            // Fire AFTER UPDATE triggers last, after RETURNING and CDC have
            // captured the row this UPDATE wrote. An AFTER trigger that rewrites
            // or deletes the row then cannot change what RETURNING reports,
            // matching SQLite.
            if let Some(btree_table) = target_table.table.btree() {
                let relevant_triggers = get_triggers_including_temp(
                    &t_ctx.resolver,
                    update_database_id,
                    TriggerEvent::Update,
                    TriggerTime::After,
                    Some(updated_column_indices.clone()),
                    &btree_table,
                );
                if !relevant_triggers.is_empty() {
                    let columns = target_table.table.columns();

                    // Compute VIRTUAL columns for NEW values
                    //TODO only emit required virtual columns
                    let bt = target_table.table.btree().ok_or_else(|| {
                        crate::LimboError::InternalError(
                            "UPDATE on virtual table has no btree".into(),
                        )
                    })?;
                    let new_ctx = DmlColumnContext::layout(columns, start, beg, layout.clone());
                    compute_virtual_columns(
                        program,
                        &btree_table.columns_topo_sort()?,
                        &new_ctx,
                        &t_ctx.resolver,
                        &bt,
                    )?;

                    // Compute VIRTUAL columns for OLD values if we have preserved OLD registers
                    if let Some(ref old_regs) = preserved_old_registers {
                        let pairs = columns.iter().zip(old_regs.iter().copied());
                        //TODO only emit required virtual columns
                        let old_ctx = DmlColumnContext::from_column_reg_mapping(pairs);
                        compute_virtual_columns(
                            program,
                            &btree_table.columns_topo_sort()?,
                            &old_ctx,
                            &t_ctx.resolver,
                            &bt,
                        )?;
                    }

                    // Build raw NEW registers. Values are encoded at this point;
                    // fire_trigger will decode them via decode_trigger_registers.
                    let new_registers_after: Vec<usize> = (0..col_len)
                        .map(|i| layout.to_register(start, i))
                        .chain(std::iter::once(effective_rowid_reg))
                        .collect();

                    // Use preserved OLD registers from BEFORE trigger
                    let old_registers_after = preserved_old_registers;

                    // Propagate conflict resolution to AFTER trigger context (same logic as BEFORE)
                    let trigger_ctx_after = update_trigger_context(
                        program,
                        &btree_table,
                        Some(new_registers_after),
                        old_registers_after,
                        or_conflict,
                        TriggerTime::After,
                    );

                    // RAISE(IGNORE) in an AFTER trigger should only abort the trigger body,
                    // not skip post-row work (RETURNING, CDC).
                    let after_trigger_done = program.allocate_label();
                    for trigger in relevant_triggers {
                        fire_trigger(
                            program,
                            &mut t_ctx.resolver,
                            trigger,
                            &trigger_ctx_after,
                            connection,
                            update_database_id,
                            after_trigger_done,
                        )?;
                    }
                    program.preassign_label_to_next_insn(after_trigger_done);
                }
            }
        }
        Table::Virtual(_) => {
            let arg_count = col_len + 2;
            program.emit_insn(Insn::VUpdate {
                cursor_id: target_table_cursor_id,
                arg_count,
                start_reg: beg,
                conflict_action: 0u16,
            });
        }
        _ => crate::bail_parse_error!("cannot UPDATE a subquery table"),
    }

    if let Some(label) = check_rowid_not_exists_label {
        program.preassign_label_to_next_insn(label);
    }
    program.preassign_label_to_next_insn(trigger_ignore_jump_label);

    t_ctx.resolver.register_affinities.clear();
    t_ctx.resolver.register_collations.clear();
    Ok(())
}
