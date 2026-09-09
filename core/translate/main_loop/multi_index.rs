use super::*;
use crate::translate::plan::{MultiIndexBranch, MultiIndexBranchAccess};

#[expect(clippy::too_many_arguments)]
fn emit_multi_index_rowset_update(
    program: &mut ProgramBuilder,
    is_intersection: bool,
    branch_idx: usize,
    rowid_reg: usize,
    rowset1_reg: usize,
    current_read_rowset: usize,
    current_write_rowset: usize,
    skip_row_label: BranchOffset,
    found_in_prev_label: Option<BranchOffset>,
) {
    if is_intersection {
        if branch_idx == 0 {
            program.emit_insn(Insn::RowSetAdd {
                rowset_reg: rowset1_reg,
                value_reg: rowid_reg,
            });
        } else {
            program.emit_insn(Insn::RowSetTest {
                rowset_reg: current_read_rowset,
                pc_if_found: found_in_prev_label
                    .expect("intersection branch must have found label"),
                value_reg: rowid_reg,
                batch: -1,
            });
            program.emit_insn(Insn::Goto {
                target_pc: skip_row_label,
            });
            program.preassign_label_to_next_insn(
                found_in_prev_label.expect("intersection branch must have found label"),
            );
            program.emit_insn(Insn::RowSetAdd {
                rowset_reg: current_write_rowset,
                value_reg: rowid_reg,
            });
        }
    } else {
        program.emit_insn(Insn::RowSetAdd {
            rowset_reg: rowset1_reg,
            value_reg: rowid_reg,
        });
    }
}

#[expect(clippy::too_many_arguments)]
fn emit_multi_index_or_residual_filters(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    table_references: &TableReferences,
    residual_exprs: &[Expr],
    jump_target: BranchOffset,
    index_cursor_id: Option<CursorID>,
    table_cursor_id: CursorID,
    requires_table_cursor: bool,
) -> Result<()> {
    if residual_exprs.is_empty() {
        return Ok(());
    }

    if requires_table_cursor {
        if let Some(index_cursor_id) = index_cursor_id {
            program.emit_insn(Insn::DeferredSeek {
                index_cursor_id,
                table_cursor_id,
            });
        }
    }

    for residual_expr in residual_exprs {
        let jump_target_when_true = program.allocate_label();
        let condition_metadata = ConditionMetadata {
            jump_if_condition_is_true: false,
            jump_target_when_true,
            jump_target_when_false: jump_target,
            jump_target_when_null: jump_target,
        };
        translate_condition_expr(
            program,
            table_references,
            residual_expr,
            condition_metadata,
            &t_ctx.resolver,
        )?;
        program.preassign_label_to_next_insn(jump_target_when_true);
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_seek_multi_index_branch(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    table: &JoinedTable,
    table_references: &TableReferences,
    branch: &MultiIndexBranch,
    table_cursor_id: CursorID,
    rowid_reg: usize,
    is_intersection: bool,
    branch_idx: usize,
    rowset1_reg: usize,
    current_read_rowset: usize,
    current_write_rowset: usize,
    found_in_prev_label: Option<BranchOffset>,
) -> Result<()> {
    let MultiIndexBranchAccess::Seek { seek_def } = &branch.access else {
        unreachable!("seek branch helper called for non-seek branch");
    };
    let branch_loop_start = program.allocate_label();
    let branch_loop_end = program.allocate_label();
    let branch_next = program.allocate_label();
    let is_index = branch.index.is_some();
    let branch_cursor_id = if let Some(index) = &branch.index {
        program.resolve_cursor_id(&CursorKey::index(table.internal_id, index.clone()))
    } else {
        table_cursor_id
    };

    if let Some(r) = &branch.union_residuals {
        emit_multi_index_or_residual_filters(
            program,
            t_ctx,
            table_references,
            &r.pre_filter_exprs,
            branch_loop_end,
            None,
            table_cursor_id,
            false,
        )?;
    }

    let max_key_regs = seek_def
        .size(&seek_def.start)
        .max(seek_def.size(&seek_def.end))
        .max(1);
    let key_start_reg = program.alloc_registers(max_key_regs);
    SeekEmitter::new(
        program,
        table_references,
        seek_def,
        t_ctx,
        branch_cursor_id,
        key_start_reg,
        branch_loop_end,
        branch.index.as_ref(),
    )
    .emit(branch_loop_start, false)?;

    if is_index {
        program.emit_insn(Insn::IdxRowId {
            cursor_id: branch_cursor_id,
            dest: rowid_reg,
        });
    } else {
        program.emit_insn(Insn::RowId {
            cursor_id: branch_cursor_id,
            dest: rowid_reg,
        });
    }

    if let Some(r) = &branch.union_residuals {
        emit_multi_index_or_residual_filters(
            program,
            t_ctx,
            table_references,
            &r.post_filter_exprs,
            branch_next,
            is_index.then_some(branch_cursor_id),
            table_cursor_id,
            r.requires_table_cursor,
        )?;
    }

    emit_multi_index_rowset_update(
        program,
        is_intersection,
        branch_idx,
        rowid_reg,
        rowset1_reg,
        current_read_rowset,
        current_write_rowset,
        branch_next,
        found_in_prev_label,
    );

    program.preassign_label_to_next_insn(branch_next);
    match seek_def.iter_dir {
        IterationDirection::Forwards => program.emit_insn(Insn::Next {
            cursor_id: branch_cursor_id,
            pc_if_next: branch_loop_start,
            fullscan: false,
            is_index: false,
        }),
        IterationDirection::Backwards => program.emit_insn(Insn::Prev {
            cursor_id: branch_cursor_id,
            pc_if_prev: branch_loop_start,
            fullscan: false,
            is_index: false,
        }),
    }
    program.preassign_label_to_next_insn(branch_loop_end);

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_in_seek_multi_index_branch(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    table: &JoinedTable,
    table_references: &TableReferences,
    branch: &MultiIndexBranch,
    table_cursor_id: CursorID,
    rowid_reg: usize,
    is_intersection: bool,
    branch_idx: usize,
    rowset1_reg: usize,
    current_read_rowset: usize,
    current_write_rowset: usize,
    found_in_prev_label: Option<BranchOffset>,
) -> Result<()> {
    let MultiIndexBranchAccess::InSeek { source } = &branch.access else {
        unreachable!("IN-seek branch helper called for non-IN branch");
    };
    let branch_cursor_id = branch.index.as_ref().map(|index| {
        program.resolve_cursor_id(&CursorKey::index(table.internal_id, index.clone()))
    });
    let ephemeral_cursor_id = open_in_seek_source_cursor(
        program,
        table_references,
        &t_ctx.resolver,
        branch.index.as_ref(),
        source,
    )?;

    let branch_loop_end = program.allocate_label();

    if let Some(r) = &branch.union_residuals {
        emit_multi_index_or_residual_filters(
            program,
            t_ctx,
            table_references,
            &r.pre_filter_exprs,
            branch_loop_end,
            None,
            table_cursor_id,
            false,
        )?;
    }

    program.emit_insn(Insn::NullRow {
        cursor_id: ephemeral_cursor_id,
    });
    program.emit_insn(Insn::Rewind {
        cursor_id: ephemeral_cursor_id,
        pc_if_empty: branch_loop_end,
    });

    let outer_loop_start = program.allocate_label();
    program.preassign_label_to_next_insn(outer_loop_start);
    let seek_reg = program.alloc_register();
    program.emit_insn(Insn::Column {
        cursor_id: ephemeral_cursor_id,
        column: 0,
        dest: seek_reg,
        default: None,
    });

    let next_value_label = program.allocate_label();
    program.emit_insn(Insn::IsNull {
        reg: seek_reg,
        target_pc: next_value_label,
    });

    if let Some(branch_cursor_id) = branch_cursor_id {
        let branch_loop_start = program.allocate_label();
        let branch_next = program.allocate_label();
        program.emit_insn(Insn::SeekGE {
            cursor_id: branch_cursor_id,
            start_reg: seek_reg,
            num_regs: 1,
            target_pc: next_value_label,
            is_index: true,
            eq_only: false,
            null_matching_mask: Default::default(),
        });
        program.preassign_label_to_next_insn(branch_loop_start);
        program.emit_insn(Insn::IdxGT {
            cursor_id: branch_cursor_id,
            start_reg: seek_reg,
            num_regs: 1,
            target_pc: next_value_label,
        });
        program.emit_insn(Insn::IdxRowId {
            cursor_id: branch_cursor_id,
            dest: rowid_reg,
        });
        if let Some(r) = &branch.union_residuals {
            emit_multi_index_or_residual_filters(
                program,
                t_ctx,
                table_references,
                &r.post_filter_exprs,
                branch_next,
                Some(branch_cursor_id),
                table_cursor_id,
                r.requires_table_cursor,
            )?;
        }
        emit_multi_index_rowset_update(
            program,
            is_intersection,
            branch_idx,
            rowid_reg,
            rowset1_reg,
            current_read_rowset,
            current_write_rowset,
            branch_next,
            found_in_prev_label,
        );
        program.preassign_label_to_next_insn(branch_next);
        program.emit_insn(Insn::Next {
            cursor_id: branch_cursor_id,
            pc_if_next: branch_loop_start,
            fullscan: false,
            is_index: false,
        });
    } else {
        program.emit_insn(Insn::SeekRowid {
            cursor_id: table_cursor_id,
            src_reg: seek_reg,
            target_pc: next_value_label,
        });
        program.emit_insn(Insn::RowId {
            cursor_id: table_cursor_id,
            dest: rowid_reg,
        });
        if let Some(r) = &branch.union_residuals {
            emit_multi_index_or_residual_filters(
                program,
                t_ctx,
                table_references,
                &r.post_filter_exprs,
                next_value_label,
                None,
                table_cursor_id,
                r.requires_table_cursor,
            )?;
        }
        emit_multi_index_rowset_update(
            program,
            is_intersection,
            branch_idx,
            rowid_reg,
            rowset1_reg,
            current_read_rowset,
            current_write_rowset,
            next_value_label,
            found_in_prev_label,
        );
    }

    program.preassign_label_to_next_insn(next_value_label);
    program.emit_insn(Insn::Next {
        cursor_id: ephemeral_cursor_id,
        pc_if_next: outer_loop_start,
        fullscan: false,
        is_index: false,
    });
    program.preassign_label_to_next_insn(branch_loop_end);

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn emit_multi_index_scan_loop(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    table: &JoinedTable,
    table_references: &TableReferences,
    multi_idx_op: &MultiIndexScanOp,
    loop_start: BranchOffset,
    loop_end: BranchOffset,
) -> Result<()> {
    let table_cursor_id = program.resolve_cursor_id(&CursorKey::table(table.internal_id));
    let rowid_reg = program.alloc_register();
    let is_intersection = matches!(multi_idx_op.set_op, SetOperation::Intersection { .. });
    let rowset1_reg = program.alloc_register();
    let rowset2_reg = if is_intersection {
        program.alloc_register()
    } else {
        rowset1_reg
    };

    program.emit_insn(Insn::Null {
        dest: rowset1_reg,
        dest_end: None,
    });
    if is_intersection {
        program.emit_insn(Insn::Null {
            dest: rowset2_reg,
            dest_end: None,
        });
    }

    let mut current_read_rowset = rowset1_reg;
    let mut current_write_rowset = if is_intersection {
        rowset2_reg
    } else {
        rowset1_reg
    };

    for (branch_idx, branch) in multi_idx_op.branches.iter().enumerate() {
        let found_in_prev_label = if is_intersection && branch_idx > 0 {
            Some(program.allocate_label())
        } else {
            None
        };
        match &branch.access {
            MultiIndexBranchAccess::Seek { .. } => emit_seek_multi_index_branch(
                program,
                t_ctx,
                table,
                table_references,
                branch,
                table_cursor_id,
                rowid_reg,
                is_intersection,
                branch_idx,
                rowset1_reg,
                current_read_rowset,
                current_write_rowset,
                found_in_prev_label,
            )?,
            MultiIndexBranchAccess::InSeek { .. } => emit_in_seek_multi_index_branch(
                program,
                t_ctx,
                table,
                table_references,
                branch,
                table_cursor_id,
                rowid_reg,
                is_intersection,
                branch_idx,
                rowset1_reg,
                current_read_rowset,
                current_write_rowset,
                found_in_prev_label,
            )?,
        }

        if is_intersection && branch_idx > 0 && branch_idx < multi_idx_op.branches.len() - 1 {
            std::mem::swap(&mut current_read_rowset, &mut current_write_rowset);
            program.emit_insn(Insn::Null {
                dest: current_write_rowset,
                dest_end: None,
            });
        }
    }

    let final_rowset = if is_intersection && multi_idx_op.branches.len() > 1 {
        let num_swaps = multi_idx_op.branches.len().saturating_sub(2);
        if num_swaps % 2 == 0 {
            rowset2_reg
        } else {
            rowset1_reg
        }
    } else {
        rowset1_reg
    };

    program.preassign_label_to_next_insn(loop_start);
    program.emit_insn(Insn::RowSetRead {
        rowset_reg: final_rowset,
        pc_if_empty: loop_end,
        dest_reg: rowid_reg,
    });

    let skip_label = program.allocate_label();
    program.emit_insn(Insn::SeekRowid {
        cursor_id: table_cursor_id,
        src_reg: rowid_reg,
        target_pc: skip_label,
    });

    let rowid_expr = Expr::RowId {
        database: None,
        table: table.internal_id,
    };
    t_ctx
        .resolver
        .cache_expr_reg(Cow::Owned(rowid_expr), rowid_reg, false, None);

    program.preassign_label_to_next_insn(skip_label);
    Ok(())
}
