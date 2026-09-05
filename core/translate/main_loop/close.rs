use super::*;
use crate::translate::main_loop::hash::{
    emit_hash_join_unmatched_build_rows, GraceHashLoop, HashProbeCloseEmitter,
};

/// Represents final step of Loop emission
pub struct CloseLoop;

impl CloseLoop {
    pub fn emit<'a>(
        program: &mut ProgramBuilder,
        t_ctx: &mut TranslateCtx<'a>,
        tables: &TableReferences,
        join_order: &[JoinOrderMember],
        mode: OperationMode,
        select_plan: Option<&'a SelectPlan>,
    ) -> Result<()> {
        // We close the loops for all tables in reverse order, i.e. innermost first.
        // OPEN t1
        //   OPEN t2
        //     OPEN t3
        //       <do stuff>
        //     CLOSE t3
        //   CLOSE t2
        // CLOSE t1
        for join in join_order.iter().rev() {
            let table_index = join.original_idx;
            let table = &tables.joined_tables()[table_index];
            let loop_labels = *t_ctx
                .labels_main_loop
                .get(table_index)
                .expect("source has no loop labels");

            // SEMI/ANTI-JOIN: emit Goto -> outer_next right after the body.
            // For semi-join: after body runs (one match found), skip inner's Next.
            // For anti-join: after body runs (inner exhausted), move to next outer row.
            let is_semi_or_anti = table
                .join_info
                .as_ref()
                .is_some_and(|ji| ji.is_semi_or_anti());
            if is_semi_or_anti {
                let sa_meta = t_ctx.meta_semi_anti_joins[table_index]
                    .as_ref()
                    .expect("semi/anti-join must have SemiAntiJoinMetadata");
                let comment = if table.join_info.as_ref().unwrap().is_semi() {
                    "semi-join: early out after first match"
                } else {
                    "anti-join: exit body, next outer row"
                };
                program.add_comment(program.offset(), comment);
                program.emit_insn(Insn::Goto {
                    target_pc: sa_meta.label_next_outer,
                });
            }

            let (table_cursor_id, index_cursor_id) =
                table.resolve_cursors(program, mode.clone())?;
            // Track the "next iteration" anchor label for semi/anti-join label resolution.
            // For most operations this is loop_labels.next itself;
            // HashJoin overrides it to anchor at the Gosub Return or HashNext instead.
            let mut semi_anti_next_anchor: Option<BranchOffset> = None;
            // Helper: preassign loop_labels.next and record it as the semi/anti anchor.
            let mut resolve_next = |program: &mut ProgramBuilder| {
                program.preassign_label_to_next_insn(loop_labels.next);
                semi_anti_next_anchor = Some(loop_labels.next);
            };
            match &table.op {
                Operation::Scan(scan) => {
                    resolve_next(program);
                    match scan {
                        Scan::BTreeTable { iter_dir, .. } => {
                            let iteration_cursor_id = if let OperationMode::UPDATE(
                                UpdateRowSource::PrebuiltEphemeralTable {
                                    ephemeral_table_cursor_id,
                                    ..
                                },
                            ) = &mode
                            {
                                *ephemeral_table_cursor_id
                            } else {
                                index_cursor_id.unwrap_or_else(|| {
                                    table_cursor_id.expect(
                                        "Either ephemeral or index or table cursor must be opened",
                                    )
                                })
                            };
                            // An unconstrained scan is a full scan no matter
                            // what it steps: the table, a covering index, or
                            // the prebuilt ephemeral copy an UPDATE scans.
                            // SQLite tags any WHERE loop without a constraint;
                            // constrained loops go through Operation::Search.
                            let fullscan = true;
                            if *iter_dir == IterationDirection::Backwards {
                                program.emit_insn(Insn::Prev {
                                    cursor_id: iteration_cursor_id,
                                    pc_if_prev: loop_labels.loop_start,
                                    fullscan,
                                    is_index: false,
                                });
                            } else {
                                program.emit_insn(Insn::Next {
                                    cursor_id: iteration_cursor_id,
                                    pc_if_next: loop_labels.loop_start,
                                    fullscan,
                                    is_index: false,
                                });
                            }
                        }
                        Scan::VirtualTable { .. } => {
                            program.emit_insn(Insn::VNext {
                                cursor_id: table_cursor_id
                                    .expect("Virtual tables do not support covering indexes"),
                                pc_if_next: loop_labels.loop_start,
                            });
                        }
                        Scan::Subquery { iter_dir } => {
                            // Check if this is a materialized CTE (EphemeralTable) or coroutine
                            if let Table::FromClauseSubquery(subquery) = &table.table {
                                if let Some(QueryDestination::EphemeralTable {
                                    cursor_id, ..
                                }) = subquery.plan.select_query_destination()
                                {
                                    if *iter_dir == IterationDirection::Backwards {
                                        program.emit_insn(Insn::Prev {
                                            cursor_id: *cursor_id,
                                            pc_if_prev: loop_labels.loop_start,
                                            fullscan: false,
                                            is_index: false,
                                        });
                                    } else {
                                        program.emit_insn(Insn::Next {
                                            cursor_id: *cursor_id,
                                            pc_if_next: loop_labels.loop_start,
                                            fullscan: false,
                                            is_index: false,
                                        });
                                    }
                                } else {
                                    turso_assert_eq!(
                                        *iter_dir,
                                        IterationDirection::Forwards,
                                        "coroutine-backed subqueries cannot scan backwards"
                                    );
                                    // Coroutine-based subquery - use Goto to Yield
                                    program.emit_insn(Insn::Goto {
                                        target_pc: loop_labels.loop_start,
                                    });
                                }
                            } else {
                                // A subquery has no cursor to call Next on, so it just emits a Goto
                                // to the Yield instruction, which in turn jumps back to the main loop of the subquery,
                                // so that the next row from the subquery can be read.
                                program.emit_insn(Insn::Goto {
                                    target_pc: loop_labels.loop_start,
                                });
                            }
                        }
                        Scan::RecursiveCteInput => {}
                    }
                    program.preassign_label_to_next_insn(loop_labels.loop_end);
                }
                Operation::Search(search) => {
                    // Materialized subqueries with ephemeral indexes are allowed
                    let is_materialized_subquery = matches!(
                        &table.table,
                        Table::FromClauseSubquery(_)
                    ) && matches!(search, Search::Seek { index: Some(idx), .. } if idx.ephemeral);
                    turso_assert_some!(
                        {
                            is_from_clause: !matches!(table.table, Table::FromClauseSubquery(_)),
                            is_materialized_subquery: is_materialized_subquery
                        },
                        "Subqueries do not support index seeks unless materialized"
                    );
                    resolve_next(program);
                    let iteration_cursor_id =
                        if let OperationMode::UPDATE(UpdateRowSource::PrebuiltEphemeralTable {
                            ephemeral_table_cursor_id,
                            ..
                        }) = &mode
                        {
                            *ephemeral_table_cursor_id
                        } else if is_materialized_subquery {
                            // Table-backed materialized subquery seeks iterate the
                            // auxiliary ephemeral index cursor.
                            index_cursor_id.expect("materialized subquery must have index cursor")
                        } else {
                            index_cursor_id.unwrap_or_else(|| {
                                table_cursor_id.expect(
                                    "Either ephemeral or index or table cursor must be opened",
                                )
                            })
                        };
                    // Rowid equality point lookups are handled with a SeekRowid instruction which does not loop, so there is no need to emit a Next instruction.
                    match search {
                        Search::RowidEq { .. } => {}
                        Search::Seek { seek_def, .. } => {
                            if seek_def.iter_dir == IterationDirection::Backwards {
                                program.emit_insn(Insn::Prev {
                                    cursor_id: iteration_cursor_id,
                                    pc_if_prev: loop_labels.loop_start,
                                    fullscan: false,
                                    is_index: false,
                                });
                            } else {
                                program.emit_insn(Insn::Next {
                                    cursor_id: iteration_cursor_id,
                                    pc_if_next: loop_labels.loop_start,
                                    fullscan: false,
                                    is_index: false,
                                });
                            }
                        }
                        Search::InSeek { index, .. } => {
                            let meta = t_ctx.meta_in_seeks[table_index]
                                .as_ref()
                                .expect("InSeek must have metadata");
                            let ephemeral_cursor_id = meta.ephemeral_cursor_id;
                            let outer_loop_start = meta.outer_loop_start;
                            let next_val_label = meta.next_val_label;

                            let can_have_multiple_matches = index.is_some();
                            if can_have_multiple_matches {
                                // Rowid InSeek uses SeekRowid, so one RHS key can produce at
                                // most one row. Index-backed InSeek can hit duplicates, so
                                // keep scanning the current key's match range before advancing
                                // the ephemeral cursor to the next IN value.
                                program.emit_insn(Insn::Next {
                                    cursor_id: iteration_cursor_id,
                                    pc_if_next: loop_labels.loop_start,
                                    fullscan: false,
                                    is_index: false,
                                });
                            }

                            // Once the current key is exhausted (or a seek found nothing),
                            // advance the outer ephemeral cursor and restart the equality seek.
                            program.preassign_label_to_next_insn(next_val_label);
                            program.emit_insn(Insn::Next {
                                cursor_id: ephemeral_cursor_id,
                                pc_if_next: outer_loop_start,
                                fullscan: false,
                                is_index: false,
                            });
                        }
                    }
                    program.preassign_label_to_next_insn(loop_labels.loop_end);
                }
                Operation::IndexMethodQuery(_) => {
                    resolve_next(program);
                    program.emit_insn(Insn::Next {
                        cursor_id: index_cursor_id.unwrap(),
                        pc_if_next: loop_labels.loop_start,
                        fullscan: false,
                        is_index: false,
                    });
                    program.preassign_label_to_next_insn(loop_labels.loop_end);
                }
                Operation::HashJoin(ref hash_join_op) => {
                    if let Some(hash_ctx) = t_ctx
                        .hash_table_contexts
                        .get(&hash_join_op.build_table_idx)
                        .cloned()
                    {
                        // Emit the close-loop teardown for a hash-join probe table.
                        semi_anti_next_anchor = HashProbeCloseEmitter::new(
                            program,
                            t_ctx,
                            hash_join_op,
                            hash_ctx,
                            select_plan,
                            table_index,
                        )
                        .emit()?
                        .semi_anti_next_anchor;
                    }

                    // Advance probe cursor.
                    program.preassign_label_to_next_insn(loop_labels.next);
                    let probe_cursor_id = table_cursor_id.expect("Probe table must have a cursor");
                    program.emit_insn(Insn::Next {
                        cursor_id: probe_cursor_id,
                        pc_if_next: loop_labels.loop_start,
                        fullscan: false,
                        is_index: false,
                    });
                    program.preassign_label_to_next_insn(loop_labels.loop_end);

                    // Outer joins: emit unmatched build rows with NULLs for the probe side.
                    // This runs BEFORE grace so that in-memory partitions (with valid
                    // matched_bits from the main probe) are scanned while still available.
                    // At runtime, the scan skips spilled partitions — those are handled
                    // per-partition inside the grace loop where matched_bits are still live.
                    if matches!(
                        hash_join_op.join_type,
                        HashJoinType::LeftOuter | HashJoinType::FullOuter
                    ) {
                        if let Some(hash_ctx) = t_ctx
                            .hash_table_contexts
                            .get(&hash_join_op.build_table_idx)
                            .cloned()
                        {
                            emit_hash_join_unmatched_build_rows(
                                program,
                                t_ctx,
                                hash_join_op,
                                &hash_ctx,
                                select_plan,
                                table_index,
                                probe_cursor_id,
                            )?;
                        }
                    }

                    // Grace hash join processing: process spilled partition pairs.
                    // At runtime, this is a no-op if the build side didn't spill.
                    // For LEFT/FULL OUTER, each grace partition gets its own unmatched
                    // scan before eviction (so matched_bits are still live).
                    if let Some(hash_ctx) = t_ctx
                        .hash_table_contexts
                        .get(&hash_join_op.build_table_idx)
                        .cloned()
                    {
                        // emit grace processing loop after the probe cursor is exhausted.
                        GraceHashLoop::emit(
                            program,
                            t_ctx,
                            hash_join_op,
                            &hash_ctx,
                            select_plan,
                            table_index,
                            probe_cursor_id,
                        )?;
                    }
                }
                Operation::MultiIndexScan(_) => {
                    // MultiIndexScan uses RowSetRead for iteration - the next is handled
                    // at the end of the RowSet read loop in emit_multi_index_scan_loop
                    resolve_next(program);
                    program.emit_insn(Insn::Goto {
                        target_pc: loop_labels.loop_start,
                    });
                    program.preassign_label_to_next_insn(loop_labels.loop_end);
                }
            }

            // Resolve any semi/anti-join "outer next" labels targeting this table.
            if let Some(anchor) = semi_anti_next_anchor {
                for meta in t_ctx.meta_semi_anti_joins.iter().flatten() {
                    if meta.outer_table_idx == table_index {
                        program.link_label_to_other_label(meta.label_next_outer, anchor);
                    }
                }
            }

            // SEMI/ANTI-JOIN: after loop_end (inner loop exhausted).
            // Semi-join: no match found -> skip outer row (Goto -> next_outer).
            // Anti-join: no match found -> run body (Goto -> label_body, jumps backward).
            if is_semi_or_anti {
                let sa_meta = t_ctx.meta_semi_anti_joins[table_index]
                    .as_ref()
                    .expect("semi/anti-join must have SemiAntiJoinMetadata");
                let join_info = table.join_info.as_ref().unwrap();
                if join_info.is_semi() {
                    program.add_comment(program.offset(), "semi-join: no match, skip outer row");
                    program.emit_insn(Insn::Goto {
                        target_pc: sa_meta.label_next_outer,
                    });
                } else {
                    // Anti-join: inner exhausted without match -> run body
                    program.add_comment(program.offset(), "anti-join: no match, emit outer row");
                    program.emit_insn(Insn::Goto {
                        target_pc: sa_meta.label_body,
                    });
                }
            }

            // OUTER JOIN: may still need to emit NULLs for the right table.
            // Outer hash join probes are handled above via check_outer / unmatched scan.
            let is_outer_hash_join_probe = matches!(
                table.op,
                Operation::HashJoin(ref hj) if matches!(
                    hj.join_type,
                    HashJoinType::LeftOuter | HashJoinType::FullOuter
                )
            );
            if let Some(join_info) = table.join_info.as_ref() {
                if join_info.is_outer() && !is_outer_hash_join_probe {
                    let lj_meta = t_ctx.meta_left_joins[table_index].as_ref().unwrap();
                    // The left join match flag is set to 1 when there is any match on the right table
                    // (e.g. SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.a).
                    // If the left join match flag has been set to 1, we jump to the next row on the outer table,
                    // i.e. continue to the next row of t1 in our example.
                    program.preassign_label_to_next_insn(lj_meta.label_match_flag_check_value);
                    let label_when_right_table_notnull = program.allocate_label();
                    program.emit_insn(Insn::IfPos {
                        reg: lj_meta.reg_match_flag,
                        target_pc: label_when_right_table_notnull,
                        decrement_by: 0,
                    });
                    // If the left join match flag is still 0, it means there was no match on the right table,
                    // but since it's a LEFT JOIN, we still need to emit a row with NULLs for the right table.
                    // In that case, we now enter the routine that does exactly that.
                    // First we set the right table cursor's "pseudo null bit" on, which means any Insn::Column will return NULL.
                    // This needs to be set for both the table and the index cursor, if present,
                    // since even if the iteration cursor is the index cursor, it might fetch values from the table cursor.
                    [table_cursor_id, index_cursor_id]
                        .iter()
                        .filter_map(|maybe_cursor_id| maybe_cursor_id.as_ref())
                        .for_each(|cursor_id| {
                            program.emit_insn(Insn::NullRow {
                                cursor_id: *cursor_id,
                            });
                        });
                    if let Table::FromClauseSubquery(from_clause_subquery) = &table.table {
                        if let Some(start_reg) = from_clause_subquery.result_columns_start_reg {
                            let column_count = from_clause_subquery.columns.len();
                            if column_count > 0 {
                                // Subqueries materialize their row into registers rather than being read back
                                // through a cursor. NullRow only affects cursor reads, so we also have to
                                // explicitly null out the cached registers or stale values would be re-emitted.
                                program.emit_insn(Insn::Null {
                                    dest: start_reg,
                                    dest_end: Some(start_reg + column_count - 1),
                                });
                            }
                        }
                    }
                    // Re-enter the loop body at match-flag set so
                    // post-join predicates are re-evaluated with right-table NULLs.
                    program.emit_insn(Insn::Goto {
                        target_pc: lj_meta.label_match_flag_set_true,
                    });
                    program.preassign_label_to_next_insn(label_when_right_table_notnull);
                }
            }
        }

        // After ALL loops are closed, emit HashClose for any hash tables that were built.
        // This must happen at the very end because hash join probe loops may be nested
        // inside outer loops that re-enter them. Hash tables used by materialization
        // subplans can be kept open and are skipped here.
        //
        // When inside a nested subquery (correlated or non-correlated), skip HashClose
        // because the hash build is protected by Once and must persist across subquery
        // re-invocations. The hash table will be cleaned up by ProgramState::reset().
        if !program.is_nested() {
            for join in join_order.iter() {
                let table_index = join.original_idx;
                let table = &tables.joined_tables()[table_index];
                if let Operation::HashJoin(hash_join_op) = &table.op {
                    let build_table = &tables.joined_tables()[hash_join_op.build_table_idx];
                    let hash_table_reg: usize = build_table.internal_id.into();
                    if !program.should_keep_hash_table_open(hash_table_reg) {
                        program.emit_insn(Insn::HashClose {
                            hash_table_id: hash_table_reg,
                        });
                        program.clear_hash_build_signature(hash_table_reg);
                    }
                }
            }
        }

        Ok(())
    }
}

pub(super) struct AutoIndexResult {
    pub(super) use_bloom_filter: bool,
}

pub(super) struct AutoIndexBuild<'a> {
    pub(super) index: &'a Arc<Index>,
    pub(super) table_cursor_id: CursorID,
    pub(super) index_cursor_id: CursorID,
    pub(super) table_has_rowid: bool,
    pub(super) num_seek_keys: usize,
    pub(super) seek_def: &'a SeekDef,
    pub(super) affinity_str: Option<&'a Arc<String>>,
    /// Table columns needed for transparent virtual column computation.
    pub(super) table_columns: Option<&'a [crate::schema::Column]>,
    pub(super) table_ref_id: turso_parser::ast::TableInternalId,
    pub(super) table_references: &'a TableReferences,
    pub(super) resolver: &'a Resolver<'a>,
}

/// Open an ephemeral index cursor and build an automatic index on a table.
/// This is used as a last-resort to avoid a nested full table scan
/// Returns the cursor id of the ephemeral index cursor.
pub(super) fn emit_autoindex(
    program: &mut ProgramBuilder,
    build: AutoIndexBuild<'_>,
) -> Result<AutoIndexResult> {
    let AutoIndexBuild {
        index,
        table_cursor_id,
        index_cursor_id,
        table_has_rowid,
        num_seek_keys,
        seek_def,
        affinity_str,
        table_columns,
        table_ref_id,
        table_references,
        resolver,
    } = build;
    turso_assert!(index.ephemeral, "index must be ephemeral", { "index_name": &index.name });
    let label_ephemeral_build_end = program.allocate_label();
    // Since this typically happens in an inner loop, we only build it once.
    program.emit_insn(Insn::Once {
        target_pc_when_reentered: label_ephemeral_build_end,
    });
    program.emit_insn(Insn::OpenAutoindex {
        cursor_id: index_cursor_id,
    });
    // Rewind source table
    let label_ephemeral_build_loop_start = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: table_cursor_id,
        pc_if_empty: label_ephemeral_build_loop_start,
    });
    program.preassign_label_to_next_insn(label_ephemeral_build_loop_start);
    let label_ephemeral_build_loop_next = program.allocate_label();
    if let Some(filter) = &index.where_clause {
        let filter_passed = program.allocate_label();
        // The table's planned operation reads from the new index. While that
        // index is being built, expressions must read the source cursor.
        program.set_cursor_override(table_ref_id, table_cursor_id);
        let result = translate_condition_expr(
            program,
            table_references,
            filter,
            ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_true: filter_passed,
                jump_target_when_false: label_ephemeral_build_loop_next,
                jump_target_when_null: label_ephemeral_build_loop_next,
            },
            resolver,
        );
        program.clear_cursor_override(table_ref_id);
        result?;
        program.preassign_label_to_next_insn(filter_passed);
    }
    // Emit all columns from source table that are needed in the ephemeral index.
    // Also reserve a register for the rowid if the source table has rowids.
    let num_regs_to_reserve = index.columns.len() + table_has_rowid as usize;
    let ephemeral_cols_start_reg = program.alloc_registers(num_regs_to_reserve);
    for (i, col) in index.columns.iter().enumerate() {
        let reg = ephemeral_cols_start_reg + i;
        if let Some(columns) = table_columns {
            if let Some(column_def) = columns.get(col.pos_in_table) {
                if column_def.is_virtual_generated() {
                    // Override the table cursor to the base table, because generated
                    // columns may need to read from it to compute their expression.
                    program.set_cursor_override(table_ref_id, table_cursor_id);
                    let result = crate::translate::expr::emit_table_column(
                        program,
                        table_cursor_id,
                        table_ref_id,
                        table_references,
                        column_def,
                        col.pos_in_table,
                        reg,
                        resolver,
                    );
                    program.clear_cursor_override(table_ref_id);
                    result?;
                    continue;
                }
            }
        }
        program.emit_column_or_rowid(table_cursor_id, col.pos_in_table, reg);
    }
    if table_has_rowid {
        program.emit_insn(Insn::RowId {
            cursor_id: table_cursor_id,
            dest: ephemeral_cols_start_reg + index.columns.len(),
        });
    }
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(ephemeral_cols_start_reg),
        count: to_u32(num_regs_to_reserve),
        dest_reg: to_u32(record_reg),
        index_name: Some(index.name.clone()),
        affinity_str: affinity_str.map(|s| (**s).clone()),
    });
    // Skip bloom filter for non-binary collations since it uses binary hashing.
    // Also skip it when any seek key component comes from a NULL-matching `IS`:
    // the probe treats a NULL key as "definitely absent", which would skip rows
    // whose key IS NULL, so such a seek never probes — and then building the
    // filter would be wasted work on every row.
    let use_bloom_filter = index.columns.iter().take(num_seek_keys).all(|col| {
        col.collation
            .is_none_or(|coll| matches!(coll, CollationSeq::Binary | CollationSeq::Unset))
    }) && seek_def.start.op.eq_only()
        && (0..num_seek_keys).all(|i| !seek_def.is_null_matching_key_component(i));
    if use_bloom_filter {
        program.emit_insn(Insn::FilterAdd {
            cursor_id: index_cursor_id,
            key_reg: ephemeral_cols_start_reg,
            num_keys: num_seek_keys,
        });
    }
    program.emit_insn(Insn::IdxInsert {
        cursor_id: index_cursor_id,
        record_reg,
        unpacked_start: Some(ephemeral_cols_start_reg),
        unpacked_count: Some(num_regs_to_reserve as u32),
        flags: IdxInsertFlags::new().use_seek(false),
    });
    program.preassign_label_to_next_insn(label_ephemeral_build_loop_next);
    program.emit_insn(Insn::Next {
        cursor_id: table_cursor_id,
        pc_if_next: label_ephemeral_build_loop_start,
        fullscan: false,
        is_index: false,
    });
    program.preassign_label_to_next_insn(label_ephemeral_build_end);
    Ok(AutoIndexResult { use_bloom_filter })
}
