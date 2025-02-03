use sqlite3_parser::ast;

use crate::{
    translate::result_row::emit_select_result,
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{CmpInsFlags, Insn},
        BranchOffset,
    },
    Result,
};

use super::{
    aggregation::translate_aggregation_step,
    emitter::{OperationMode, TranslateCtx},
    expr::{translate_condition_expr, translate_expr, ConditionMetadata},
    order_by::{order_by_sorter_insert, sorter_insert},
    plan::{
        IterationDirection, Operation, Search, SelectPlan, SelectQueryType, TableReference,
        WhereTerm,
    },
};

// Metadata for handling LEFT JOIN operations
#[derive(Debug)]
pub struct LeftJoinMetadata {
    // integer register that holds a flag that is set to true if the current row has a match for the left join
    pub reg_match_flag: usize,
    // label for the instruction that sets the match flag to true
    pub label_match_flag_set_true: BranchOffset,
    // label for the instruction that checks if the match flag is true
    pub label_match_flag_check_value: BranchOffset,
}

/// Jump labels for each loop in the query's main execution loop
#[derive(Debug, Clone, Copy)]
pub struct LoopLabels {
    /// jump to the start of the loop body
    loop_start: BranchOffset,
    /// jump to the NextAsync instruction (or equivalent)
    next: BranchOffset,
    /// jump to the end of the loop, exiting it
    loop_end: BranchOffset,
}

impl LoopLabels {
    pub fn new(program: &mut ProgramBuilder) -> Self {
        Self {
            loop_start: program.allocate_label(),
            next: program.allocate_label(),
            loop_end: program.allocate_label(),
        }
    }
}

/// Initialize resources needed for the source operators (tables, joins, etc)
pub fn init_loop(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    tables: &[TableReference],
    mode: &OperationMode,
) -> Result<()> {
    assert!(
        t_ctx.meta_left_joins.len() == tables.len(),
        "meta_left_joins length does not match tables length"
    );
    for (table_index, table) in tables.iter().enumerate() {
        // Initialize bookkeeping for OUTER JOIN
        if let Some(join_info) = table.join_info.as_ref() {
            if join_info.outer {
                let lj_metadata = LeftJoinMetadata {
                    reg_match_flag: program.alloc_register(),
                    label_match_flag_set_true: program.allocate_label(),
                    label_match_flag_check_value: program.allocate_label(),
                };
                t_ctx.meta_left_joins[table_index] = Some(lj_metadata);
            }
        }
        match &table.op {
            Operation::Scan { .. } => {
                let cursor_id = program.alloc_cursor_id(
                    Some(table.identifier.clone()),
                    CursorType::BTreeTable(table.btree().unwrap().clone()),
                );
                let root_page = table.table.get_root_page();

                match mode {
                    OperationMode::SELECT => {
                        program.emit_insn(Insn::OpenReadAsync {
                            cursor_id,
                            root_page,
                        });
                        program.emit_insn(Insn::OpenReadAwait {});
                    }
                    OperationMode::DELETE => {
                        program.emit_insn(Insn::OpenWriteAsync {
                            cursor_id,
                            root_page,
                        });
                        program.emit_insn(Insn::OpenWriteAwait {});
                    }
                    _ => {
                        unimplemented!()
                    }
                }
            }
            Operation::Search(search) => {
                let table_cursor_id = program.alloc_cursor_id(
                    Some(table.identifier.clone()),
                    CursorType::BTreeTable(table.btree().unwrap().clone()),
                );

                match mode {
                    OperationMode::SELECT => {
                        program.emit_insn(Insn::OpenReadAsync {
                            cursor_id: table_cursor_id,
                            root_page: table.table.get_root_page(),
                        });
                        program.emit_insn(Insn::OpenReadAwait {});
                    }
                    OperationMode::DELETE => {
                        program.emit_insn(Insn::OpenWriteAsync {
                            cursor_id: table_cursor_id,
                            root_page: table.table.get_root_page(),
                        });
                        program.emit_insn(Insn::OpenWriteAwait {});
                    }
                    _ => {
                        unimplemented!()
                    }
                }

                if let Search::IndexSearch { index, .. } = search {
                    let index_cursor_id = program.alloc_cursor_id(
                        Some(index.name.clone()),
                        CursorType::BTreeIndex(index.clone()),
                    );

                    match mode {
                        OperationMode::SELECT => {
                            program.emit_insn(Insn::OpenReadAsync {
                                cursor_id: index_cursor_id,
                                root_page: index.root_page,
                            });
                            program.emit_insn(Insn::OpenReadAwait);
                        }
                        OperationMode::DELETE => {
                            program.emit_insn(Insn::OpenWriteAsync {
                                cursor_id: index_cursor_id,
                                root_page: index.root_page,
                            });
                            program.emit_insn(Insn::OpenWriteAwait {});
                        }
                        _ => {
                            unimplemented!()
                        }
                    }
                }
            }
            _ => {}
        }
    }

    Ok(())
}

/// Set up the main query execution loop
/// For example in the case of a nested table scan, this means emitting the RewindAsync instruction
/// for all tables involved, outermost first.
pub fn open_loop(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    tables: &[TableReference],
    predicates: &[WhereTerm],
) -> Result<()> {
    for (table_index, table) in tables.iter().enumerate() {
        let LoopLabels {
            loop_start,
            loop_end,
            next,
        } = *t_ctx
            .labels_main_loop
            .get(table_index)
            .expect("table has no loop labels");

        // Each OUTER JOIN has a "match flag" that is initially set to false,
        // and is set to true when a match is found for the OUTER JOIN.
        // This is used to determine whether to emit actual columns or NULLs for the columns of the right table.
        if let Some(join_info) = table.join_info.as_ref() {
            if join_info.outer {
                let lj_meta = t_ctx.meta_left_joins[table_index].as_ref().unwrap();
                program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: lj_meta.reg_match_flag,
                });
            }
        }

        match &table.op {
            Operation::Subquery { plan, .. } => {
                let (yield_reg, coroutine_implementation_start) = match &plan.query_type {
                    SelectQueryType::Subquery {
                        yield_reg,
                        coroutine_implementation_start,
                    } => (*yield_reg, *coroutine_implementation_start),
                    _ => unreachable!("Subquery operator with non-subquery query type"),
                };
                // In case the subquery is an inner loop, it needs to be reinitialized on each iteration of the outer loop.
                program.emit_insn(Insn::InitCoroutine {
                    yield_reg,
                    jump_on_definition: BranchOffset::Offset(0),
                    start_offset: coroutine_implementation_start,
                });
                program.resolve_label(loop_start, program.offset());
                // A subquery within the main loop of a parent query has no cursor, so instead of advancing the cursor,
                // it emits a Yield which jumps back to the main loop of the subquery itself to retrieve the next row.
                // When the subquery coroutine completes, this instruction jumps to the label at the top of the termination_label_stack,
                // which in this case is the end of the Yield-Goto loop in the parent query.
                program.emit_insn(Insn::Yield {
                    yield_reg,
                    end_offset: loop_end,
                });

                // These are predicates evaluated outside of the subquery,
                // so they are translated here.
                // E.g. SELECT foo FROM (SELECT bar as foo FROM t1) sub WHERE sub.foo > 10
                for cond in predicates
                    .iter()
                    .filter(|cond| cond.eval_at_loop == table_index)
                {
                    let jump_target_when_true = program.allocate_label();
                    let condition_metadata = ConditionMetadata {
                        jump_if_condition_is_true: false,
                        jump_target_when_true,
                        jump_target_when_false: next,
                    };
                    translate_condition_expr(
                        program,
                        tables,
                        &cond.expr,
                        condition_metadata,
                        &t_ctx.resolver,
                    )?;
                    program.resolve_label(jump_target_when_true, program.offset());
                }
            }
            Operation::Scan { iter_dir } => {
                let cursor_id = program.resolve_cursor_id(&table.identifier);
                if iter_dir
                    .as_ref()
                    .is_some_and(|dir| *dir == IterationDirection::Backwards)
                {
                    program.emit_insn(Insn::LastAsync { cursor_id });
                } else {
                    program.emit_insn(Insn::RewindAsync { cursor_id });
                }
                program.emit_insn(
                    if iter_dir
                        .as_ref()
                        .is_some_and(|dir| *dir == IterationDirection::Backwards)
                    {
                        Insn::LastAwait {
                            cursor_id,
                            pc_if_empty: loop_end,
                        }
                    } else {
                        Insn::RewindAwait {
                            cursor_id,
                            pc_if_empty: loop_end,
                        }
                    },
                );
                program.resolve_label(loop_start, program.offset());

                for cond in predicates
                    .iter()
                    .filter(|cond| cond.eval_at_loop == table_index)
                {
                    let jump_target_when_true = program.allocate_label();
                    let condition_metadata = ConditionMetadata {
                        jump_if_condition_is_true: false,
                        jump_target_when_true,
                        jump_target_when_false: next,
                    };
                    translate_condition_expr(
                        program,
                        tables,
                        &cond.expr,
                        condition_metadata,
                        &t_ctx.resolver,
                    )?;
                    program.resolve_label(jump_target_when_true, program.offset());
                }
            }
            Operation::Search(search) => {
                let table_cursor_id = program.resolve_cursor_id(&table.identifier);
                // Open the loop for the index search.
                // Rowid equality point lookups are handled with a SeekRowid instruction which does not loop, since it is a single row lookup.
                if !matches!(search, Search::RowidEq { .. }) {
                    let index_cursor_id = if let Search::IndexSearch { index, .. } = search {
                        Some(program.resolve_cursor_id(&index.name))
                    } else {
                        None
                    };
                    let cmp_reg = program.alloc_register();
                    let (cmp_expr, cmp_op) = match search {
                        Search::IndexSearch {
                            cmp_expr, cmp_op, ..
                        } => (cmp_expr, cmp_op),
                        Search::RowidSearch { cmp_expr, cmp_op } => (cmp_expr, cmp_op),
                        Search::RowidEq { .. } => unreachable!(),
                    };

                    // TODO this only handles ascending indexes
                    match cmp_op {
                        ast::Operator::Equals
                        | ast::Operator::Greater
                        | ast::Operator::GreaterEquals => {
                            translate_expr(
                                program,
                                Some(tables),
                                &cmp_expr.expr,
                                cmp_reg,
                                &t_ctx.resolver,
                            )?;
                        }
                        ast::Operator::Less | ast::Operator::LessEquals => {
                            program.emit_insn(Insn::Null {
                                dest: cmp_reg,
                                dest_end: None,
                            });
                        }
                        _ => unreachable!(),
                    }
                    // If we try to seek to a key that is not present in the table/index, we exit the loop entirely.
                    program.emit_insn(match cmp_op {
                        ast::Operator::Equals | ast::Operator::GreaterEquals => Insn::SeekGE {
                            is_index: index_cursor_id.is_some(),
                            cursor_id: index_cursor_id.unwrap_or(table_cursor_id),
                            start_reg: cmp_reg,
                            num_regs: 1,
                            target_pc: loop_end,
                        },
                        ast::Operator::Greater
                        | ast::Operator::Less
                        | ast::Operator::LessEquals => Insn::SeekGT {
                            is_index: index_cursor_id.is_some(),
                            cursor_id: index_cursor_id.unwrap_or(table_cursor_id),
                            start_reg: cmp_reg,
                            num_regs: 1,
                            target_pc: loop_end,
                        },
                        _ => unreachable!(),
                    });
                    if *cmp_op == ast::Operator::Less || *cmp_op == ast::Operator::LessEquals {
                        translate_expr(
                            program,
                            Some(tables),
                            &cmp_expr.expr,
                            cmp_reg,
                            &t_ctx.resolver,
                        )?;
                    }

                    program.resolve_label(loop_start, program.offset());
                    // TODO: We are currently only handling ascending indexes.
                    // For conditions like index_key > 10, we have already seeked to the first key greater than 10, and can just scan forward.
                    // For conditions like index_key < 10, we are at the beginning of the index, and will scan forward and emit IdxGE(10) with a conditional jump to the end.
                    // For conditions like index_key = 10, we have already seeked to the first key greater than or equal to 10, and can just scan forward and emit IdxGT(10) with a conditional jump to the end.
                    // For conditions like index_key >= 10, we have already seeked to the first key greater than or equal to 10, and can just scan forward.
                    // For conditions like index_key <= 10, we are at the beginning of the index, and will scan forward and emit IdxGT(10) with a conditional jump to the end.
                    // For conditions like index_key != 10, TODO. probably the optimal way is not to use an index at all.
                    //
                    // For primary key searches we emit RowId and then compare it to the seek value.

                    match cmp_op {
                        ast::Operator::Equals | ast::Operator::LessEquals => {
                            if let Some(index_cursor_id) = index_cursor_id {
                                program.emit_insn(Insn::IdxGT {
                                    cursor_id: index_cursor_id,
                                    start_reg: cmp_reg,
                                    num_regs: 1,
                                    target_pc: loop_end,
                                });
                            } else {
                                let rowid_reg = program.alloc_register();
                                program.emit_insn(Insn::RowId {
                                    cursor_id: table_cursor_id,
                                    dest: rowid_reg,
                                });
                                program.emit_insn(Insn::Gt {
                                    lhs: rowid_reg,
                                    rhs: cmp_reg,
                                    target_pc: loop_end,
                                    flags: CmpInsFlags::default(),
                                });
                            }
                        }
                        ast::Operator::Less => {
                            if let Some(index_cursor_id) = index_cursor_id {
                                program.emit_insn(Insn::IdxGE {
                                    cursor_id: index_cursor_id,
                                    start_reg: cmp_reg,
                                    num_regs: 1,
                                    target_pc: loop_end,
                                });
                            } else {
                                let rowid_reg = program.alloc_register();
                                program.emit_insn(Insn::RowId {
                                    cursor_id: table_cursor_id,
                                    dest: rowid_reg,
                                });
                                program.emit_insn(Insn::Ge {
                                    lhs: rowid_reg,
                                    rhs: cmp_reg,
                                    target_pc: loop_end,
                                    flags: CmpInsFlags::default(),
                                });
                            }
                        }
                        _ => {}
                    }

                    if let Some(index_cursor_id) = index_cursor_id {
                        program.emit_insn(Insn::DeferredSeek {
                            index_cursor_id,
                            table_cursor_id,
                        });
                    }
                }

                if let Search::RowidEq { cmp_expr } = search {
                    let src_reg = program.alloc_register();
                    translate_expr(
                        program,
                        Some(tables),
                        &cmp_expr.expr,
                        src_reg,
                        &t_ctx.resolver,
                    )?;
                    program.emit_insn(Insn::SeekRowid {
                        cursor_id: table_cursor_id,
                        src_reg,
                        target_pc: next,
                    });
                }
                for cond in predicates
                    .iter()
                    .filter(|cond| cond.eval_at_loop == table_index)
                {
                    let jump_target_when_true = program.allocate_label();
                    let condition_metadata = ConditionMetadata {
                        jump_if_condition_is_true: false,
                        jump_target_when_true,
                        jump_target_when_false: next,
                    };
                    translate_condition_expr(
                        program,
                        tables,
                        &cond.expr,
                        condition_metadata,
                        &t_ctx.resolver,
                    )?;
                    program.resolve_label(jump_target_when_true, program.offset());
                }
            }
        }

        // Set the match flag to true if this is a LEFT JOIN.
        // At this point of execution we are going to emit columns for the left table,
        // and either emit columns or NULLs for the right table, depending on whether the null_flag is set
        // for the right table's cursor.
        if let Some(join_info) = table.join_info.as_ref() {
            if join_info.outer {
                let lj_meta = t_ctx.meta_left_joins[table_index].as_ref().unwrap();
                program.resolve_label(lj_meta.label_match_flag_set_true, program.offset());
                program.emit_insn(Insn::Integer {
                    value: 1,
                    dest: lj_meta.reg_match_flag,
                });
            }
        }
    }

    Ok(())
}

/// SQLite (and so Limbo) processes joins as a nested loop.
/// The loop may emit rows to various destinations depending on the query:
/// - a GROUP BY sorter (grouping is done by sorting based on the GROUP BY keys and aggregating while the GROUP BY keys match)
/// - an ORDER BY sorter (when there is no GROUP BY, but there is an ORDER BY)
/// - an AggStep (the columns are collected for aggregation, which is finished later)
/// - a QueryResult (there is none of the above, so the loop either emits a ResultRow, or if it's a subquery, yields to the parent query)
enum LoopEmitTarget {
    GroupBySorter,
    OrderBySorter,
    AggStep,
    QueryResult,
}

/// Emits the bytecode for the inner loop of a query.
/// At this point the cursors for all tables have been opened and rewound.
pub fn emit_loop(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &mut SelectPlan,
) -> Result<()> {
    // if we have a group by, we emit a record into the group by sorter.
    if plan.group_by.is_some() {
        return emit_loop_source(program, t_ctx, plan, LoopEmitTarget::GroupBySorter);
    }
    // if we DONT have a group by, but we have aggregates, we emit without ResultRow.
    // we also do not need to sort because we are emitting a single row.
    if !plan.aggregates.is_empty() {
        return emit_loop_source(program, t_ctx, plan, LoopEmitTarget::AggStep);
    }
    // if we DONT have a group by, but we have an order by, we emit a record into the order by sorter.
    if plan.order_by.is_some() {
        return emit_loop_source(program, t_ctx, plan, LoopEmitTarget::OrderBySorter);
    }
    // if we have neither, we emit a ResultRow. In that case, if we have a Limit, we handle that with DecrJumpZero.
    emit_loop_source(program, t_ctx, plan, LoopEmitTarget::QueryResult)
}

/// This is a helper function for inner_loop_emit,
/// which does a different thing depending on the emit target.
/// See the InnerLoopEmitTarget enum for more details.
fn emit_loop_source(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
    emit_target: LoopEmitTarget,
) -> Result<()> {
    match emit_target {
        LoopEmitTarget::GroupBySorter => {
            let group_by = plan.group_by.as_ref().unwrap();
            let aggregates = &plan.aggregates;
            let sort_keys_count = group_by.exprs.len();
            let aggregate_arguments_count = plan
                .aggregates
                .iter()
                .map(|agg| agg.args.len())
                .sum::<usize>();
            let column_count = sort_keys_count + aggregate_arguments_count;
            let start_reg = program.alloc_registers(column_count);
            let mut cur_reg = start_reg;

            // The group by sorter rows will contain the grouping keys first. They are also the sort keys.
            for expr in group_by.exprs.iter() {
                let key_reg = cur_reg;
                cur_reg += 1;
                translate_expr(
                    program,
                    Some(&plan.table_references),
                    expr,
                    key_reg,
                    &t_ctx.resolver,
                )?;
            }
            // Then we have the aggregate arguments.
            for agg in aggregates.iter() {
                // Here we are collecting scalars for the group by sorter, which will include
                // both the group by expressions and the aggregate arguments.
                // e.g. in `select u.first_name, sum(u.age) from users group by u.first_name`
                // the sorter will have two scalars: u.first_name and u.age.
                // these are then sorted by u.first_name, and for each u.first_name, we sum the u.age.
                // the actual aggregation is done later.
                for expr in agg.args.iter() {
                    let agg_reg = cur_reg;
                    cur_reg += 1;
                    translate_expr(
                        program,
                        Some(&plan.table_references),
                        expr,
                        agg_reg,
                        &t_ctx.resolver,
                    )?;
                }
            }

            // TODO: although it's less often useful, SQLite does allow for expressions in the SELECT that are not part of a GROUP BY or aggregate.
            // We currently ignore those and only emit the GROUP BY keys and aggregate arguments. This should be fixed.

            let group_by_metadata = t_ctx.meta_group_by.as_ref().unwrap();

            sorter_insert(
                program,
                start_reg,
                column_count,
                group_by_metadata.sort_cursor,
                group_by_metadata.reg_sorter_key,
            );

            Ok(())
        }
        LoopEmitTarget::OrderBySorter => order_by_sorter_insert(program, t_ctx, plan),
        LoopEmitTarget::AggStep => {
            let num_aggs = plan.aggregates.len();
            let start_reg = program.alloc_registers(num_aggs);
            t_ctx.reg_agg_start = Some(start_reg);

            // In planner.rs, we have collected all aggregates from the SELECT clause, including ones where the aggregate is embedded inside
            // a more complex expression. Some examples: length(sum(x)), sum(x) + avg(y), sum(x) + 1, etc.
            // The result of those more complex expressions depends on the final result of the aggregate, so we don't translate the complete expressions here.
            // Instead, we accumulate the intermediate results of all aggreagates, and evaluate any expressions that do not contain aggregates.
            for (i, agg) in plan.aggregates.iter().enumerate() {
                let reg = start_reg + i;
                translate_aggregation_step(
                    program,
                    &plan.table_references,
                    agg,
                    reg,
                    &t_ctx.resolver,
                )?;
            }
            for (i, rc) in plan.result_columns.iter().enumerate() {
                if rc.contains_aggregates {
                    // Do nothing, aggregates are computed above
                    // if this result column is e.g. something like sum(x) + 1 or length(sum(x)), we do not want to translate that (+1) or length() yet,
                    // it will be computed after the aggregations are finalized.
                    continue;
                }
                let reg = start_reg + num_aggs + i;
                translate_expr(
                    program,
                    Some(&plan.table_references),
                    &rc.expr,
                    reg,
                    &t_ctx.resolver,
                )?;
            }
            Ok(())
        }
        LoopEmitTarget::QueryResult => {
            assert!(
                plan.aggregates.is_empty(),
                "We should not get here with aggregates"
            );
            let offset_jump_to = t_ctx
                .labels_main_loop
                .get(0)
                .map(|l| l.next)
                .or_else(|| t_ctx.label_main_loop_end);
            emit_select_result(
                program,
                t_ctx,
                plan,
                t_ctx.label_main_loop_end,
                offset_jump_to,
            )?;

            Ok(())
        }
    }
}

/// Closes the loop for a given source operator.
/// For example in the case of a nested table scan, this means emitting the NextAsync instruction
/// for all tables involved, innermost first.
pub fn close_loop(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    tables: &[TableReference],
) -> Result<()> {
    // We close the loops for all tables in reverse order, i.e. innermost first.
    // OPEN t1
    //   OPEN t2
    //     OPEN t3
    //       <do stuff>
    //     CLOSE t3
    //   CLOSE t2
    // CLOSE t1
    for (idx, table) in tables.iter().rev().enumerate() {
        let table_index = tables.len() - idx - 1;
        let loop_labels = *t_ctx
            .labels_main_loop
            .get(table_index)
            .expect("source has no loop labels");

        match &table.op {
            Operation::Subquery { .. } => {
                program.resolve_label(loop_labels.next, program.offset());
                // A subquery has no cursor to call NextAsync on, so it just emits a Goto
                // to the Yield instruction, which in turn jumps back to the main loop of the subquery,
                // so that the next row from the subquery can be read.
                program.emit_insn(Insn::Goto {
                    target_pc: loop_labels.loop_start,
                });
            }
            Operation::Scan { iter_dir, .. } => {
                program.resolve_label(loop_labels.next, program.offset());
                let cursor_id = program.resolve_cursor_id(&table.identifier);
                if iter_dir
                    .as_ref()
                    .is_some_and(|dir| *dir == IterationDirection::Backwards)
                {
                    program.emit_insn(Insn::PrevAsync { cursor_id });
                } else {
                    program.emit_insn(Insn::NextAsync { cursor_id });
                }
                if iter_dir
                    .as_ref()
                    .is_some_and(|dir| *dir == IterationDirection::Backwards)
                {
                    program.emit_insn(Insn::PrevAwait {
                        cursor_id,
                        pc_if_next: loop_labels.loop_start,
                    });
                } else {
                    program.emit_insn(Insn::NextAwait {
                        cursor_id,
                        pc_if_next: loop_labels.loop_start,
                    });
                }
            }
            Operation::Search(search) => {
                program.resolve_label(loop_labels.next, program.offset());
                // Rowid equality point lookups are handled with a SeekRowid instruction which does not loop, so there is no need to emit a NextAsync instruction.
                if !matches!(search, Search::RowidEq { .. }) {
                    let cursor_id = match search {
                        Search::IndexSearch { index, .. } => program.resolve_cursor_id(&index.name),
                        Search::RowidSearch { .. } => program.resolve_cursor_id(&table.identifier),
                        Search::RowidEq { .. } => unreachable!(),
                    };

                    program.emit_insn(Insn::NextAsync { cursor_id });
                    program.emit_insn(Insn::NextAwait {
                        cursor_id,
                        pc_if_next: loop_labels.loop_start,
                    });
                }
            }
        }

        program.resolve_label(loop_labels.loop_end, program.offset());

        // Handle OUTER JOIN logic. The reason this comes after the "loop end" mark is that we may need to still jump back
        // and emit a row with NULLs for the right table, and then jump back to the next row of the left table.
        if let Some(join_info) = table.join_info.as_ref() {
            if join_info.outer {
                let lj_meta = t_ctx.meta_left_joins[table_index].as_ref().unwrap();
                // The left join match flag is set to 1 when there is any match on the right table
                // (e.g. SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.a).
                // If the left join match flag has been set to 1, we jump to the next row on the outer table,
                // i.e. continue to the next row of t1 in our example.
                program.resolve_label(lj_meta.label_match_flag_check_value, program.offset());
                let jump_offset = program.offset().add(3u32);
                program.emit_insn(Insn::IfPos {
                    reg: lj_meta.reg_match_flag,
                    target_pc: jump_offset,
                    decrement_by: 0,
                });
                // If the left join match flag is still 0, it means there was no match on the right table,
                // but since it's a LEFT JOIN, we still need to emit a row with NULLs for the right table.
                // In that case, we now enter the routine that does exactly that.
                // First we set the right table cursor's "pseudo null bit" on, which means any Insn::Column will return NULL
                let right_cursor_id = match &table.op {
                    Operation::Scan { .. } => program.resolve_cursor_id(&table.identifier),
                    Operation::Search { .. } => program.resolve_cursor_id(&table.identifier),
                    _ => unreachable!(),
                };
                program.emit_insn(Insn::NullRow {
                    cursor_id: right_cursor_id,
                });
                // Then we jump to setting the left join match flag to 1 again,
                // but this time the right table cursor will set everything to null.
                // This leads to emitting a row with cols from the left + nulls from the right,
                // and we will end up back in the IfPos instruction above, which will then
                // check the match flag again, and since it is now 1, we will jump to the
                // next row in the left table.
                program.emit_insn(Insn::Goto {
                    target_pc: lj_meta.label_match_flag_set_true,
                });

                assert_eq!(program.offset(), jump_offset);
            }
        }
    }
    Ok(())
}
