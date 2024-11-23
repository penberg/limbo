use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::{Rc, Weak};

use sqlite3_parser::ast;

use crate::schema::{Column, PseudoTable, Table};
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::translate::plan::{IterationDirection, Search};
use crate::types::{OwnedRecord, OwnedValue};
use crate::vdbe::builder::ProgramBuilder;
use crate::vdbe::{BranchOffset, Insn, Program};
use crate::{Connection, Result};

use super::expr::{
    translate_aggregation, translate_aggregation_groupby, translate_condition_expr, translate_expr,
    ConditionMetadata,
};
use super::optimizer::Optimizable;
use super::plan::{Aggregate, BTreeTableReference, Direction, Plan};
use super::plan::{ResultSetColumn, SourceOperator};

#[derive(Debug)]
pub struct LeftJoinMetadata {
    // integer register that holds a flag that is set to true if the current row has a match for the left join
    pub match_flag_register: usize,
    // label for the instruction that sets the match flag to true
    pub set_match_flag_true_label: BranchOffset,
    // label for the instruction that checks if the match flag is true
    pub check_match_flag_label: BranchOffset,
    // label for the instruction where the program jumps to if the current row has a match for the left join
    pub on_match_jump_to_label: BranchOffset,
}

#[derive(Debug)]
pub struct SortMetadata {
    // cursor id for the Sorter table where the sorted rows are stored
    pub sort_cursor: usize,
    // label where the SorterData instruction is emitted; SorterNext will jump here if there is more data to read
    pub sorter_data_label: BranchOffset,
    // label for the instruction immediately following SorterNext; SorterSort will jump here in case there is no data
    pub done_label: BranchOffset,
    // register where the sorter data is inserted and later retrieved from
    pub sorter_data_register: usize,
}

#[derive(Debug)]
pub struct GroupByMetadata {
    // Cursor ID for the Sorter table where the grouped rows are stored
    pub sort_cursor: usize,
    // Label for the subroutine that clears the accumulator registers (temporary storage for per-group aggregate calculations)
    pub subroutine_accumulator_clear_label: BranchOffset,
    // Register holding the return offset for the accumulator clear subroutine
    pub subroutine_accumulator_clear_return_offset_register: usize,
    // Label for the subroutine that outputs the accumulator contents
    pub subroutine_accumulator_output_label: BranchOffset,
    // Register holding the return offset for the accumulator output subroutine
    pub subroutine_accumulator_output_return_offset_register: usize,
    // Label for the instruction that sets the accumulator indicator to true (indicating data exists in the accumulator for the current group)
    pub accumulator_indicator_set_true_label: BranchOffset,
    // Label for the instruction where SorterData is emitted (used for fetching sorted data)
    pub sorter_data_label: BranchOffset,
    // Register holding the key used for sorting in the Sorter
    pub sorter_key_register: usize,
    // Label for the instruction signaling the completion of grouping operations
    pub grouping_done_label: BranchOffset,
    // Register holding a flag to abort the grouping process if necessary
    pub abort_flag_register: usize,
    // Register holding a boolean indicating whether there's data in the accumulator (used for aggregation)
    pub data_in_accumulator_indicator_register: usize,
    // Register holding the start of the accumulator group registers (i.e. the groups, not the aggregates)
    pub group_exprs_accumulator_register: usize,
    // Starting index of the register(s) that hold the comparison result between the current row and the previous row
    // The comparison result is used to determine if the current row belongs to the same group as the previous row
    // Each group by expression has a corresponding register
    pub group_exprs_comparison_register: usize,
}

/// The Metadata struct holds various information and labels used during bytecode generation.
/// It is used for maintaining state and control flow during the bytecode
/// generation process.
#[derive(Debug)]
pub struct Metadata {
    // labels for the instructions that terminate the execution when a conditional check evaluates to false. typically jumps to Halt, but can also jump to AggFinal if a parent in the tree is an aggregation
    termination_label_stack: Vec<BranchOffset>,
    // labels for the instructions that jump to the next row in the current operator.
    // for example, in a join with two nested scans, the inner loop will jump to its Next instruction when the join condition is false;
    // in a join with a scan and a seek, the seek will jump to the scan's Next instruction when the join condition is false.
    next_row_labels: HashMap<usize, BranchOffset>,
    // labels for the instructions beginning the inner loop of a scan operator.
    scan_loop_body_labels: Vec<BranchOffset>,
    // metadata for the group by operator
    group_by_metadata: Option<GroupByMetadata>,
    // metadata for the order by operator
    sort_metadata: Option<SortMetadata>,
    // mapping between Join operator id and associated metadata (for left joins only)
    left_joins: HashMap<usize, LeftJoinMetadata>,
    // First register of the aggregation results
    pub aggregation_start_register: Option<usize>,
    // We need to emit result columns in the order they are present in the SELECT, but they may not be in the same order in the ORDER BY sorter.
    // This vector holds the indexes of the result columns in the ORDER BY sorter.
    pub result_column_indexes_in_orderby_sorter: HashMap<usize, usize>,
    // We might skip adding a SELECT result column into the ORDER BY sorter if it is an exact match in the ORDER BY keys.
    // This vector holds the indexes of the result columns that we need to skip.
    pub result_columns_to_skip_in_orderby_sorter: Option<Vec<usize>>,
}

fn prologue() -> Result<(ProgramBuilder, Metadata, BranchOffset, BranchOffset)> {
    let mut program = ProgramBuilder::new();
    let init_label = program.allocate_label();
    let halt_label = program.allocate_label();

    program.emit_insn_with_label_dependency(
        Insn::Init {
            target_pc: init_label,
        },
        init_label,
    );

    let start_offset = program.offset();

    let metadata = Metadata {
        termination_label_stack: vec![halt_label],
        group_by_metadata: None,
        left_joins: HashMap::new(),
        next_row_labels: HashMap::new(),
        scan_loop_body_labels: vec![],
        sort_metadata: None,
        aggregation_start_register: None,
        result_column_indexes_in_orderby_sorter: HashMap::new(),
        result_columns_to_skip_in_orderby_sorter: None,
    };

    Ok((program, metadata, init_label, start_offset))
}

fn epilogue(
    program: &mut ProgramBuilder,
    metadata: &mut Metadata,
    init_label: BranchOffset,
    start_offset: BranchOffset,
) -> Result<()> {
    let halt_label = metadata.termination_label_stack.pop().unwrap();
    program.resolve_label(halt_label, program.offset());
    program.emit_insn(Insn::Halt {
        err_code: 0,
        description: String::new(),
    });

    program.resolve_label(init_label, program.offset());
    program.emit_insn(Insn::Transaction { write: false });

    program.emit_constant_insns();
    program.emit_insn(Insn::Goto {
        target_pc: start_offset,
    });

    program.resolve_deferred_labels();

    Ok(())
}

pub fn emit_program(
    database_header: Rc<RefCell<DatabaseHeader>>,
    mut plan: Plan,
    connection: Weak<Connection>,
) -> Result<Program> {
    let (mut program, mut metadata, init_label, start_offset) = prologue()?;

    // Trivial exit on LIMIT 0
    if let Some(limit) = plan.limit {
        if limit == 0 {
            epilogue(&mut program, &mut metadata, init_label, start_offset)?;
            return Ok(program.build(database_header, connection));
        }
    }

    // OPEN CURSORS ETC
    if let Some(ref mut order_by) = plan.order_by {
        init_order_by(&mut program, order_by, &mut metadata)?;
    }

    if let Some(ref mut group_by) = plan.group_by {
        let aggregates = plan.aggregates.as_mut().unwrap();
        init_group_by(&mut program, group_by, aggregates, &mut metadata)?;
    }
    init_source(&mut program, &plan.source, &mut metadata)?;

    // REWIND CURSORS, EMIT CONDITIONS
    open_loop(
        &mut program,
        &mut plan.source,
        &plan.referenced_tables,
        &mut metadata,
    )?;

    // EMIT COLUMNS AND OTHER EXPRS IN INNER LOOP
    inner_loop_emit(&mut program, &mut plan, &mut metadata)?;

    // CLOSE LOOP
    close_loop(
        &mut program,
        &mut plan.source,
        &mut metadata,
        &plan.referenced_tables,
    )?;

    let mut order_by_necessary = plan.order_by.is_some();

    // IF GROUP BY, SORT BY GROUPS AND DO AGGREGATION
    if let Some(ref mut group_by) = plan.group_by {
        group_by_emit(
            &mut program,
            &plan.result_columns,
            group_by,
            plan.order_by.as_ref(),
            &plan.aggregates.as_ref().unwrap(),
            plan.limit.clone(),
            &plan.referenced_tables,
            &mut metadata,
        )?;
    } else if let Some(ref mut aggregates) = plan.aggregates {
        // Example: SELECT sum(x), count(*) FROM t;
        agg_without_group_by_emit(
            &mut program,
            &plan.referenced_tables,
            &plan.result_columns,
            aggregates,
            &mut metadata,
        )?;
        // If we have an aggregate without a group by, we don't need an order by because currently
        // there can only be a single row result in those cases.
        order_by_necessary = false;
    }

    // IF ORDER BY, SORT BY ORDER BY
    if let Some(ref mut order_by) = plan.order_by {
        if order_by_necessary {
            sort_order_by(
                &mut program,
                order_by,
                &plan.result_columns,
                plan.limit.clone(),
                &mut metadata,
            )?;
        }
    }

    // EPILOGUE
    epilogue(&mut program, &mut metadata, init_label, start_offset)?;

    Ok(program.build(database_header, connection))
}

const ORDER_BY_ID: usize = 0;

fn init_order_by(
    program: &mut ProgramBuilder,
    order_by: &Vec<(ast::Expr, Direction)>,
    m: &mut Metadata,
) -> Result<()> {
    m.termination_label_stack.push(program.allocate_label());
    let sort_cursor = program.alloc_cursor_id(None, None);
    m.sort_metadata = Some(SortMetadata {
        sort_cursor,
        sorter_data_register: program.alloc_register(),
        sorter_data_label: program.allocate_label(),
        done_label: program.allocate_label(),
    });
    let mut order = Vec::new();
    for (_, direction) in order_by.iter() {
        order.push(OwnedValue::Integer(*direction as i64));
    }
    program.emit_insn(Insn::SorterOpen {
        cursor_id: sort_cursor,
        columns: order_by.len(),
        order: OwnedRecord::new(order),
    });
    Ok(())
}

fn init_group_by(
    program: &mut ProgramBuilder,
    group_by: &Vec<ast::Expr>,
    aggregates: &Vec<Aggregate>,
    m: &mut Metadata,
) -> Result<()> {
    let agg_final_label = program.allocate_label();
    m.termination_label_stack.push(agg_final_label);
    let num_aggs = aggregates.len();

    let sort_cursor = program.alloc_cursor_id(None, None);

    let abort_flag_register = program.alloc_register();
    let data_in_accumulator_indicator_register = program.alloc_register();
    let group_exprs_comparison_register = program.alloc_registers(group_by.len());
    let group_exprs_accumulator_register = program.alloc_registers(group_by.len());
    let agg_exprs_start_reg = program.alloc_registers(num_aggs);
    let sorter_key_register = program.alloc_register();

    let subroutine_accumulator_clear_label = program.allocate_label();
    let subroutine_accumulator_output_label = program.allocate_label();
    let sorter_data_label = program.allocate_label();
    let grouping_done_label = program.allocate_label();

    let mut order = Vec::new();
    const ASCENDING: i64 = 0;
    for _ in group_by.iter() {
        order.push(OwnedValue::Integer(ASCENDING));
    }
    program.emit_insn(Insn::SorterOpen {
        cursor_id: sort_cursor,
        columns: aggregates.len() + group_by.len(),
        order: OwnedRecord::new(order),
    });

    program.add_comment(program.offset(), "clear group by abort flag");
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: abort_flag_register,
    });

    program.add_comment(
        program.offset(),
        "initialize group by comparison registers to NULL",
    );
    program.emit_insn(Insn::Null {
        dest: group_exprs_comparison_register,
        dest_end: if group_by.len() > 1 {
            Some(group_exprs_comparison_register + group_by.len() - 1)
        } else {
            None
        },
    });

    program.add_comment(program.offset(), "go to clear accumulator subroutine");

    let subroutine_accumulator_clear_return_offset_register = program.alloc_register();
    program.emit_insn_with_label_dependency(
        Insn::Gosub {
            target_pc: subroutine_accumulator_clear_label,
            return_reg: subroutine_accumulator_clear_return_offset_register,
        },
        subroutine_accumulator_clear_label,
    );

    m.aggregation_start_register = Some(agg_exprs_start_reg);

    m.group_by_metadata = Some(GroupByMetadata {
        sort_cursor,
        subroutine_accumulator_clear_label,
        subroutine_accumulator_clear_return_offset_register,
        subroutine_accumulator_output_label,
        subroutine_accumulator_output_return_offset_register: program.alloc_register(),
        accumulator_indicator_set_true_label: program.allocate_label(),
        sorter_data_label,
        grouping_done_label,
        abort_flag_register,
        data_in_accumulator_indicator_register,
        group_exprs_accumulator_register,
        group_exprs_comparison_register,
        sorter_key_register,
    });
    Ok(())
}

// fn init_agg_without_group_by(
//     program: &mut ProgramBuilder,
//     aggregates: &Vec<Aggregate>,
//     m: &mut Metadata,
// ) -> Result<()> {

//     Ok(())
// }

fn init_source(
    program: &mut ProgramBuilder,
    source: &SourceOperator,
    m: &mut Metadata,
) -> Result<()> {
    match source {
        SourceOperator::Join {
            id,
            left,
            right,
            outer,
            ..
        } => {
            if *outer {
                let lj_metadata = LeftJoinMetadata {
                    match_flag_register: program.alloc_register(),
                    set_match_flag_true_label: program.allocate_label(),
                    check_match_flag_label: program.allocate_label(),
                    on_match_jump_to_label: program.allocate_label(),
                };
                m.left_joins.insert(*id, lj_metadata);
            }
            init_source(program, left, m)?;
            init_source(program, right, m)?;

            return Ok(());
        }
        SourceOperator::Scan {
            id,
            table_reference,
            ..
        } => {
            let cursor_id = program.alloc_cursor_id(
                Some(table_reference.table_identifier.clone()),
                Some(Table::BTree(table_reference.table.clone())),
            );
            let root_page = table_reference.table.root_page;
            let next_row_label = program.allocate_label();
            m.next_row_labels.insert(*id, next_row_label);
            program.emit_insn(Insn::OpenReadAsync {
                cursor_id,
                root_page,
            });
            program.emit_insn(Insn::OpenReadAwait);

            return Ok(());
        }
        SourceOperator::Search {
            id,
            table_reference,
            search,
            ..
        } => {
            let table_cursor_id = program.alloc_cursor_id(
                Some(table_reference.table_identifier.clone()),
                Some(Table::BTree(table_reference.table.clone())),
            );

            let next_row_label = program.allocate_label();

            if !matches!(search, Search::PrimaryKeyEq { .. }) {
                // Primary key equality search is handled with a SeekRowid instruction which does not loop, since it is a single row lookup.
                m.next_row_labels.insert(*id, next_row_label);
            }

            let scan_loop_body_label = program.allocate_label();
            m.scan_loop_body_labels.push(scan_loop_body_label);
            program.emit_insn(Insn::OpenReadAsync {
                cursor_id: table_cursor_id,
                root_page: table_reference.table.root_page,
            });
            program.emit_insn(Insn::OpenReadAwait);

            if let Search::IndexSearch { index, .. } = search {
                let index_cursor_id = program
                    .alloc_cursor_id(Some(index.name.clone()), Some(Table::Index(index.clone())));
                program.emit_insn(Insn::OpenReadAsync {
                    cursor_id: index_cursor_id,
                    root_page: index.root_page,
                });
                program.emit_insn(Insn::OpenReadAwait);
            }

            return Ok(());
        }
        SourceOperator::Nothing => {
            return Ok(());
        }
    }
}

fn open_loop(
    program: &mut ProgramBuilder,
    source: &mut SourceOperator,
    referenced_tables: &[BTreeTableReference],
    m: &mut Metadata,
) -> Result<()> {
    match source {
        SourceOperator::Join {
            id,
            left,
            right,
            predicates,
            outer,
            ..
        } => {
            open_loop(program, left, referenced_tables, m)?;

            let mut jump_target_when_false = *m
                .next_row_labels
                .get(&right.id())
                .or(m.next_row_labels.get(&left.id()))
                .unwrap_or(m.termination_label_stack.last().unwrap());

            if *outer {
                let lj_meta = m.left_joins.get(id).unwrap();
                program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: lj_meta.match_flag_register,
                });
                jump_target_when_false = lj_meta.check_match_flag_label;
            }
            m.next_row_labels.insert(right.id(), jump_target_when_false);

            open_loop(program, right, referenced_tables, m)?;

            if let Some(predicates) = predicates {
                let jump_target_when_true = program.allocate_label();
                let condition_metadata = ConditionMetadata {
                    jump_if_condition_is_true: false,
                    jump_target_when_true,
                    jump_target_when_false,
                };
                for predicate in predicates.iter() {
                    translate_condition_expr(
                        program,
                        referenced_tables,
                        predicate,
                        None,
                        condition_metadata,
                        None,
                    )?;
                }
                program.resolve_label(jump_target_when_true, program.offset());
            }

            if *outer {
                let lj_meta = m.left_joins.get(id).unwrap();
                program.defer_label_resolution(
                    lj_meta.set_match_flag_true_label,
                    program.offset() as usize,
                );
                program.emit_insn(Insn::Integer {
                    value: 1,
                    dest: lj_meta.match_flag_register,
                });
            }

            return Ok(());
        }
        SourceOperator::Scan {
            id,
            table_reference,
            predicates,
            iter_dir,
        } => {
            let cursor_id = program.resolve_cursor_id(&table_reference.table_identifier, None);
            if iter_dir
                .as_ref()
                .is_some_and(|dir| *dir == IterationDirection::Backwards)
            {
                program.emit_insn(Insn::LastAsync { cursor_id });
            } else {
                program.emit_insn(Insn::RewindAsync { cursor_id });
            }
            let scan_loop_body_label = program.allocate_label();
            let halt_label = m.termination_label_stack.last().unwrap();
            program.emit_insn_with_label_dependency(
                if iter_dir
                    .as_ref()
                    .is_some_and(|dir| *dir == IterationDirection::Backwards)
                {
                    Insn::LastAwait {
                        cursor_id,
                        pc_if_empty: *halt_label,
                    }
                } else {
                    Insn::RewindAwait {
                        cursor_id,
                        pc_if_empty: *halt_label,
                    }
                },
                *halt_label,
            );
            m.scan_loop_body_labels.push(scan_loop_body_label);
            program.defer_label_resolution(scan_loop_body_label, program.offset() as usize);

            let jump_label = m.next_row_labels.get(id).unwrap_or(halt_label);
            if let Some(preds) = predicates {
                for expr in preds {
                    let jump_target_when_true = program.allocate_label();
                    let condition_metadata = ConditionMetadata {
                        jump_if_condition_is_true: false,
                        jump_target_when_true,
                        jump_target_when_false: *jump_label,
                    };
                    translate_condition_expr(
                        program,
                        referenced_tables,
                        expr,
                        None,
                        condition_metadata,
                        None,
                    )?;
                    program.resolve_label(jump_target_when_true, program.offset());
                }
            }

            return Ok(());
        }
        SourceOperator::Search {
            id,
            table_reference,
            search,
            predicates,
            ..
        } => {
            let table_cursor_id =
                program.resolve_cursor_id(&table_reference.table_identifier, None);

            // Open the loop for the index search.
            // Primary key equality search is handled with a SeekRowid instruction which does not loop, since it is a single row lookup.
            if !matches!(search, Search::PrimaryKeyEq { .. }) {
                let index_cursor_id = if let Search::IndexSearch { index, .. } = search {
                    Some(program.resolve_cursor_id(&index.name, None))
                } else {
                    None
                };
                let scan_loop_body_label = *m.scan_loop_body_labels.last().unwrap();
                let cmp_reg = program.alloc_register();
                let (cmp_expr, cmp_op) = match search {
                    Search::IndexSearch {
                        cmp_expr, cmp_op, ..
                    } => (cmp_expr, cmp_op),
                    Search::PrimaryKeySearch { cmp_expr, cmp_op } => (cmp_expr, cmp_op),
                    Search::PrimaryKeyEq { .. } => unreachable!(),
                };
                // TODO this only handles ascending indexes
                match cmp_op {
                    ast::Operator::Equals
                    | ast::Operator::Greater
                    | ast::Operator::GreaterEquals => {
                        translate_expr(
                            program,
                            Some(referenced_tables),
                            cmp_expr,
                            cmp_reg,
                            None,
                            None,
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
                program.emit_insn_with_label_dependency(
                    match cmp_op {
                        ast::Operator::Equals | ast::Operator::GreaterEquals => Insn::SeekGE {
                            is_index: index_cursor_id.is_some(),
                            cursor_id: index_cursor_id.unwrap_or(table_cursor_id),
                            start_reg: cmp_reg,
                            num_regs: 1,
                            target_pc: *m.termination_label_stack.last().unwrap(),
                        },
                        ast::Operator::Greater
                        | ast::Operator::Less
                        | ast::Operator::LessEquals => Insn::SeekGT {
                            is_index: index_cursor_id.is_some(),
                            cursor_id: index_cursor_id.unwrap_or(table_cursor_id),
                            start_reg: cmp_reg,
                            num_regs: 1,
                            target_pc: *m.termination_label_stack.last().unwrap(),
                        },
                        _ => unreachable!(),
                    },
                    *m.termination_label_stack.last().unwrap(),
                );
                if *cmp_op == ast::Operator::Less || *cmp_op == ast::Operator::LessEquals {
                    translate_expr(
                        program,
                        Some(referenced_tables),
                        cmp_expr,
                        cmp_reg,
                        None,
                        None,
                    )?;
                }

                program.defer_label_resolution(scan_loop_body_label, program.offset() as usize);
                // TODO: We are currently only handling ascending indexes.
                // For conditions like index_key > 10, we have already seeked to the first key greater than 10, and can just scan forward.
                // For conditions like index_key < 10, we are at the beginning of the index, and will scan forward and emit IdxGE(10) with a conditional jump to the end.
                // For conditions like index_key = 10, we have already seeked to the first key greater than or equal to 10, and can just scan forward and emit IdxGT(10) with a conditional jump to the end.
                // For conditions like index_key >= 10, we have already seeked to the first key greater than or equal to 10, and can just scan forward.
                // For conditions like index_key <= 10, we are at the beginning of the index, and will scan forward and emit IdxGT(10) with a conditional jump to the end.
                // For conditions like index_key != 10, TODO. probably the optimal way is not to use an index at all.
                //
                // For primary key searches we emit RowId and then compare it to the seek value.

                let abort_jump_target = *m
                    .next_row_labels
                    .get(id)
                    .unwrap_or(m.termination_label_stack.last().unwrap());
                match cmp_op {
                    ast::Operator::Equals | ast::Operator::LessEquals => {
                        if let Some(index_cursor_id) = index_cursor_id {
                            program.emit_insn_with_label_dependency(
                                Insn::IdxGT {
                                    cursor_id: index_cursor_id,
                                    start_reg: cmp_reg,
                                    num_regs: 1,
                                    target_pc: abort_jump_target,
                                },
                                abort_jump_target,
                            );
                        } else {
                            let rowid_reg = program.alloc_register();
                            program.emit_insn(Insn::RowId {
                                cursor_id: table_cursor_id,
                                dest: rowid_reg,
                            });
                            program.emit_insn_with_label_dependency(
                                Insn::Gt {
                                    lhs: rowid_reg,
                                    rhs: cmp_reg,
                                    target_pc: abort_jump_target,
                                },
                                abort_jump_target,
                            );
                        }
                    }
                    ast::Operator::Less => {
                        if let Some(index_cursor_id) = index_cursor_id {
                            program.emit_insn_with_label_dependency(
                                Insn::IdxGE {
                                    cursor_id: index_cursor_id,
                                    start_reg: cmp_reg,
                                    num_regs: 1,
                                    target_pc: abort_jump_target,
                                },
                                abort_jump_target,
                            );
                        } else {
                            let rowid_reg = program.alloc_register();
                            program.emit_insn(Insn::RowId {
                                cursor_id: table_cursor_id,
                                dest: rowid_reg,
                            });
                            program.emit_insn_with_label_dependency(
                                Insn::Ge {
                                    lhs: rowid_reg,
                                    rhs: cmp_reg,
                                    target_pc: abort_jump_target,
                                },
                                abort_jump_target,
                            );
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

            let jump_label = m
                .next_row_labels
                .get(id)
                .unwrap_or(m.termination_label_stack.last().unwrap());

            if let Search::PrimaryKeyEq { cmp_expr } = search {
                let src_reg = program.alloc_register();
                translate_expr(
                    program,
                    Some(referenced_tables),
                    cmp_expr,
                    src_reg,
                    None,
                    None,
                )?;
                program.emit_insn_with_label_dependency(
                    Insn::SeekRowid {
                        cursor_id: table_cursor_id,
                        src_reg,
                        target_pc: *jump_label,
                    },
                    *jump_label,
                );
            }
            if let Some(predicates) = predicates {
                for predicate in predicates.iter() {
                    let jump_target_when_true = program.allocate_label();
                    let condition_metadata = ConditionMetadata {
                        jump_if_condition_is_true: false,
                        jump_target_when_true,
                        jump_target_when_false: *jump_label,
                    };
                    translate_condition_expr(
                        program,
                        referenced_tables,
                        predicate,
                        None,
                        condition_metadata,
                        None,
                    )?;
                    program.resolve_label(jump_target_when_true, program.offset());
                }
            }

            return Ok(());
        }
        SourceOperator::Nothing => {
            return Ok(());
        }
    }
}

pub enum InnerLoopEmitTarget<'a> {
    GroupBySorter {
        group_by: &'a Vec<ast::Expr>,
        aggregates: &'a Vec<Aggregate>,
    },
    OrderBySorter {
        order_by: &'a Vec<(ast::Expr, Direction)>,
    },
    ResultRow {
        limit: Option<usize>,
    },
    AggStep,
}

fn inner_loop_emit(program: &mut ProgramBuilder, plan: &mut Plan, m: &mut Metadata) -> Result<()> {
    if let Some(wc) = &plan.where_clause {
        for predicate in wc.iter() {
            if predicate.is_always_false()? {
                return Ok(());
            } else if predicate.is_always_true()? {
                // do nothing
            } else {
                unreachable!(
                    "all WHERE clause terms that are not trivially true or false should have been pushed down to the source"
                );
            }
        }
    }
    // if we have a group by, we emit a record into the group by sorter.
    if let Some(group_by) = &plan.group_by {
        return inner_loop_source_emit(
            program,
            &plan.result_columns,
            &plan.aggregates,
            m,
            InnerLoopEmitTarget::GroupBySorter {
                group_by,
                aggregates: &plan.aggregates.as_ref().unwrap(),
            },
            &plan.referenced_tables,
        );
    }
    // if we DONT have a group by, but we have aggregates, we emit without ResultRow.
    // we also do not need to sort because we are emitting a single row.
    if plan.aggregates.is_some() {
        return inner_loop_source_emit(
            program,
            &plan.result_columns,
            &plan.aggregates,
            m,
            InnerLoopEmitTarget::AggStep,
            &plan.referenced_tables,
        );
    }
    // if we DONT have a group by, but we have an order by, we emit a record into the order by sorter.
    if let Some(order_by) = &plan.order_by {
        return inner_loop_source_emit(
            program,
            &plan.result_columns,
            &plan.aggregates,
            m,
            InnerLoopEmitTarget::OrderBySorter { order_by },
            &plan.referenced_tables,
        );
    }
    // if we have neither, we emit a ResultRow. In that case, if we have a Limit, we handle that with DecrJumpZero.
    return inner_loop_source_emit(
        program,
        &plan.result_columns,
        &plan.aggregates,
        m,
        InnerLoopEmitTarget::ResultRow { limit: plan.limit },
        &plan.referenced_tables,
    );
}

fn inner_loop_source_emit(
    program: &mut ProgramBuilder,
    result_columns: &Vec<ResultSetColumn>,
    aggregates: &Option<Vec<Aggregate>>,
    m: &mut Metadata,
    emit_target: InnerLoopEmitTarget,
    referenced_tables: &[BTreeTableReference],
) -> Result<()> {
    match emit_target {
        InnerLoopEmitTarget::GroupBySorter {
            group_by,
            aggregates,
        } => {
            let sort_keys_count = group_by.len();
            let aggregate_arguments_count =
                aggregates.iter().map(|agg| agg.args.len()).sum::<usize>();
            let column_count = sort_keys_count + aggregate_arguments_count;
            let start_reg = program.alloc_registers(column_count);
            let mut cur_reg = start_reg;
            for expr in group_by.iter() {
                let key_reg = cur_reg;
                cur_reg += 1;
                translate_expr(program, Some(referenced_tables), expr, key_reg, None, None)?;
            }
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
                    translate_expr(program, Some(referenced_tables), expr, agg_reg, None, None)?;
                }
            }

            let group_by_metadata = m.group_by_metadata.as_ref().unwrap();

            program.emit_insn(Insn::MakeRecord {
                start_reg,
                count: column_count,
                dest_reg: group_by_metadata.sorter_key_register,
            });

            program.emit_insn(Insn::SorterInsert {
                cursor_id: group_by_metadata.sort_cursor,
                record_reg: group_by_metadata.sorter_key_register,
            });

            Ok(())
        }
        InnerLoopEmitTarget::OrderBySorter { order_by } => {
            // We need to handle the case where we are emitting to sorter.
            // In that case the first columns should be the sort key columns, and the rest is the result columns of the select.
            // In case any of the sort keys are exactly equal to a result column, we need to skip emitting that result column.
            // We need to do this before rewriting the result columns to registers because we need to know which columns to skip.
            // Moreover, we need to keep track what index in the ORDER BY sorter the result columns have, because the result columns
            // should be emitted in the SELECT clause order, not the ORDER BY clause order.
            let mut result_columns_to_skip: Option<Vec<usize>> = None;
            for (i, rc) in result_columns.iter().enumerate() {
                match rc {
                    ResultSetColumn::Expr {
                        expr,
                        contains_aggregates,
                    } => {
                        assert!(!*contains_aggregates);
                        let found = order_by.iter().enumerate().find(|(_, (e, _))| e == expr);
                        if let Some((j, _)) = found {
                            if let Some(ref mut v) = result_columns_to_skip {
                                v.push(i);
                            } else {
                                result_columns_to_skip = Some(vec![i]);
                            }
                            m.result_column_indexes_in_orderby_sorter.insert(i, j);
                        }
                    }
                    ResultSetColumn::Agg(agg) => {
                        let found = order_by
                            .iter()
                            .enumerate()
                            .find(|(_, (expr, _))| expr == &agg.original_expr);
                        if let Some((j, _)) = found {
                            if let Some(ref mut v) = result_columns_to_skip {
                                v.push(i);
                            } else {
                                result_columns_to_skip = Some(vec![i]);
                            }
                            m.result_column_indexes_in_orderby_sorter.insert(i, j);
                        }
                    }
                }
            }
            let order_by_len = order_by.len();
            let result_columns_to_skip_len = result_columns_to_skip
                .as_ref()
                .map(|v| v.len())
                .unwrap_or(0);
            let orderby_sorter_column_count =
                order_by_len + result_columns.len() - result_columns_to_skip_len;
            let start_reg = program.alloc_registers(orderby_sorter_column_count);
            for (i, (expr, _)) in order_by.iter().enumerate() {
                let key_reg = start_reg + i;
                translate_expr(program, Some(referenced_tables), expr, key_reg, None, None)?;
            }
            let mut cur_reg = start_reg + order_by_len;
            let mut cur_idx_in_orderby_sorter = order_by_len;
            for (i, rc) in result_columns.iter().enumerate() {
                if let Some(ref v) = result_columns_to_skip {
                    if v.contains(&i) {
                        continue;
                    }
                }
                match rc {
                    ResultSetColumn::Expr {
                        expr,
                        contains_aggregates,
                    } => {
                        assert!(!*contains_aggregates);
                        translate_expr(
                            program,
                            Some(referenced_tables),
                            expr,
                            cur_reg,
                            None,
                            None,
                        )?;
                    }
                    other => unreachable!("{:?}", other),
                }
                m.result_column_indexes_in_orderby_sorter
                    .insert(i, cur_idx_in_orderby_sorter);
                cur_idx_in_orderby_sorter += 1;
                cur_reg += 1;
            }

            let sort_metadata = m.sort_metadata.as_mut().unwrap();
            program.emit_insn(Insn::MakeRecord {
                start_reg,
                count: orderby_sorter_column_count,
                dest_reg: sort_metadata.sorter_data_register,
            });

            program.emit_insn(Insn::SorterInsert {
                cursor_id: sort_metadata.sort_cursor,
                record_reg: sort_metadata.sorter_data_register,
            });

            Ok(())
        }
        InnerLoopEmitTarget::AggStep => {
            let aggregates = aggregates.as_ref().unwrap();
            let agg_final_label = program.allocate_label();
            m.termination_label_stack.push(agg_final_label);
            let num_aggs = aggregates.len();
            let start_reg = program.alloc_registers(result_columns.len());
            m.aggregation_start_register = Some(start_reg);
            for (i, agg) in aggregates.iter().enumerate() {
                let reg = start_reg + i;
                translate_aggregation(program, referenced_tables, agg, reg, None)?;
            }
            for (i, expr) in result_columns.iter().enumerate() {
                match expr {
                    ResultSetColumn::Expr {
                        expr,
                        contains_aggregates,
                    } => {
                        if *contains_aggregates {
                            // Do nothing, aggregates will be computed above and this full result expression will be
                            // computed later
                            continue;
                        }
                        let reg = start_reg + num_aggs + i;
                        translate_expr(program, Some(referenced_tables), expr, reg, None, None)?;
                    }
                    ResultSetColumn::Agg(_) => { /* do nothing, aggregates are computed above */ }
                }
            }
            Ok(())
        }
        InnerLoopEmitTarget::ResultRow { limit } => {
            assert!(aggregates.is_none());
            let start_reg = program.alloc_registers(result_columns.len());
            for (i, expr) in result_columns.iter().enumerate() {
                match expr {
                    ResultSetColumn::Expr {
                        expr,
                        contains_aggregates,
                    } => {
                        assert!(!*contains_aggregates);
                        let reg = start_reg + i;
                        translate_expr(program, Some(referenced_tables), expr, reg, None, None)?;
                    }
                    other => unreachable!(
                        "Unexpected non-scalar result column in inner loop: {:?}",
                        other
                    ),
                }
            }
            program.emit_insn(Insn::ResultRow {
                start_reg,
                count: result_columns.len(),
            });
            if let Some(limit) = limit {
                let jump_label = m.termination_label_stack.last().unwrap();
                let limit_reg = program.alloc_register();
                program.emit_insn(Insn::Integer {
                    value: limit as i64,
                    dest: limit_reg,
                });
                program.mark_last_insn_constant();
                program.emit_insn_with_label_dependency(
                    Insn::DecrJumpZero {
                        reg: limit_reg,
                        target_pc: *jump_label,
                    },
                    *jump_label,
                );
            }

            Ok(())
        }
    }
}

fn close_loop(
    program: &mut ProgramBuilder,
    source: &SourceOperator,
    m: &mut Metadata,
    referenced_tables: &[BTreeTableReference],
) -> Result<()> {
    match source {
        SourceOperator::Join {
            id,
            left,
            right,
            outer,
            ..
        } => {
            close_loop(program, right, m, referenced_tables)?;

            if *outer {
                let lj_meta = m.left_joins.get(id).unwrap();
                // If the left join match flag has been set to 1, we jump to the next row on the outer table (result row has been emitted already)
                program.resolve_label(lj_meta.check_match_flag_label, program.offset());
                program.emit_insn_with_label_dependency(
                    Insn::IfPos {
                        reg: lj_meta.match_flag_register,
                        target_pc: lj_meta.on_match_jump_to_label,
                        decrement_by: 0,
                    },
                    lj_meta.on_match_jump_to_label,
                );
                // If not, we set the right table cursor's "pseudo null bit" on, which means any Insn::Column will return NULL
                let right_cursor_id = match right.as_ref() {
                    SourceOperator::Scan {
                        table_reference, ..
                    } => program.resolve_cursor_id(&table_reference.table_identifier, None),
                    SourceOperator::Search {
                        table_reference, ..
                    } => program.resolve_cursor_id(&table_reference.table_identifier, None),
                    _ => unreachable!(),
                };
                program.emit_insn(Insn::NullRow {
                    cursor_id: right_cursor_id,
                });
                // Jump to setting the left join match flag to 1 again, but this time the right table cursor will set everything to null
                program.emit_insn_with_label_dependency(
                    Insn::Goto {
                        target_pc: lj_meta.set_match_flag_true_label,
                    },
                    lj_meta.set_match_flag_true_label,
                );
            }
            let next_row_label = if *outer {
                m.left_joins.get(id).unwrap().on_match_jump_to_label
            } else {
                *m.next_row_labels.get(&right.id()).unwrap()
            };
            // This points to the NextAsync instruction of the left table
            program.resolve_label(next_row_label, program.offset());
            close_loop(program, left, m, referenced_tables)?;

            Ok(())
        }
        SourceOperator::Scan {
            id,
            table_reference,
            iter_dir,
            ..
        } => {
            let cursor_id = program.resolve_cursor_id(&table_reference.table_identifier, None);
            program.resolve_label(*m.next_row_labels.get(id).unwrap(), program.offset());
            if iter_dir
                .as_ref()
                .is_some_and(|dir| *dir == IterationDirection::Backwards)
            {
                program.emit_insn(Insn::PrevAsync { cursor_id });
            } else {
                program.emit_insn(Insn::NextAsync { cursor_id });
            }
            let jump_label = m.scan_loop_body_labels.pop().unwrap();

            if iter_dir
                .as_ref()
                .is_some_and(|dir| *dir == IterationDirection::Backwards)
            {
                program.emit_insn_with_label_dependency(
                    Insn::PrevAwait {
                        cursor_id,
                        pc_if_next: jump_label,
                    },
                    jump_label,
                );
            } else {
                program.emit_insn_with_label_dependency(
                    Insn::NextAwait {
                        cursor_id,
                        pc_if_next: jump_label,
                    },
                    jump_label,
                );
            }
            Ok(())
        }
        SourceOperator::Search {
            id,
            table_reference,
            search,
            ..
        } => {
            if matches!(search, Search::PrimaryKeyEq { .. }) {
                // Primary key equality search is handled with a SeekRowid instruction which does not loop, so there is no need to emit a NextAsync instruction.
                return Ok(());
            }
            let cursor_id = match search {
                Search::IndexSearch { index, .. } => program.resolve_cursor_id(&index.name, None),
                Search::PrimaryKeySearch { .. } => {
                    program.resolve_cursor_id(&table_reference.table_identifier, None)
                }
                Search::PrimaryKeyEq { .. } => unreachable!(),
            };
            program.resolve_label(*m.next_row_labels.get(id).unwrap(), program.offset());
            program.emit_insn(Insn::NextAsync { cursor_id });
            let jump_label = m.scan_loop_body_labels.pop().unwrap();
            program.emit_insn_with_label_dependency(
                Insn::NextAwait {
                    cursor_id,
                    pc_if_next: jump_label,
                },
                jump_label,
            );

            Ok(())
        }
        SourceOperator::Nothing => Ok(()),
    }
}

fn group_by_emit(
    program: &mut ProgramBuilder,
    result_columns: &Vec<ResultSetColumn>,
    group_by: &Vec<ast::Expr>,
    order_by: Option<&Vec<(ast::Expr, Direction)>>,
    aggregates: &Vec<Aggregate>,
    limit: Option<usize>,
    referenced_tables: &[BTreeTableReference],
    m: &mut Metadata,
) -> Result<()> {
    let group_by_metadata = m.group_by_metadata.as_mut().unwrap();

    let GroupByMetadata {
        group_exprs_comparison_register: comparison_register,
        subroutine_accumulator_output_return_offset_register,
        subroutine_accumulator_output_label,
        subroutine_accumulator_clear_return_offset_register,
        subroutine_accumulator_clear_label,
        data_in_accumulator_indicator_register,
        accumulator_indicator_set_true_label,
        group_exprs_accumulator_register: group_exprs_start_register,
        abort_flag_register,
        sorter_key_register,
        ..
    } = *group_by_metadata;
    let halt_label = *m.termination_label_stack.first().unwrap();

    // all group by columns and all arguments of agg functions are in the sorter.
    // the sort keys are the group by columns (the aggregation within groups is done based on how long the sort keys remain the same)
    let sorter_column_count =
        group_by.len() + aggregates.iter().map(|agg| agg.args.len()).sum::<usize>();
    // sorter column names do not matter
    let pseudo_columns = (0..sorter_column_count)
        .map(|i| Column {
            name: i.to_string(),
            primary_key: false,
            ty: crate::schema::Type::Null,
        })
        .collect::<Vec<_>>();

    // A pseudo table is a "fake" table to which we read one row at a time from the sorter
    let pseudo_table = Rc::new(PseudoTable {
        columns: pseudo_columns,
    });

    let pseudo_cursor = program.alloc_cursor_id(None, Some(Table::Pseudo(pseudo_table.clone())));

    program.emit_insn(Insn::OpenPseudo {
        cursor_id: pseudo_cursor,
        content_reg: sorter_key_register,
        num_fields: sorter_column_count,
    });

    // Sort the sorter based on the group by columns
    program.emit_insn_with_label_dependency(
        Insn::SorterSort {
            cursor_id: group_by_metadata.sort_cursor,
            pc_if_empty: group_by_metadata.grouping_done_label,
        },
        group_by_metadata.grouping_done_label,
    );

    program.defer_label_resolution(
        group_by_metadata.sorter_data_label,
        program.offset() as usize,
    );
    // Read a row from the sorted data in the sorter into the pseudo cursor
    program.emit_insn(Insn::SorterData {
        cursor_id: group_by_metadata.sort_cursor,
        dest_reg: group_by_metadata.sorter_key_register,
        pseudo_cursor,
    });

    // Read the group by columns from the pseudo cursor
    let groups_start_reg = program.alloc_registers(group_by.len());
    for i in 0..group_by.len() {
        let sorter_column_index = i;
        let group_reg = groups_start_reg + i;
        program.emit_insn(Insn::Column {
            cursor_id: pseudo_cursor,
            column: sorter_column_index,
            dest: group_reg,
        });
    }

    // Compare the group by columns to the previous group by columns to see if we are at a new group or not
    program.emit_insn(Insn::Compare {
        start_reg_a: comparison_register,
        start_reg_b: groups_start_reg,
        count: group_by.len(),
    });

    let agg_step_label = program.allocate_label();

    program.add_comment(
        program.offset(),
        "start new group if comparison is not equal",
    );
    // If we are at a new group, continue. If we are at the same group, jump to the aggregation step (i.e. accumulate more values into the aggregations)
    program.emit_insn_with_label_dependency(
        Insn::Jump {
            target_pc_lt: program.offset() + 1,
            target_pc_eq: agg_step_label,
            target_pc_gt: program.offset() + 1,
        },
        agg_step_label,
    );

    // New group, move current group by columns into the comparison register
    program.emit_insn(Insn::Move {
        source_reg: groups_start_reg,
        dest_reg: comparison_register,
        count: group_by.len(),
    });

    program.add_comment(
        program.offset(),
        "check if ended group had data, and output if so",
    );
    program.emit_insn_with_label_dependency(
        Insn::Gosub {
            target_pc: subroutine_accumulator_output_label,
            return_reg: subroutine_accumulator_output_return_offset_register,
        },
        subroutine_accumulator_output_label,
    );

    program.add_comment(program.offset(), "check abort flag");
    program.emit_insn_with_label_dependency(
        Insn::IfPos {
            reg: abort_flag_register,
            target_pc: halt_label,
            decrement_by: 0,
        },
        m.termination_label_stack[0],
    );

    program.add_comment(program.offset(), "goto clear accumulator subroutine");
    program.emit_insn_with_label_dependency(
        Insn::Gosub {
            target_pc: subroutine_accumulator_clear_label,
            return_reg: subroutine_accumulator_clear_return_offset_register,
        },
        subroutine_accumulator_clear_label,
    );

    // Accumulate the values into the aggregations
    program.resolve_label(agg_step_label, program.offset());
    let start_reg = m.aggregation_start_register.unwrap();
    let mut cursor_index = group_by.len();
    for (i, agg) in aggregates.iter().enumerate() {
        let agg_result_reg = start_reg + i;
        translate_aggregation_groupby(
            program,
            referenced_tables,
            pseudo_cursor,
            cursor_index,
            agg,
            agg_result_reg,
        )?;
        cursor_index += agg.args.len();
    }

    // We only emit the group by columns if we are going to start a new group (i.e. the prev group will not accumulate any more values into the aggregations)
    program.add_comment(
        program.offset(),
        "don't emit group columns if continuing existing group",
    );
    program.emit_insn_with_label_dependency(
        Insn::If {
            target_pc: accumulator_indicator_set_true_label,
            reg: data_in_accumulator_indicator_register,
            null_reg: 0, // unused in this case
        },
        accumulator_indicator_set_true_label,
    );

    // Read the group by columns for a finished group
    for i in 0..group_by.len() {
        let key_reg = group_exprs_start_register + i;
        let sorter_column_index = i;
        program.emit_insn(Insn::Column {
            cursor_id: pseudo_cursor,
            column: sorter_column_index,
            dest: key_reg,
        });
    }

    program.resolve_label(accumulator_indicator_set_true_label, program.offset());
    program.add_comment(program.offset(), "indicate data in accumulator");
    program.emit_insn(Insn::Integer {
        value: 1,
        dest: data_in_accumulator_indicator_register,
    });

    program.emit_insn_with_label_dependency(
        Insn::SorterNext {
            cursor_id: group_by_metadata.sort_cursor,
            pc_if_next: group_by_metadata.sorter_data_label,
        },
        group_by_metadata.sorter_data_label,
    );

    program.resolve_label(group_by_metadata.grouping_done_label, program.offset());

    program.add_comment(program.offset(), "emit row for final group");
    program.emit_insn_with_label_dependency(
        Insn::Gosub {
            target_pc: group_by_metadata.subroutine_accumulator_output_label,
            return_reg: group_by_metadata.subroutine_accumulator_output_return_offset_register,
        },
        group_by_metadata.subroutine_accumulator_output_label,
    );

    program.add_comment(program.offset(), "group by finished");
    let termination_label = m.termination_label_stack[m.termination_label_stack.len() - 2];
    program.emit_insn_with_label_dependency(
        Insn::Goto {
            target_pc: termination_label,
        },
        termination_label,
    );
    program.emit_insn(Insn::Integer {
        value: 1,
        dest: group_by_metadata.abort_flag_register,
    });
    program.emit_insn(Insn::Return {
        return_reg: group_by_metadata.subroutine_accumulator_output_return_offset_register,
    });

    program.resolve_label(
        group_by_metadata.subroutine_accumulator_output_label,
        program.offset(),
    );

    program.add_comment(program.offset(), "output group by row subroutine start");
    let termination_label = *m.termination_label_stack.last().unwrap();
    program.emit_insn_with_label_dependency(
        Insn::IfPos {
            reg: group_by_metadata.data_in_accumulator_indicator_register,
            target_pc: termination_label,
            decrement_by: 0,
        },
        termination_label,
    );
    program.emit_insn(Insn::Return {
        return_reg: group_by_metadata.subroutine_accumulator_output_return_offset_register,
    });

    let agg_start_reg = m.aggregation_start_register.unwrap();
    program.resolve_label(m.termination_label_stack.pop().unwrap(), program.offset());
    for (i, agg) in aggregates.iter().enumerate() {
        let agg_result_reg = agg_start_reg + i;
        program.emit_insn(Insn::AggFinal {
            register: agg_result_reg,
            func: agg.func.clone(),
        });
    }

    // we now have the group by columns in registers (group_exprs_start_register..group_exprs_start_register + group_by.len() - 1)
    // and the agg results in (agg_start_reg..agg_start_reg + aggregates.len() - 1)
    // we need to call translate_expr on each result column, but replace the expr with a register copy in case any part of the
    // result column expression matches a) a group by column or b) an aggregation result.
    let mut precomputed_exprs_to_register = Vec::with_capacity(aggregates.len() + group_by.len());
    for (i, expr) in group_by.iter().enumerate() {
        precomputed_exprs_to_register.push((expr, group_exprs_start_register + i));
    }
    for (i, agg) in aggregates.iter().enumerate() {
        precomputed_exprs_to_register.push((&agg.original_expr, agg_start_reg + i));
    }

    // We need to handle the case where we are emitting to sorter.
    // In that case the first columns should be the sort key columns, and the rest is the result columns of the select.
    // In case any of the sort keys are exactly equal to a result column, we need to skip emitting that result column.
    // We need to do this before rewriting the result columns to registers because we need to know which columns to skip.
    // Moreover, we need to keep track what index in the ORDER BY sorter the result columns have, because the result columns
    // should be emitted in the SELECT clause order, not the ORDER BY clause order.
    let mut result_columns_to_skip: Option<Vec<usize>> = None;
    if let Some(order_by) = order_by {
        for (i, rc) in result_columns.iter().enumerate() {
            match rc {
                ResultSetColumn::Expr { expr, .. } => {
                    let found = order_by.iter().enumerate().find(|(_, (e, _))| e == expr);
                    if let Some((j, _)) = found {
                        if let Some(ref mut v) = result_columns_to_skip {
                            v.push(i);
                        } else {
                            result_columns_to_skip = Some(vec![i]);
                        }
                        m.result_column_indexes_in_orderby_sorter.insert(i, j);
                    }
                }
                ResultSetColumn::Agg(agg) => {
                    let found = order_by
                        .iter()
                        .enumerate()
                        .find(|(_, (expr, _))| expr == &agg.original_expr);
                    if let Some((j, _)) = found {
                        if let Some(ref mut v) = result_columns_to_skip {
                            v.push(i);
                        } else {
                            result_columns_to_skip = Some(vec![i]);
                        }
                        m.result_column_indexes_in_orderby_sorter.insert(i, j);
                    }
                }
            }
        }
    }
    let order_by_len = order_by.as_ref().map(|v| v.len()).unwrap_or(0);
    let result_columns_to_skip_len = result_columns_to_skip
        .as_ref()
        .map(|v| v.len())
        .unwrap_or(0);
    let output_column_count = result_columns.len() + order_by_len - result_columns_to_skip_len;
    let output_row_start_reg = program.alloc_registers(output_column_count);
    let mut cur_reg = output_row_start_reg;
    if let Some(order_by) = order_by {
        for (expr, _) in order_by.iter() {
            translate_expr(
                program,
                Some(referenced_tables),
                expr,
                cur_reg,
                None,
                Some(&precomputed_exprs_to_register),
            )?;
            cur_reg += 1;
        }
    }
    let mut res_col_idx_in_orderby_sorter = order_by_len;
    for (i, rc) in result_columns.iter().enumerate() {
        if let Some(ref v) = result_columns_to_skip {
            if v.contains(&i) {
                continue;
            }
        }
        match rc {
            ResultSetColumn::Expr { expr, .. } => {
                translate_expr(
                    program,
                    Some(referenced_tables),
                    expr,
                    cur_reg,
                    None,
                    Some(&precomputed_exprs_to_register),
                )?;
            }
            ResultSetColumn::Agg(agg) => {
                let found = aggregates.iter().enumerate().find(|(_, a)| **a == *agg);
                if let Some((i, _)) = found {
                    program.emit_insn(Insn::Copy {
                        src_reg: agg_start_reg + i,
                        dst_reg: cur_reg,
                        amount: 0,
                    });
                } else {
                    unreachable!("agg {:?} not found", agg);
                }
            }
        }
        m.result_column_indexes_in_orderby_sorter
            .insert(i, res_col_idx_in_orderby_sorter);
        res_col_idx_in_orderby_sorter += 1;
        cur_reg += 1;
    }

    match order_by {
        None => {
            if let Some(limit) = limit {
                let limit_reg = program.alloc_register();
                program.emit_insn(Insn::Integer {
                    value: limit as i64,
                    dest: limit_reg,
                });
                program.mark_last_insn_constant();
                program.emit_insn(Insn::ResultRow {
                    start_reg: output_row_start_reg,
                    count: output_column_count,
                });
                program.emit_insn_with_label_dependency(
                    Insn::DecrJumpZero {
                        reg: limit_reg,
                        target_pc: *m.termination_label_stack.last().unwrap(),
                    },
                    *m.termination_label_stack.last().unwrap(),
                );
            }
        }
        Some(_) => {
            program.emit_insn(Insn::MakeRecord {
                start_reg: output_row_start_reg,
                count: output_column_count,
                dest_reg: group_by_metadata.sorter_key_register,
            });

            program.emit_insn(Insn::SorterInsert {
                cursor_id: m.sort_metadata.as_ref().unwrap().sort_cursor,
                record_reg: group_by_metadata.sorter_key_register,
            });
        }
    }

    program.emit_insn(Insn::Return {
        return_reg: group_by_metadata.subroutine_accumulator_output_return_offset_register,
    });

    program.add_comment(program.offset(), "clear accumulator subroutine start");
    program.resolve_label(
        group_by_metadata.subroutine_accumulator_clear_label,
        program.offset(),
    );
    let start_reg = group_by_metadata.group_exprs_accumulator_register;
    program.emit_insn(Insn::Null {
        dest: start_reg,
        dest_end: Some(start_reg + group_by.len() + aggregates.len() - 1),
    });

    program.emit_insn(Insn::Integer {
        value: 0,
        dest: group_by_metadata.data_in_accumulator_indicator_register,
    });
    program.emit_insn(Insn::Return {
        return_reg: group_by_metadata.subroutine_accumulator_clear_return_offset_register,
    });

    m.result_columns_to_skip_in_orderby_sorter = result_columns_to_skip;

    Ok(())
}

fn agg_without_group_by_emit(
    program: &mut ProgramBuilder,
    referenced_tables: &Vec<BTreeTableReference>,
    result_columns: &Vec<ResultSetColumn>,
    aggregates: &Vec<Aggregate>,
    m: &mut Metadata,
) -> Result<()> {
    let agg_start_reg = m.aggregation_start_register.unwrap();
    for (i, agg) in aggregates.iter().enumerate() {
        let agg_result_reg = agg_start_reg + i;
        program.emit_insn(Insn::AggFinal {
            register: agg_result_reg,
            func: agg.func.clone(),
        });
    }
    // we now have the group by columns in registers (group_exprs_start_register..group_exprs_start_register + group_by.len() - 1)
    // and the agg results in (agg_start_reg..agg_start_reg + aggregates.len() - 1)
    // we need to call translate_expr on each result column, but replace the expr with a register copy in case any part of the
    // result column expression matches a) a group by column or b) an aggregation result.
    let mut precomputed_exprs_to_register = Vec::with_capacity(aggregates.len());
    for (i, agg) in aggregates.iter().enumerate() {
        precomputed_exprs_to_register.push((&agg.original_expr, agg_start_reg + i));
    }

    let output_reg = program.alloc_registers(result_columns.len());
    for (i, rc) in result_columns.iter().enumerate() {
        match rc {
            ResultSetColumn::Expr { expr, .. } => {
                translate_expr(
                    program,
                    Some(referenced_tables),
                    expr,
                    output_reg + i,
                    None,
                    Some(&precomputed_exprs_to_register),
                )?;
            }
            ResultSetColumn::Agg(agg) => {
                let found = aggregates.iter().enumerate().find(|(_, a)| **a == *agg);
                if let Some((i, _)) = found {
                    program.emit_insn(Insn::Copy {
                        src_reg: agg_start_reg + i,
                        dst_reg: output_reg + i,
                        amount: 0,
                    });
                } else {
                    unreachable!("agg {:?} not found", agg);
                }
            }
        }
    }
    // This always emits a ResultRow because currently it can only be used for a single row result
    program.emit_insn(Insn::ResultRow {
        start_reg: output_reg,
        count: result_columns.len(),
    });

    Ok(())
}

fn sort_order_by(
    program: &mut ProgramBuilder,
    order_by: &Vec<(ast::Expr, Direction)>,
    result_columns: &Vec<ResultSetColumn>,
    limit: Option<usize>,
    m: &mut Metadata,
) -> Result<()> {
    // TODO: DOESNT WORK YET
    program.resolve_label(m.termination_label_stack.pop().unwrap(), program.offset());
    let mut pseudo_columns = vec![];
    for (i, _) in order_by.iter().enumerate() {
        pseudo_columns.push(Column {
            name: format!("sort_key_{}", i),
            primary_key: false,
            ty: crate::schema::Type::Null,
        });
    }
    for (i, expr) in result_columns.iter().enumerate() {
        if let Some(ref v) = m.result_columns_to_skip_in_orderby_sorter {
            if v.contains(&i) {
                continue;
            }
        }
        pseudo_columns.push(Column {
            name: match expr {
                ResultSetColumn::Expr { expr, .. } => expr.to_string(),
                ResultSetColumn::Agg(agg) => agg.to_string(),
            },
            primary_key: false,
            ty: crate::schema::Type::Null,
        });
    }

    let num_columns_in_sorter = order_by.len() + result_columns.len()
        - m.result_columns_to_skip_in_orderby_sorter
            .as_ref()
            .map(|v| v.len())
            .unwrap_or(0);

    let pseudo_cursor = program.alloc_cursor_id(
        None,
        Some(Table::Pseudo(Rc::new(PseudoTable {
            columns: pseudo_columns,
        }))),
    );
    let sort_metadata = m.sort_metadata.as_mut().unwrap();

    program.emit_insn(Insn::OpenPseudo {
        cursor_id: pseudo_cursor,
        content_reg: sort_metadata.sorter_data_register,
        num_fields: num_columns_in_sorter,
    });

    program.emit_insn_with_label_dependency(
        Insn::SorterSort {
            cursor_id: sort_metadata.sort_cursor,
            pc_if_empty: sort_metadata.done_label,
        },
        sort_metadata.done_label,
    );

    program.defer_label_resolution(sort_metadata.sorter_data_label, program.offset() as usize);
    program.emit_insn(Insn::SorterData {
        cursor_id: sort_metadata.sort_cursor,
        dest_reg: sort_metadata.sorter_data_register,
        pseudo_cursor,
    });

    let sort_metadata = m.sort_metadata.as_mut().unwrap();

    // EMIT COLUMNS FROM SORTER AND EMIT ROW
    let cursor_id = pseudo_cursor;
    let start_reg = program.alloc_registers(result_columns.len());
    for i in 0..result_columns.len() {
        let reg = start_reg + i;
        program.emit_insn(Insn::Column {
            cursor_id,
            column: m.result_column_indexes_in_orderby_sorter[&i],
            dest: reg,
        });
    }
    program.emit_insn(Insn::ResultRow {
        start_reg,
        count: result_columns.len(),
    });

    if let Some(limit) = limit {
        let limit_reg = program.alloc_register();
        program.emit_insn(Insn::Integer {
            value: limit as i64,
            dest: limit_reg,
        });
        program.mark_last_insn_constant();
        program.emit_insn_with_label_dependency(
            Insn::DecrJumpZero {
                reg: limit_reg,
                target_pc: sort_metadata.done_label,
            },
            sort_metadata.done_label,
        );
    }

    program.emit_insn_with_label_dependency(
        Insn::SorterNext {
            cursor_id: sort_metadata.sort_cursor,
            pc_if_next: sort_metadata.sorter_data_label,
        },
        sort_metadata.sorter_data_label,
    );

    program.resolve_label(sort_metadata.done_label, program.offset());

    Ok(())
}
