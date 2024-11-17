use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::{Rc, Weak};
use std::usize;

use sqlite3_parser::ast;

use crate::schema::{Column, PseudoTable, Table};
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::translate::expr::resolve_ident_pseudo_table;
use crate::translate::plan::Search;
use crate::types::{OwnedRecord, OwnedValue};
use crate::vdbe::builder::ProgramBuilder;
use crate::vdbe::{BranchOffset, Insn, Program};
use crate::{Connection, Result};

use super::expr::{
    translate_aggregation, translate_condition_expr, translate_expr, translate_table_columns,
    ConditionMetadata,
};
use super::optimizer::ExpressionResultCache;
use super::plan::{BTreeTableReference, Plan};
use super::plan::{Operator, ProjectionColumn};

/**
 * The Emitter trait is used to emit bytecode instructions for a given operator in the query plan.
 *
 * - step: perform a single step of the operator, emitting bytecode instructions as needed,
     and returning a result indicating whether the operator is ready to emit a result row
*/
pub trait Emitter {
    fn step(
        &mut self,
        pb: &mut ProgramBuilder,
        m: &mut Metadata,
        referenced_tables: &[BTreeTableReference],
    ) -> Result<OpStepResult>;
    fn result_columns(
        &self,
        program: &mut ProgramBuilder,
        referenced_tables: &[BTreeTableReference],
        metadata: &mut Metadata,
        cursor_override: Option<&SortCursorOverride>,
    ) -> Result<usize>;
    fn result_row(
        &mut self,
        program: &mut ProgramBuilder,
        referenced_tables: &[BTreeTableReference],
        metadata: &mut Metadata,
        cursor_override: Option<&SortCursorOverride>,
    ) -> Result<()>;
}

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
    // cursor id for the Pseudo table where rows are temporarily inserted from the Sorter table
    pub pseudo_table_cursor: usize,
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

#[derive(Debug)]
pub struct SortCursorOverride {
    pub cursor_id: usize,
    pub pseudo_table: Table,
    pub sort_key_len: usize,
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
    // labels for the Rewind instructions.
    scan_loop_body_labels: Vec<BranchOffset>,
    // mapping between Aggregation operator id and the register that holds the start of the aggregation result
    aggregation_start_registers: HashMap<usize, usize>,
    // mapping between Aggregation operator id and associated metadata (if the aggregation has a group by clause)
    group_bys: HashMap<usize, GroupByMetadata>,
    // mapping between Order operator id and associated metadata
    sorts: HashMap<usize, SortMetadata>,
    // mapping between Join operator id and associated metadata (for left joins only)
    left_joins: HashMap<usize, LeftJoinMetadata>,
    expr_result_cache: ExpressionResultCache,
}

/// Emitters return one of three possible results from the step() method:
/// - Continue: the operator is not yet ready to emit a result row
/// - ReadyToEmit: the operator is ready to emit a result row
/// - Done: the operator has completed execution
///   For example, a Scan operator will return Continue until it has opened a cursor, rewound it and applied any predicates.
///   At that point, it will return ReadyToEmit.
///   Finally, when the Scan operator has emitted a Next instruction, it will return Done.
///
/// Parent operators are free to make decisions based on the result a child operator's step() method.
///
/// When the root operator of a Plan returns ReadyToEmit, a ResultRow will always be emitted.
/// When the root operator returns Done, the bytecode plan is complete.
#[derive(Debug, PartialEq)]
pub enum OpStepResult {
    Continue,
    ReadyToEmit,
    Done,
}

impl Emitter for Operator {
    fn step(
        &mut self,
        program: &mut ProgramBuilder,
        m: &mut Metadata,
        referenced_tables: &[BTreeTableReference],
    ) -> Result<OpStepResult> {
        let current_operator_column_count = self.column_count(referenced_tables);
        match self {
            Operator::Scan {
                table_reference,
                id,
                step,
                predicates,
                ..
            } => {
                *step += 1;
                const SCAN_OPEN_READ: usize = 1;
                const SCAN_BODY: usize = 2;
                const SCAN_NEXT: usize = 3;
                match *step {
                    SCAN_OPEN_READ => {
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

                        Ok(OpStepResult::Continue)
                    }
                    SCAN_BODY => {
                        let cursor_id =
                            program.resolve_cursor_id(&table_reference.table_identifier, None);
                        program.emit_insn(Insn::RewindAsync { cursor_id });
                        let scan_loop_body_label = program.allocate_label();
                        let halt_label = m.termination_label_stack.last().unwrap();
                        program.emit_insn_with_label_dependency(
                            Insn::RewindAwait {
                                cursor_id,
                                pc_if_empty: *halt_label,
                            },
                            *halt_label,
                        );
                        m.scan_loop_body_labels.push(scan_loop_body_label);
                        program.defer_label_resolution(
                            scan_loop_body_label,
                            program.offset() as usize,
                        );

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
                                )?;
                                program.resolve_label(jump_target_when_true, program.offset());
                            }
                        }

                        Ok(OpStepResult::ReadyToEmit)
                    }
                    SCAN_NEXT => {
                        let cursor_id =
                            program.resolve_cursor_id(&table_reference.table_identifier, None);
                        program
                            .resolve_label(*m.next_row_labels.get(id).unwrap(), program.offset());
                        program.emit_insn(Insn::NextAsync { cursor_id });
                        let jump_label = m.scan_loop_body_labels.pop().unwrap();
                        program.emit_insn_with_label_dependency(
                            Insn::NextAwait {
                                cursor_id,
                                pc_if_next: jump_label,
                            },
                            jump_label,
                        );
                        Ok(OpStepResult::Done)
                    }
                    _ => Ok(OpStepResult::Done),
                }
            }
            Operator::Search {
                table_reference,
                search,
                predicates,
                step,
                id,
                ..
            } => {
                *step += 1;
                const SEARCH_OPEN_READ: usize = 1;
                const SEARCH_BODY: usize = 2;
                const SEARCH_NEXT: usize = 3;
                match *step {
                    SEARCH_OPEN_READ => {
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
                            let index_cursor_id = program.alloc_cursor_id(
                                Some(index.name.clone()),
                                Some(Table::Index(index.clone())),
                            );
                            program.emit_insn(Insn::OpenReadAsync {
                                cursor_id: index_cursor_id,
                                root_page: index.root_page,
                            });
                            program.emit_insn(Insn::OpenReadAwait);
                        }
                        Ok(OpStepResult::Continue)
                    }
                    SEARCH_BODY => {
                        let table_cursor_id =
                            program.resolve_cursor_id(&table_reference.table_identifier, None);

                        // Open the loop for the index search.
                        // Primary key equality search is handled with a SeekRowid instruction which does not loop, since it is a single row lookup.
                        if !matches!(search, Search::PrimaryKeyEq { .. }) {
                            let index_cursor_id = if let Search::IndexSearch { index, .. } = search
                            {
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
                                    ast::Operator::Equals | ast::Operator::GreaterEquals => {
                                        Insn::SeekGE {
                                            is_index: index_cursor_id.is_some(),
                                            cursor_id: index_cursor_id.unwrap_or(table_cursor_id),
                                            start_reg: cmp_reg,
                                            num_regs: 1,
                                            target_pc: *m.termination_label_stack.last().unwrap(),
                                        }
                                    }
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
                            if *cmp_op == ast::Operator::Less
                                || *cmp_op == ast::Operator::LessEquals
                            {
                                translate_expr(
                                    program,
                                    Some(referenced_tables),
                                    cmp_expr,
                                    cmp_reg,
                                    None,
                                    None,
                                )?;
                            }

                            program.defer_label_resolution(
                                scan_loop_body_label,
                                program.offset() as usize,
                            );
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
                                )?;
                                program.resolve_label(jump_target_when_true, program.offset());
                            }
                        }

                        Ok(OpStepResult::ReadyToEmit)
                    }
                    SEARCH_NEXT => {
                        if matches!(search, Search::PrimaryKeyEq { .. }) {
                            // Primary key equality search is handled with a SeekRowid instruction which does not loop, so there is no need to emit a NextAsync instruction.
                            return Ok(OpStepResult::Done);
                        }
                        let cursor_id = match search {
                            Search::IndexSearch { index, .. } => {
                                program.resolve_cursor_id(&index.name, None)
                            }
                            Search::PrimaryKeySearch { .. } => {
                                program.resolve_cursor_id(&table_reference.table_identifier, None)
                            }
                            Search::PrimaryKeyEq { .. } => unreachable!(),
                        };
                        program
                            .resolve_label(*m.next_row_labels.get(id).unwrap(), program.offset());
                        program.emit_insn(Insn::NextAsync { cursor_id });
                        let jump_label = m.scan_loop_body_labels.pop().unwrap();
                        program.emit_insn_with_label_dependency(
                            Insn::NextAwait {
                                cursor_id,
                                pc_if_next: jump_label,
                            },
                            jump_label,
                        );
                        Ok(OpStepResult::Done)
                    }
                    _ => Ok(OpStepResult::Done),
                }
            }
            Operator::Join {
                left,
                right,
                outer,
                predicates,
                step,
                id,
                ..
            } => {
                *step += 1;
                const JOIN_INIT: usize = 1;
                const JOIN_DO_JOIN: usize = 2;
                const JOIN_END: usize = 3;
                match *step {
                    JOIN_INIT => {
                        if *outer {
                            let lj_metadata = LeftJoinMetadata {
                                match_flag_register: program.alloc_register(),
                                set_match_flag_true_label: program.allocate_label(),
                                check_match_flag_label: program.allocate_label(),
                                on_match_jump_to_label: program.allocate_label(),
                            };
                            m.left_joins.insert(*id, lj_metadata);
                        }
                        left.step(program, m, referenced_tables)?;
                        right.step(program, m, referenced_tables)?;

                        Ok(OpStepResult::Continue)
                    }
                    JOIN_DO_JOIN => {
                        left.step(program, m, referenced_tables)?;

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

                        right.step(program, m, referenced_tables)?;

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

                        Ok(OpStepResult::ReadyToEmit)
                    }
                    JOIN_END => {
                        right.step(program, m, referenced_tables)?;

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
                                Operator::Scan {
                                    table_reference, ..
                                } => program
                                    .resolve_cursor_id(&table_reference.table_identifier, None),
                                Operator::Search {
                                    table_reference, ..
                                } => program
                                    .resolve_cursor_id(&table_reference.table_identifier, None),
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
                        left.step(program, m, referenced_tables)?;

                        Ok(OpStepResult::Done)
                    }
                    _ => Ok(OpStepResult::Done),
                }
            }
            Operator::Aggregate {
                id,
                source,
                aggregates,
                group_by,
                step,
                ..
            } => {
                *step += 1;

                // Group by aggregation eg. SELECT a, b, sum(c) FROM t GROUP BY a, b
                if let Some(group_by) = group_by {
                    const GROUP_BY_INIT: usize = 1;
                    const GROUP_BY_INSERT_INTO_SORTER: usize = 2;
                    const GROUP_BY_SORT_AND_COMPARE: usize = 3;
                    const GROUP_BY_PREPARE_ROW: usize = 4;
                    const GROUP_BY_CLEAR_ACCUMULATOR_SUBROUTINE: usize = 5;
                    match *step {
                        GROUP_BY_INIT => {
                            let agg_final_label = program.allocate_label();
                            m.termination_label_stack.push(agg_final_label);
                            let num_aggs = aggregates.len();

                            let sort_cursor = program.alloc_cursor_id(None, None);

                            let abort_flag_register = program.alloc_register();
                            let data_in_accumulator_indicator_register = program.alloc_register();
                            let group_exprs_comparison_register =
                                program.alloc_registers(group_by.len());
                            let group_exprs_accumulator_register =
                                program.alloc_registers(group_by.len());
                            let agg_exprs_start_reg = program.alloc_registers(num_aggs);
                            m.aggregation_start_registers
                                .insert(*id, agg_exprs_start_reg);
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
                                columns: current_operator_column_count,
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

                            program.add_comment(
                                program.offset(),
                                "go to clear accumulator subroutine",
                            );

                            let subroutine_accumulator_clear_return_offset_register =
                                program.alloc_register();
                            program.emit_insn_with_label_dependency(
                                Insn::Gosub {
                                    target_pc: subroutine_accumulator_clear_label,
                                    return_reg: subroutine_accumulator_clear_return_offset_register,
                                },
                                subroutine_accumulator_clear_label,
                            );

                            m.group_bys.insert(
                                *id,
                                GroupByMetadata {
                                    sort_cursor,
                                    subroutine_accumulator_clear_label,
                                    subroutine_accumulator_clear_return_offset_register,
                                    subroutine_accumulator_output_label,
                                    subroutine_accumulator_output_return_offset_register: program
                                        .alloc_register(),
                                    accumulator_indicator_set_true_label: program.allocate_label(),
                                    sorter_data_label,
                                    grouping_done_label,
                                    abort_flag_register,
                                    data_in_accumulator_indicator_register,
                                    group_exprs_accumulator_register,
                                    group_exprs_comparison_register,
                                    sorter_key_register,
                                },
                            );

                            loop {
                                match source.step(program, m, referenced_tables)? {
                                    OpStepResult::Continue => continue,
                                    OpStepResult::ReadyToEmit => {
                                        return Ok(OpStepResult::Continue);
                                    }
                                    OpStepResult::Done => {
                                        return Ok(OpStepResult::Done);
                                    }
                                }
                            }
                        }
                        GROUP_BY_INSERT_INTO_SORTER => {
                            let sort_keys_count = group_by.len();
                            let start_reg = program.alloc_registers(current_operator_column_count);
                            for (i, expr) in group_by.iter().enumerate() {
                                let key_reg = start_reg + i;
                                translate_expr(
                                    program,
                                    Some(referenced_tables),
                                    expr,
                                    key_reg,
                                    None,
                                    None,
                                )?;
                            }
                            for (i, agg) in aggregates.iter().enumerate() {
                                // TODO it's a hack to assume aggregate functions have exactly one argument.
                                // Counterpoint e.g. GROUP_CONCAT(expr, separator).
                                //
                                // Here we are collecting scalars for the group by sorter, which will include
                                // both the group by expressions and the aggregate arguments.
                                // e.g. in `select u.first_name, sum(u.age) from users group by u.first_name`
                                // the sorter will have two scalars: u.first_name and u.age.
                                // these are then sorted by u.first_name, and for each u.first_name, we sum the u.age.
                                // the actual aggregation is done later in GROUP_BY_SORT_AND_COMPARE below.
                                //
                                // This is why we take the first argument of each aggregate function currently.
                                // It's mostly an artifact of the current architecture being a bit poor; we should recognize
                                // which scalars are dependencies of aggregate functions and explicitly collect those.
                                let expr = &agg.args[0];
                                let agg_reg = start_reg + sort_keys_count + i;
                                translate_expr(
                                    program,
                                    Some(referenced_tables),
                                    expr,
                                    agg_reg,
                                    None,
                                    None,
                                )?;
                            }

                            let group_by_metadata = m.group_bys.get(id).unwrap();

                            program.emit_insn(Insn::MakeRecord {
                                start_reg,
                                count: current_operator_column_count,
                                dest_reg: group_by_metadata.sorter_key_register,
                            });

                            let group_by_metadata = m.group_bys.get(id).unwrap();
                            program.emit_insn(Insn::SorterInsert {
                                cursor_id: group_by_metadata.sort_cursor,
                                record_reg: group_by_metadata.sorter_key_register,
                            });

                            return Ok(OpStepResult::Continue);
                        }
                        #[allow(clippy::never_loop)]
                        GROUP_BY_SORT_AND_COMPARE => {
                            loop {
                                match source.step(program, m, referenced_tables)? {
                                    OpStepResult::Done => {
                                        break;
                                    }
                                    _ => unreachable!(),
                                }
                            }

                            let group_by_metadata = m.group_bys.get_mut(id).unwrap();

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

                            let mut column_names =
                                Vec::with_capacity(current_operator_column_count);
                            for expr in group_by
                                .iter()
                                .chain(aggregates.iter().map(|agg| &agg.args[0]))
                            // FIXME: just blindly taking the first arg is a hack
                            {
                                // Sorter column names for group by are now just determined by stringifying the expression, since the group by
                                // columns and aggregations can be practically anything.
                                // FIXME: either come up with something more robust, or make this something like expr.to_canonical_string() so that we can handle
                                // things like `count(1)` and `COUNT(1)` the same way
                                column_names.push(expr.to_string());
                            }
                            let pseudo_columns = column_names
                                .iter()
                                .map(|name| Column {
                                    name: name.clone(),
                                    primary_key: false,
                                    ty: crate::schema::Type::Null,
                                })
                                .collect::<Vec<_>>();

                            let pseudo_table = Rc::new(PseudoTable {
                                columns: pseudo_columns,
                            });

                            let pseudo_cursor = program
                                .alloc_cursor_id(None, Some(Table::Pseudo(pseudo_table.clone())));

                            program.emit_insn(Insn::OpenPseudo {
                                cursor_id: pseudo_cursor,
                                content_reg: sorter_key_register,
                                num_fields: current_operator_column_count,
                            });

                            let group_by_metadata = m.group_bys.get(id).unwrap();
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
                            program.emit_insn(Insn::SorterData {
                                cursor_id: group_by_metadata.sort_cursor,
                                dest_reg: group_by_metadata.sorter_key_register,
                                pseudo_cursor,
                            });

                            let groups_start_reg = program.alloc_registers(group_by.len());
                            for (i, expr) in group_by.iter().enumerate() {
                                let sorter_column_index =
                                    resolve_ident_pseudo_table(&expr.to_string(), &pseudo_table)?;
                                let group_reg = groups_start_reg + i;
                                program.emit_insn(Insn::Column {
                                    cursor_id: pseudo_cursor,
                                    column: sorter_column_index,
                                    dest: group_reg,
                                });
                            }

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
                            program.emit_insn_with_label_dependency(
                                Insn::Jump {
                                    target_pc_lt: program.offset() + 1,
                                    target_pc_eq: agg_step_label,
                                    target_pc_gt: program.offset() + 1,
                                },
                                agg_step_label,
                            );

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
                                    return_reg:
                                        subroutine_accumulator_output_return_offset_register,
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

                            program
                                .add_comment(program.offset(), "goto clear accumulator subroutine");
                            program.emit_insn_with_label_dependency(
                                Insn::Gosub {
                                    target_pc: subroutine_accumulator_clear_label,
                                    return_reg: subroutine_accumulator_clear_return_offset_register,
                                },
                                subroutine_accumulator_clear_label,
                            );

                            program.resolve_label(agg_step_label, program.offset());
                            let start_reg = m.aggregation_start_registers.get(id).unwrap();
                            for (i, agg) in aggregates.iter().enumerate() {
                                let agg_result_reg = start_reg + i;
                                translate_aggregation(
                                    program,
                                    referenced_tables,
                                    agg,
                                    agg_result_reg,
                                    Some(pseudo_cursor),
                                )?;
                            }

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

                            for (i, expr) in group_by.iter().enumerate() {
                                let key_reg = group_exprs_start_register + i;
                                let sorter_column_index =
                                    resolve_ident_pseudo_table(&expr.to_string(), &pseudo_table)?;
                                program.emit_insn(Insn::Column {
                                    cursor_id: pseudo_cursor,
                                    column: sorter_column_index,
                                    dest: key_reg,
                                });
                            }

                            program.resolve_label(
                                accumulator_indicator_set_true_label,
                                program.offset(),
                            );
                            program.add_comment(program.offset(), "indicate data in accumulator");
                            program.emit_insn(Insn::Integer {
                                value: 1,
                                dest: data_in_accumulator_indicator_register,
                            });

                            return Ok(OpStepResult::Continue);
                        }
                        GROUP_BY_PREPARE_ROW => {
                            let group_by_metadata = m.group_bys.get(id).unwrap();
                            program.emit_insn_with_label_dependency(
                                Insn::SorterNext {
                                    cursor_id: group_by_metadata.sort_cursor,
                                    pc_if_next: group_by_metadata.sorter_data_label,
                                },
                                group_by_metadata.sorter_data_label,
                            );

                            program.resolve_label(
                                group_by_metadata.grouping_done_label,
                                program.offset(),
                            );

                            program.add_comment(program.offset(), "emit row for final group");
                            program.emit_insn_with_label_dependency(
                                Insn::Gosub {
                                    target_pc: group_by_metadata
                                        .subroutine_accumulator_output_label,
                                    return_reg: group_by_metadata
                                        .subroutine_accumulator_output_return_offset_register,
                                },
                                group_by_metadata.subroutine_accumulator_output_label,
                            );

                            program.add_comment(program.offset(), "group by finished");
                            let termination_label =
                                m.termination_label_stack[m.termination_label_stack.len() - 2];
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
                                return_reg: group_by_metadata
                                    .subroutine_accumulator_output_return_offset_register,
                            });

                            program.resolve_label(
                                group_by_metadata.subroutine_accumulator_output_label,
                                program.offset(),
                            );

                            program.add_comment(
                                program.offset(),
                                "output group by row subroutine start",
                            );
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
                                return_reg: group_by_metadata
                                    .subroutine_accumulator_output_return_offset_register,
                            });

                            return Ok(OpStepResult::ReadyToEmit);
                        }
                        GROUP_BY_CLEAR_ACCUMULATOR_SUBROUTINE => {
                            let group_by_metadata = m.group_bys.get(id).unwrap();
                            program.emit_insn(Insn::Return {
                                return_reg: group_by_metadata
                                    .subroutine_accumulator_output_return_offset_register,
                            });

                            program.add_comment(
                                program.offset(),
                                "clear accumulator subroutine start",
                            );
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
                                return_reg: group_by_metadata
                                    .subroutine_accumulator_clear_return_offset_register,
                            });
                        }
                        _ => {
                            return Ok(OpStepResult::Done);
                        }
                    }
                }

                // Non-grouped aggregation e.g. SELECT COUNT(*) FROM t

                const AGGREGATE_INIT: usize = 1;
                const AGGREGATE_WAIT_UNTIL_SOURCE_READY: usize = 2;
                match *step {
                    AGGREGATE_INIT => {
                        let agg_final_label = program.allocate_label();
                        m.termination_label_stack.push(agg_final_label);
                        let num_aggs = aggregates.len();
                        let start_reg = program.alloc_registers(num_aggs);
                        m.aggregation_start_registers.insert(*id, start_reg);

                        Ok(OpStepResult::Continue)
                    }
                    AGGREGATE_WAIT_UNTIL_SOURCE_READY => loop {
                        match source.step(program, m, referenced_tables)? {
                            OpStepResult::Continue => {}
                            OpStepResult::ReadyToEmit => {
                                let start_reg = m.aggregation_start_registers.get(id).unwrap();
                                for (i, agg) in aggregates.iter().enumerate() {
                                    let agg_result_reg = start_reg + i;
                                    translate_aggregation(
                                        program,
                                        referenced_tables,
                                        agg,
                                        agg_result_reg,
                                        None,
                                    )?;
                                }
                            }
                            OpStepResult::Done => {
                                return Ok(OpStepResult::ReadyToEmit);
                            }
                        }
                    },
                    _ => Ok(OpStepResult::Done),
                }
            }
            Operator::Filter { .. } => unreachable!("predicates have been pushed down"),
            Operator::Limit { source, step, .. } => {
                *step += 1;
                loop {
                    match source.step(program, m, referenced_tables)? {
                        OpStepResult::Continue => continue,
                        OpStepResult::ReadyToEmit => {
                            return Ok(OpStepResult::ReadyToEmit);
                        }
                        OpStepResult::Done => return Ok(OpStepResult::Done),
                    }
                }
            }
            Operator::Order {
                id,
                source,
                key,
                step,
            } => {
                *step += 1;
                const ORDER_INIT: usize = 1;
                const ORDER_INSERT_INTO_SORTER: usize = 2;
                const ORDER_SORT_AND_OPEN_LOOP: usize = 3;
                const ORDER_NEXT: usize = 4;
                match *step {
                    ORDER_INIT => {
                        m.termination_label_stack.push(program.allocate_label());
                        let sort_cursor = program.alloc_cursor_id(None, None);
                        m.sorts.insert(
                            *id,
                            SortMetadata {
                                sort_cursor,
                                pseudo_table_cursor: usize::MAX, // will be set later
                                sorter_data_register: program.alloc_register(),
                                sorter_data_label: program.allocate_label(),
                                done_label: program.allocate_label(),
                            },
                        );
                        let mut order = Vec::new();
                        for (_, direction) in key.iter() {
                            order.push(OwnedValue::Integer(*direction as i64));
                        }
                        program.emit_insn(Insn::SorterOpen {
                            cursor_id: sort_cursor,
                            columns: key.len(),
                            order: OwnedRecord::new(order),
                        });

                        loop {
                            match source.step(program, m, referenced_tables)? {
                                OpStepResult::Continue => continue,
                                OpStepResult::ReadyToEmit => {
                                    return Ok(OpStepResult::Continue);
                                }
                                OpStepResult::Done => {
                                    return Ok(OpStepResult::Done);
                                }
                            }
                        }
                    }
                    ORDER_INSERT_INTO_SORTER => {
                        let sort_keys_count = key.len();
                        let source_cols_count = source.column_count(referenced_tables);
                        let start_reg = program.alloc_registers(sort_keys_count);
                        source.result_columns(program, referenced_tables, m, None)?;

                        for (i, (expr, _)) in key.iter().enumerate() {
                            let key_reg = start_reg + i;
                            translate_expr(
                                program,
                                Some(referenced_tables),
                                expr,
                                key_reg,
                                None,
                                m.expr_result_cache
                                    .get_cached_result_registers(*id, i)
                                    .as_ref(),
                            )?;
                        }

                        let sort_metadata = m.sorts.get_mut(id).unwrap();
                        program.emit_insn(Insn::MakeRecord {
                            start_reg,
                            count: sort_keys_count + source_cols_count,
                            dest_reg: sort_metadata.sorter_data_register,
                        });

                        program.emit_insn(Insn::SorterInsert {
                            cursor_id: sort_metadata.sort_cursor,
                            record_reg: sort_metadata.sorter_data_register,
                        });

                        Ok(OpStepResult::Continue)
                    }
                    #[allow(clippy::never_loop)]
                    ORDER_SORT_AND_OPEN_LOOP => {
                        loop {
                            match source.step(program, m, referenced_tables)? {
                                OpStepResult::Done => {
                                    break;
                                }
                                _ => unreachable!(),
                            }
                        }
                        program.resolve_label(
                            m.termination_label_stack.pop().unwrap(),
                            program.offset(),
                        );
                        let column_names = source.column_names();
                        let mut pseudo_columns = vec![];
                        for (i, _) in key.iter().enumerate() {
                            pseudo_columns.push(Column {
                                name: format!("sort_key_{}", i),
                                primary_key: false,
                                ty: crate::schema::Type::Null,
                            });
                        }
                        for name in column_names {
                            pseudo_columns.push(Column {
                                name: name.clone(),
                                primary_key: false,
                                ty: crate::schema::Type::Null,
                            });
                        }

                        let num_fields = pseudo_columns.len();

                        let pseudo_cursor = program.alloc_cursor_id(
                            None,
                            Some(Table::Pseudo(Rc::new(PseudoTable {
                                columns: pseudo_columns,
                            }))),
                        );
                        let sort_metadata = m.sorts.get(id).unwrap();

                        program.emit_insn(Insn::OpenPseudo {
                            cursor_id: pseudo_cursor,
                            content_reg: sort_metadata.sorter_data_register,
                            num_fields,
                        });

                        program.emit_insn_with_label_dependency(
                            Insn::SorterSort {
                                cursor_id: sort_metadata.sort_cursor,
                                pc_if_empty: sort_metadata.done_label,
                            },
                            sort_metadata.done_label,
                        );

                        program.defer_label_resolution(
                            sort_metadata.sorter_data_label,
                            program.offset() as usize,
                        );
                        program.emit_insn(Insn::SorterData {
                            cursor_id: sort_metadata.sort_cursor,
                            dest_reg: sort_metadata.sorter_data_register,
                            pseudo_cursor,
                        });

                        let sort_metadata = m.sorts.get_mut(id).unwrap();

                        sort_metadata.pseudo_table_cursor = pseudo_cursor;

                        Ok(OpStepResult::ReadyToEmit)
                    }
                    ORDER_NEXT => {
                        let sort_metadata = m.sorts.get(id).unwrap();
                        program.emit_insn_with_label_dependency(
                            Insn::SorterNext {
                                cursor_id: sort_metadata.sort_cursor,
                                pc_if_next: sort_metadata.sorter_data_label,
                            },
                            sort_metadata.sorter_data_label,
                        );

                        program.resolve_label(sort_metadata.done_label, program.offset());

                        Ok(OpStepResult::Done)
                    }
                    _ => unreachable!(),
                }
            }
            Operator::Projection { source, step, .. } => {
                *step += 1;
                const PROJECTION_WAIT_UNTIL_SOURCE_READY: usize = 1;
                const PROJECTION_FINALIZE_SOURCE: usize = 2;
                match *step {
                    PROJECTION_WAIT_UNTIL_SOURCE_READY => loop {
                        match source.step(program, m, referenced_tables)? {
                            OpStepResult::Continue => continue,
                            OpStepResult::ReadyToEmit | OpStepResult::Done => {
                                if matches!(**source, Operator::Aggregate { .. }) {
                                    source.result_columns(program, referenced_tables, m, None)?;
                                }
                                return Ok(OpStepResult::ReadyToEmit);
                            }
                        }
                    },
                    PROJECTION_FINALIZE_SOURCE => {
                        match source.step(program, m, referenced_tables)? {
                            OpStepResult::Done => Ok(OpStepResult::Done),
                            _ => unreachable!(),
                        }
                    }
                    _ => Ok(OpStepResult::Done),
                }
            }
            Operator::Nothing => Ok(OpStepResult::Done),
        }
    }
    fn result_columns(
        &self,
        program: &mut ProgramBuilder,
        referenced_tables: &[BTreeTableReference],
        m: &mut Metadata,
        cursor_override: Option<&SortCursorOverride>,
    ) -> Result<usize> {
        let col_count = self.column_count(referenced_tables);
        match self {
            Operator::Scan {
                table_reference, ..
            } => {
                let start_reg = program.alloc_registers(col_count);
                let table = cursor_override
                    .map(|c| c.pseudo_table.clone())
                    .unwrap_or_else(|| Table::BTree(table_reference.table.clone()));
                let cursor_id = cursor_override.map(|c| c.cursor_id).unwrap_or_else(|| {
                    program.resolve_cursor_id(&table_reference.table_identifier, None)
                });
                let start_column_offset = cursor_override.map(|c| c.sort_key_len).unwrap_or(0);
                translate_table_columns(program, cursor_id, &table, start_column_offset, start_reg);

                Ok(start_reg)
            }
            Operator::Search {
                table_reference, ..
            } => {
                let start_reg = program.alloc_registers(col_count);
                let table = cursor_override
                    .map(|c| c.pseudo_table.clone())
                    .unwrap_or_else(|| Table::BTree(table_reference.table.clone()));
                let cursor_id = cursor_override.map(|c| c.cursor_id).unwrap_or_else(|| {
                    program.resolve_cursor_id(&table_reference.table_identifier, None)
                });
                let start_column_offset = cursor_override.map(|c| c.sort_key_len).unwrap_or(0);
                translate_table_columns(program, cursor_id, &table, start_column_offset, start_reg);

                Ok(start_reg)
            }
            Operator::Join { left, right, .. } => {
                let left_start_reg =
                    left.result_columns(program, referenced_tables, m, cursor_override)?;
                right.result_columns(program, referenced_tables, m, cursor_override)?;

                Ok(left_start_reg)
            }
            Operator::Aggregate {
                id,
                aggregates,
                group_by,
                ..
            } => {
                let agg_start_reg = m.aggregation_start_registers.get(id).unwrap();
                program.resolve_label(m.termination_label_stack.pop().unwrap(), program.offset());
                let mut result_column_idx = 0;
                for (i, agg) in aggregates.iter().enumerate() {
                    let agg_result_reg = *agg_start_reg + i;
                    program.emit_insn(Insn::AggFinal {
                        register: agg_result_reg,
                        func: agg.func.clone(),
                    });
                    m.expr_result_cache.cache_result_register(
                        *id,
                        result_column_idx,
                        agg_result_reg,
                        agg.original_expr.clone(),
                    );
                    result_column_idx += 1;
                }

                if let Some(group_by) = group_by {
                    let output_row_start_reg =
                        program.alloc_registers(aggregates.len() + group_by.len());
                    let group_by_metadata = m.group_bys.get(id).unwrap();
                    program.emit_insn(Insn::Copy {
                        src_reg: group_by_metadata.group_exprs_accumulator_register,
                        dst_reg: output_row_start_reg,
                        amount: group_by.len() - 1,
                    });
                    for (i, source_expr) in group_by.iter().enumerate() {
                        m.expr_result_cache.cache_result_register(
                            *id,
                            result_column_idx + i,
                            output_row_start_reg + i,
                            source_expr.clone(),
                        );
                    }
                    program.emit_insn(Insn::Copy {
                        src_reg: *agg_start_reg,
                        dst_reg: output_row_start_reg + group_by.len(),
                        amount: aggregates.len() - 1,
                    });

                    Ok(output_row_start_reg)
                } else {
                    Ok(*agg_start_reg)
                }
            }
            Operator::Filter { .. } => unreachable!("predicates have been pushed down"),
            Operator::Limit { .. } => {
                unimplemented!()
            }
            Operator::Order { id, key, .. } => {
                let cursor_id = m.sorts.get(id).unwrap().pseudo_table_cursor;
                let pseudo_table = program.resolve_cursor_to_table(cursor_id).unwrap();
                let start_column_offset = key.len();
                let column_count = pseudo_table.columns().len() - start_column_offset;
                let start_reg = program.alloc_registers(column_count);
                translate_table_columns(
                    program,
                    cursor_id,
                    &pseudo_table,
                    start_column_offset,
                    start_reg,
                );

                Ok(start_reg)
            }
            Operator::Projection {
                expressions, id, ..
            } => {
                let expr_count = expressions
                    .iter()
                    .map(|e| e.column_count(referenced_tables))
                    .sum();
                let start_reg = program.alloc_registers(expr_count);
                let mut cur_reg = start_reg;
                for expr in expressions {
                    match expr {
                        ProjectionColumn::Column(expr) => {
                            translate_expr(
                                program,
                                Some(referenced_tables),
                                expr,
                                cur_reg,
                                cursor_override.map(|c| c.cursor_id),
                                m.expr_result_cache
                                    .get_cached_result_registers(*id, cur_reg - start_reg)
                                    .as_ref(),
                            )?;
                            m.expr_result_cache.cache_result_register(
                                *id,
                                cur_reg - start_reg,
                                cur_reg,
                                expr.clone(),
                            );
                            cur_reg += 1;
                        }
                        ProjectionColumn::Star => {
                            for table_reference in referenced_tables.iter() {
                                let table = cursor_override
                                    .map(|c| c.pseudo_table.clone())
                                    .unwrap_or_else(|| Table::BTree(table_reference.table.clone()));
                                let cursor_id =
                                    cursor_override.map(|c| c.cursor_id).unwrap_or_else(|| {
                                        program.resolve_cursor_id(
                                            &table_reference.table_identifier,
                                            None,
                                        )
                                    });
                                let start_column_offset =
                                    cursor_override.map(|c| c.sort_key_len).unwrap_or(0);
                                cur_reg = translate_table_columns(
                                    program,
                                    cursor_id,
                                    &table,
                                    start_column_offset,
                                    cur_reg,
                                );
                            }
                        }
                        ProjectionColumn::TableStar(table_reference) => {
                            let table_ref = referenced_tables
                                .iter()
                                .find(|t| t.table_identifier == table_reference.table_identifier)
                                .unwrap();

                            let table = cursor_override
                                .map(|c| c.pseudo_table.clone())
                                .unwrap_or_else(|| Table::BTree(table_ref.table.clone()));
                            let cursor_id =
                                cursor_override.map(|c| c.cursor_id).unwrap_or_else(|| {
                                    program
                                        .resolve_cursor_id(&table_reference.table_identifier, None)
                                });
                            let start_column_offset =
                                cursor_override.map(|c| c.sort_key_len).unwrap_or(0);
                            cur_reg = translate_table_columns(
                                program,
                                cursor_id,
                                &table,
                                start_column_offset,
                                cur_reg,
                            );
                        }
                    }
                }

                Ok(start_reg)
            }
            Operator::Nothing => unimplemented!(),
        }
    }
    fn result_row(
        &mut self,
        program: &mut ProgramBuilder,
        referenced_tables: &[BTreeTableReference],
        m: &mut Metadata,
        cursor_override: Option<&SortCursorOverride>,
    ) -> Result<()> {
        match self {
            Operator::Limit { source, limit, .. } => {
                source.result_row(program, referenced_tables, m, cursor_override)?;
                let limit_reg = program.alloc_register();
                program.emit_insn(Insn::Integer {
                    value: *limit as i64,
                    dest: limit_reg,
                });
                program.mark_last_insn_constant();
                let jump_label = m.termination_label_stack.first().unwrap();
                program.emit_insn_with_label_dependency(
                    Insn::DecrJumpZero {
                        reg: limit_reg,
                        target_pc: *jump_label,
                    },
                    *jump_label,
                );

                Ok(())
            }
            operator => {
                let start_reg =
                    operator.result_columns(program, referenced_tables, m, cursor_override)?;
                program.emit_insn(Insn::ResultRow {
                    start_reg,
                    count: operator.column_count(referenced_tables),
                });
                Ok(())
            }
        }
    }
}

fn prologue(
    cache: ExpressionResultCache,
) -> Result<(ProgramBuilder, Metadata, BranchOffset, BranchOffset)> {
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
        expr_result_cache: cache,
        aggregation_start_registers: HashMap::new(),
        group_bys: HashMap::new(),
        left_joins: HashMap::new(),
        next_row_labels: HashMap::new(),
        scan_loop_body_labels: vec![],
        sorts: HashMap::new(),
    };

    Ok((program, metadata, init_label, start_offset))
}

fn epilogue(
    program: &mut ProgramBuilder,
    metadata: &mut Metadata,
    init_label: BranchOffset,
    start_offset: BranchOffset,
) -> Result<()> {
    program.resolve_label(
        metadata.termination_label_stack.pop().unwrap(),
        program.offset(),
    );
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
    cache: ExpressionResultCache,
    connection: Weak<Connection>,
) -> Result<Program> {
    let (mut program, mut metadata, init_label, start_offset) = prologue(cache)?;
    loop {
        match plan
            .root_operator
            .step(&mut program, &mut metadata, &plan.referenced_tables)?
        {
            OpStepResult::Continue => {}
            OpStepResult::ReadyToEmit => {
                plan.root_operator.result_row(
                    &mut program,
                    &plan.referenced_tables,
                    &mut metadata,
                    None,
                )?;
            }
            OpStepResult::Done => {
                epilogue(&mut program, &mut metadata, init_label, start_offset)?;
                return Ok(program.build(database_header, connection));
            }
        }
    }
}
