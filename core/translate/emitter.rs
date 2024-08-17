use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::usize;

use crate::schema::{BTreeTable, Column, PseudoTable, Table};
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::types::{OwnedRecord, OwnedValue};
use crate::vdbe::builder::ProgramBuilder;
use crate::vdbe::{BranchOffset, Insn, Program};
use crate::Result;

use super::expr::maybe_apply_affinity;
use super::expr::{
    translate_aggregation, translate_condition_expr, translate_expr, ConditionMetadata,
};
use super::plan::Plan;
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
        referenced_tables: &[(Rc<BTreeTable>, String)],
    ) -> Result<OpStepResult>;
    fn result_columns(
        &self,
        program: &mut ProgramBuilder,
        referenced_tables: &[(Rc<BTreeTable>, String)],
        metadata: &mut Metadata,
        cursor_override: Option<usize>,
    ) -> Result<usize>;
    fn result_row(
        &mut self,
        program: &mut ProgramBuilder,
        referenced_tables: &[(Rc<BTreeTable>, String)],
        metadata: &mut Metadata,
        cursor_override: Option<usize>,
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
}

#[derive(Debug)]
pub struct Metadata {
    // labels for the instructions that terminate the execution when a conditional check evaluates to false. typically jumps to Halt, but can also jump to AggFinal if a parent in the tree is an aggregation
    termination_labels: Vec<BranchOffset>,
    // labels for the instructions that jump to the next row in the current operator.
    // for example, in a join with two nested scans, the inner loop will jump to its Next instruction when the join condition is false;
    // in a join with a scan and a seek, the seek will jump to the scan's Next instruction when the join condition is false.
    next_row_labels: HashMap<usize, BranchOffset>,
    // labels for the Rewind instructions.
    rewind_labels: Vec<BranchOffset>,
    // mapping between Aggregation operator id and the register that holds the start of the aggregation result
    aggregation_start_registers: HashMap<usize, usize>,
    // mapping between Order operator id and associated metadata
    sorts: HashMap<usize, SortMetadata>,
    // mapping between Join operator id and associated metadata (for left joins only)
    left_joins: HashMap<usize, LeftJoinMetadata>,
}

/**
*  Emitters return one of three possible results from the step() method:
*  - Continue: the operator is not yet ready to emit a result row
*  - ReadyToEmit: the operator is ready to emit a result row
*  - Done: the operator has completed execution
*  For example, a Scan operator will return Continue until it has opened a cursor, rewound it and applied any predicates.
*  At that point, it will return ReadyToEmit.
*  Finally, when the Scan operator has emitted a Next instruction, it will return Done.
*
*  Parent operators are free to make decisions based on the result a child operator's step() method.
*
*  When the root operator of a Plan returns ReadyToEmit, a ResultRow will always be emitted.
*  When the root operator returns Done, the bytecode plan is complete.
*

*/
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
        referenced_tables: &[(Rc<BTreeTable>, String)],
    ) -> Result<OpStepResult> {
        match self {
            Operator::Scan {
                table,
                table_identifier,
                id,
                step,
                predicates,
                ..
            } => {
                *step += 1;
                const SCAN_OPEN_READ: usize = 1;
                const SCAN_REWIND_AND_CONDITIONS: usize = 2;
                const SCAN_NEXT: usize = 3;
                match *step {
                    SCAN_OPEN_READ => {
                        let cursor_id = program.alloc_cursor_id(
                            Some(table_identifier.clone()),
                            Some(Table::BTree(table.clone())),
                        );
                        let root_page = table.root_page;
                        let next_row_label = program.allocate_label();
                        m.next_row_labels.insert(*id, next_row_label);
                        program.emit_insn(Insn::OpenReadAsync {
                            cursor_id,
                            root_page,
                        });
                        program.emit_insn(Insn::OpenReadAwait);

                        Ok(OpStepResult::Continue)
                    }
                    SCAN_REWIND_AND_CONDITIONS => {
                        let cursor_id = program.resolve_cursor_id(table_identifier, None);
                        program.emit_insn(Insn::RewindAsync { cursor_id });
                        let rewind_label = program.allocate_label();
                        let halt_label = m.termination_labels.last().unwrap();
                        m.rewind_labels.push(rewind_label);
                        program.defer_label_resolution(rewind_label, program.offset() as usize);
                        program.emit_insn_with_label_dependency(
                            Insn::RewindAwait {
                                cursor_id,
                                pc_if_empty: *halt_label,
                            },
                            *halt_label,
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
                        let cursor_id = program.resolve_cursor_id(table_identifier, None);
                        program
                            .resolve_label(*m.next_row_labels.get(id).unwrap(), program.offset());
                        program.emit_insn(Insn::NextAsync { cursor_id });
                        let jump_label = m.rewind_labels.pop().unwrap();
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
            Operator::SeekRowid {
                table,
                table_identifier,
                rowid_predicate,
                predicates,
                step,
                id,
                ..
            } => {
                *step += 1;
                const SEEKROWID_OPEN_READ: usize = 1;
                const SEEKROWID_SEEK_AND_CONDITIONS: usize = 2;
                match *step {
                    SEEKROWID_OPEN_READ => {
                        let cursor_id = program.alloc_cursor_id(
                            Some(table_identifier.clone()),
                            Some(Table::BTree(table.clone())),
                        );
                        let root_page = table.root_page;
                        program.emit_insn(Insn::OpenReadAsync {
                            cursor_id,
                            root_page,
                        });
                        program.emit_insn(Insn::OpenReadAwait);

                        Ok(OpStepResult::Continue)
                    }
                    SEEKROWID_SEEK_AND_CONDITIONS => {
                        let cursor_id = program.resolve_cursor_id(table_identifier, None);
                        let rowid_reg = program.alloc_register();
                        translate_expr(
                            program,
                            Some(referenced_tables),
                            rowid_predicate,
                            rowid_reg,
                            None,
                        )?;
                        let jump_label = m
                            .next_row_labels
                            .get(id)
                            .unwrap_or(&m.termination_labels.last().unwrap());
                        program.emit_insn_with_label_dependency(
                            Insn::SeekRowid {
                                cursor_id,
                                src_reg: rowid_reg,
                                target_pc: *jump_label,
                            },
                            *jump_label,
                        );
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
                            .unwrap_or(&m.termination_labels.last().unwrap());

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
                                    table_identifier, ..
                                } => program.resolve_cursor_id(table_identifier, None),
                                Operator::SeekRowid {
                                    table_identifier, ..
                                } => program.resolve_cursor_id(table_identifier, None),
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
                            // This points to the NextAsync instruction of the left table
                            program.resolve_label(lj_meta.on_match_jump_to_label, program.offset());
                        }
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
                step,
            } => {
                *step += 1;
                const AGGREGATE_INIT: usize = 1;
                const AGGREGATE_WAIT_UNTIL_SOURCE_READY: usize = 2;
                match *step {
                    AGGREGATE_INIT => {
                        let agg_final_label = program.allocate_label();
                        m.termination_labels.push(agg_final_label);
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
                        let sort_cursor = program.alloc_cursor_id(None, None);
                        m.sorts.insert(
                            *id,
                            SortMetadata {
                                sort_cursor,
                                pseudo_table_cursor: usize::MAX, // will be set later
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
                        for (i, (expr, _)) in key.iter().enumerate() {
                            let key_reg = start_reg + i;
                            translate_expr(program, Some(referenced_tables), expr, key_reg, None)?;
                        }
                        source.result_columns(program, referenced_tables, m, None)?;

                        let dest = program.alloc_register();
                        program.emit_insn(Insn::MakeRecord {
                            start_reg,
                            count: sort_keys_count + source_cols_count,
                            dest_reg: dest,
                        });

                        let sort_metadata = m.sorts.get_mut(id).unwrap();
                        program.emit_insn(Insn::SorterInsert {
                            cursor_id: sort_metadata.sort_cursor,
                            record_reg: dest,
                        });

                        Ok(OpStepResult::Continue)
                    }
                    ORDER_SORT_AND_OPEN_LOOP => {
                        loop {
                            match source.step(program, m, referenced_tables)? {
                                OpStepResult::Done => {
                                    break;
                                }
                                _ => unreachable!(),
                            }
                        }
                        let column_names = source.column_names();
                        let pseudo_columns = column_names
                            .iter()
                            .map(|name| Column {
                                name: name.clone(),
                                primary_key: false,
                                ty: crate::schema::Type::Null,
                            })
                            .collect::<Vec<_>>();

                        let pseudo_cursor = program.alloc_cursor_id(
                            None,
                            Some(Table::Pseudo(Rc::new(PseudoTable {
                                columns: pseudo_columns,
                            }))),
                        );

                        let pseudo_content_reg = program.alloc_register();
                        program.emit_insn(Insn::OpenPseudo {
                            cursor_id: pseudo_cursor,
                            content_reg: pseudo_content_reg,
                            num_fields: key.len() + source.column_count(referenced_tables),
                        });

                        let sort_metadata = m.sorts.get(id).unwrap();
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
                            dest_reg: pseudo_content_reg,
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
                                return Ok(OpStepResult::ReadyToEmit);
                            }
                        }
                    },
                    PROJECTION_FINALIZE_SOURCE => {
                        match source.step(program, m, referenced_tables)? {
                            OpStepResult::Done => {
                                return Ok(OpStepResult::Done);
                            }
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
        referenced_tables: &[(Rc<BTreeTable>, String)],
        m: &mut Metadata,
        cursor_override: Option<usize>,
    ) -> Result<usize> {
        let col_count = self.column_count(referenced_tables);
        match self {
            Operator::Scan {
                table,
                table_identifier,
                ..
            } => {
                let start_reg = program.alloc_registers(col_count);
                table_columns(program, table, table_identifier, cursor_override, start_reg);

                Ok(start_reg)
            }
            Operator::Join { left, right, .. } => {
                let left_start_reg =
                    left.result_columns(program, referenced_tables, m, cursor_override)?;
                right.result_columns(program, referenced_tables, m, cursor_override)?;

                Ok(left_start_reg)
            }
            Operator::Aggregate { id, aggregates, .. } => {
                let start_reg = m.aggregation_start_registers.get(id).unwrap();
                for (i, agg) in aggregates.iter().enumerate() {
                    let agg_result_reg = *start_reg + i;
                    program.emit_insn(Insn::AggFinal {
                        register: agg_result_reg,
                        func: agg.func.clone(),
                    });
                }

                Ok(*start_reg)
            }
            Operator::Filter { .. } => unreachable!("predicates have been pushed down"),
            Operator::SeekRowid {
                table_identifier,
                table,
                ..
            } => {
                let start_reg = program.alloc_registers(col_count);
                table_columns(program, table, table_identifier, cursor_override, start_reg);

                Ok(start_reg)
            }
            Operator::Limit { .. } => {
                unimplemented!()
            }
            Operator::Order {
                id, source, key, ..
            } => {
                let sort_metadata = m.sorts.get(id).unwrap();
                let cursor_override = Some(sort_metadata.sort_cursor);
                let sort_keys_count = key.len();
                let start_reg = program.alloc_registers(sort_keys_count);
                for (i, (expr, _)) in key.iter().enumerate() {
                    let key_reg = start_reg + i;
                    translate_expr(
                        program,
                        Some(referenced_tables),
                        expr,
                        key_reg,
                        cursor_override,
                    )?;
                }
                source.result_columns(program, referenced_tables, m, cursor_override)?;

                Ok(start_reg)
            }
            Operator::Projection { expressions, .. } => {
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
                                cursor_override,
                            )?;
                            cur_reg += 1;
                        }
                        ProjectionColumn::Star => {
                            for (table, table_identifier) in referenced_tables.iter() {
                                cur_reg = table_columns(
                                    program,
                                    table,
                                    table_identifier,
                                    cursor_override,
                                    cur_reg,
                                );
                            }
                        }
                        ProjectionColumn::TableStar(_, table_identifier) => {
                            let (table, table_identifier) = referenced_tables
                                .iter()
                                .find(|(_, id)| id == table_identifier)
                                .unwrap();
                            cur_reg = table_columns(
                                program,
                                table,
                                table_identifier,
                                cursor_override,
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
        referenced_tables: &[(Rc<BTreeTable>, String)],
        m: &mut Metadata,
        cursor_override: Option<usize>,
    ) -> Result<()> {
        match self {
            Operator::Order { id, source, .. } => {
                let sort_metadata = m.sorts.get(id).unwrap();
                source.result_row(
                    program,
                    referenced_tables,
                    m,
                    Some(sort_metadata.pseudo_table_cursor),
                )?;

                Ok(())
            }
            Operator::Limit { source, limit, .. } => {
                source.result_row(program, referenced_tables, m, cursor_override)?;
                let limit_reg = program.alloc_register();
                program.emit_insn(Insn::Integer {
                    value: *limit as i64,
                    dest: limit_reg,
                });
                program.mark_last_insn_constant();
                let jump_label = m.termination_labels.last().unwrap();
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

pub fn emit_program(
    database_header: Rc<RefCell<DatabaseHeader>>,
    mut select_plan: Plan,
) -> Result<Program> {
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

    let mut metadata = Metadata {
        termination_labels: vec![halt_label],
        next_row_labels: HashMap::new(),
        rewind_labels: Vec::new(),
        aggregation_start_registers: HashMap::new(),
        sorts: HashMap::new(),
        left_joins: HashMap::new(),
    };

    loop {
        match select_plan.root_operator.step(
            &mut program,
            &mut metadata,
            &select_plan.referenced_tables,
        )? {
            OpStepResult::Continue => {}
            OpStepResult::ReadyToEmit => {
                select_plan.root_operator.result_row(
                    &mut program,
                    &select_plan.referenced_tables,
                    &mut metadata,
                    None,
                )?;
            }
            OpStepResult::Done => {
                break;
            }
        }
    }

    program.resolve_label(halt_label, program.offset());
    program.emit_insn(Insn::Halt);

    program.resolve_label(init_label, program.offset());
    program.emit_insn(Insn::Transaction);

    program.emit_constant_insns();
    program.emit_insn(Insn::Goto {
        target_pc: start_offset,
    });

    program.resolve_deferred_labels();
    Ok(program.build(database_header))
}

fn table_columns(
    program: &mut ProgramBuilder,
    table: &Rc<BTreeTable>,
    table_identifier: &str,
    cursor_override: Option<usize>,
    start_reg: usize,
) -> usize {
    let mut cur_reg = start_reg;
    let cursor_id = cursor_override.unwrap_or(program.resolve_cursor_id(table_identifier, None));
    for i in 0..table.columns.len() {
        let is_rowid = table.column_is_rowid_alias(&table.columns[i]);
        let col_type = &table.columns[i].ty;
        if is_rowid {
            program.emit_insn(Insn::RowId {
                cursor_id,
                dest: cur_reg,
            });
        } else {
            program.emit_insn(Insn::Column {
                cursor_id,
                column: i,
                dest: cur_reg,
            });
        }
        maybe_apply_affinity(*col_type, cur_reg, program);
        cur_reg += 1;
    }
    cur_reg
}
