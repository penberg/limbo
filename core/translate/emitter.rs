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
 * - start: open cursors, etc.
 * - emit: open loops, emit conditional jumps etc.
 * - end: close loops, etc.
 * - result_columns: emit the bytecode instructions for the result columns.
 * - result_row: emit the bytecode instructions for a result row.
*/
pub trait Emitter {
    fn start(
        &mut self,
        pb: &mut ProgramBuilder,
        m: &mut Metadata,
        referenced_tables: &[(Rc<BTreeTable>, String)],
    ) -> Result<()>;
    fn emit(
        &mut self,
        pb: &mut ProgramBuilder,
        m: &mut Metadata,
        referenced_tables: &[(Rc<BTreeTable>, String)],
        is_root: bool,
    ) -> Result<bool>;
    fn end(
        &mut self,
        pb: &mut ProgramBuilder,
        m: &mut Metadata,
        referenced_tables: &[(Rc<BTreeTable>, String)],
    ) -> Result<()>;
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
    ) -> Result<bool>;
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
    pub sort_cursor: usize,
    pub sort_register: usize,
    pub next_row_label: BranchOffset,
    pub done_label: BranchOffset,
}

#[derive(Debug)]
pub struct Metadata {
    termination_labels: Vec<BranchOffset>,
    next_row_labels: HashMap<usize, BranchOffset>,
    rewind_labels: Vec<BranchOffset>,
    aggregations: HashMap<usize, usize>,
    sorts: HashMap<usize, SortMetadata>,
    left_joins: HashMap<usize, LeftJoinMetadata>,
}

impl Emitter for Operator {
    fn start(
        &mut self,
        program: &mut ProgramBuilder,
        m: &mut Metadata,
        referenced_tables: &[(Rc<BTreeTable>, String)],
    ) -> Result<()> {
        match self {
            Operator::Scan {
                table,
                table_identifier,
                id,
                ..
            } => {
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

                Ok(())
            }
            Operator::SeekRowid {
                table,
                table_identifier,
                ..
            } => {
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

                Ok(())
            }
            Operator::Join {
                left,
                right,
                outer,
                id,
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
                left.start(program, m, referenced_tables)?;
                right.start(program, m, referenced_tables)
            }
            Operator::Aggregate {
                id,
                source,
                aggregates,
            } => {
                let can_continue = source.start(program, m, referenced_tables)?;

                let agg_final_label = program.allocate_label();
                m.termination_labels.push(agg_final_label);
                source.emit(program, m, referenced_tables, false)?;

                let num_aggs = aggregates.len();
                let start_reg = program.alloc_registers(num_aggs);
                m.aggregations.insert(*id, start_reg);

                Ok(can_continue)
            }
            Operator::Filter { .. } => unreachable!("predicates have been pushed down"),
            Operator::Limit { source, .. } => source.start(program, m, referenced_tables),
            Operator::Order { id, source, key } => {
                let sort_cursor = program.alloc_cursor_id(None, None);
                m.sorts.insert(
                    *id,
                    SortMetadata {
                        sort_cursor,
                        sort_register: usize::MAX, // will be set later
                        next_row_label: program.allocate_label(),
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

                source.start(program, m, referenced_tables)
            }
            Operator::Projection { source, .. } => source.start(program, m, referenced_tables),
            Operator::Nothing => Ok(()),
        }
    }
    fn emit(
        &mut self,
        program: &mut ProgramBuilder,
        m: &mut Metadata,
        referenced_tables: &[(Rc<BTreeTable>, String)],
        is_root: bool,
    ) -> Result<bool> {
        match self {
            Operator::Aggregate {
                source,
                aggregates,
                id,
            } => {
                let can_continue = source.emit(program, m, referenced_tables, false)?;
                if !can_continue {
                    return Ok(false);
                }
                let start_reg = m.aggregations.get(id).unwrap();
                for (i, agg) in aggregates.iter().enumerate() {
                    let agg_result_reg = start_reg + i;
                    translate_aggregation(program, referenced_tables, agg, agg_result_reg, None)?;
                }

                Ok(false)
            }
            Operator::Filter { .. } => unreachable!("predicates have been pushed down"),
            Operator::SeekRowid {
                rowid_predicate,
                predicates,
                table_identifier,
                id,
                ..
            } => {
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
                Ok(true)
            }
            Operator::Limit { source, .. } => {
                source.emit(program, m, referenced_tables, false)?;
                Ok(true)
            }
            Operator::Join {
                left,
                right,
                predicates,
                outer,
                id,
            } => {
                left.emit(program, m, referenced_tables, false)?;

                let mut jump_target_when_false = *m
                    .next_row_labels
                    .get(&right.id())
                    .unwrap_or(&m.termination_labels.last().unwrap());

                if *outer {
                    let lj_meta = m.left_joins.get(id).unwrap();
                    program.emit_insn(Insn::Integer {
                        value: 0,
                        dest: lj_meta.match_flag_register,
                    });
                    jump_target_when_false = lj_meta.check_match_flag_label;
                    m.next_row_labels.insert(right.id(), jump_target_when_false);
                }

                right.emit(program, m, referenced_tables, false)?;

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

                if is_root {
                    return self.result_row(program, referenced_tables, m, None);
                }
                Ok(true)
            }
            Operator::Order { source, key, id } => {
                source.emit(program, m, referenced_tables, false)?;
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
                sort_metadata.sort_register = start_reg;

                if is_root {
                    return self.result_row(program, referenced_tables, m, None);
                }

                Ok(true)
            }
            Operator::Projection { source, .. } => {
                source.emit(program, m, referenced_tables, false)?;
                if is_root {
                    return self.result_row(program, referenced_tables, m, None);
                }

                Ok(true)
            }
            Operator::Scan {
                predicates,
                table_identifier,
                id,
                ..
            } => {
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

                if is_root {
                    return self.result_row(program, referenced_tables, m, None);
                }

                Ok(true)
            }
            Operator::Nothing => Ok(false),
        }
    }
    fn end(
        &mut self,
        program: &mut ProgramBuilder,
        m: &mut Metadata,
        referenced_tables: &[(Rc<BTreeTable>, String)],
    ) -> Result<()> {
        match self {
            Operator::Scan {
                table_identifier,
                id,
                ..
            } => {
                let cursor_id = program.resolve_cursor_id(table_identifier, None);
                program.resolve_label(*m.next_row_labels.get(id).unwrap(), program.offset());
                program.emit_insn(Insn::NextAsync { cursor_id });
                let jump_label = m.rewind_labels.pop().unwrap();
                program.emit_insn_with_label_dependency(
                    Insn::NextAwait {
                        cursor_id,
                        pc_if_next: jump_label,
                    },
                    jump_label,
                );
                Ok(())
            }
            Operator::Join {
                left,
                right,
                outer,
                id,
                ..
            } => {
                right.end(program, m, referenced_tables)?;

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
                left.end(program, m, referenced_tables)
            }
            Operator::Aggregate {
                id,
                source,
                aggregates,
            } => {
                source.end(program, m, referenced_tables)?;

                program.resolve_label(m.termination_labels.pop().unwrap(), program.offset());
                let start_reg = m.aggregations.get(id).unwrap();
                for (i, agg) in aggregates.iter().enumerate() {
                    let agg_result_reg = *start_reg + i;
                    program.emit_insn(Insn::AggFinal {
                        register: agg_result_reg,
                        func: agg.func.clone(),
                    });
                }
                program.emit_insn(Insn::ResultRow {
                    start_reg: *start_reg,
                    count: aggregates.len(),
                });
                Ok(())
            }
            Operator::Filter { .. } => unreachable!("predicates have been pushed down"),
            Operator::SeekRowid { .. } => Ok(()),
            Operator::Limit { source, limit, .. } => {
                source.result_row(program, referenced_tables, m, None)?;
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

                source.end(program, m, referenced_tables)?;

                Ok(())
            }
            Operator::Order { id, .. } => {
                let sort_metadata = m.sorts.get(id).unwrap();
                program.emit_insn_with_label_dependency(
                    Insn::SorterNext {
                        cursor_id: sort_metadata.sort_cursor,
                        pc_if_next: sort_metadata.next_row_label,
                    },
                    sort_metadata.next_row_label,
                );

                program.resolve_label(sort_metadata.done_label, program.offset());

                Ok(())
            }
            Operator::Projection { source, .. } => source.end(program, m, referenced_tables),
            Operator::Nothing => Ok(()),
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
                let start_reg = m.aggregations.get(id).unwrap();
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
            Operator::Order { .. } => {
                todo!()
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
                        ProjectionColumn::TableStar(table, table_identifier) => {
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
    ) -> Result<bool> {
        match self {
            Operator::Order { id, source, key } => {
                source.end(program, m, referenced_tables)?;
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
                    sort_metadata.next_row_label,
                    program.offset() as usize,
                );
                program.emit_insn(Insn::SorterData {
                    cursor_id: sort_metadata.sort_cursor,
                    dest_reg: pseudo_content_reg,
                    pseudo_cursor,
                });

                let done_label = sort_metadata.done_label;

                source.result_row(program, referenced_tables, m, Some(pseudo_cursor))?;

                program.resolve_label(done_label, program.offset());

                Ok(true)
            }
            operator => {
                let start_reg =
                    operator.result_columns(program, referenced_tables, m, cursor_override)?;
                program.emit_insn(Insn::ResultRow {
                    start_reg,
                    count: operator.column_count(referenced_tables),
                });
                Ok(true)
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
        aggregations: HashMap::new(),
        sorts: HashMap::new(),
        left_joins: HashMap::new(),
    };

    select_plan
        .root_operator
        .start(&mut program, &mut metadata, &select_plan.referenced_tables)?;
    select_plan.root_operator.emit(
        &mut program,
        &mut metadata,
        &select_plan.referenced_tables,
        true,
    )?;
    select_plan
        .root_operator
        .end(&mut program, &mut metadata, &select_plan.referenced_tables)?;

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
