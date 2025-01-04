// This module contains code for emitting bytecode instructions for SQL query execution.
// It handles translating high-level SQL operations into low-level bytecode that can be executed by the virtual machine.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::{Rc, Weak};

use sqlite3_parser::ast::{self};

use crate::schema::{Column, PseudoTable, Table};
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::translate::plan::{DeletePlan, IterationDirection, Plan, Search};
use crate::types::{OwnedRecord, OwnedValue};
use crate::util::exprs_are_equivalent;
use crate::vdbe::builder::ProgramBuilder;
use crate::vdbe::{insn::Insn, BranchOffset, Program};
use crate::{Connection, Result, SymbolTable};

use super::expr::{
    translate_aggregation, translate_aggregation_groupby, translate_condition_expr, translate_expr,
    ConditionMetadata,
};
use super::plan::{
    Aggregate, Direction, GroupBy, SelectPlan, SelectQueryType, TableReference, TableReferenceType,
};
use super::plan::{ResultSetColumn, SourceOperator};

// Metadata for handling LEFT JOIN operations
#[derive(Debug)]
pub struct LeftJoinMetadata {
    // integer register that holds a flag that is set to true if the current row has a match for the left join
    pub match_flag_register: usize,
    // label for the instruction that sets the match flag to true
    pub set_match_flag_true_label: BranchOffset,
    // label for the instruction that checks if the match flag is true
    pub check_match_flag_label: BranchOffset,
}

// Metadata for handling ORDER BY operations
#[derive(Debug)]
pub struct SortMetadata {
    // cursor id for the Sorter table where the sorted rows are stored
    pub sort_cursor: usize,
    // register where the sorter data is inserted and later retrieved from
    pub sorter_data_register: usize,
}

// Metadata for handling GROUP BY operations
#[derive(Debug)]
pub struct GroupByMetadata {
    // Cursor ID for the Sorter table where the grouped rows are stored
    pub sort_cursor: usize,
    // Label for the subroutine that clears the accumulator registers (temporary storage for per-group aggregate calculations)
    pub label_subrtn_acc_clear: BranchOffset,
    // Label for the instruction that sets the accumulator indicator to true (indicating data exists in the accumulator for the current group)
    pub label_acc_indicator_set_flag_true: BranchOffset,
    // Register holding the return offset for the accumulator clear subroutine
    pub reg_subrtn_acc_clear_return_offset: usize,
    // Register holding the key used for sorting in the Sorter
    pub reg_sorter_key: usize,
    // Register holding a flag to abort the grouping process if necessary
    pub reg_abort_flag: usize,
    // Register holding the start of the accumulator group registers (i.e. the groups, not the aggregates)
    pub reg_group_exprs_acc: usize,
    // Starting index of the register(s) that hold the comparison result between the current row and the previous row
    // The comparison result is used to determine if the current row belongs to the same group as the previous row
    // Each group by expression has a corresponding register
    pub reg_group_exprs_cmp: usize,
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

/// The TranslateCtx struct holds various information and labels used during bytecode generation.
/// It is used for maintaining state and control flow during the bytecode
/// generation process.
#[derive(Debug)]
pub struct TranslateCtx {
    // A typical query plan is a nested loop. Each loop has its own LoopLabels (see the definition of LoopLabels for more details)
    loop_labels: HashMap<usize, LoopLabels>,
    // label for the instruction that jumps to the next phase of the query after the main loop
    // we don't know ahead of time what that is (GROUP BY, ORDER BY, etc.)
    after_main_loop_label: Option<BranchOffset>,
    // metadata for the group by operator
    group_by_metadata: Option<GroupByMetadata>,
    // metadata for the order by operator
    sort_metadata: Option<SortMetadata>,
    // mapping between Join operator id and associated metadata (for left joins only)
    left_joins: HashMap<usize, LeftJoinMetadata>,
    // First register of the aggregation results
    pub aggregation_start_register: Option<usize>,
    // First register of the result columns of the query
    pub result_column_start_register: Option<usize>,
    // We need to emit result columns in the order they are present in the SELECT, but they may not be in the same order in the ORDER BY sorter.
    // This vector holds the indexes of the result columns in the ORDER BY sorter.
    pub result_column_indexes_in_orderby_sorter: HashMap<usize, usize>,
    // We might skip adding a SELECT result column into the ORDER BY sorter if it is an exact match in the ORDER BY keys.
    // This vector holds the indexes of the result columns that we need to skip.
    pub result_columns_to_skip_in_orderby_sorter: Option<Vec<usize>>,
    // The register holding the limit value, if any.
    pub limit_reg: Option<usize>,
}

/// Used to distinguish database operations
#[allow(clippy::upper_case_acronyms, dead_code)]
#[derive(Debug, Clone)]
pub enum OperationMode {
    SELECT,
    INSERT,
    UPDATE,
    DELETE,
}

/// Initialize the program with basic setup and return initial metadata and labels
fn prologue() -> Result<(ProgramBuilder, TranslateCtx, BranchOffset, BranchOffset)> {
    let mut program = ProgramBuilder::new();
    let init_label = program.allocate_label();

    program.emit_insn_with_label_dependency(
        Insn::Init {
            target_pc: init_label,
        },
        init_label,
    );

    let start_offset = program.offset();

    let t_ctx = TranslateCtx {
        loop_labels: HashMap::new(),
        after_main_loop_label: None,
        group_by_metadata: None,
        left_joins: HashMap::new(),
        sort_metadata: None,
        aggregation_start_register: None,
        result_column_start_register: None,
        result_column_indexes_in_orderby_sorter: HashMap::new(),
        result_columns_to_skip_in_orderby_sorter: None,
        limit_reg: None,
    };

    Ok((program, t_ctx, init_label, start_offset))
}

/// Clean up and finalize the program, resolving any remaining labels
/// Note that although these are the final instructions, typically an SQLite
/// query will jump to the Transaction instruction via init_label.
fn epilogue(
    program: &mut ProgramBuilder,
    init_label: BranchOffset,
    start_offset: BranchOffset,
) -> Result<()> {
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

/// Main entry point for emitting bytecode for a SQL query
/// Takes a query plan and generates the corresponding bytecode program
pub fn emit_program(
    database_header: Rc<RefCell<DatabaseHeader>>,
    plan: Plan,
    connection: Weak<Connection>,
    syms: &SymbolTable,
) -> Result<Program> {
    match plan {
        Plan::Select(plan) => emit_program_for_select(database_header, plan, connection, syms),
        Plan::Delete(plan) => emit_program_for_delete(database_header, plan, connection, syms),
    }
}

fn emit_program_for_select(
    database_header: Rc<RefCell<DatabaseHeader>>,
    mut plan: SelectPlan,
    connection: Weak<Connection>,
    syms: &SymbolTable,
) -> Result<Program> {
    let (mut program, mut t_ctx, init_label, start_offset) = prologue()?;

    // Trivial exit on LIMIT 0
    if let Some(limit) = plan.limit {
        if limit == 0 {
            epilogue(&mut program, init_label, start_offset)?;
            return Ok(program.build(database_header, connection));
        }
    }

    // Emit main parts of query
    emit_query(&mut program, &mut plan, &mut t_ctx, syms)?;

    // Finalize program
    epilogue(&mut program, init_label, start_offset)?;

    Ok(program.build(database_header, connection))
}

/// Emit the subqueries contained in the FROM clause.
/// This is done first so the results can be read in the main query loop.
fn emit_subqueries(
    program: &mut ProgramBuilder,
    referenced_tables: &mut [TableReference],
    source: &mut SourceOperator,
    syms: &SymbolTable,
) -> Result<()> {
    match source {
        SourceOperator::Subquery {
            table_reference,
            plan,
            ..
        } => {
            // Emit the subquery and get the start register of the result columns.
            let result_columns_start = emit_subquery(program, plan, syms)?;
            // Set the result_columns_start_reg in the TableReference object.
            // This is done so that translate_expr() can read the result columns of the subquery,
            // as if it were reading from a regular table.
            let table_ref = referenced_tables
                .iter_mut()
                .find(|t| t.table_identifier == table_reference.table_identifier)
                .unwrap();
            if let TableReferenceType::Subquery {
                result_columns_start_reg,
                ..
            } = &mut table_ref.reference_type
            {
                *result_columns_start_reg = result_columns_start;
            } else {
                unreachable!("emit_subqueries called on non-subquery");
            }
            Ok(())
        }
        SourceOperator::Join { left, right, .. } => {
            emit_subqueries(program, referenced_tables, left, syms)?;
            emit_subqueries(program, referenced_tables, right, syms)?;
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Emit a subquery and return the start register of the result columns.
/// This is done by emitting a coroutine that stores the result columns in sequential registers.
/// Each subquery in a FROM clause has its own separate SelectPlan which is wrapped in a coroutine.
///
/// The resulting bytecode from a subquery is mostly exactly the same as a regular query, except:
/// - it ends in an EndCoroutine instead of a Halt.
/// - instead of emitting ResultRows, the coroutine yields to the main query loop.
/// - the first register of the result columns is returned to the parent query,
///   so that translate_expr() can read the result columns of the subquery,
///   as if it were reading from a regular table.
///
/// Since a subquery has its own SelectPlan, it can contain nested subqueries,
/// which can contain even more nested subqueries, etc.
fn emit_subquery(
    program: &mut ProgramBuilder,
    plan: &mut SelectPlan,
    syms: &SymbolTable,
) -> Result<usize> {
    let yield_reg = program.alloc_register();
    let coroutine_implementation_start_offset = program.offset() + 1;
    match &mut plan.query_type {
        SelectQueryType::Subquery {
            yield_reg: y,
            coroutine_implementation_start,
        } => {
            // The parent query will use this register to jump to/from the subquery.
            *y = yield_reg;
            // The parent query will use this register to reinitialize the coroutine when it needs to run multiple times.
            *coroutine_implementation_start = coroutine_implementation_start_offset;
        }
        _ => unreachable!("emit_subquery called on non-subquery"),
    }
    let end_coroutine_label = program.allocate_label();
    let mut metadata = TranslateCtx {
        loop_labels: HashMap::new(),
        after_main_loop_label: None,
        group_by_metadata: None,
        left_joins: HashMap::new(),
        sort_metadata: None,
        aggregation_start_register: None,
        result_column_start_register: None,
        result_column_indexes_in_orderby_sorter: HashMap::new(),
        result_columns_to_skip_in_orderby_sorter: None,
        limit_reg: plan.limit.map(|_| program.alloc_register()),
    };
    let subquery_body_end_label = program.allocate_label();
    program.emit_insn_with_label_dependency(
        Insn::InitCoroutine {
            yield_reg,
            jump_on_definition: subquery_body_end_label,
            start_offset: coroutine_implementation_start_offset,
        },
        subquery_body_end_label,
    );
    // Normally we mark each LIMIT value as a constant insn that is emitted only once, but in the case of a subquery,
    // we need to initialize it every time the subquery is run; otherwise subsequent runs of the subquery will already
    // have the LIMIT counter at 0, and will never return rows.
    if let Some(limit) = plan.limit {
        program.emit_insn(Insn::Integer {
            value: limit as i64,
            dest: metadata.limit_reg.unwrap(),
        });
    }
    let result_column_start_reg = emit_query(program, plan, &mut metadata, syms)?;
    program.resolve_label(end_coroutine_label, program.offset());
    program.emit_insn(Insn::EndCoroutine { yield_reg });
    program.resolve_label(subquery_body_end_label, program.offset());
    Ok(result_column_start_reg)
}

fn emit_query(
    program: &mut ProgramBuilder,
    plan: &mut SelectPlan,
    t_ctx: &mut TranslateCtx,
    syms: &SymbolTable,
) -> Result<usize> {
    // Emit subqueries first so the results can be read in the main query loop.
    emit_subqueries(program, &mut plan.referenced_tables, &mut plan.source, syms)?;

    if t_ctx.limit_reg.is_none() {
        t_ctx.limit_reg = plan.limit.map(|_| program.alloc_register());
    }

    // No rows will be read from source table loops if there is a constant false condition eg. WHERE 0
    // however an aggregation might still happen,
    // e.g. SELECT COUNT(*) WHERE 0 returns a row with 0, not an empty result set
    let after_main_loop_label = program.allocate_label();
    t_ctx.after_main_loop_label = Some(after_main_loop_label);
    if plan.contains_constant_false_condition {
        program.emit_insn_with_label_dependency(
            Insn::Goto {
                target_pc: after_main_loop_label,
            },
            after_main_loop_label,
        );
    }

    // Allocate registers for result columns
    t_ctx.result_column_start_register = Some(program.alloc_registers(plan.result_columns.len()));

    // Initialize cursors and other resources needed for query execution
    if let Some(ref mut order_by) = plan.order_by {
        init_order_by(program, order_by, t_ctx)?;
    }

    if let Some(ref mut group_by) = plan.group_by {
        init_group_by(program, group_by, &plan.aggregates, t_ctx)?;
    }
    init_source(program, &plan.source, t_ctx, &OperationMode::SELECT)?;

    // Set up main query execution loop
    open_loop(
        program,
        &mut plan.source,
        &plan.referenced_tables,
        t_ctx,
        syms,
    )?;

    // Process result columns and expressions in the inner loop
    emit_loop(program, plan, t_ctx, syms)?;

    // Clean up and close the main execution loop
    close_loop(program, &plan.source, t_ctx)?;

    program.resolve_label(after_main_loop_label, program.offset());

    let mut order_by_necessary = plan.order_by.is_some() && !plan.contains_constant_false_condition;

    // Handle GROUP BY and aggregation processing
    if let Some(ref mut group_by) = plan.group_by {
        emit_group_by(
            program,
            &plan.result_columns,
            group_by,
            plan.order_by.as_ref(),
            &plan.aggregates,
            plan.limit,
            &plan.referenced_tables,
            t_ctx,
            syms,
            &plan.query_type,
        )?;
    } else if !plan.aggregates.is_empty() {
        // Handle aggregation without GROUP BY
        emit_ungrouped_aggregation(
            program,
            &plan.referenced_tables,
            &plan.result_columns,
            &plan.aggregates,
            t_ctx,
            syms,
            &plan.query_type,
        )?;
        // Single row result for aggregates without GROUP BY, so ORDER BY not needed
        order_by_necessary = false;
    }

    // Process ORDER BY results if needed
    if let Some(ref mut order_by) = plan.order_by {
        if order_by_necessary {
            emit_order_by(
                program,
                order_by,
                &plan.result_columns,
                plan.limit,
                t_ctx,
                &plan.query_type,
            )?;
        }
    }

    Ok(t_ctx.result_column_start_register.unwrap())
}

fn emit_program_for_delete(
    database_header: Rc<RefCell<DatabaseHeader>>,
    mut plan: DeletePlan,
    connection: Weak<Connection>,
    syms: &SymbolTable,
) -> Result<Program> {
    let (mut program, mut t_ctx, init_label, start_offset) = prologue()?;

    // No rows will be read from source table loops if there is a constant false condition eg. WHERE 0
    let after_main_loop_label = program.allocate_label();
    if plan.contains_constant_false_condition {
        program.emit_insn_with_label_dependency(
            Insn::Goto {
                target_pc: after_main_loop_label,
            },
            after_main_loop_label,
        );
    }

    // Initialize cursors and other resources needed for query execution
    init_source(
        &mut program,
        &plan.source,
        &mut t_ctx,
        &OperationMode::DELETE,
    )?;

    // Set up main query execution loop
    open_loop(
        &mut program,
        &mut plan.source,
        &plan.referenced_tables,
        &mut t_ctx,
        syms,
    )?;

    emit_delete_insns(&mut program, &plan.source, &plan.limit, &t_ctx)?;

    // Clean up and close the main execution loop
    close_loop(&mut program, &plan.source, &mut t_ctx)?;

    program.resolve_label(after_main_loop_label, program.offset());

    // Finalize program
    epilogue(&mut program, init_label, start_offset)?;

    Ok(program.build(database_header, connection))
}

/// Initialize resources needed for ORDER BY processing
fn init_order_by(
    program: &mut ProgramBuilder,
    order_by: &[(ast::Expr, Direction)],
    t_ctx: &mut TranslateCtx,
) -> Result<()> {
    let sort_cursor = program.alloc_cursor_id(None, None);
    t_ctx.sort_metadata = Some(SortMetadata {
        sort_cursor,
        sorter_data_register: program.alloc_register(),
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

/// Initialize resources needed for GROUP BY processing
fn init_group_by(
    program: &mut ProgramBuilder,
    group_by: &GroupBy,
    aggregates: &[Aggregate],
    t_ctx: &mut TranslateCtx,
) -> Result<()> {
    let num_aggs = aggregates.len();

    let sort_cursor = program.alloc_cursor_id(None, None);

    let reg_abort_flag = program.alloc_register();
    let reg_group_exprs_cmp = program.alloc_registers(group_by.exprs.len());
    let reg_group_exprs_acc = program.alloc_registers(group_by.exprs.len());
    let reg_agg_exprs_start = program.alloc_registers(num_aggs);
    let reg_sorter_key = program.alloc_register();

    let label_subrtn_acc_clear = program.allocate_label();

    let mut order = Vec::new();
    const ASCENDING: i64 = 0;
    for _ in group_by.exprs.iter() {
        order.push(OwnedValue::Integer(ASCENDING));
    }
    program.emit_insn(Insn::SorterOpen {
        cursor_id: sort_cursor,
        columns: aggregates.len() + group_by.exprs.len(),
        order: OwnedRecord::new(order),
    });

    program.add_comment(program.offset(), "clear group by abort flag");
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: reg_abort_flag,
    });

    program.add_comment(
        program.offset(),
        "initialize group by comparison registers to NULL",
    );
    program.emit_insn(Insn::Null {
        dest: reg_group_exprs_cmp,
        dest_end: if group_by.exprs.len() > 1 {
            Some(reg_group_exprs_cmp + group_by.exprs.len() - 1)
        } else {
            None
        },
    });

    program.add_comment(program.offset(), "go to clear accumulator subroutine");

    let reg_subrtn_acc_clear_return_offset = program.alloc_register();
    program.emit_insn_with_label_dependency(
        Insn::Gosub {
            target_pc: label_subrtn_acc_clear,
            return_reg: reg_subrtn_acc_clear_return_offset,
        },
        label_subrtn_acc_clear,
    );

    t_ctx.aggregation_start_register = Some(reg_agg_exprs_start);

    t_ctx.group_by_metadata = Some(GroupByMetadata {
        sort_cursor,
        label_subrtn_acc_clear,
        label_acc_indicator_set_flag_true: program.allocate_label(),
        reg_subrtn_acc_clear_return_offset,
        reg_abort_flag,
        reg_group_exprs_acc,
        reg_group_exprs_cmp,
        reg_sorter_key,
    });
    Ok(())
}

/// Initialize resources needed for the source operators (tables, joins, etc)
fn init_source(
    program: &mut ProgramBuilder,
    source: &SourceOperator,
    t_ctx: &mut TranslateCtx,
    mode: &OperationMode,
) -> Result<()> {
    let operator_id = source.id();
    let loop_labels = LoopLabels {
        next: program.allocate_label(),
        loop_start: program.allocate_label(),
        loop_end: program.allocate_label(),
    };
    t_ctx.loop_labels.insert(operator_id, loop_labels);

    match source {
        SourceOperator::Subquery { .. } => Ok(()),
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
                };
                t_ctx.left_joins.insert(*id, lj_metadata);
            }
            init_source(program, left, t_ctx, mode)?;
            init_source(program, right, t_ctx, mode)?;

            Ok(())
        }
        SourceOperator::Scan {
            table_reference, ..
        } => {
            let cursor_id = program.alloc_cursor_id(
                Some(table_reference.table_identifier.clone()),
                Some(table_reference.table.clone()),
            );
            let root_page = table_reference.table.get_root_page();

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

            Ok(())
        }
        SourceOperator::Search {
            table_reference,
            search,
            ..
        } => {
            let table_cursor_id = program.alloc_cursor_id(
                Some(table_reference.table_identifier.clone()),
                Some(table_reference.table.clone()),
            );

            match mode {
                OperationMode::SELECT => {
                    program.emit_insn(Insn::OpenReadAsync {
                        cursor_id: table_cursor_id,
                        root_page: table_reference.table.get_root_page(),
                    });
                    program.emit_insn(Insn::OpenReadAwait {});
                }
                OperationMode::DELETE => {
                    program.emit_insn(Insn::OpenWriteAsync {
                        cursor_id: table_cursor_id,
                        root_page: table_reference.table.get_root_page(),
                    });
                    program.emit_insn(Insn::OpenWriteAwait {});
                }
                _ => {
                    unimplemented!()
                }
            }

            if let Search::IndexSearch { index, .. } = search {
                let index_cursor_id = program
                    .alloc_cursor_id(Some(index.name.clone()), Some(Table::Index(index.clone())));

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

            Ok(())
        }
        SourceOperator::Nothing { .. } => Ok(()),
    }
}

/// Set up the main query execution loop
/// For example in the case of a nested table scan, this means emitting the RewindAsync instruction
/// for all tables involved, outermost first.
fn open_loop(
    program: &mut ProgramBuilder,
    source: &mut SourceOperator,
    referenced_tables: &[TableReference],
    t_ctx: &mut TranslateCtx,
    syms: &SymbolTable,
) -> Result<()> {
    match source {
        SourceOperator::Subquery {
            id,
            predicates,
            plan,
            ..
        } => {
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
                jump_on_definition: 0,
                start_offset: coroutine_implementation_start,
            });
            let loop_labels = t_ctx
                .loop_labels
                .get(id)
                .expect("subquery has no loop labels");
            program.defer_label_resolution(loop_labels.loop_start, program.offset() as usize);
            // A subquery within the main loop of a parent query has no cursor, so instead of advancing the cursor,
            // it emits a Yield which jumps back to the main loop of the subquery itself to retrieve the next row.
            // When the subquery coroutine completes, this instruction jumps to the label at the top of the termination_label_stack,
            // which in this case is the end of the Yield-Goto loop in the parent query.
            program.emit_insn_with_label_dependency(
                Insn::Yield {
                    yield_reg,
                    end_offset: loop_labels.loop_end,
                },
                loop_labels.loop_end,
            );

            // These are predicates evaluated outside of the subquery,
            // so they are translated here.
            // E.g. SELECT foo FROM (SELECT bar as foo FROM t1) sub WHERE sub.foo > 10
            if let Some(preds) = predicates {
                for expr in preds {
                    let jump_target_when_true = program.allocate_label();
                    let condition_metadata = ConditionMetadata {
                        jump_if_condition_is_true: false,
                        jump_target_when_true,
                        jump_target_when_false: loop_labels.next,
                    };
                    translate_condition_expr(
                        program,
                        referenced_tables,
                        expr,
                        condition_metadata,
                        None,
                        syms,
                    )?;
                    program.resolve_label(jump_target_when_true, program.offset());
                }
            }

            Ok(())
        }
        SourceOperator::Join {
            id,
            left,
            right,
            predicates,
            outer,
            ..
        } => {
            open_loop(program, left, referenced_tables, t_ctx, syms)?;

            let loop_labels = t_ctx
                .loop_labels
                .get(&right.id())
                .expect("right side of join has no loop labels");

            let mut jump_target_when_false = loop_labels.next;

            if *outer {
                let lj_meta = t_ctx.left_joins.get(id).unwrap();
                program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: lj_meta.match_flag_register,
                });
                jump_target_when_false = lj_meta.check_match_flag_label;
            }

            open_loop(program, right, referenced_tables, t_ctx, syms)?;

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
                        condition_metadata,
                        None,
                        syms,
                    )?;
                }
                program.resolve_label(jump_target_when_true, program.offset());
            }

            if *outer {
                let lj_meta = t_ctx.left_joins.get(id).unwrap();
                program.defer_label_resolution(
                    lj_meta.set_match_flag_true_label,
                    program.offset() as usize,
                );
                program.emit_insn(Insn::Integer {
                    value: 1,
                    dest: lj_meta.match_flag_register,
                });
            }

            Ok(())
        }
        SourceOperator::Scan {
            id,
            table_reference,
            predicates,
            iter_dir,
        } => {
            let cursor_id = program.resolve_cursor_id(&table_reference.table_identifier);
            if iter_dir
                .as_ref()
                .is_some_and(|dir| *dir == IterationDirection::Backwards)
            {
                program.emit_insn(Insn::LastAsync { cursor_id });
            } else {
                program.emit_insn(Insn::RewindAsync { cursor_id });
            }
            let loop_labels = t_ctx.loop_labels.get(id).expect("scan has no loop labels");
            program.emit_insn_with_label_dependency(
                if iter_dir
                    .as_ref()
                    .is_some_and(|dir| *dir == IterationDirection::Backwards)
                {
                    Insn::LastAwait {
                        cursor_id,
                        pc_if_empty: loop_labels.loop_end,
                    }
                } else {
                    Insn::RewindAwait {
                        cursor_id,
                        pc_if_empty: loop_labels.loop_end,
                    }
                },
                loop_labels.loop_end,
            );
            program.defer_label_resolution(loop_labels.loop_start, program.offset() as usize);

            if let Some(preds) = predicates {
                for expr in preds {
                    let jump_target_when_true = program.allocate_label();
                    let condition_metadata = ConditionMetadata {
                        jump_if_condition_is_true: false,
                        jump_target_when_true,
                        jump_target_when_false: loop_labels.next,
                    };
                    translate_condition_expr(
                        program,
                        referenced_tables,
                        expr,
                        condition_metadata,
                        None,
                        syms,
                    )?;
                    program.resolve_label(jump_target_when_true, program.offset());
                }
            }

            Ok(())
        }
        SourceOperator::Search {
            id,
            table_reference,
            search,
            predicates,
            ..
        } => {
            let table_cursor_id = program.resolve_cursor_id(&table_reference.table_identifier);
            let loop_labels = t_ctx
                .loop_labels
                .get(id)
                .expect("search has no loop labels");
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
                            Some(referenced_tables),
                            cmp_expr,
                            cmp_reg,
                            None,
                            syms,
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
                let end_of_loop_label = loop_labels.loop_end;
                program.emit_insn_with_label_dependency(
                    match cmp_op {
                        ast::Operator::Equals | ast::Operator::GreaterEquals => Insn::SeekGE {
                            is_index: index_cursor_id.is_some(),
                            cursor_id: index_cursor_id.unwrap_or(table_cursor_id),
                            start_reg: cmp_reg,
                            num_regs: 1,
                            target_pc: end_of_loop_label,
                        },
                        ast::Operator::Greater
                        | ast::Operator::Less
                        | ast::Operator::LessEquals => Insn::SeekGT {
                            is_index: index_cursor_id.is_some(),
                            cursor_id: index_cursor_id.unwrap_or(table_cursor_id),
                            start_reg: cmp_reg,
                            num_regs: 1,
                            target_pc: end_of_loop_label,
                        },
                        _ => unreachable!(),
                    },
                    end_of_loop_label,
                );
                if *cmp_op == ast::Operator::Less || *cmp_op == ast::Operator::LessEquals {
                    translate_expr(
                        program,
                        Some(referenced_tables),
                        cmp_expr,
                        cmp_reg,
                        None,
                        syms,
                    )?;
                }

                program.defer_label_resolution(loop_labels.loop_start, program.offset() as usize);
                // TODO: We are currently only handling ascending indexes.
                // For conditions like index_key > 10, we have already seeked to the first key greater than 10, and can just scan forward.
                // For conditions like index_key < 10, we are at the beginning of the index, and will scan forward and emit IdxGE(10) with a conditional jump to the end.
                // For conditions like index_key = 10, we have already seeked to the first key greater than or equal to 10, and can just scan forward and emit IdxGT(10) with a conditional jump to the end.
                // For conditions like index_key >= 10, we have already seeked to the first key greater than or equal to 10, and can just scan forward.
                // For conditions like index_key <= 10, we are at the beginning of the index, and will scan forward and emit IdxGT(10) with a conditional jump to the end.
                // For conditions like index_key != 10, TODO. probably the optimal way is not to use an index at all.
                //
                // For primary key searches we emit RowId and then compare it to the seek value.

                let abort_jump_target = loop_labels.next;
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

            if let Search::RowidEq { cmp_expr } = search {
                let src_reg = program.alloc_register();
                translate_expr(
                    program,
                    Some(referenced_tables),
                    cmp_expr,
                    src_reg,
                    None,
                    syms,
                )?;
                program.emit_insn_with_label_dependency(
                    Insn::SeekRowid {
                        cursor_id: table_cursor_id,
                        src_reg,
                        target_pc: loop_labels.next,
                    },
                    loop_labels.next,
                );
            }
            if let Some(predicates) = predicates {
                for predicate in predicates.iter() {
                    let jump_target_when_true = program.allocate_label();
                    let condition_metadata = ConditionMetadata {
                        jump_if_condition_is_true: false,
                        jump_target_when_true,
                        jump_target_when_false: loop_labels.next,
                    };
                    translate_condition_expr(
                        program,
                        referenced_tables,
                        predicate,
                        condition_metadata,
                        None,
                        syms,
                    )?;
                    program.resolve_label(jump_target_when_true, program.offset());
                }
            }

            Ok(())
        }
        SourceOperator::Nothing { .. } => Ok(()),
    }
}

/// SQLite (and so Limbo) processes joins as a nested loop.
/// The loop may emit rows to various destinations depending on the query:
/// - a GROUP BY sorter (grouping is done by sorting based on the GROUP BY keys and aggregating while the GROUP BY keys match)
/// - an ORDER BY sorter (when there is no GROUP BY, but there is an ORDER BY)
/// - an AggStep (the columns are collected for aggregation, which is finished later)
/// - a QueryResult (there is none of the above, so the loop either emits a ResultRow, or if it's a subquery, yields to the parent query)
pub enum LoopEmitTarget<'a> {
    GroupBySorter {
        group_by: &'a GroupBy,
        aggregates: &'a Vec<Aggregate>,
    },
    OrderBySorter {
        order_by: &'a Vec<(ast::Expr, Direction)>,
    },
    AggStep,
    QueryResult {
        query_type: &'a SelectQueryType,
        limit: Option<usize>,
    },
}

/// Emits the bytecode for the inner loop of a query.
/// At this point the cursors for all tables have been opened and rewound.
fn emit_loop(
    program: &mut ProgramBuilder,
    plan: &mut SelectPlan,
    t_ctx: &mut TranslateCtx,
    syms: &SymbolTable,
) -> Result<()> {
    // if we have a group by, we emit a record into the group by sorter.
    if let Some(group_by) = &plan.group_by {
        return emit_loop_source(
            program,
            &plan.result_columns,
            &plan.aggregates,
            t_ctx,
            LoopEmitTarget::GroupBySorter {
                group_by,
                aggregates: &plan.aggregates,
            },
            &plan.referenced_tables,
            syms,
        );
    }
    // if we DONT have a group by, but we have aggregates, we emit without ResultRow.
    // we also do not need to sort because we are emitting a single row.
    if !plan.aggregates.is_empty() {
        return emit_loop_source(
            program,
            &plan.result_columns,
            &plan.aggregates,
            t_ctx,
            LoopEmitTarget::AggStep,
            &plan.referenced_tables,
            syms,
        );
    }
    // if we DONT have a group by, but we have an order by, we emit a record into the order by sorter.
    if let Some(order_by) = &plan.order_by {
        return emit_loop_source(
            program,
            &plan.result_columns,
            &plan.aggregates,
            t_ctx,
            LoopEmitTarget::OrderBySorter { order_by },
            &plan.referenced_tables,
            syms,
        );
    }
    // if we have neither, we emit a ResultRow. In that case, if we have a Limit, we handle that with DecrJumpZero.
    emit_loop_source(
        program,
        &plan.result_columns,
        &plan.aggregates,
        t_ctx,
        LoopEmitTarget::QueryResult {
            query_type: &plan.query_type,
            limit: plan.limit,
        },
        &plan.referenced_tables,
        syms,
    )
}

/// This is a helper function for inner_loop_emit,
/// which does a different thing depending on the emit target.
/// See the InnerLoopEmitTarget enum for more details.
fn emit_loop_source(
    program: &mut ProgramBuilder,
    result_columns: &[ResultSetColumn],
    aggregates: &[Aggregate],
    t_ctx: &mut TranslateCtx,
    emit_target: LoopEmitTarget,
    referenced_tables: &[TableReference],
    syms: &SymbolTable,
) -> Result<()> {
    match emit_target {
        LoopEmitTarget::GroupBySorter {
            group_by,
            aggregates,
        } => {
            let sort_keys_count = group_by.exprs.len();
            let aggregate_arguments_count =
                aggregates.iter().map(|agg| agg.args.len()).sum::<usize>();
            let column_count = sort_keys_count + aggregate_arguments_count;
            let start_reg = program.alloc_registers(column_count);
            let mut cur_reg = start_reg;

            // The group by sorter rows will contain the grouping keys first. They are also the sort keys.
            for expr in group_by.exprs.iter() {
                let key_reg = cur_reg;
                cur_reg += 1;
                translate_expr(program, Some(referenced_tables), expr, key_reg, None, syms)?;
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
                    translate_expr(program, Some(referenced_tables), expr, agg_reg, None, syms)?;
                }
            }

            // TODO: although it's less often useful, SQLite does allow for expressions in the SELECT that are not part of a GROUP BY or aggregate.
            // We currently ignore those and only emit the GROUP BY keys and aggregate arguments. This should be fixed.

            let group_by_metadata = t_ctx.group_by_metadata.as_ref().unwrap();

            sorter_insert(
                program,
                start_reg,
                column_count,
                group_by_metadata.sort_cursor,
                group_by_metadata.reg_sorter_key,
            );

            Ok(())
        }
        LoopEmitTarget::OrderBySorter { order_by } => {
            order_by_sorter_insert(
                program,
                referenced_tables,
                order_by,
                result_columns,
                &mut t_ctx.result_column_indexes_in_orderby_sorter,
                t_ctx.sort_metadata.as_ref().unwrap(),
                None,
                syms,
            )?;
            Ok(())
        }
        LoopEmitTarget::AggStep => {
            let num_aggs = aggregates.len();
            let start_reg = program.alloc_registers(num_aggs);
            t_ctx.aggregation_start_register = Some(start_reg);

            // In planner.rs, we have collected all aggregates from the SELECT clause, including ones where the aggregate is embedded inside
            // a more complex expression. Some examples: length(sum(x)), sum(x) + avg(y), sum(x) + 1, etc.
            // The result of those more complex expressions depends on the final result of the aggregate, so we don't translate the complete expressions here.
            // Instead, we translate the aggregates + any expressions that do not contain aggregates.
            for (i, agg) in aggregates.iter().enumerate() {
                let reg = start_reg + i;
                translate_aggregation(program, referenced_tables, agg, reg, syms)?;
            }
            for (i, rc) in result_columns.iter().enumerate() {
                if rc.contains_aggregates {
                    // Do nothing, aggregates are computed above
                    // if this result column is e.g. something like sum(x) + 1 or length(sum(x)), we do not want to translate that (+1) or length() yet,
                    // it will be computed after the aggregations are finalized.
                    continue;
                }
                let reg = start_reg + num_aggs + i;
                translate_expr(program, Some(referenced_tables), &rc.expr, reg, None, syms)?;
            }
            Ok(())
        }
        LoopEmitTarget::QueryResult { query_type, limit } => {
            assert!(
                aggregates.is_empty(),
                "We should not get here with aggregates"
            );
            emit_select_result(
                program,
                referenced_tables,
                result_columns,
                t_ctx.result_column_start_register.unwrap(),
                None,
                limit.map(|l| {
                    (
                        l,
                        t_ctx.limit_reg.unwrap(),
                        t_ctx.after_main_loop_label.unwrap(),
                    )
                }),
                syms,
                query_type,
            )?;

            Ok(())
        }
    }
}

/// Closes the loop for a given source operator.
/// For example in the case of a nested table scan, this means emitting the NextAsync instruction
/// for all tables involved, innermost first.
fn close_loop(
    program: &mut ProgramBuilder,
    source: &SourceOperator,
    t_ctx: &mut TranslateCtx,
) -> Result<()> {
    let loop_labels = *t_ctx
        .loop_labels
        .get(&source.id())
        .expect("source has no loop labels");
    match source {
        SourceOperator::Subquery { .. } => {
            program.resolve_label(loop_labels.next, program.offset());
            // A subquery has no cursor to call NextAsync on, so it just emits a Goto
            // to the Yield instruction, which in turn jumps back to the main loop of the subquery,
            // so that the next row from the subquery can be read.
            program.emit_insn_with_label_dependency(
                Insn::Goto {
                    target_pc: loop_labels.loop_start,
                },
                loop_labels.loop_start,
            );
        }
        SourceOperator::Join {
            id,
            left,
            right,
            outer,
            ..
        } => {
            close_loop(program, right, t_ctx)?;

            if *outer {
                let lj_meta = t_ctx.left_joins.get(id).unwrap();
                // The left join match flag is set to 1 when there is any match on the right table
                // (e.g. SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.a).
                // If the left join match flag has been set to 1, we jump to the next row on the outer table,
                // i.e. continue to the next row of t1 in our example.
                program.resolve_label(lj_meta.check_match_flag_label, program.offset());
                let jump_offset = program.offset() + 3;
                program.emit_insn(Insn::IfPos {
                    reg: lj_meta.match_flag_register,
                    target_pc: jump_offset,
                    decrement_by: 0,
                });
                // If the left join match flag is still 0, it means there was no match on the right table,
                // but since it's a LEFT JOIN, we still need to emit a row with NULLs for the right table.
                // In that case, we now enter the routine that does exactly that.
                // First we set the right table cursor's "pseudo null bit" on, which means any Insn::Column will return NULL
                let right_cursor_id = match right.as_ref() {
                    SourceOperator::Scan {
                        table_reference, ..
                    } => program.resolve_cursor_id(&table_reference.table_identifier),
                    SourceOperator::Search {
                        table_reference, ..
                    } => program.resolve_cursor_id(&table_reference.table_identifier),
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
                program.emit_insn_with_label_dependency(
                    Insn::Goto {
                        target_pc: lj_meta.set_match_flag_true_label,
                    },
                    lj_meta.set_match_flag_true_label,
                );

                assert!(program.offset() == jump_offset);
            }

            close_loop(program, left, t_ctx)?;
        }
        SourceOperator::Scan {
            table_reference,
            iter_dir,
            ..
        } => {
            let cursor_id = program.resolve_cursor_id(&table_reference.table_identifier);
            program.resolve_label(loop_labels.next, program.offset());
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
                program.emit_insn_with_label_dependency(
                    Insn::PrevAwait {
                        cursor_id,
                        pc_if_next: loop_labels.loop_start,
                    },
                    loop_labels.loop_start,
                );
            } else {
                program.emit_insn_with_label_dependency(
                    Insn::NextAwait {
                        cursor_id,
                        pc_if_next: loop_labels.loop_start,
                    },
                    loop_labels.loop_start,
                );
            }
        }
        SourceOperator::Search {
            table_reference,
            search,
            ..
        } => {
            program.resolve_label(loop_labels.next, program.offset());
            if matches!(search, Search::RowidEq { .. }) {
                // Rowid equality point lookups are handled with a SeekRowid instruction which does not loop, so there is no need to emit a NextAsync instruction.
                return Ok(());
            }
            let cursor_id = match search {
                Search::IndexSearch { index, .. } => program.resolve_cursor_id(&index.name),
                Search::RowidSearch { .. } => {
                    program.resolve_cursor_id(&table_reference.table_identifier)
                }
                Search::RowidEq { .. } => unreachable!(),
            };

            program.emit_insn(Insn::NextAsync { cursor_id });
            program.emit_insn_with_label_dependency(
                Insn::NextAwait {
                    cursor_id,
                    pc_if_next: loop_labels.loop_start,
                },
                loop_labels.loop_start,
            );
        }
        SourceOperator::Nothing { .. } => {}
    };

    program.resolve_label(loop_labels.loop_end, program.offset());
    Ok(())
}

fn emit_delete_insns(
    program: &mut ProgramBuilder,
    source: &SourceOperator,
    limit: &Option<usize>,
    t_ctx: &TranslateCtx,
) -> Result<()> {
    let cursor_id = match source {
        SourceOperator::Scan {
            table_reference, ..
        } => program.resolve_cursor_id(&table_reference.table_identifier),
        SourceOperator::Search {
            table_reference,
            search,
            ..
        } => match search {
            Search::RowidEq { .. } | Search::RowidSearch { .. } => {
                program.resolve_cursor_id(&table_reference.table_identifier)
            }
            Search::IndexSearch { index, .. } => program.resolve_cursor_id(&index.name),
        },
        _ => return Ok(()),
    };

    // Emit the instructions to delete the row
    let key_reg = program.alloc_register();
    program.emit_insn(Insn::RowId {
        cursor_id,
        dest: key_reg,
    });
    program.emit_insn(Insn::DeleteAsync { cursor_id });
    program.emit_insn(Insn::DeleteAwait { cursor_id });
    if let Some(limit) = limit {
        let limit_reg = program.alloc_register();
        program.emit_insn(Insn::Integer {
            value: *limit as i64,
            dest: limit_reg,
        });
        program.mark_last_insn_constant();
        program.emit_insn_with_label_dependency(
            Insn::DecrJumpZero {
                reg: limit_reg,
                target_pc: t_ctx.after_main_loop_label.unwrap(),
            },
            t_ctx.after_main_loop_label.unwrap(),
        )
    }

    Ok(())
}

/// Emits the bytecode for processing a GROUP BY clause.
/// This is called when the main query execution loop has finished processing,
/// and we now have data in the GROUP BY sorter.
#[allow(clippy::too_many_arguments)]
fn emit_group_by(
    program: &mut ProgramBuilder,
    result_columns: &[ResultSetColumn],
    group_by: &GroupBy,
    order_by: Option<&Vec<(ast::Expr, Direction)>>,
    aggregates: &[Aggregate],
    limit: Option<usize>,
    referenced_tables: &[TableReference],
    t_ctx: &mut TranslateCtx,
    syms: &SymbolTable,
    query_type: &SelectQueryType,
) -> Result<()> {
    // Label for the first instruction of the grouping loop.
    // This is the start of the loop that reads the sorted data and groups&aggregates it.
    let label_grouping_loop_start = program.allocate_label();
    // Label for the instruction immediately after the grouping loop.
    let label_grouping_loop_end = program.allocate_label();
    // Label for the instruction where a row for a finished group is output.
    // Distinct from subroutine_accumulator_output_label, which is the start of the subroutine, but may still skip emitting a row.
    let label_agg_final = program.allocate_label();
    // Label for the instruction immediately after the entire group by phase.
    let label_group_by_end = program.allocate_label();
    // Label for the beginning of the subroutine that potentially outputs a row for a finished group.
    let label_subrtn_acc_output = program.allocate_label();
    // Register holding the return offset of the subroutine that potentially outputs a row for a finished group.
    let reg_subrtn_acc_output_return_offset = program.alloc_register();
    // Register holding a boolean indicating whether there's data in the accumulator (used for aggregation)
    let reg_data_in_acc_flag = program.alloc_register();

    let group_by_metadata = t_ctx.group_by_metadata.as_mut().unwrap();
    let GroupByMetadata {
        reg_group_exprs_cmp,
        reg_subrtn_acc_clear_return_offset,
        reg_group_exprs_acc,
        reg_abort_flag,
        reg_sorter_key,
        label_subrtn_acc_clear,
        label_acc_indicator_set_flag_true,
        ..
    } = *group_by_metadata;

    // all group by columns and all arguments of agg functions are in the sorter.
    // the sort keys are the group by columns (the aggregation within groups is done based on how long the sort keys remain the same)
    let sorter_column_count =
        group_by.exprs.len() + aggregates.iter().map(|agg| agg.args.len()).sum::<usize>();
    // sorter column names do not matter
    let pseudo_columns = (0..sorter_column_count)
        .map(|i| Column {
            name: i.to_string(),
            primary_key: false,
            ty: crate::schema::Type::Null,
            is_rowid_alias: false,
        })
        .collect::<Vec<_>>();

    // A pseudo table is a "fake" table to which we read one row at a time from the sorter
    let pseudo_table = Rc::new(PseudoTable {
        columns: pseudo_columns,
    });

    let pseudo_cursor = program.alloc_cursor_id(None, Some(Table::Pseudo(pseudo_table.clone())));

    program.emit_insn(Insn::OpenPseudo {
        cursor_id: pseudo_cursor,
        content_reg: reg_sorter_key,
        num_fields: sorter_column_count,
    });

    // Sort the sorter based on the group by columns
    program.emit_insn_with_label_dependency(
        Insn::SorterSort {
            cursor_id: group_by_metadata.sort_cursor,
            pc_if_empty: label_grouping_loop_end,
        },
        label_grouping_loop_end,
    );

    program.defer_label_resolution(label_grouping_loop_start, program.offset() as usize);
    // Read a row from the sorted data in the sorter into the pseudo cursor
    program.emit_insn(Insn::SorterData {
        cursor_id: group_by_metadata.sort_cursor,
        dest_reg: group_by_metadata.reg_sorter_key,
        pseudo_cursor,
    });

    // Read the group by columns from the pseudo cursor
    let groups_start_reg = program.alloc_registers(group_by.exprs.len());
    for i in 0..group_by.exprs.len() {
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
        start_reg_a: reg_group_exprs_cmp,
        start_reg_b: groups_start_reg,
        count: group_by.exprs.len(),
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
        dest_reg: reg_group_exprs_cmp,
        count: group_by.exprs.len(),
    });

    program.add_comment(
        program.offset(),
        "check if ended group had data, and output if so",
    );
    program.emit_insn_with_label_dependency(
        Insn::Gosub {
            target_pc: label_subrtn_acc_output,
            return_reg: reg_subrtn_acc_output_return_offset,
        },
        label_subrtn_acc_output,
    );

    program.add_comment(program.offset(), "check abort flag");
    program.emit_insn_with_label_dependency(
        Insn::IfPos {
            reg: reg_abort_flag,
            target_pc: label_group_by_end,
            decrement_by: 0,
        },
        label_group_by_end,
    );

    program.add_comment(program.offset(), "goto clear accumulator subroutine");
    program.emit_insn_with_label_dependency(
        Insn::Gosub {
            target_pc: label_subrtn_acc_clear,
            return_reg: reg_subrtn_acc_clear_return_offset,
        },
        label_subrtn_acc_clear,
    );

    // Accumulate the values into the aggregations
    program.resolve_label(agg_step_label, program.offset());
    let start_reg = t_ctx.aggregation_start_register.unwrap();
    let mut cursor_index = group_by.exprs.len();
    for (i, agg) in aggregates.iter().enumerate() {
        let agg_result_reg = start_reg + i;
        translate_aggregation_groupby(
            program,
            referenced_tables,
            pseudo_cursor,
            cursor_index,
            agg,
            agg_result_reg,
            syms,
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
            target_pc: label_acc_indicator_set_flag_true,
            reg: reg_data_in_acc_flag,
            null_reg: 0, // unused in this case
        },
        label_acc_indicator_set_flag_true,
    );

    // Read the group by columns for a finished group
    for i in 0..group_by.exprs.len() {
        let key_reg = reg_group_exprs_acc + i;
        let sorter_column_index = i;
        program.emit_insn(Insn::Column {
            cursor_id: pseudo_cursor,
            column: sorter_column_index,
            dest: key_reg,
        });
    }

    program.resolve_label(label_acc_indicator_set_flag_true, program.offset());
    program.add_comment(program.offset(), "indicate data in accumulator");
    program.emit_insn(Insn::Integer {
        value: 1,
        dest: reg_data_in_acc_flag,
    });

    program.emit_insn_with_label_dependency(
        Insn::SorterNext {
            cursor_id: group_by_metadata.sort_cursor,
            pc_if_next: label_grouping_loop_start,
        },
        label_grouping_loop_start,
    );

    program.resolve_label(label_grouping_loop_end, program.offset());

    program.add_comment(program.offset(), "emit row for final group");
    program.emit_insn_with_label_dependency(
        Insn::Gosub {
            target_pc: label_subrtn_acc_output,
            return_reg: reg_subrtn_acc_output_return_offset,
        },
        label_subrtn_acc_output,
    );

    program.add_comment(program.offset(), "group by finished");
    program.emit_insn_with_label_dependency(
        Insn::Goto {
            target_pc: label_group_by_end,
        },
        label_group_by_end,
    );
    program.emit_insn(Insn::Integer {
        value: 1,
        dest: group_by_metadata.reg_abort_flag,
    });
    program.emit_insn(Insn::Return {
        return_reg: reg_subrtn_acc_output_return_offset,
    });

    program.resolve_label(label_subrtn_acc_output, program.offset());

    program.add_comment(program.offset(), "output group by row subroutine start");
    program.emit_insn_with_label_dependency(
        Insn::IfPos {
            reg: reg_data_in_acc_flag,
            target_pc: label_agg_final,
            decrement_by: 0,
        },
        label_agg_final,
    );
    let group_by_end_without_emitting_row_label = program.allocate_label();
    program.defer_label_resolution(
        group_by_end_without_emitting_row_label,
        program.offset() as usize,
    );
    program.emit_insn(Insn::Return {
        return_reg: reg_subrtn_acc_output_return_offset,
    });

    let agg_start_reg = t_ctx.aggregation_start_register.unwrap();
    // Resolve the label for the start of the group by output row subroutine
    program.resolve_label(label_agg_final, program.offset());
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
    let mut precomputed_exprs_to_register =
        Vec::with_capacity(aggregates.len() + group_by.exprs.len());
    for (i, expr) in group_by.exprs.iter().enumerate() {
        precomputed_exprs_to_register.push((expr, reg_group_exprs_acc + i));
    }
    for (i, agg) in aggregates.iter().enumerate() {
        precomputed_exprs_to_register.push((&agg.original_expr, agg_start_reg + i));
    }

    if let Some(having) = &group_by.having {
        for expr in having.iter() {
            translate_condition_expr(
                program,
                referenced_tables,
                expr,
                ConditionMetadata {
                    jump_if_condition_is_true: false,
                    jump_target_when_false: group_by_end_without_emitting_row_label,
                    jump_target_when_true: i64::MAX, // unused
                },
                Some(&precomputed_exprs_to_register),
                syms,
            )?;
        }
    }

    match order_by {
        None => {
            emit_select_result(
                program,
                referenced_tables,
                result_columns,
                t_ctx.result_column_start_register.unwrap(),
                Some(&precomputed_exprs_to_register),
                limit.map(|l| (l, t_ctx.limit_reg.unwrap(), label_group_by_end)),
                syms,
                query_type,
            )?;
        }
        Some(order_by) => {
            order_by_sorter_insert(
                program,
                referenced_tables,
                order_by,
                result_columns,
                &mut t_ctx.result_column_indexes_in_orderby_sorter,
                t_ctx.sort_metadata.as_ref().unwrap(),
                Some(&precomputed_exprs_to_register),
                syms,
            )?;
        }
    }

    program.emit_insn(Insn::Return {
        return_reg: reg_subrtn_acc_output_return_offset,
    });

    program.add_comment(program.offset(), "clear accumulator subroutine start");
    program.resolve_label(group_by_metadata.label_subrtn_acc_clear, program.offset());
    let start_reg = group_by_metadata.reg_group_exprs_acc;
    program.emit_insn(Insn::Null {
        dest: start_reg,
        dest_end: Some(start_reg + group_by.exprs.len() + aggregates.len() - 1),
    });

    program.emit_insn(Insn::Integer {
        value: 0,
        dest: reg_data_in_acc_flag,
    });
    program.emit_insn(Insn::Return {
        return_reg: reg_subrtn_acc_clear_return_offset,
    });

    program.resolve_label(label_group_by_end, program.offset());

    Ok(())
}

/// Emits the bytecode for processing an aggregate without a GROUP BY clause.
/// This is called when the main query execution loop has finished processing,
/// and we can now materialize the aggregate results.
fn emit_ungrouped_aggregation(
    program: &mut ProgramBuilder,
    referenced_tables: &[TableReference],
    result_columns: &[ResultSetColumn],
    aggregates: &[Aggregate],
    t_ctx: &mut TranslateCtx,
    syms: &SymbolTable,
    query_type: &SelectQueryType,
) -> Result<()> {
    let agg_start_reg = t_ctx.aggregation_start_register.unwrap();
    for (i, agg) in aggregates.iter().enumerate() {
        let agg_result_reg = agg_start_reg + i;
        program.emit_insn(Insn::AggFinal {
            register: agg_result_reg,
            func: agg.func.clone(),
        });
    }
    // we now have the agg results in (agg_start_reg..agg_start_reg + aggregates.len() - 1)
    // we need to call translate_expr on each result column, but replace the expr with a register copy in case any part of the
    // result column expression matches a) a group by column or b) an aggregation result.
    let mut precomputed_exprs_to_register = Vec::with_capacity(aggregates.len());
    for (i, agg) in aggregates.iter().enumerate() {
        precomputed_exprs_to_register.push((&agg.original_expr, agg_start_reg + i));
    }

    // This always emits a ResultRow because currently it can only be used for a single row result
    // Limit is None because we early exit on limit 0 and the max rows here is 1
    emit_select_result(
        program,
        referenced_tables,
        result_columns,
        t_ctx.result_column_start_register.unwrap(),
        Some(&precomputed_exprs_to_register),
        None,
        syms,
        query_type,
    )?;

    Ok(())
}

/// Emits the bytecode for outputting rows from an ORDER BY sorter.
/// This is called when the main query execution loop has finished processing,
/// and we can now emit rows from the ORDER BY sorter.
fn emit_order_by(
    program: &mut ProgramBuilder,
    order_by: &[(ast::Expr, Direction)],
    result_columns: &[ResultSetColumn],
    limit: Option<usize>,
    t_ctx: &mut TranslateCtx,
    query_type: &SelectQueryType,
) -> Result<()> {
    let sort_loop_start_label = program.allocate_label();
    let sort_loop_end_label = program.allocate_label();
    let mut pseudo_columns = vec![];
    for (i, _) in order_by.iter().enumerate() {
        pseudo_columns.push(Column {
            // Names don't matter. We are tracking which result column is in which position in the ORDER BY clause in m.result_column_indexes_in_orderby_sorter.
            name: format!("sort_key_{}", i),
            primary_key: false,
            ty: crate::schema::Type::Null,
            is_rowid_alias: false,
        });
    }
    for (i, rc) in result_columns.iter().enumerate() {
        // If any result columns are not in the ORDER BY sorter, it's because they are equal to a sort key and were already added to the pseudo columns above.
        if let Some(ref v) = t_ctx.result_columns_to_skip_in_orderby_sorter {
            if v.contains(&i) {
                continue;
            }
        }
        pseudo_columns.push(Column {
            name: rc.expr.to_string(),
            primary_key: false,
            ty: crate::schema::Type::Null,
            is_rowid_alias: false,
        });
    }

    let num_columns_in_sorter = order_by.len() + result_columns.len()
        - t_ctx
            .result_columns_to_skip_in_orderby_sorter
            .as_ref()
            .map(|v| v.len())
            .unwrap_or(0);

    let pseudo_cursor = program.alloc_cursor_id(
        None,
        Some(Table::Pseudo(Rc::new(PseudoTable {
            columns: pseudo_columns,
        }))),
    );
    let sort_metadata = t_ctx.sort_metadata.as_mut().unwrap();

    program.emit_insn(Insn::OpenPseudo {
        cursor_id: pseudo_cursor,
        content_reg: sort_metadata.sorter_data_register,
        num_fields: num_columns_in_sorter,
    });

    program.emit_insn_with_label_dependency(
        Insn::SorterSort {
            cursor_id: sort_metadata.sort_cursor,
            pc_if_empty: sort_loop_end_label,
        },
        sort_loop_end_label,
    );

    program.defer_label_resolution(sort_loop_start_label, program.offset() as usize);
    program.emit_insn(Insn::SorterData {
        cursor_id: sort_metadata.sort_cursor,
        dest_reg: sort_metadata.sorter_data_register,
        pseudo_cursor,
    });

    // We emit the columns in SELECT order, not sorter order (sorter always has the sort keys first).
    // This is tracked in m.result_column_indexes_in_orderby_sorter.
    let cursor_id = pseudo_cursor;
    let start_reg = t_ctx.result_column_start_register.unwrap();
    for i in 0..result_columns.len() {
        let reg = start_reg + i;
        program.emit_insn(Insn::Column {
            cursor_id,
            column: t_ctx.result_column_indexes_in_orderby_sorter[&i],
            dest: reg,
        });
    }

    emit_result_row_and_limit(
        program,
        start_reg,
        result_columns.len(),
        limit.map(|l| (l, t_ctx.limit_reg.unwrap(), sort_loop_end_label)),
        query_type,
    )?;

    program.emit_insn_with_label_dependency(
        Insn::SorterNext {
            cursor_id: sort_metadata.sort_cursor,
            pc_if_next: sort_loop_start_label,
        },
        sort_loop_start_label,
    );

    program.resolve_label(sort_loop_end_label, program.offset());

    Ok(())
}

/// Emits the bytecode for:
/// - result row (or if a subquery, yields to the parent query)
/// - limit
fn emit_result_row_and_limit(
    program: &mut ProgramBuilder,
    start_reg: usize,
    result_columns_len: usize,
    limit: Option<(usize, usize, BranchOffset)>,
    query_type: &SelectQueryType,
) -> Result<()> {
    match query_type {
        SelectQueryType::TopLevel => {
            program.emit_insn(Insn::ResultRow {
                start_reg,
                count: result_columns_len,
            });
        }
        SelectQueryType::Subquery { yield_reg, .. } => {
            program.emit_insn(Insn::Yield {
                yield_reg: *yield_reg,
                end_offset: 0,
            });
        }
    }

    if let Some((limit, limit_reg, jump_label_on_limit_reached)) = limit {
        program.emit_insn(Insn::Integer {
            value: limit as i64,
            dest: limit_reg,
        });
        program.mark_last_insn_constant();
        program.emit_insn_with_label_dependency(
            Insn::DecrJumpZero {
                reg: limit_reg,
                target_pc: jump_label_on_limit_reached,
            },
            jump_label_on_limit_reached,
        );
    }
    Ok(())
}

/// Emits the bytecode for:
/// - all result columns
/// - result row (or if a subquery, yields to the parent query)
/// - limit
fn emit_select_result(
    program: &mut ProgramBuilder,
    referenced_tables: &[TableReference],
    result_columns: &[ResultSetColumn],
    result_column_start_register: usize,
    precomputed_exprs_to_register: Option<&Vec<(&ast::Expr, usize)>>,
    limit: Option<(usize, usize, BranchOffset)>,
    syms: &SymbolTable,
    query_type: &SelectQueryType,
) -> Result<()> {
    let start_reg = result_column_start_register;
    for (i, rc) in result_columns.iter().enumerate() {
        let reg = start_reg + i;
        translate_expr(
            program,
            Some(referenced_tables),
            &rc.expr,
            reg,
            precomputed_exprs_to_register,
            syms,
        )?;
    }
    emit_result_row_and_limit(program, start_reg, result_columns.len(), limit, query_type)?;
    Ok(())
}

/// Emits the bytecode for inserting a row into a sorter.
/// This can be either a GROUP BY sorter or an ORDER BY sorter.
fn sorter_insert(
    program: &mut ProgramBuilder,
    start_reg: usize,
    column_count: usize,
    cursor_id: usize,
    record_reg: usize,
) {
    program.emit_insn(Insn::MakeRecord {
        start_reg,
        count: column_count,
        dest_reg: record_reg,
    });
    program.emit_insn(Insn::SorterInsert {
        cursor_id,
        record_reg,
    });
}

/// Emits the bytecode for inserting a row into an ORDER BY sorter.
fn order_by_sorter_insert(
    program: &mut ProgramBuilder,
    referenced_tables: &[TableReference],
    order_by: &[(ast::Expr, Direction)],
    result_columns: &[ResultSetColumn],
    result_column_indexes_in_orderby_sorter: &mut HashMap<usize, usize>,
    sort_metadata: &SortMetadata,
    precomputed_exprs_to_register: Option<&Vec<(&ast::Expr, usize)>>,
    syms: &SymbolTable,
) -> Result<()> {
    let order_by_len = order_by.len();
    // If any result columns can be skipped due to being an exact duplicate of a sort key, we need to know which ones and their new index in the ORDER BY sorter.
    let result_columns_to_skip = order_by_deduplicate_result_columns(order_by, result_columns);
    let result_columns_to_skip_len = result_columns_to_skip
        .as_ref()
        .map(|v| v.len())
        .unwrap_or(0);

    // The ORDER BY sorter has the sort keys first, then the result columns.
    let orderby_sorter_column_count =
        order_by_len + result_columns.len() - result_columns_to_skip_len;
    let start_reg = program.alloc_registers(orderby_sorter_column_count);
    for (i, (expr, _)) in order_by.iter().enumerate() {
        let key_reg = start_reg + i;
        translate_expr(
            program,
            Some(referenced_tables),
            expr,
            key_reg,
            precomputed_exprs_to_register,
            syms,
        )?;
    }
    let mut cur_reg = start_reg + order_by_len;
    let mut cur_idx_in_orderby_sorter = order_by_len;
    for (i, rc) in result_columns.iter().enumerate() {
        if let Some(ref v) = result_columns_to_skip {
            let found = v.iter().find(|(skipped_idx, _)| *skipped_idx == i);
            // If the result column is in the list of columns to skip, we need to know its new index in the ORDER BY sorter.
            if let Some((_, result_column_idx)) = found {
                result_column_indexes_in_orderby_sorter.insert(i, *result_column_idx);
                continue;
            }
        }
        translate_expr(
            program,
            Some(referenced_tables),
            &rc.expr,
            cur_reg,
            precomputed_exprs_to_register,
            syms,
        )?;
        result_column_indexes_in_orderby_sorter.insert(i, cur_idx_in_orderby_sorter);
        cur_idx_in_orderby_sorter += 1;
        cur_reg += 1;
    }

    sorter_insert(
        program,
        start_reg,
        orderby_sorter_column_count,
        sort_metadata.sort_cursor,
        sort_metadata.sorter_data_register,
    );
    Ok(())
}

/// In case any of the ORDER BY sort keys are exactly equal to a result column, we can skip emitting that result column.
/// If we skip a result column, we need to keep track what index in the ORDER BY sorter the result columns have,
/// because the result columns should be emitted in the SELECT clause order, not the ORDER BY clause order.
///
/// If any result columns can be skipped, this returns list of 2-tuples of (SkippedResultColumnIndex: usize, ResultColumnIndexInOrderBySorter: usize)
fn order_by_deduplicate_result_columns(
    order_by: &[(ast::Expr, Direction)],
    result_columns: &[ResultSetColumn],
) -> Option<Vec<(usize, usize)>> {
    let mut result_column_remapping: Option<Vec<(usize, usize)>> = None;
    for (i, rc) in result_columns.iter().enumerate() {
        let found = order_by
            .iter()
            .enumerate()
            .find(|(_, (expr, _))| exprs_are_equivalent(expr, &rc.expr));
        if let Some((j, _)) = found {
            if let Some(ref mut v) = result_column_remapping {
                v.push((i, j));
            } else {
                result_column_remapping = Some(vec![(i, j)]);
            }
        }
    }

    result_column_remapping
}
