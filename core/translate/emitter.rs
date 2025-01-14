// This module contains code for emitting bytecode instructions for SQL query execution.
// It handles translating high-level SQL operations into low-level bytecode that can be executed by the virtual machine.

use std::collections::HashMap;

use sqlite3_parser::ast::{self};

use crate::function::Func;
use crate::translate::plan::{DeletePlan, Plan, Search};
use crate::util::exprs_are_equivalent;
use crate::vdbe::builder::ProgramBuilder;
use crate::vdbe::{insn::Insn, BranchOffset};
use crate::{Result, SymbolTable};

use super::aggregation::emit_ungrouped_aggregation;
use super::group_by::{emit_group_by, init_group_by, GroupByMetadata};
use super::main_loop::{close_loop, emit_loop, init_loop, open_loop, LeftJoinMetadata, LoopLabels};
use super::order_by::{emit_order_by, init_order_by, SortMetadata};
use super::plan::SelectPlan;
use super::plan::SourceOperator;
use super::subquery::emit_subqueries;

#[derive(Debug)]
pub struct Resolver<'a> {
    pub symbol_table: &'a SymbolTable,
    pub expr_to_reg_cache: Vec<(&'a ast::Expr, usize)>,
}

impl<'a> Resolver<'a> {
    pub fn new(symbol_table: &'a SymbolTable) -> Self {
        Self {
            symbol_table,
            expr_to_reg_cache: Vec::new(),
        }
    }

    pub fn resolve_function(&self, func_name: &str, arg_count: usize) -> Option<Func> {
        let func_type = match Func::resolve_function(&func_name, arg_count).ok() {
            Some(func) => Some(func),
            None => self
                .symbol_table
                .resolve_function(&func_name, arg_count)
                .map(|func| Func::External(func)),
        };
        func_type
    }

    pub fn resolve_cached_expr_reg(&self, expr: &ast::Expr) -> Option<usize> {
        self.expr_to_reg_cache
            .iter()
            .find(|(e, _)| exprs_are_equivalent(expr, e))
            .map(|(_, reg)| *reg)
    }
}

/// The TranslateCtx struct holds various information and labels used during bytecode generation.
/// It is used for maintaining state and control flow during the bytecode
/// generation process.
#[derive(Debug)]
pub struct TranslateCtx<'a> {
    // A typical query plan is a nested loop. Each loop has its own LoopLabels (see the definition of LoopLabels for more details)
    pub labels_main_loop: HashMap<usize, LoopLabels>,
    // label for the instruction that jumps to the next phase of the query after the main loop
    // we don't know ahead of time what that is (GROUP BY, ORDER BY, etc.)
    pub label_main_loop_end: Option<BranchOffset>,
    // First register of the aggregation results
    pub reg_agg_start: Option<usize>,
    // First register of the result columns of the query
    pub reg_result_cols_start: Option<usize>,
    // The register holding the limit value, if any.
    pub reg_limit: Option<usize>,
    // metadata for the group by operator
    pub meta_group_by: Option<GroupByMetadata>,
    // metadata for the order by operator
    pub meta_sort: Option<SortMetadata>,
    // mapping between Join operator id and associated metadata (for left joins only)
    pub meta_left_joins: HashMap<usize, LeftJoinMetadata>,
    // We need to emit result columns in the order they are present in the SELECT, but they may not be in the same order in the ORDER BY sorter.
    // This vector holds the indexes of the result columns in the ORDER BY sorter.
    pub result_column_indexes_in_orderby_sorter: HashMap<usize, usize>,
    // We might skip adding a SELECT result column into the ORDER BY sorter if it is an exact match in the ORDER BY keys.
    // This vector holds the indexes of the result columns that we need to skip.
    pub result_columns_to_skip_in_orderby_sorter: Option<Vec<usize>>,
    pub resolver: Resolver<'a>,
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
fn prologue<'a>(
    program: &mut ProgramBuilder,
    syms: &'a SymbolTable,
) -> Result<(TranslateCtx<'a>, BranchOffset, BranchOffset)> {
    let init_label = program.allocate_label();

    program.emit_insn(Insn::Init {
        target_pc: init_label,
    });

    let start_offset = program.offset();

    let t_ctx = TranslateCtx {
        labels_main_loop: HashMap::new(),
        label_main_loop_end: None,
        reg_agg_start: None,
        reg_limit: None,
        reg_result_cols_start: None,
        meta_group_by: None,
        meta_left_joins: HashMap::new(),
        meta_sort: None,
        result_column_indexes_in_orderby_sorter: HashMap::new(),
        result_columns_to_skip_in_orderby_sorter: None,
        resolver: Resolver::new(syms),
    };

    Ok((t_ctx, init_label, start_offset))
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

    Ok(())
}

/// Main entry point for emitting bytecode for a SQL query
/// Takes a query plan and generates the corresponding bytecode program
pub fn emit_program(program: &mut ProgramBuilder, plan: Plan, syms: &SymbolTable) -> Result<()> {
    match plan {
        Plan::Select(plan) => emit_program_for_select(program, plan, syms),
        Plan::Delete(plan) => emit_program_for_delete(program, plan, syms),
    }
}

fn emit_program_for_select(
    program: &mut ProgramBuilder,
    mut plan: SelectPlan,
    syms: &SymbolTable,
) -> Result<()> {
    let (mut t_ctx, init_label, start_offset) = prologue(program, syms)?;

    // Trivial exit on LIMIT 0
    if let Some(limit) = plan.limit {
        if limit == 0 {
            epilogue(program, init_label, start_offset)?;
        }
    }

    // Emit main parts of query
    emit_query(program, &mut plan, &mut t_ctx)?;

    // Finalize program
    epilogue(program, init_label, start_offset)?;

    Ok(())
}

pub fn emit_query<'a>(
    program: &'a mut ProgramBuilder,
    plan: &'a mut SelectPlan,
    t_ctx: &'a mut TranslateCtx<'a>,
) -> Result<usize> {
    // Emit subqueries first so the results can be read in the main query loop.
    emit_subqueries(
        program,
        t_ctx,
        &mut plan.referenced_tables,
        &mut plan.source,
    )?;

    if t_ctx.reg_limit.is_none() {
        t_ctx.reg_limit = plan.limit.map(|_| program.alloc_register());
    }

    // No rows will be read from source table loops if there is a constant false condition eg. WHERE 0
    // however an aggregation might still happen,
    // e.g. SELECT COUNT(*) WHERE 0 returns a row with 0, not an empty result set
    let after_main_loop_label = program.allocate_label();
    t_ctx.label_main_loop_end = Some(after_main_loop_label);
    if plan.contains_constant_false_condition {
        program.emit_insn(Insn::Goto {
            target_pc: after_main_loop_label,
        });
    }

    // Allocate registers for result columns
    t_ctx.reg_result_cols_start = Some(program.alloc_registers(plan.result_columns.len()));

    // Initialize cursors and other resources needed for query execution
    if let Some(ref mut order_by) = plan.order_by {
        init_order_by(program, t_ctx, order_by)?;
    }

    if let Some(ref mut group_by) = plan.group_by {
        init_group_by(program, t_ctx, group_by, &plan.aggregates)?;
    }
    init_loop(program, t_ctx, &plan.source, &OperationMode::SELECT)?;

    // Set up main query execution loop
    open_loop(program, t_ctx, &mut plan.source, &plan.referenced_tables)?;

    // Process result columns and expressions in the inner loop
    emit_loop(program, t_ctx, plan)?;

    // Clean up and close the main execution loop
    close_loop(program, t_ctx, &plan.source)?;

    program.resolve_label(after_main_loop_label, program.offset());

    let mut order_by_necessary = plan.order_by.is_some() && !plan.contains_constant_false_condition;
    let order_by = plan.order_by.as_ref();
    // Handle GROUP BY and aggregation processing
    if plan.group_by.is_some() {
        emit_group_by(program, t_ctx, plan)?;
    } else if !plan.aggregates.is_empty() {
        // Handle aggregation without GROUP BY
        emit_ungrouped_aggregation(program, t_ctx, plan)?;
        // Single row result for aggregates without GROUP BY, so ORDER BY not needed
        order_by_necessary = false;
    }

    // Process ORDER BY results if needed
    if order_by.is_some() && order_by_necessary {
        emit_order_by(program, t_ctx, plan)?;
    }

    Ok(t_ctx.reg_result_cols_start.unwrap())
}

fn emit_program_for_delete(
    program: &mut ProgramBuilder,
    mut plan: DeletePlan,
    syms: &SymbolTable,
) -> Result<()> {
    let (mut t_ctx, init_label, start_offset) = prologue(program, syms)?;

    // No rows will be read from source table loops if there is a constant false condition eg. WHERE 0
    let after_main_loop_label = program.allocate_label();
    if plan.contains_constant_false_condition {
        program.emit_insn(Insn::Goto {
            target_pc: after_main_loop_label,
        });
    }

    // Initialize cursors and other resources needed for query execution
    init_loop(program, &mut t_ctx, &plan.source, &OperationMode::DELETE)?;

    // Set up main query execution loop
    open_loop(
        program,
        &mut t_ctx,
        &mut plan.source,
        &plan.referenced_tables,
    )?;

    emit_delete_insns(program, &mut t_ctx, &plan.source, &plan.limit)?;

    // Clean up and close the main execution loop
    close_loop(program, &mut t_ctx, &plan.source)?;

    program.resolve_label(after_main_loop_label, program.offset());

    // Finalize program
    epilogue(program, init_label, start_offset)?;

    Ok(())
}

fn emit_delete_insns<'a>(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    source: &SourceOperator,
    limit: &Option<usize>,
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
        program.emit_insn(Insn::DecrJumpZero {
            reg: limit_reg,
            target_pc: t_ctx.label_main_loop_end.unwrap(),
        })
    }

    Ok(())
}
