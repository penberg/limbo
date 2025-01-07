use std::collections::HashMap;

use crate::{
    vdbe::{builder::ProgramBuilder, insn::Insn},
    Result,
};

use super::{
    emitter::{emit_query, Resolver, TranslateCtx},
    plan::{SelectPlan, SelectQueryType, SourceOperator, TableReference, TableReferenceType},
};

/// Emit the subqueries contained in the FROM clause.
/// This is done first so the results can be read in the main query loop.
pub fn emit_subqueries<'a>(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    referenced_tables: &mut [TableReference],
    source: &mut SourceOperator,
) -> Result<()> {
    match source {
        SourceOperator::Subquery {
            table_reference,
            plan,
            ..
        } => {
            // Emit the subquery and get the start register of the result columns.
            let result_columns_start = emit_subquery(program, plan, t_ctx)?;
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
            emit_subqueries(program, t_ctx, referenced_tables, left)?;
            emit_subqueries(program, t_ctx, referenced_tables, right)?;
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
pub fn emit_subquery<'a>(
    program: &mut ProgramBuilder,
    plan: &mut SelectPlan,
    t_ctx: &mut TranslateCtx<'a>,
) -> Result<usize> {
    let yield_reg = program.alloc_register();
    let coroutine_implementation_start_offset = program.offset().add(1u32);
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
        labels_main_loop: HashMap::new(),
        label_main_loop_end: None,
        meta_group_by: None,
        meta_left_joins: HashMap::new(),
        meta_sort: None,
        reg_agg_start: None,
        reg_result_cols_start: None,
        result_column_indexes_in_orderby_sorter: HashMap::new(),
        result_columns_to_skip_in_orderby_sorter: None,
        reg_limit: plan.limit.map(|_| program.alloc_register()),
        resolver: Resolver::new(t_ctx.resolver.symbol_table),
    };
    let subquery_body_end_label = program.allocate_label();
    program.emit_insn(Insn::InitCoroutine {
        yield_reg,
        jump_on_definition: subquery_body_end_label,
        start_offset: coroutine_implementation_start_offset,
    });
    // Normally we mark each LIMIT value as a constant insn that is emitted only once, but in the case of a subquery,
    // we need to initialize it every time the subquery is run; otherwise subsequent runs of the subquery will already
    // have the LIMIT counter at 0, and will never return rows.
    if let Some(limit) = plan.limit {
        program.emit_insn(Insn::Integer {
            value: limit as i64,
            dest: metadata.reg_limit.unwrap(),
        });
    }
    let result_column_start_reg = emit_query(program, plan, &mut metadata)?;
    program.resolve_label(end_coroutine_label, program.offset());
    program.emit_insn(Insn::EndCoroutine { yield_reg });
    program.resolve_label(subquery_body_end_label, program.offset());
    Ok(result_column_start_reg)
}
