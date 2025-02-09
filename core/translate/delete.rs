use crate::schema::Table;
use crate::translate::emitter::emit_program;
use crate::translate::optimizer::optimize_plan;
use crate::translate::plan::{DeletePlan, Operation, Plan};
use crate::translate::planner::{parse_limit, parse_where};
use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts, QueryMode};
use crate::{schema::Schema, Result, SymbolTable};
use sqlite3_parser::ast::{Expr, Limit, QualifiedName};

use super::plan::TableReference;

pub fn translate_delete(
    query_mode: QueryMode,
    schema: &Schema,
    tbl_name: &QualifiedName,
    where_clause: Option<Box<Expr>>,
    limit: Option<Box<Limit>>,
    syms: &SymbolTable,
) -> Result<ProgramBuilder> {
    let mut delete_plan = prepare_delete_plan(schema, tbl_name, where_clause, limit)?;
    optimize_plan(&mut delete_plan, schema)?;
    let Plan::Delete(ref delete) = delete_plan else {
        panic!("delete_plan is not a DeletePlan");
    };
    let mut program = ProgramBuilder::new(ProgramBuilderOpts {
        query_mode,
        num_cursors: 1,
        approx_num_insns: estimate_num_instructions(&delete),
        approx_num_labels: 0,
    });
    emit_program(&mut program, delete_plan, syms)?;
    Ok(program)
}

pub fn prepare_delete_plan(
    schema: &Schema,
    tbl_name: &QualifiedName,
    where_clause: Option<Box<Expr>>,
    limit: Option<Box<Limit>>,
) -> Result<Plan> {
    let table = match schema.get_table(tbl_name.name.0.as_str()) {
        Some(table) => table,
        None => crate::bail_corrupt_error!("Parse error: no such table: {}", tbl_name),
    };

    let table_references = vec![TableReference {
        table: Table::BTree(table.clone()),
        identifier: table.name.clone(),
        op: Operation::Scan { iter_dir: None },
        join_info: None,
    }];

    let mut where_predicates = vec![];

    // Parse the WHERE clause
    parse_where(
        where_clause.map(|e| *e),
        &table_references,
        None,
        &mut where_predicates,
    )?;

    // Parse the LIMIT/OFFSET clause
    let (resolved_limit, resolved_offset) = limit.map_or(Ok((None, None)), |l| parse_limit(*l))?;

    let plan = DeletePlan {
        table_references,
        result_columns: vec![],
        where_clause: where_predicates,
        order_by: None,
        limit: resolved_limit,
        offset: resolved_offset,
        contains_constant_false_condition: false,
    };

    Ok(Plan::Delete(plan))
}

fn estimate_num_instructions(plan: &DeletePlan) -> usize {
    let base = 20;

    let num_instructions = base + plan.table_references.len() * 10;

    num_instructions
}
