use crate::schema::Table;
use crate::translate::emitter::emit_program;
use crate::translate::optimizer::optimize_plan;
use crate::translate::plan::{DeletePlan, Plan, SourceOperator};
use crate::translate::planner::{parse_limit, parse_where};
use crate::vdbe::builder::ProgramBuilder;
use crate::{schema::Schema, Result, SymbolTable};
use sqlite3_parser::ast::{Expr, Limit, QualifiedName};

use super::plan::{TableReference, TableReferenceType};

pub fn translate_delete(
    program: &mut ProgramBuilder,
    schema: &Schema,
    tbl_name: &QualifiedName,
    where_clause: Option<Expr>,
    limit: Option<Box<Limit>>,
    syms: &SymbolTable,
) -> Result<()> {
    let mut delete_plan = prepare_delete_plan(schema, tbl_name, where_clause, limit)?;
    optimize_plan(&mut delete_plan)?;
    emit_program(program, delete_plan, syms)
}

pub fn prepare_delete_plan(
    schema: &Schema,
    tbl_name: &QualifiedName,
    where_clause: Option<Expr>,
    limit: Option<Box<Limit>>,
) -> Result<Plan> {
    let table = match schema.get_table(tbl_name.name.0.as_str()) {
        Some(table) => table,
        None => crate::bail_corrupt_error!("Parse error: no such table: {}", tbl_name),
    };

    let btree_table_ref = TableReference {
        table: Table::BTree(table.clone()),
        table_identifier: table.name.clone(),
        table_index: 0,
        reference_type: TableReferenceType::BTreeTable,
    };
    let referenced_tables = vec![btree_table_ref.clone()];

    // Parse the WHERE clause
    let resolved_where_clauses = parse_where(where_clause, &referenced_tables)?;

    // Parse the LIMIT clause
    let resolved_limit = limit.and_then(|l| parse_limit(*l));

    let plan = DeletePlan {
        source: SourceOperator::Scan {
            id: 0,
            table_reference: btree_table_ref,
            predicates: resolved_where_clauses.clone(),
            iter_dir: None,
        },
        result_columns: vec![],
        where_clause: resolved_where_clauses,
        order_by: None,
        limit: resolved_limit,
        referenced_tables,
        available_indexes: vec![],
        contains_constant_false_condition: false,
    };

    Ok(Plan::Delete(plan))
}
