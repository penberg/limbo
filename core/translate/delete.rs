use crate::translate::emitter::emit_program;
use crate::translate::optimizer::optimize_plan;
use crate::translate::plan::{BTreeTableReference, DeletePlan, Plan, SourceOperator};
use crate::translate::planner::{parse_limit, parse_where};
use crate::{schema::Schema, storage::sqlite3_ondisk::DatabaseHeader, vdbe::Program};
use crate::{Connection, Result};
use sqlite3_parser::ast::{Expr, Limit, QualifiedName};
use std::rc::Weak;
use std::{cell::RefCell, rc::Rc};

pub fn translate_delete(
    schema: &Schema,
    tbl_name: &QualifiedName,
    where_clause: Option<Expr>,
    limit: Option<Limit>,
    database_header: Rc<RefCell<DatabaseHeader>>,
    connection: Weak<Connection>,
) -> Result<Program> {
    let delete_plan = prepare_delete_plan(schema, tbl_name, where_clause, limit)?;
    let optimized_plan = optimize_plan(delete_plan)?;
    emit_program(database_header, optimized_plan, connection)
}

pub fn prepare_delete_plan(
    schema: &Schema,
    tbl_name: &QualifiedName,
    where_clause: Option<Expr>,
    limit: Option<Limit>,
) -> Result<Plan> {
    let table = match schema.get_table(tbl_name.name.0.as_str()) {
        Some(table) => table,
        None => crate::bail_corrupt_error!("Parse error: no such table: {}", tbl_name),
    };

    let table_ref = BTreeTableReference {
        table: table.clone(),
        table_identifier: table.name.clone(),
        table_index: 0,
    };
    let referenced_tables = vec![table_ref.clone()];

    // Parse the WHERE clause
    let resolved_where_clauses = parse_where(where_clause, &[table_ref.clone()])?;

    // Parse the LIMIT clause
    let resolved_limit = limit.and_then(parse_limit);

    let plan = DeletePlan {
        source: SourceOperator::Scan {
            id: 0,
            table_reference: table_ref.clone(),
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
