use crate::translate::emitter::emit_program;
use crate::translate::optimizer::optimize_plan;
use crate::translate::planner::prepare_delete_plan;
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
