use crate::translate::emitter::emit_program_for_delete;
use crate::translate::optimizer::optimize_delete_plan;
use crate::translate::planner::prepare_delete_plan;
use crate::{
    schema::Schema,
    storage::sqlite3_ondisk::DatabaseHeader,
    vdbe::Program,
};
use crate::{Connection, Result};
use sqlite3_parser::ast::{Expr, QualifiedName, ResultColumn};
use std::rc::Weak;
use std::{cell::RefCell, rc::Rc};

pub fn translate_delete(
    schema: &Schema,
    tbl_name: &QualifiedName,
    where_clause: Option<Expr>,
    _returning: &Option<Vec<ResultColumn>>,
    database_header: Rc<RefCell<DatabaseHeader>>,
    connection: Weak<Connection>,
) -> Result<Program> {
    let delete_plan = prepare_delete_plan(schema, tbl_name, where_clause)?;
    let optimized_plan = optimize_delete_plan(delete_plan)?;
    emit_program_for_delete(database_header, optimized_plan, connection)
}
