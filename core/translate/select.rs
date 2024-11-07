use std::rc::Weak;
use std::{cell::RefCell, rc::Rc};

use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::Connection;
use crate::{schema::Schema, vdbe::Program, Result};
use sqlite3_parser::ast;

use super::emitter::emit_program;
use super::optimizer::optimize_plan;
use super::planner::prepare_select_plan;

pub fn translate_select(
    schema: &Schema,
    select: ast::Select,
    database_header: Rc<RefCell<DatabaseHeader>>,
    connection: Weak<Connection>,
) -> Result<Program> {
    let select_plan = prepare_select_plan(schema, select)?;
    let (optimized_plan, expr_result_cache) = optimize_plan(select_plan)?;
    emit_program(
        database_header,
        optimized_plan,
        expr_result_cache,
        connection,
    )
}
