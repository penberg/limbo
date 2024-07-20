use std::{ops::Deref, rc::Rc};

use sqlite3_parser::ast::{
    DistinctNames, InsertBody, Name, QualifiedName, ResolveType, ResultColumn, Select, With,
};

use crate::Result;
use crate::{
    schema::{self, Schema, Table},
    translate::expr::resolve_ident_qualified,
    vdbe::{builder::ProgramBuilder, Insn, Program},
};

pub fn translate_insert(
    schema: &Schema,
    with: &Option<With>,
    or_conflict: &Option<ResolveType>,
    tbl_name: &QualifiedName,
    columns: &Option<DistinctNames>,
    body: &InsertBody,
    returning: &Option<Vec<ResultColumn>>,
) -> Result<Program> {
    assert!(with.is_none());
    assert!(or_conflict.is_none());
    let mut program = ProgramBuilder::new();
    let init_label = program.allocate_label();
    program.emit_insn_with_label_dependency(
        Insn::Init {
            target_pc: init_label,
        },
        init_label,
    );
    let start_offset = program.offset();

    dbg!(tbl_name);
    dbg!(columns);
    dbg!(returning);
    dbg!(with);
    dbg!(body);

    let yield_reg = program.alloc_register();
    let jump_on_definition_label = program.allocate_label();
    program.emit_insn(Insn::InitCoroutine {
        yield_reg,
        jump_on_definition: jump_on_definition_label,
        start_offset: program.offset() + 1,
    });
    match body {
        InsertBody::Select(select, None) => match &select.body.select {
            sqlite3_parser::ast::OneSelect::Select {
                distinctness: _,
                columns: _,
                from: _,
                where_clause: _,
                group_by: _,
                window_clause: _,
            } => todo!(),
            sqlite3_parser::ast::OneSelect::Values(values) => {}
        },
        InsertBody::DefaultValues => todo!("default values not yet supported"),
        _ => todo!(),
    }
    program.emit_insn(Insn::EndCoroutine { yield_reg });

    // open table
    let table_name = &tbl_name.name;

    let table = match schema.get_table(table_name.0.as_str()) {
        Some(table) => table,
        None => crate::bail_corrupt_error!("Parse error: no such table: {}", table_name),
    };
    let table = Rc::new(Table::BTree(table));
    let cursor_id = program.alloc_cursor_id(
        Some(table_name.0.clone()),
        Some(table.clone().deref().clone()),
    );
    let root_page = match table.as_ref() {
        Table::BTree(btree) => btree.root_page,
        Table::Pseudo(_) => todo!(),
    };
    program.emit_insn(Insn::OpenWriteAsync {
        cursor_id,
        root_page,
    });
    program.emit_insn(Insn::OpenWriteAwait {});

    program.emit_insn(Insn::Halt);
    program.resolve_label(init_label, program.offset());
    program.emit_insn(Insn::Transaction);
    program.emit_constant_insns();
    program.emit_insn(Insn::Goto {
        target_pc: start_offset,
    });
    program.resolve_deferred_labels();
    Ok(program.build())
}
