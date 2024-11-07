use std::rc::Weak;
use std::{cell::RefCell, ops::Deref, rc::Rc};

use sqlite3_parser::ast::{
    DistinctNames, InsertBody, QualifiedName, ResolveType, ResultColumn, With,
};

use crate::error::SQLITE_CONSTRAINT_PRIMARYKEY;
use crate::{
    schema::{Schema, Table},
    storage::sqlite3_ondisk::DatabaseHeader,
    translate::expr::translate_expr,
    vdbe::{builder::ProgramBuilder, Insn, Program},
};
use crate::{Connection, Result};

#[allow(clippy::too_many_arguments)]
pub fn translate_insert(
    schema: &Schema,
    with: &Option<With>,
    or_conflict: &Option<ResolveType>,
    tbl_name: &QualifiedName,
    _columns: &Option<DistinctNames>,
    body: &InsertBody,
    _returning: &Option<Vec<ResultColumn>>,
    database_header: Rc<RefCell<DatabaseHeader>>,
    connection: Weak<Connection>,
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
        Table::Index(index) => index.root_page,
        Table::Pseudo(_) => todo!(),
    };

    let mut num_cols = table.columns().len();
    if table.has_rowid() {
        num_cols += 1;
    }
    // column_registers_start[0] == rowid if has rowid
    let column_registers_start = program.alloc_registers(num_cols);

    // Coroutine for values
    let yield_reg = program.alloc_register();
    let jump_on_definition_label = program.allocate_label();
    {
        program.emit_insn_with_label_dependency(
            Insn::InitCoroutine {
                yield_reg,
                jump_on_definition: jump_on_definition_label,
                start_offset: program.offset() + 1,
            },
            jump_on_definition_label,
        );
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
                sqlite3_parser::ast::OneSelect::Values(values) => {
                    for value in values {
                        for (col, expr) in value.iter().enumerate() {
                            let mut col = col;
                            if table.has_rowid() {
                                col += 1;
                            }
                            translate_expr(
                                &mut program,
                                None,
                                expr,
                                column_registers_start + col,
                                None,
                                None,
                            )?;
                        }
                        program.emit_insn(Insn::Yield {
                            yield_reg,
                            end_offset: 0,
                        });
                    }
                }
            },
            InsertBody::DefaultValues => todo!("default values not yet supported"),
            _ => todo!(),
        }
        program.emit_insn(Insn::EndCoroutine { yield_reg });
    }

    program.resolve_label(jump_on_definition_label, program.offset());
    program.emit_insn(Insn::OpenWriteAsync {
        cursor_id,
        root_page,
    });
    program.emit_insn(Insn::OpenWriteAwait {});

    // Main loop
    let record_register = program.alloc_register();
    let halt_label = program.allocate_label();
    let loop_start_offset = program.offset();
    program.emit_insn_with_label_dependency(
        Insn::Yield {
            yield_reg,
            end_offset: halt_label,
        },
        halt_label,
    );

    if table.has_rowid() {
        let row_id_reg = column_registers_start;
        if let Some(rowid_alias_column) = table.get_rowid_alias_column() {
            let key_reg = column_registers_start + 1 + rowid_alias_column.0;
            // copy key to rowid
            program.emit_insn(Insn::Copy {
                src_reg: key_reg,
                dst_reg: row_id_reg,
                amount: 0,
            });
            program.emit_insn(Insn::SoftNull { reg: key_reg });
        }

        let notnull_label = program.allocate_label();
        program.emit_insn_with_label_dependency(
            Insn::NotNull {
                reg: row_id_reg,
                target_pc: notnull_label,
            },
            notnull_label,
        );
        program.emit_insn(Insn::NewRowid {
            cursor: cursor_id,
            rowid_reg: row_id_reg,
            prev_largest_reg: 0,
        });

        program.resolve_label(notnull_label, program.offset());
        program.emit_insn(Insn::MustBeInt { reg: row_id_reg });
        let make_record_label = program.allocate_label();
        program.emit_insn_with_label_dependency(
            Insn::NotExists {
                cursor: cursor_id,
                rowid_reg: row_id_reg,
                target_pc: make_record_label,
            },
            make_record_label,
        );
        // TODO: rollback
        program.emit_insn(Insn::Halt {
            err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
            description: format!(
                "{}.{}",
                table.get_name(),
                table.column_index_to_name(0).unwrap()
            ),
        });
        program.resolve_label(make_record_label, program.offset());
        program.emit_insn(Insn::MakeRecord {
            start_reg: column_registers_start + 1,
            count: num_cols - 1,
            dest_reg: record_register,
        });
        program.emit_insn(Insn::InsertAsync {
            cursor: cursor_id,
            key_reg: column_registers_start,
            record_reg: record_register,
            flag: 0,
        });
        program.emit_insn(Insn::InsertAwait { cursor_id });
    }

    program.emit_insn(Insn::Goto {
        target_pc: loop_start_offset,
    });

    program.resolve_label(halt_label, program.offset());
    program.emit_insn(Insn::Halt {
        err_code: 0,
        description: String::new(),
    });
    program.resolve_label(init_label, program.offset());
    program.emit_insn(Insn::Transaction { write: true });
    program.emit_constant_insns();
    program.emit_insn(Insn::Goto {
        target_pc: start_offset,
    });
    program.resolve_deferred_labels();
    Ok(program.build(database_header, connection))
}
