use std::rc::Weak;
use std::{cell::RefCell, rc::Rc};

use sqlite3_parser::ast::{self, QualifiedName, With, Operator};

use crate::{
    schema::{Schema, Table},
    storage::sqlite3_ondisk::DatabaseHeader,
    translate::expr::{translate_expr}, 
    translate::planner::bind_column_references,
    vdbe::{builder::ProgramBuilder, Insn, Program},
};
use crate::{Connection, Result};

use super::plan::BTreeTableReference;

pub fn translate_delete(
    schema: &Schema,
    with: &Option<With>,
    qualified_table_name: &QualifiedName, 
    where_clause: &Option<Box<ast::Expr>>,
    database_header: Rc<RefCell<DatabaseHeader>>,
    connection: Weak<Connection>,
) -> Result<Program> {
    assert!(with.is_none()); // Don't handle WITH clause yet

    let mut program = ProgramBuilder::new();

    let init_label = program.allocate_label();
    program.emit_insn_with_label_dependency(
        Insn::Init {
            target_pc: init_label,
        },
        init_label,
    );
    let start_offset = program.offset();

    let table_name = &qualified_table_name.name.0;
    let table = match schema.get_table(table_name) {
        Some(t) => t,
        None => crate::bail_corrupt_error!("No such table: {}", table_name),
    };

    let table_ref = BTreeTableReference {
        table: table.clone(),
        table_identifier: table_name.clone(),
        table_index: 0,
    };
    let referenced_tables = vec![table_ref];

    let cursor_id = program.alloc_cursor_id(Some(table_name.clone()), Some(Table::BTree(table.clone())));
    program.emit_insn(Insn::OpenWriteAsync {
        cursor_id,
        root_page: table.root_page,
    });
    program.emit_insn(Insn::OpenWriteAwait {});

    if let Some(where_expr) = where_clause {
        let mut expr = *where_expr.clone();
        bind_column_references(&mut expr, &referenced_tables)?;
        
        // Handle WHERE id = value case
        if let ast::Expr::Binary(lhs, op, rhs) = expr {
            if let ast::Expr::Column { column: _, is_rowid_alias, .. } = *lhs {
                if is_rowid_alias && matches!(op, Operator::Equals) {
                    let key_reg = program.alloc_register();
                    translate_expr(&mut program, Some(&referenced_tables), &rhs, key_reg, None)?;
    
                    // Find row by key
                    program.emit_insn(Insn::MustBeInt {
                        reg: key_reg,
                    });
    
                    let end_label = program.allocate_label();
                    
                    // Move to the rowid
                    program.emit_insn(Insn::SeekRowid {
                        cursor_id,
                        src_reg: key_reg,
                        target_pc: end_label,  // Skip to end if not found
                    });
    
                    // Delete the record
                    program.emit_insn(Insn::DeleteAsync {
                        cursor_id,
                    });
                    program.emit_insn(Insn::DeleteAwait {
                        cursor_id,
                    });
    
                    program.resolve_label(end_label, program.offset());
                }
            }
        } else {
            let next_row_label = program.allocate_label();
            program.resolve_label(next_row_label, program.offset());
            
            let where_reg = program.alloc_register();
            translate_expr(&mut program, Some(&referenced_tables), &expr, where_reg, None)?;
            
            let skip_label = program.allocate_label();
            program.emit_insn(Insn::IfNot {
                reg: where_reg,
                target_pc: skip_label,
                null_reg: where_reg,
            });
            
            program.emit_insn(Insn::DeleteAsync { cursor_id });
            program.emit_insn(Insn::DeleteAwait { cursor_id });
            
            program.resolve_label(skip_label, program.offset());
            
            program.emit_insn(Insn::NextAsync { cursor_id });
            program.emit_insn(Insn::NextAwait {
                cursor_id,
                pc_if_next: next_row_label,
            });
        }
    }

    program.emit_insn(Insn::Close {
        cursor_id,
    });
    program.emit_insn(Insn::Halt {
        err_code: 0,
        description: String::new(),
    });

    program.resolve_label(init_label, program.offset());
    program.emit_insn(Insn::Transaction {
        write: true,
    });
    program.emit_constant_insns();
    program.emit_insn(Insn::Goto {
        target_pc: start_offset,
    });

    Ok(program.build(database_header, connection))
}