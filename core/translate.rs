use crate::schema::Schema;
use crate::vdbe::{Insn, Program, ProgramBuilder};

use anyhow::Result;
use sqlite3_parser::ast::{Expr, OneSelect, Select, Stmt};

pub fn translate(schema: &Schema, stmt: Stmt) -> Result<Program> {
    match stmt {
        Stmt::Select(select) => translate_select(schema, select),
        _ => todo!(),
    }
}

fn translate_select(schema: &Schema, select: Select) -> Result<Program> {
    match select.body.select {
        OneSelect::Select {
            columns,
            from: Some(from),
            ..
        } => {
            let cursor_id = 0;
            let table_name = match from.select {
                Some(select_table) => match *select_table {
                    sqlite3_parser::ast::SelectTable::Table(name, ..) => name.name,
                    _ => todo!(),
                },
                None => todo!(),
            };
            let table_name = table_name.0;
            let table = match schema.get_table(&table_name) {
                Some(table) => table,
                None => anyhow::bail!("Parse error: no such table: {}", table_name),
            };
            let root_page = table.root_page;
            let mut program = ProgramBuilder::new();
            let init_offset = program.emit_placeholder();
            let start_offset = program.offset();
            let limit_reg = if let Some(limit) = select.limit {
                assert!(limit.offset.is_none());
                Some(translate_expr(&mut program, &limit.expr))
            } else {
                None
            };
            program.emit_insn(Insn::OpenReadAsync {
                cursor_id: 0,
                root_page,
            });
            program.emit_insn(Insn::OpenReadAwait);
            program.emit_insn(Insn::RewindAsync { cursor_id });
            let rewind_await_offset = program.emit_placeholder();
            let limit_decr_insn = limit_reg.map(|_| program.emit_placeholder());
            let (register_start, register_end) =
                translate_columns(&mut program, Some(cursor_id), Some(table), columns);
            program.emit_insn(Insn::ResultRow {
                register_start,
                register_end,
            });
            program.emit_insn(Insn::NextAsync { cursor_id });
            program.emit_insn(Insn::NextAwait {
                cursor_id,
                pc_if_next: rewind_await_offset,
            });
            program.fixup_insn(
                rewind_await_offset,
                Insn::RewindAwait {
                    cursor_id,
                    pc_if_empty: program.offset(),
                },
            );
            if let Some(limit_decr_insn) = limit_decr_insn {
                program.fixup_insn(
                    limit_decr_insn,
                    Insn::DecrJumpZero {
                        reg: limit_reg.unwrap(),
                        target_pc: program.offset(),
                    },
                );
            }
            program.emit_insn(Insn::Halt);
            program.fixup_insn(
                init_offset,
                Insn::Init {
                    target_pc: program.offset(),
                },
            );
            program.emit_insn(Insn::Transaction);
            program.emit_insn(Insn::Goto {
                target_pc: start_offset,
            });
            Ok(program.build())
        }
        OneSelect::Select {
            columns,
            from: None,
            ..
        } => {
            let mut program = ProgramBuilder::new();
            let init_offset = program.emit_placeholder();
            let after_init_offset = program.offset();
            let (register_start, register_end) =
                translate_columns(&mut program, None, None, columns);
            program.emit_insn(Insn::ResultRow {
                register_start,
                register_end,
            });
            program.emit_insn(Insn::Halt);
            program.fixup_insn(
                init_offset,
                Insn::Init {
                    target_pc: program.offset(),
                },
            );
            program.emit_insn(Insn::Goto {
                target_pc: after_init_offset,
            });
            Ok(program.build())
        }
        _ => todo!(),
    }
}

fn translate_columns(
    program: &mut ProgramBuilder,
    cursor_id: Option<usize>,
    table: Option<&crate::schema::Table>,
    columns: Vec<sqlite3_parser::ast::ResultColumn>,
) -> (usize, usize) {
    let register_start = program.next_free_register();
    for col in columns {
        match col {
            sqlite3_parser::ast::ResultColumn::Expr(expr, _) => {
                let _ = translate_expr(program, &expr);
            }
            sqlite3_parser::ast::ResultColumn::Star => {
                for (i, col) in table.unwrap().columns.iter().enumerate() {
                    let dest = program.alloc_register();
                    if col.primary_key {
                        program.emit_insn(Insn::RowId {
                            cursor_id: cursor_id.unwrap(),
                            dest,
                        });
                    } else {
                        program.emit_insn(Insn::Column {
                            column: i,
                            dest,
                            cursor_id: cursor_id.unwrap(),
                        });
                    }
                }
            }
            sqlite3_parser::ast::ResultColumn::TableStar(_) => todo!(),
        }
    }
    let register_end = program.next_free_register();
    (register_start, register_end)
}

fn translate_expr(program: &mut ProgramBuilder, expr: &Expr) -> usize {
    match expr {
        sqlite3_parser::ast::Expr::Between { .. } => todo!(),
        sqlite3_parser::ast::Expr::Binary(_, _, _) => todo!(),
        sqlite3_parser::ast::Expr::Case { .. } => todo!(),
        sqlite3_parser::ast::Expr::Cast { .. } => todo!(),
        sqlite3_parser::ast::Expr::Collate(_, _) => todo!(),
        sqlite3_parser::ast::Expr::DoublyQualified(_, _, _) => todo!(),
        sqlite3_parser::ast::Expr::Exists(_) => todo!(),
        sqlite3_parser::ast::Expr::FunctionCall { .. } => todo!(),
        sqlite3_parser::ast::Expr::FunctionCallStar { .. } => todo!(),
        sqlite3_parser::ast::Expr::Id(_) => todo!(),
        sqlite3_parser::ast::Expr::InList { .. } => todo!(),
        sqlite3_parser::ast::Expr::InSelect { .. } => todo!(),
        sqlite3_parser::ast::Expr::InTable { .. } => todo!(),
        sqlite3_parser::ast::Expr::IsNull(_) => todo!(),
        sqlite3_parser::ast::Expr::Like { .. } => todo!(),
        sqlite3_parser::ast::Expr::Literal(lit) => match lit {
            sqlite3_parser::ast::Literal::Numeric(val) => {
                let dest = program.alloc_register();
                program.emit_insn(Insn::Integer {
                    value: val.parse().unwrap(),
                    dest,
                });
                dest
            }
            sqlite3_parser::ast::Literal::String(_) => todo!(),
            sqlite3_parser::ast::Literal::Blob(_) => todo!(),
            sqlite3_parser::ast::Literal::Keyword(_) => todo!(),
            sqlite3_parser::ast::Literal::Null => todo!(),
            sqlite3_parser::ast::Literal::CurrentDate => todo!(),
            sqlite3_parser::ast::Literal::CurrentTime => todo!(),
            sqlite3_parser::ast::Literal::CurrentTimestamp => todo!(),
        },
        sqlite3_parser::ast::Expr::Name(_) => todo!(),
        sqlite3_parser::ast::Expr::NotNull(_) => todo!(),
        sqlite3_parser::ast::Expr::Parenthesized(_) => todo!(),
        sqlite3_parser::ast::Expr::Qualified(_, _) => todo!(),
        sqlite3_parser::ast::Expr::Raise(_, _) => todo!(),
        sqlite3_parser::ast::Expr::Subquery(_) => todo!(),
        sqlite3_parser::ast::Expr::Unary(_, _) => todo!(),
        sqlite3_parser::ast::Expr::Variable(_) => todo!(),
    }
}
