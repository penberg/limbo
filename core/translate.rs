use crate::vdbe::{Insn, Program, ProgramBuilder};
use crate::schema::Schema;

use anyhow::Result;
use sqlite3_parser::ast::{OneSelect, Select, Stmt};

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
            let open_read_offset = program.offset();
            program.emit_insn(Insn::OpenReadAsync {
                cursor_id: 0,
                root_page,
            });
            program.emit_insn(Insn::OpenReadAwait);
            program.emit_insn(Insn::RewindAsync { cursor_id });
            let rewind_await_offset = program.emit_placeholder();
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
            program.emit_insn(Insn::Halt);
            program.fixup_insn(
                init_offset,
                Insn::Init {
                    target_pc: program.offset(),
                },
            );
            program.emit_insn(Insn::Transaction);
            program.emit_insn(Insn::Goto {
                target_pc: open_read_offset,
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
            sqlite3_parser::ast::ResultColumn::Expr(expr, _) => match expr {
                sqlite3_parser::ast::Expr::Between {
                    lhs,
                    not,
                    start,
                    end,
                } => todo!(),
                sqlite3_parser::ast::Expr::Binary(_, _, _) => todo!(),
                sqlite3_parser::ast::Expr::Case {
                    base,
                    when_then_pairs,
                    else_expr,
                } => todo!(),
                sqlite3_parser::ast::Expr::Cast { expr, type_name } => todo!(),
                sqlite3_parser::ast::Expr::Collate(_, _) => todo!(),
                sqlite3_parser::ast::Expr::DoublyQualified(_, _, _) => todo!(),
                sqlite3_parser::ast::Expr::Exists(_) => todo!(),
                sqlite3_parser::ast::Expr::FunctionCall {
                    name,
                    distinctness,
                    args,
                    filter_over,
                } => todo!(),
                sqlite3_parser::ast::Expr::FunctionCallStar { name, filter_over } => todo!(),
                sqlite3_parser::ast::Expr::Id(_) => todo!(),
                sqlite3_parser::ast::Expr::InList { lhs, not, rhs } => todo!(),
                sqlite3_parser::ast::Expr::InSelect { lhs, not, rhs } => todo!(),
                sqlite3_parser::ast::Expr::InTable {
                    lhs,
                    not,
                    rhs,
                    args,
                } => todo!(),
                sqlite3_parser::ast::Expr::IsNull(_) => todo!(),
                sqlite3_parser::ast::Expr::Like {
                    lhs,
                    not,
                    op,
                    rhs,
                    escape,
                } => todo!(),
                sqlite3_parser::ast::Expr::Literal(lit) => match lit {
                    sqlite3_parser::ast::Literal::Numeric(val) => {
                        let dest = program.alloc_register();
                        program.emit_insn(Insn::Integer {
                            value: val.parse().unwrap(),
                            dest,
                        });
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
            },
            sqlite3_parser::ast::ResultColumn::Star => {
                for i in 0..table.unwrap().columns.len() {
                    let dest = program.alloc_register();
                    program.emit_insn(Insn::Column {
                        column: i,
                        dest,
                        cursor_id: cursor_id.unwrap(),
                    });
                }
            }
            sqlite3_parser::ast::ResultColumn::TableStar(_) => todo!(),
        }
    }
    let register_end = program.next_free_register();
    (register_start, register_end)
}