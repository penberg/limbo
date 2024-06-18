use crate::schema::Schema;
use crate::sqlite3_ondisk::DatabaseHeader;
use crate::vdbe::{Insn, Program, ProgramBuilder};
use anyhow::Result;
use sqlite3_parser::ast::{
    Expr, Literal, OneSelect, PragmaBody, QualifiedName, Select, Stmt, UnaryOperator,
};

/// Translate SQL statement into bytecode program.
pub fn translate(schema: &Schema, stmt: Stmt, database_header: &DatabaseHeader) -> Result<Program> {
    match stmt {
        Stmt::Select(select) => translate_select(schema, select),
        Stmt::Pragma(name, body) => translate_pragma(&name, body, database_header),
        _ => todo!(),
    }
}

fn translate_select(schema: &Schema, select: Select) -> Result<Program> {
    let mut program = ProgramBuilder::new();
    let init_offset = program.emit_placeholder();
    let start_offset = program.offset();
    match select.body.select {
        OneSelect::Select {
            columns,
            from: Some(from),
            ..
        } => {
            let cursor_id = program.alloc_cursor_id();
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
            let limit_reg = if let Some(limit) = select.limit {
                assert!(limit.offset.is_none());
                Some(translate_expr(
                    &mut program,
                    Some(cursor_id),
                    Some(table),
                    &limit.expr,
                ))
            } else {
                None
            };
            program.emit_insn(Insn::OpenReadAsync {
                cursor_id,
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
        }
        OneSelect::Select {
            columns,
            from: None,
            ..
        } => {
            let (register_start, register_end) =
                translate_columns(&mut program, None, None, columns);
            program.emit_insn(Insn::ResultRow {
                register_start,
                register_end,
            });
        }
        _ => todo!(),
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
                let _ = translate_expr(program, cursor_id, table, &expr);
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

fn translate_expr(
    program: &mut ProgramBuilder,
    cursor_id: Option<usize>,
    table: Option<&crate::schema::Table>,
    expr: &Expr,
) -> usize {
    match expr {
        Expr::Between { .. } => todo!(),
        Expr::Binary(_, _, _) => todo!(),
        Expr::Case { .. } => todo!(),
        Expr::Cast { .. } => todo!(),
        Expr::Collate(_, _) => todo!(),
        Expr::DoublyQualified(_, _, _) => todo!(),
        Expr::Exists(_) => todo!(),
        Expr::FunctionCall { .. } => todo!(),
        Expr::FunctionCallStar { .. } => todo!(),
        Expr::Id(ident) => {
            let (idx, col) = table.unwrap().get_column(&ident.0).unwrap();
            let dest = program.alloc_register();
            if col.primary_key {
                program.emit_insn(Insn::RowId {
                    cursor_id: cursor_id.unwrap(),
                    dest,
                });
            } else {
                program.emit_insn(Insn::Column {
                    column: idx,
                    dest,
                    cursor_id: cursor_id.unwrap(),
                });
            }
            dest
        }
        Expr::InList { .. } => todo!(),
        Expr::InSelect { .. } => todo!(),
        Expr::InTable { .. } => todo!(),
        Expr::IsNull(_) => todo!(),
        Expr::Like { .. } => todo!(),
        Expr::Literal(lit) => match lit {
            Literal::Numeric(val) => {
                let dest = program.alloc_register();
                program.emit_insn(Insn::Integer {
                    value: val.parse().unwrap(),
                    dest,
                });
                dest
            }
            Literal::String(s) => {
                let dest = program.alloc_register();
                program.emit_insn(Insn::String8 {
                    value: s[1..s.len() - 1].to_string(),
                    dest,
                });
                dest
            }
            Literal::Blob(_) => todo!(),
            Literal::Keyword(_) => todo!(),
            Literal::Null => todo!(),
            Literal::CurrentDate => todo!(),
            Literal::CurrentTime => todo!(),
            Literal::CurrentTimestamp => todo!(),
        },
        Expr::Name(_) => todo!(),
        Expr::NotNull(_) => todo!(),
        Expr::Parenthesized(_) => todo!(),
        Expr::Qualified(_, _) => todo!(),
        Expr::Raise(_, _) => todo!(),
        Expr::Subquery(_) => todo!(),
        Expr::Unary(_, _) => todo!(),
        Expr::Variable(_) => todo!(),
    }
}

fn translate_pragma(
    name: &QualifiedName,
    body: Option<PragmaBody>,
    database_header: &DatabaseHeader,
) -> Result<Program> {
    let mut program = ProgramBuilder::new();
    let init_offset = program.emit_placeholder();
    let start_offset = program.offset();
    let (change_pragma, new_value) = match body {
        None => {
            let pragma_result = program.alloc_register();

            program.emit_insn(Insn::Integer {
                value: database_header.default_cache_size.into(),
                dest: pragma_result,
            });

            let pragma_result_end = program.next_free_register();
            program.emit_insn(Insn::ResultRow {
                register_start: pragma_result,
                register_end: pragma_result_end,
            });
            (false, 0)
        }
        Some(PragmaBody::Equals(value)) => {
            let value_to_update = match value {
                Expr::Literal(Literal::Numeric(numeric_value)) => {
                    numeric_value.parse::<i64>().unwrap()
                }
                Expr::Unary(UnaryOperator::Negative, expr) => match *expr {
                    Expr::Literal(Literal::Numeric(numeric_value)) => {
                        -numeric_value.parse::<i64>().unwrap()
                    }
                    _ => 0,
                },
                _ => 0,
            };
            println!("{:?}", value_to_update);
            (true, value_to_update)
        }
        Some(PragmaBody::Call(_)) => {
            todo!()
        }
    };
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
    if change_pragma {
        return Ok(program.build_pragma_change(name.name.to_string(), new_value));
    }
    Ok(program.build())
}
