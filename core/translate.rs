use std::cell::RefCell;
use std::rc::Rc;

use crate::pager::Pager;
use crate::schema::{self, PseudoTable, Schema, Table};
use crate::sqlite3_ondisk::{DatabaseHeader, MIN_PAGE_CACHE_SIZE};
use crate::vdbe::{Insn, Program, ProgramBuilder};
use anyhow::Result;
use sqlite3_parser::ast::{
    Expr, Literal, OneSelect, PragmaBody, QualifiedName, Select, Stmt, UnaryOperator,
};

/// Translate SQL statement into bytecode program.
pub fn translate(
    schema: &Schema,
    stmt: Stmt,
    database_header: Rc<RefCell<DatabaseHeader>>,
    pager: Rc<Pager>,
) -> Result<Program> {
    match stmt {
        Stmt::Select(select) => translate_select(schema, select),
        Stmt::Pragma(name, body) => translate_pragma(&name, body, database_header, pager),
        _ => todo!(),
    }
}

/// Generate code for a SELECT statement.
fn translate_select(schema: &Schema, select: Select) -> Result<Program> {
    let mut program = ProgramBuilder::new();
    let init_offset = program.emit_placeholder();
    let start_offset = program.offset();
    let limit_reg = if let Some(limit) = select.limit {
        assert!(limit.offset.is_none());
        Some(translate_expr(&mut program, None, None, &limit.expr))
    } else {
        None
    };
    let sorter_cursor = match select.order_by {
        Some(_) => {
            let cursor_id = program.alloc_cursor_id();
            program.emit_insn(Insn::SorterOpen { cursor_id });
            Some(cursor_id)
        }
        None => None,
    };
    let (table, columns, limit_decr_insn) = match select.body.select {
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
            let table = Table::BTree(table);
            program.emit_insn(Insn::OpenReadAsync {
                cursor_id,
                root_page,
            });
            program.emit_insn(Insn::OpenReadAwait);
            program.emit_insn(Insn::RewindAsync { cursor_id });
            let rewind_await_offset = program.emit_placeholder();
            let limit_decr_insn = limit_reg.map(|_| program.emit_placeholder());
            match sorter_cursor {
                Some(sorter_cursor_id) => {
                    let start_reg = program.next_free_register();
                    let order_by = select.order_by.unwrap();
                    for column in order_by {
                        let _ = translate_expr(
                            &mut program,
                            Some(cursor_id),
                            Some(&table),
                            &column.expr,
                        );
                    }
                    let (_, end_reg) = translate_columns(
                        &mut program,
                        Some(cursor_id),
                        Some(&table),
                        columns.clone(),
                    );
                    let count = end_reg - start_reg;
                    let record_reg = program.alloc_register();
                    program.emit_insn(Insn::MakeRecord {
                        start_reg,
                        count,
                        dest_reg: record_reg,
                    });
                    program.emit_insn(Insn::SorterInsert {
                        cursor_id: sorter_cursor_id,
                        record_reg,
                    });
                }
                None => {
                    let (start_reg, end_reg) = translate_columns(
                        &mut program,
                        Some(cursor_id),
                        Some(&table),
                        columns.clone(),
                    );
                    let count = end_reg - start_reg;
                    program.emit_insn(Insn::ResultRow { start_reg, count });
                }
            }
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
            (Some(table), columns, limit_decr_insn)
        }
        OneSelect::Select {
            columns,
            from: None,
            ..
        } => {
            let (start_reg, end_reg) = translate_columns(&mut program, None, None, columns.clone());
            let count = end_reg - start_reg;
            program.emit_insn(Insn::ResultRow { start_reg, count });
            let limit_decr_insn = limit_reg.map(|_| program.emit_placeholder());
            (None, columns, limit_decr_insn)
        }
        _ => todo!(),
    };
    match sorter_cursor {
        Some(sorter_cursor_id) => {
            let pseudo_num_fields = columns.len();
            let pseudo_table = schema::build_pseudo_table(&columns);
            let pseudo_table = Some(Table::Pseudo(&pseudo_table));
            let pseudo_cursor_id = program.alloc_cursor_id();
            let pseudo_content_reg = program.alloc_register();
            program.emit_insn(Insn::OpenPseudo {
                cursor_id: pseudo_cursor_id,
                content_reg: pseudo_content_reg,
                num_fields: pseudo_num_fields,
            });
            program.emit_insn(Insn::SorterSort {
                cursor_id: sorter_cursor_id,
            });
            let pc_if_next = program.offset();
            program.emit_insn(Insn::SorterData {
                cursor_id: sorter_cursor_id,
                dest_reg: pseudo_content_reg,
            });
            let (start_reg, end_reg) = translate_columns(
                &mut program,
                Some(pseudo_cursor_id),
                pseudo_table.as_ref(),
                columns.clone(),
            );
            let count = end_reg - start_reg;
            program.emit_insn(Insn::ResultRow { start_reg, count });
            program.emit_insn(Insn::SorterNext {
                cursor_id: sorter_cursor_id,
                pc_if_next,
            });
        }
        None => {}
    }
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

fn translate_columns(
    program: &mut ProgramBuilder,
    cursor_id: Option<usize>,
    table: Option<&crate::schema::Table>,
    columns: Vec<sqlite3_parser::ast::ResultColumn>,
) -> (usize, usize) {
    let register_start = program.next_free_register();
    for col in columns {
        translate_column(program, cursor_id, table, col);
    }
    let register_end = program.next_free_register();
    (register_start, register_end)
}

fn translate_column(
    program: &mut ProgramBuilder,
    cursor_id: Option<usize>,
    table: Option<&crate::schema::Table>,
    col: sqlite3_parser::ast::ResultColumn,
) {
    match col {
        sqlite3_parser::ast::ResultColumn::Expr(expr, _) => {
            let _ = translate_expr(program, cursor_id, table, &expr);
        }
        sqlite3_parser::ast::ResultColumn::Star => {
            let table = table.unwrap();
            for (i, col) in table.columns().iter().enumerate() {
                let dest = program.alloc_register();
                if !table.is_pseudo() && col.is_rowid_alias() {
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
            let table = table.unwrap();
            let (idx, col) = table.get_column(&ident.0).unwrap();
            let dest = program.alloc_register();
            if !table.is_pseudo() && col.is_rowid_alias() {
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
    database_header: Rc<RefCell<DatabaseHeader>>,
    pager: Rc<Pager>,
) -> Result<Program> {
    let mut program = ProgramBuilder::new();
    let init_offset = program.emit_placeholder();
    let start_offset = program.offset();
    match body {
        None => {
            let pragma_result = program.alloc_register();

            program.emit_insn(Insn::Integer {
                value: database_header.borrow().default_cache_size.into(),
                dest: pragma_result,
            });

            let pragma_result_end = program.next_free_register();
            program.emit_insn(Insn::ResultRow {
                start_reg: pragma_result,
                count: pragma_result_end - pragma_result,
            });
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
            update_pragma(&name.name.0, value_to_update, database_header, pager);
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
    Ok(program.build())
}

fn update_pragma(name: &String, value: i64, header: Rc<RefCell<DatabaseHeader>>, pager: Rc<Pager>) {
    match name.as_str() {
        "cache_size" => {
            let mut cache_size_unformatted = value;
            let mut cache_size = if cache_size_unformatted < 0 {
                let kb = cache_size_unformatted.abs() * 1024;
                kb / 512 // assume 512 page size for now
            } else {
                value
            } as usize;
            if cache_size < MIN_PAGE_CACHE_SIZE {
                // update both in memory and stored disk value
                cache_size = MIN_PAGE_CACHE_SIZE;
                cache_size_unformatted = MIN_PAGE_CACHE_SIZE as i64;
            }

            // update in-memory header
            header.borrow_mut().default_cache_size = cache_size_unformatted
                .try_into()
                .expect(&format!("invalid value, too big for a i32 {}", value));

            // update in disk
            let header_copy = header.borrow().clone();
            pager.write_database_header(&header_copy);

            // update cache size
            pager.change_page_cache_size(cache_size);
        }
        _ => todo!(),
    }
}
