use std::cell::RefCell;
use std::rc::Rc;

use crate::function::AggFunc;
use crate::pager::Pager;
use crate::schema::{Schema, Table};
use crate::sqlite3_ondisk::{DatabaseHeader, MIN_PAGE_CACHE_SIZE};
use crate::util::normalize_ident;
use crate::vdbe::{Insn, Program, ProgramBuilder};
use anyhow::Result;
use sqlite3_parser::ast;

struct Select<'a> {
    columns: Vec<ast::ResultColumn>,
    column_info: Vec<ColumnInfo>,
    from: Option<&'a Table>,
    limit: Option<ast::Limit>,
    exist_aggregation: bool,
}

struct ColumnInfo {
    func: Option<AggFunc>,
    args: Option<Vec<ast::Expr>>,
    columns_to_allocate: usize, /* number of result columns this col will result on */
}

impl ColumnInfo {
    pub fn new() -> Self {
        Self {
            func: None,
            args: None,
            columns_to_allocate: 1,
        }
    }

    pub fn is_aggregation_function(&self) -> bool {
        self.func.is_some()
    }
}

/// Translate SQL statement into bytecode program.
pub fn translate(
    schema: &Schema,
    stmt: ast::Stmt,
    database_header: Rc<RefCell<DatabaseHeader>>,
    pager: Rc<Pager>,
) -> Result<Program> {
    match stmt {
        ast::Stmt::Select(select) => {
            let select = build_select(schema, select)?;
            translate_select(select)
        }
        ast::Stmt::Pragma(name, body) => translate_pragma(&name, body, database_header, pager),
        _ => todo!(),
    }
}

fn build_select(schema: &Schema, select: ast::Select) -> Result<Select> {
    match select.body.select {
        ast::OneSelect::Select {
            columns,
            from: Some(from),
            ..
        } => {
            let table_name = match from.select {
                Some(select_table) => match *select_table {
                    ast::SelectTable::Table(name, ..) => name.name,
                    _ => todo!(),
                },
                None => todo!(),
            };
            let table_name = table_name.0;
            let table = match schema.get_table(&table_name) {
                Some(table) => table,
                None => anyhow::bail!("Parse error: no such table: {}", table_name),
            };
            let column_info = analyze_columns(&columns, Some(&table));
            let exist_aggregation = column_info.iter().any(|info| info.func.is_some());
            Ok(Select {
                columns,
                column_info,
                from: Some(&table),
                limit: select.limit.clone(),
                exist_aggregation,
            })
        }
        ast::OneSelect::Select {
            columns,
            from: None,
            ..
        } => {
            let column_info = analyze_columns(&columns, None);
            let exist_aggregation = column_info.iter().any(|info| info.func.is_some());
            Ok(Select {
                columns,
                column_info,
                from: None,
                limit: select.limit.clone(),
                exist_aggregation,
            })
        }
        _ => todo!(),
    }
}

/// Generate code for a SELECT statement.
fn translate_select(select: Select) -> Result<Program> {
    let mut program = ProgramBuilder::new();
    let init_offset = program.emit_placeholder();
    let start_offset = program.offset();
    let limit_reg = if let Some(limit) = &select.limit {
        assert!(limit.offset.is_none());
        let target_register = program.alloc_register();
        Some(translate_expr(
            &mut program,
            None,
            None,
            &limit.expr,
            target_register,
        ))
    } else {
        None
    };
    let cursor_id = program.alloc_cursor_id();
    let limit_decr_insn = match select.from {
        Some(table) => {
            let root_page = table.root_page;
            program.emit_insn(Insn::OpenReadAsync {
                cursor_id,
                root_page,
            });
            program.emit_insn(Insn::OpenReadAwait);
            program.emit_insn(Insn::RewindAsync { cursor_id });
            let rewind_await_offset = program.emit_placeholder();
            let (register_start, register_end) =
                translate_columns(&mut program, Some(cursor_id), &select);
            let limit_decr_insn = if select.exist_aggregation {
                program.emit_insn(Insn::NextAsync { cursor_id });
                program.emit_insn(Insn::NextAwait {
                    cursor_id,
                    pc_if_next: rewind_await_offset,
                });
                let mut target = register_start;
                for info in &select.column_info {
                    if let Some(func) = &info.func {
                        program.emit_insn(Insn::AggFinal {
                            register: target,
                            func: func.clone(),
                        });
                    }
                    target += info.columns_to_allocate;
                }
                // only one result row
                program.emit_insn(Insn::ResultRow {
                    register_start,
                    register_end,
                });
                limit_reg.map(|_| program.emit_placeholder())
            } else {
                program.emit_insn(Insn::ResultRow {
                    register_start,
                    register_end,
                });
                let limit_decr_insn = limit_reg.map(|_| program.emit_placeholder());
                program.emit_insn(Insn::NextAsync { cursor_id });
                program.emit_insn(Insn::NextAwait {
                    cursor_id,
                    pc_if_next: rewind_await_offset,
                });
                limit_decr_insn
            };
            program.fixup_insn(
                rewind_await_offset,
                Insn::RewindAwait {
                    cursor_id,
                    pc_if_empty: program.offset(),
                },
            );
            limit_decr_insn
        }
        None => {
            assert!(!select.exist_aggregation);
            let (register_start, register_end) = translate_columns(&mut program, None, &select);
            program.emit_insn(Insn::ResultRow {
                register_start,
                register_end,
            });
            limit_reg.map(|_| program.emit_placeholder())
        }
    };
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
    select: &Select,
) -> (usize, usize) {
    let register_start = program.next_free_register();

    // allocate one register as output for each col
    let registers: usize = select
        .column_info
        .iter()
        .map(|col| col.columns_to_allocate)
        .sum();
    program.alloc_registers(registers);
    let register_end = program.next_free_register();

    let mut target = register_start;
    for (col, info) in select.columns.iter().zip(select.column_info.iter()) {
        translate_column(program, cursor_id, select.from, col, info, target);
        target += info.columns_to_allocate;
    }
    (register_start, register_end)
}

fn translate_column(
    program: &mut ProgramBuilder,
    cursor_id: Option<usize>,
    table: Option<&crate::schema::Table>,
    col: &sqlite3_parser::ast::ResultColumn,
    info: &ColumnInfo,
    target_register: usize, // where to store the result, in case of star it will be the start of registers added
) {
    match col {
        sqlite3_parser::ast::ResultColumn::Expr(expr, _) => {
            if info.is_aggregation_function() {
                let _ =
                    translate_aggregation(program, cursor_id, table, expr, info, target_register);
            } else {
                let _ = translate_expr(program, cursor_id, table, expr, target_register);
            }
        }
        sqlite3_parser::ast::ResultColumn::Star => {
            for (i, col) in table.unwrap().columns.iter().enumerate() {
                if col.is_rowid_alias() {
                    program.emit_insn(Insn::RowId {
                        cursor_id: cursor_id.unwrap(),
                        dest: target_register + i,
                    });
                } else {
                    program.emit_insn(Insn::Column {
                        column: i,
                        dest: target_register + i,
                        cursor_id: cursor_id.unwrap(),
                    });
                }
            }
        }
        sqlite3_parser::ast::ResultColumn::TableStar(_) => todo!(),
    }
}

fn analyze_columns(
    columns: &Vec<sqlite3_parser::ast::ResultColumn>,
    table: Option<&crate::schema::Table>,
) -> Vec<ColumnInfo> {
    let mut column_information_list = Vec::with_capacity(columns.len());
    for column in columns {
        let mut info = ColumnInfo::new();
        info.columns_to_allocate = 1;
        if let sqlite3_parser::ast::ResultColumn::Star = column {
            info.columns_to_allocate = table.unwrap().columns.len();
        } else {
            analyze_column(column, &mut info);
        }
        column_information_list.push(info);
    }
    column_information_list
}

/// Analyze a column expression.
///
/// The function walks a column expression trying to find aggregation functions.
/// If it finds one it will save information about it.
fn analyze_column(column: &sqlite3_parser::ast::ResultColumn, column_info_out: &mut ColumnInfo) {
    match column {
        sqlite3_parser::ast::ResultColumn::Expr(expr, _) => match expr {
            ast::Expr::FunctionCall {
                name,
                distinctness: _,
                args,
                filter_over: _,
            } => {
                let func_type = match normalize_ident(name.0.as_str()).as_str() {
                    "avg" => Some(AggFunc::Avg),
                    "count" => Some(AggFunc::Count),
                    "group_concat" => Some(AggFunc::GroupConcat),
                    "max" => Some(AggFunc::Max),
                    "min" => Some(AggFunc::Min),
                    "string_agg" => Some(AggFunc::StringAgg),
                    "sum" => Some(AggFunc::Sum),
                    "total" => Some(AggFunc::Total),
                    _ => None,
                };
                if func_type.is_none() {
                    analyze_column(column, column_info_out);
                } else {
                    column_info_out.func = func_type;
                    // TODO(pere): use lifetimes for args? Arenas would be lovely here :(
                    column_info_out.args = args.clone();
                }
            }
            ast::Expr::FunctionCallStar { .. } => todo!(),
            _ => {}
        },
        ast::ResultColumn::Star => {}
        ast::ResultColumn::TableStar(_) => {}
    }
}

fn translate_expr(
    program: &mut ProgramBuilder,
    cursor_id: Option<usize>,
    table: Option<&crate::schema::Table>,
    expr: &ast::Expr,
    target_register: usize,
) -> usize {
    match expr {
        ast::Expr::Between { .. } => todo!(),
        ast::Expr::Binary(_, _, _) => todo!(),
        ast::Expr::Case { .. } => todo!(),
        ast::Expr::Cast { .. } => todo!(),
        ast::Expr::Collate(_, _) => todo!(),
        ast::Expr::DoublyQualified(_, _, _) => todo!(),
        ast::Expr::Exists(_) => todo!(),
        ast::Expr::FunctionCall { .. } => todo!(),
        ast::Expr::FunctionCallStar { .. } => todo!(),
        ast::Expr::Id(ident) => {
            let (idx, col) = table.unwrap().get_column(&ident.0).unwrap();
            if col.primary_key {
                program.emit_insn(Insn::RowId {
                    cursor_id: cursor_id.unwrap(),
                    dest: target_register,
                });
            } else {
                program.emit_insn(Insn::Column {
                    column: idx,
                    dest: target_register,
                    cursor_id: cursor_id.unwrap(),
                });
            }
            target_register
        }
        ast::Expr::InList { .. } => todo!(),
        ast::Expr::InSelect { .. } => todo!(),
        ast::Expr::InTable { .. } => todo!(),
        ast::Expr::IsNull(_) => todo!(),
        ast::Expr::Like { .. } => todo!(),
        ast::Expr::Literal(lit) => match lit {
            ast::Literal::Numeric(val) => {
                let maybe_int = val.parse::<i64>();
                if maybe_int.is_ok() {
                    program.emit_insn(Insn::Integer {
                        value: maybe_int.unwrap(),
                        dest: target_register,
                    });
                } else {
                    // must be a float
                    program.emit_insn(Insn::Real {
                        value: val.parse().unwrap(),
                        dest: target_register,
                    });
                }
                target_register
            }
            ast::Literal::String(s) => {
                program.emit_insn(Insn::String8 {
                    value: s[1..s.len() - 1].to_string(),
                    dest: target_register,
                });
                target_register
            }
            ast::Literal::Blob(_) => todo!(),
            ast::Literal::Keyword(_) => todo!(),
            ast::Literal::Null => todo!(),
            ast::Literal::CurrentDate => todo!(),
            ast::Literal::CurrentTime => todo!(),
            ast::Literal::CurrentTimestamp => todo!(),
        },
        ast::Expr::Name(_) => todo!(),
        ast::Expr::NotNull(_) => todo!(),
        ast::Expr::Parenthesized(_) => todo!(),
        ast::Expr::Qualified(_, _) => todo!(),
        ast::Expr::Raise(_, _) => todo!(),
        ast::Expr::Subquery(_) => todo!(),
        ast::Expr::Unary(_, _) => todo!(),
        ast::Expr::Variable(_) => todo!(),
    }
}

fn translate_aggregation(
    program: &mut ProgramBuilder,
    cursor_id: Option<usize>,
    table: Option<&crate::schema::Table>,
    expr: &ast::Expr,
    info: &ColumnInfo,
    target_register: usize,
) -> Result<usize> {
    let _ = expr;
    assert!(info.func.is_some());
    let func = info.func.as_ref().unwrap();
    let args = info.args.as_ref().unwrap();
    let dest = match func {
        AggFunc::Avg => {
            if args.len() != 1 {
                anyhow::bail!("Parse error: avg bad number of arguments");
            }
            let expr = &args[0];
            let expr_reg = program.alloc_register();
            let _ = translate_expr(program, cursor_id, table, expr, expr_reg);
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                func: AggFunc::Avg,
            });
            target_register
        }
        AggFunc::Count => todo!(),
        AggFunc::GroupConcat => todo!(),
        AggFunc::Max => todo!(),
        AggFunc::Min => todo!(),
        AggFunc::StringAgg => todo!(),
        AggFunc::Sum => {
            if args.len() != 1 {
                anyhow::bail!("Parse error: sum bad number of arguments");
            }
            let expr = &args[0];
            let expr_reg = program.alloc_register();
            let _ = translate_expr(program, cursor_id, table, expr, expr_reg);
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                func: AggFunc::Sum,
            });
            target_register
        }
        AggFunc::Total => todo!(),
    };
    Ok(dest)
}

fn translate_pragma(
    name: &ast::QualifiedName,
    body: Option<ast::PragmaBody>,
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
                register_start: pragma_result,
                register_end: pragma_result_end,
            });
        }
        Some(ast::PragmaBody::Equals(value)) => {
            let value_to_update = match value {
                ast::Expr::Literal(ast::Literal::Numeric(numeric_value)) => {
                    numeric_value.parse::<i64>().unwrap()
                }
                ast::Expr::Unary(ast::UnaryOperator::Negative, expr) => match *expr {
                    ast::Expr::Literal(ast::Literal::Numeric(numeric_value)) => {
                        -numeric_value.parse::<i64>().unwrap()
                    }
                    _ => 0,
                },
                _ => 0,
            };
            update_pragma(&name.name.0, value_to_update, database_header, pager);
        }
        Some(ast::PragmaBody::Call(_)) => {
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
                .unwrap_or_else(|_| panic!("invalid value, too big for a i32 {}", value));

            // update in disk
            let header_copy = header.borrow().clone();
            pager.write_database_header(&header_copy);

            // update cache size
            pager.change_page_cache_size(cache_size);
        }
        _ => todo!(),
    }
}
