use std::cell::RefCell;
use std::rc::Rc;

use crate::pager::Pager;
use crate::schema::Schema;
use crate::sqlite3_ondisk::{DatabaseHeader, MIN_PAGE_CACHE_SIZE};
use crate::vdbe::{AggFunc, Insn, Program, ProgramBuilder};
use anyhow::Result;
use sqlite3_parser::ast::{
    Expr, Literal, OneSelect, PragmaBody, QualifiedName, Select, Stmt, UnaryOperator,
};

enum AggregationFunc {
    Avg,
    Count,
    GroupConcat,
    Max,
    Min,
    StringAgg,
    Sum,
    Total,
}

struct ColumnAggregationInfo {
    func: Option<AggregationFunc>,
    args: Option<Vec<Expr>>,
    columns_to_allocate: usize, /* number of result columns this col will result on */
}

impl ColumnAggregationInfo {
    pub fn new() -> Self {
        Self {
            func: None,
            args: None,
            columns_to_allocate: 1,
        }
    }

    pub fn is_aggregation_function(&self) -> bool {
        return self.func.is_some();
    }
}

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
    let limit_decr_insn = match select.body.select {
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
            program.emit_insn(Insn::OpenReadAsync {
                cursor_id,
                root_page,
            });
            program.emit_insn(Insn::OpenReadAwait);
            program.emit_insn(Insn::RewindAsync { cursor_id });
            let rewind_await_offset = program.emit_placeholder();
            let info_per_columns = analyze_columns(&columns, Some(table));
            let exist_aggregation = info_per_columns.iter().any(|info| info.func.is_some());
            let (register_start, register_end) = translate_columns(
                &mut program,
                Some(cursor_id),
                Some(table),
                &columns,
                &info_per_columns,
                exist_aggregation,
            );
            if exist_aggregation {
                program.emit_insn(Insn::NextAsync { cursor_id });
                program.emit_insn(Insn::NextAwait {
                    cursor_id,
                    pc_if_next: rewind_await_offset,
                });
                let mut target = register_start;
                for info in &info_per_columns {
                    if info.is_aggregation_function() {
                        let func = match info.func.as_ref().unwrap() {
                            AggregationFunc::Avg => AggFunc::Avg,
                            AggregationFunc::Count => todo!(),
                            AggregationFunc::GroupConcat => todo!(),
                            AggregationFunc::Max => todo!(),
                            AggregationFunc::Min => todo!(),
                            AggregationFunc::StringAgg => todo!(),
                            AggregationFunc::Sum => todo!(),
                            AggregationFunc::Total => todo!(),
                        };
                        program.emit_insn(Insn::AggFinal {
                            register: target,
                            func,
                        });
                    }
                    target += info.columns_to_allocate;
                }
                // only one result row
                program.emit_insn(Insn::ResultRow {
                    register_start,
                    register_end,
                });
            } else {
                program.emit_insn(Insn::ResultRow {
                    register_start,
                    register_end,
                });
                program.emit_insn(Insn::NextAsync { cursor_id });
                program.emit_insn(Insn::NextAwait {
                    cursor_id,
                    pc_if_next: rewind_await_offset,
                });
            }
            let limit_decr_insn = limit_reg.map(|_| program.emit_placeholder());
            program.fixup_insn(
                rewind_await_offset,
                Insn::RewindAwait {
                    cursor_id,
                    pc_if_empty: program.offset(),
                },
            );
            limit_decr_insn
        }
        OneSelect::Select {
            columns,
            from: None,
            ..
        } => {
            let info_per_columns = analyze_columns(&columns, None);
            let exist_aggregation = info_per_columns.iter().any(|info| info.func.is_some());
            let (register_start, register_end) = translate_columns(
                &mut program,
                None,
                None,
                &columns,
                &info_per_columns,
                exist_aggregation,
            );
            program.emit_insn(Insn::ResultRow {
                register_start,
                register_end,
            });
            let limit_decr_insn = limit_reg.map(|_| program.emit_placeholder());
            limit_decr_insn
        }
        _ => todo!(),
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
    table: Option<&crate::schema::Table>,
    columns: &Vec<sqlite3_parser::ast::ResultColumn>,
    info_per_columns: &Vec<ColumnAggregationInfo>,
    exist_aggregation: bool,
) -> (usize, usize) {
    let register_start = program.next_free_register();

    // allocate one register as output for each col
    let registers: usize = info_per_columns
        .iter()
        .map(|col| col.columns_to_allocate)
        .sum();
    program.alloc_registers(registers);
    let register_end = program.next_free_register();

    let mut target = register_start;
    for (i, (col, info)) in columns.iter().zip(info_per_columns).enumerate() {
        translate_column(
            program,
            cursor_id,
            table,
            col,
            info,
            exist_aggregation,
            target,
        );
        target += info.columns_to_allocate;
    }
    (register_start, register_end)
}

fn translate_column(
    program: &mut ProgramBuilder,
    cursor_id: Option<usize>,
    table: Option<&crate::schema::Table>,
    col: &sqlite3_parser::ast::ResultColumn,
    info: &ColumnAggregationInfo,
    exist_aggregation: bool, // notify this column there is aggregation going on in other columns (or this one)
    target_register: usize, // where to store the result, in case of star it will be the start of registers added
) {
    if exist_aggregation && !info.is_aggregation_function() {
        // FIXME: let's do nothing
        return;
    }

    match col {
        sqlite3_parser::ast::ResultColumn::Expr(expr, _) => {
            if info.is_aggregation_function() {
                translate_aggregation(program, cursor_id, table, &expr, info, target_register);
            } else {
                let _ = translate_expr(program, cursor_id, table, &expr, target_register);
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
) -> Vec<ColumnAggregationInfo> {
    let mut column_information_list = Vec::new();
    column_information_list.reserve(columns.len());

    for column in columns {
        let mut info = ColumnAggregationInfo::new();
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

fn analyze_column(
    column: &sqlite3_parser::ast::ResultColumn,
    column_info_out: &mut ColumnAggregationInfo,
) {
    match column {
        sqlite3_parser::ast::ResultColumn::Expr(expr, _) => match expr {
            Expr::FunctionCall {
                name,
                distinctness,
                args,
                filter_over,
            } => {
                let func_type = match name.0.as_str() {
                    "avg" => Some(AggregationFunc::Avg),
                    "count" => Some(AggregationFunc::Count),
                    "group_concat" => Some(AggregationFunc::GroupConcat),
                    "max" => Some(AggregationFunc::Max),
                    "min" => Some(AggregationFunc::Min),
                    "string_agg" => Some(AggregationFunc::StringAgg),
                    "sum" => Some(AggregationFunc::Sum),
                    "total" => Some(AggregationFunc::Total),
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
            Expr::FunctionCallStar { .. } => todo!(),
            _ => {}
        },
        sqlite3_parser::ast::ResultColumn::Star => {}
        sqlite3_parser::ast::ResultColumn::TableStar(_) => {}
    }
}

fn translate_expr(
    program: &mut ProgramBuilder,
    cursor_id: Option<usize>,
    table: Option<&crate::schema::Table>,
    expr: &Expr,
    target_register: usize,
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
        Expr::InList { .. } => todo!(),
        Expr::InSelect { .. } => todo!(),
        Expr::InTable { .. } => todo!(),
        Expr::IsNull(_) => todo!(),
        Expr::Like { .. } => todo!(),
        Expr::Literal(lit) => match lit {
            Literal::Numeric(val) => {
                program.emit_insn(Insn::Integer {
                    value: val.parse().unwrap(),
                    dest: target_register,
                });
                target_register
            }
            Literal::String(s) => {
                program.emit_insn(Insn::String8 {
                    value: s[1..s.len() - 1].to_string(),
                    dest: target_register,
                });
                target_register
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

fn translate_aggregation(
    program: &mut ProgramBuilder,
    cursor_id: Option<usize>,
    table: Option<&crate::schema::Table>,
    expr: &Expr,
    info: &ColumnAggregationInfo,
    target_register: usize,
) -> Result<usize> {
    assert!(info.func.is_some());
    let func = info.func.as_ref().unwrap();
    let args = info.args.as_ref().unwrap();
    let dest = match func {
        AggregationFunc::Avg => {
            if args.len() != 1 {
                anyhow::bail!("Parse error: avg bad number of arguments");
            }
            let expr = &args[0];
            let expr_reg = program.alloc_register();
            let _ = translate_expr(program, cursor_id, table, &expr, expr_reg);
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                func: crate::vdbe::AggFunc::Avg,
            });
            target_register
        }
        AggregationFunc::Count => todo!(),
        AggregationFunc::GroupConcat => todo!(),
        AggregationFunc::Max => todo!(),
        AggregationFunc::Min => todo!(),
        AggregationFunc::StringAgg => todo!(),
        AggregationFunc::Sum => todo!(),
        AggregationFunc::Total => todo!(),
    };
    Ok(dest)
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
                register_start: pragma_result,
                register_end: pragma_result_end,
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
