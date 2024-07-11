use std::cell::RefCell;
use std::rc::Rc;

use crate::function::AggFunc;
use crate::pager::Pager;
use crate::schema::{Column, Schema, Table};
use crate::sqlite3_ondisk::{DatabaseHeader, MIN_PAGE_CACHE_SIZE};
use crate::util::normalize_ident;
use crate::vdbe::{Insn, Program, ProgramBuilder};
use anyhow::Result;
use sqlite3_parser::ast::{self, Expr};

struct Select {
    columns: Vec<ast::ResultColumn>,
    column_info: Vec<ColumnInfo>,
    src_tables: Vec<SrcTable>, // Tables we use to get data from. This includes "from" and "joins"
    limit: Option<ast::Limit>,
    exist_aggregation: bool,
    /// Ordered list of opened read table loops
    /// Used for generating a loop that looks like this:
    /// cursor 0 = open table 0
    /// for each row in cursor 0
    ///     cursor 1 = open table 1
    ///     for each row in cursor 1
    ///         ...
    ///     end cursor 1
    /// end cursor 0
    loops: Vec<LoopInfo>,
}

struct LoopInfo {
    table: Table,
    rewind_offset: usize,
    open_cursor: usize,
}

struct SrcTable {
    table: Table,
    join_info: Option<ast::JoinedSelectTable>, // FIXME: preferably this should be a reference with lifetime == Select ast expr
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
            let mut joins = Vec::new();
            joins.push(SrcTable {
                table: Table::BTree(table.clone()),
                join_info: None,
            });
            match from.joins {
                Some(selected_joins) => {
                    for join in selected_joins {
                        let table_name = match &join.table {
                            ast::SelectTable::Table(name, ..) => name.name.clone(),
                            _ => todo!(),
                        };
                        let table_name = &table_name.0;
                        let table = match schema.get_table(table_name) {
                            Some(table) => table,
                            None => anyhow::bail!("Parse error: no such table: {}", table_name),
                        };
                        joins.push(SrcTable {
                            table: Table::BTree(table),
                            join_info: Some(join.clone()),
                        });
                    }
                }
                None => {}
            };

            let table = Table::BTree(table);
            let column_info = analyze_columns(&columns, &joins);
            let exist_aggregation = column_info.iter().any(|info| info.func.is_some());
            Ok(Select {
                columns,
                column_info,
                src_tables: joins,
                limit: select.limit.clone(),
                exist_aggregation,
                loops: Vec::new(),
            })
        }
        ast::OneSelect::Select {
            columns,
            from: None,
            ..
        } => {
            let column_info = analyze_columns(&columns, &Vec::new());
            let exist_aggregation = column_info.iter().any(|info| info.func.is_some());
            Ok(Select {
                columns,
                column_info,
                src_tables: Vec::new(),
                limit: select.limit.clone(),
                exist_aggregation,
                loops: Vec::new(),
            })
        }
        _ => todo!(),
    }
}

/// Generate code for a SELECT statement.
fn translate_select(mut select: Select) -> Result<Program> {
    let mut program = ProgramBuilder::new();
    let init_offset = program.emit_placeholder();
    let start_offset = program.offset();
    let limit_reg = if let Some(limit) = &select.limit {
        assert!(limit.offset.is_none());
        let target_register = program.alloc_register();
        Some(translate_expr(
            &mut program,
            &select,
            &limit.expr,
            target_register,
        )?)
    } else {
        None
    };
    let parsed_limit = select.limit.as_ref().and_then(|limit| {
        if let ast::Expr::Literal(ast::Literal::Numeric(num)) = &limit.expr {
            num.parse::<i64>().ok()
        } else {
            None
        }
    });
    let limit_goto = match parsed_limit {
        Some(0) => Some(program.emit_placeholder()),
        _ => None,
    };
    let limit_insn = if !select.src_tables.is_empty() {
        translate_tables_begin(&mut program, &mut select);

        let (register_start, register_end) = translate_columns(&mut program, &select)?;

        let mut limit_insn: Option<usize> = None;
        if !select.exist_aggregation {
            program.emit_insn(Insn::ResultRow {
                start_reg: register_start,
                count: register_end - register_start,
            });
            limit_insn = limit_reg.map(|_| program.emit_placeholder());
        }

        translate_tables_end(&mut program, &select);

        if select.exist_aggregation {
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
                start_reg: register_start,
                count: register_end - register_start,
            });
            limit_insn = limit_reg.map(|_| program.emit_placeholder());
        }
        limit_insn
    } else {
        assert!(!select.exist_aggregation);
        let (register_start, register_end) = translate_columns(&mut program, &select)?;
        program.emit_insn(Insn::ResultRow {
            start_reg: register_start,
            count: register_end - register_start,
        });
        limit_reg.map(|_| program.emit_placeholder())
    };
    program.emit_insn(Insn::Halt);
    let halt_offset = program.offset() - 1;
    if let Some(limit_goto) = limit_goto {
        program.fixup_insn(
            limit_goto,
            Insn::Goto {
                target_pc: halt_offset,
            },
        );
    }
    if let Some(limit_insn) = limit_insn {
        let insn = match parsed_limit {
            Some(0) => Insn::Goto {
                target_pc: halt_offset,
            },
            _ => Insn::DecrJumpZero {
                reg: limit_reg.unwrap(),
                target_pc: halt_offset,
            },
        };
        program.fixup_insn(limit_insn, insn);
    }
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

fn translate_tables_begin(program: &mut ProgramBuilder, select: &mut Select) {
    for join in &select.src_tables {
        let table = &join.table;
        let loop_info = translate_table_open_cursor(program, table);
        select.loops.push(loop_info);
    }

    for loop_info in &mut select.loops {
        translate_table_open_loop(program, loop_info);
    }
}

fn translate_tables_end(program: &mut ProgramBuilder, select: &Select) {
    // iterate in reverse order as we open cursors in order
    for table_loop in select.loops.iter().rev() {
        let cursor_id = table_loop.open_cursor;
        program.emit_insn(Insn::NextAsync { cursor_id });
        program.emit_insn(Insn::NextAwait {
            cursor_id,
            pc_if_next: table_loop.rewind_offset,
        });
        program.fixup_insn(
            table_loop.rewind_offset,
            Insn::RewindAwait {
                cursor_id: table_loop.open_cursor,
                pc_if_empty: program.offset(),
            },
        );
    }
}

fn translate_table_open_cursor(program: &mut ProgramBuilder, table: &Table) -> LoopInfo {
    let cursor_id = program.alloc_cursor_id();
    let root_page = match table {
        Table::BTree(btree) => btree.root_page,
        Table::Pseudo(_) => todo!(),
    };
    program.emit_insn(Insn::OpenReadAsync {
        cursor_id,
        root_page,
    });
    program.emit_insn(Insn::OpenReadAwait);
    LoopInfo {
        table: table.clone(),
        open_cursor: cursor_id,
        rewind_offset: 0,
    }
}

fn translate_table_open_loop(program: &mut ProgramBuilder, loop_info: &mut LoopInfo) {
    program.emit_insn(Insn::RewindAsync {
        cursor_id: loop_info.open_cursor,
    });
    let rewind_await_offset = program.emit_placeholder();
    loop_info.rewind_offset = rewind_await_offset;
}

fn translate_columns(program: &mut ProgramBuilder, select: &Select) -> Result<(usize, usize)> {
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
        translate_column(program, select, col, info, target)?;
        target += info.columns_to_allocate;
    }
    Ok((register_start, register_end))
}

fn translate_column(
    program: &mut ProgramBuilder,
    select: &Select,
    col: &ast::ResultColumn,
    info: &ColumnInfo,
    target_register: usize, // where to store the result, in case of star it will be the start of registers added
) -> Result<()> {
    match col {
        ast::ResultColumn::Expr(expr, _) => {
            if info.is_aggregation_function() {
                let _ = translate_aggregation(program, select, expr, info, target_register)?;
            } else {
                let _ = translate_expr(program, select, expr, target_register)?;
            }
        }
        ast::ResultColumn::Star => {
            let mut target_register = target_register;
            for join in &select.src_tables {
                let table = &join.table;
                translate_table_star(table, program, &select, target_register);
                target_register += table.columns().len();
            }
        }
        ast::ResultColumn::TableStar(_) => todo!(),
    }
    Ok(())
}

fn translate_table_star(
    table: &Table,
    program: &mut ProgramBuilder,
    select: &Select,
    target_register: usize,
) {
    let table_cursor = select
        .loops
        .iter()
        .find(|v| v.table == *table)
        .unwrap()
        .open_cursor;
    for (i, col) in table.columns().iter().enumerate() {
        let col_target_register = target_register + i;
        if table.column_is_rowid_alias(col) {
            program.emit_insn(Insn::RowId {
                cursor_id: table_cursor,
                dest: col_target_register,
            });
        } else {
            program.emit_insn(Insn::Column {
                column: i,
                dest: col_target_register,
                cursor_id: table_cursor,
            });
            maybe_apply_affinity(col, col_target_register, program);
        }
    }
}

fn analyze_columns(columns: &Vec<ast::ResultColumn>, joins: &Vec<SrcTable>) -> Vec<ColumnInfo> {
    let mut column_information_list = Vec::with_capacity(columns.len());
    for column in columns {
        let mut info = ColumnInfo::new();
        if let ast::ResultColumn::Star = column {
            info.columns_to_allocate = 0;
            for join in joins {
                info.columns_to_allocate += join.table.columns().len();
            }
        } else {
            info.columns_to_allocate = 1;
            analyze_column(column, &mut info);
        }
        column_information_list.push(info);
    }
    column_information_list
}

/// Analyze a column expression.
///
/// This function will walk all columns and find information about:
/// * Aggregation functions.
fn analyze_column(column: &ast::ResultColumn, column_info_out: &mut ColumnInfo) {
    match column {
        ast::ResultColumn::Expr(expr, _) => analyze_expr(expr, column_info_out),
        ast::ResultColumn::Star => {}
        ast::ResultColumn::TableStar(_) => {}
    }
}

fn analyze_expr(expr: &Expr, column_info_out: &mut ColumnInfo) {
    match expr {
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
                let args = args.as_ref().unwrap();
                if args.len() > 0 {
                    analyze_expr(&args.get(0).unwrap(), column_info_out);
                }
            } else {
                column_info_out.func = func_type;
                // TODO(pere): use lifetimes for args? Arenas would be lovely here :(
                column_info_out.args.clone_from(args);
            }
        }
        ast::Expr::FunctionCallStar { .. } => todo!(),
        _ => {}
    }
}

fn translate_expr(
    program: &mut ProgramBuilder,
    select: &Select,
    expr: &ast::Expr,
    target_register: usize,
) -> Result<usize> {
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
            // let (idx, col) = table.unwrap().get_column(&ident.0).unwrap();
            let (idx, col, cursor_id) = resolve_ident_table(&ident.0, select)?;
            if col.primary_key {
                program.emit_insn(Insn::RowId {
                    cursor_id,
                    dest: target_register,
                });
            } else {
                program.emit_insn(Insn::Column {
                    column: idx,
                    dest: target_register,
                    cursor_id,
                });
            }
            maybe_apply_affinity(col, target_register, program);
            Ok(target_register)
        }
        ast::Expr::InList { .. } => todo!(),
        ast::Expr::InSelect { .. } => todo!(),
        ast::Expr::InTable { .. } => todo!(),
        ast::Expr::IsNull(_) => todo!(),
        ast::Expr::Like { .. } => todo!(),
        ast::Expr::Literal(lit) => match lit {
            ast::Literal::Numeric(val) => {
                let maybe_int = val.parse::<i64>();
                if let Ok(int_value) = maybe_int {
                    program.emit_insn(Insn::Integer {
                        value: int_value,
                        dest: target_register,
                    });
                } else {
                    // must be a float
                    program.emit_insn(Insn::Real {
                        value: val.parse().unwrap(),
                        dest: target_register,
                    });
                }
                Ok(target_register)
            }
            ast::Literal::String(s) => {
                program.emit_insn(Insn::String8 {
                    value: s[1..s.len() - 1].to_string(),
                    dest: target_register,
                });
                Ok(target_register)
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

fn resolve_ident_table<'a>(
    ident: &String,
    select: &'a Select,
) -> Result<(usize, &'a Column, usize)> {
    for join in &select.src_tables {
        let res = join
            .table
            .columns()
            .iter()
            .enumerate()
            .find(|(_, col)| col.name == *ident);
        if res.is_some() {
            let (idx, col) = res.unwrap();
            let cursor_id = select
                .loops
                .iter()
                .find(|l| l.table == join.table)
                .unwrap()
                .open_cursor;
            return Ok((idx, col, cursor_id));
        }
    }
    anyhow::bail!("Parse error: column with name {} not found", ident.as_str());
}

fn translate_aggregation(
    program: &mut ProgramBuilder,
    select: &Select,
    expr: &ast::Expr,
    info: &ColumnInfo,
    target_register: usize,
) -> Result<usize> {
    let _ = expr;
    assert!(info.func.is_some());
    let func = info.func.as_ref().unwrap();
    let empty_args = &Vec::<ast::Expr>::new();
    let args = info.args.as_ref().unwrap_or(empty_args);
    let dest = match func {
        AggFunc::Avg => {
            if args.len() != 1 {
                anyhow::bail!("Parse error: avg bad number of arguments");
            }
            let expr = &args[0];
            let expr_reg = program.alloc_register();
            let _ = translate_expr(program, select, expr, expr_reg)?;
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                func: AggFunc::Avg,
            });
            target_register
        }
        AggFunc::Count => {
            let expr_reg = if args.is_empty() {
                program.alloc_register()
            } else {
                let expr = &args[0];
                let expr_reg = program.alloc_register();
                let _ = translate_expr(program, select, expr, expr_reg);
                expr_reg
            };
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                func: AggFunc::Count,
            });
            target_register
        }
        AggFunc::GroupConcat => todo!(),
        AggFunc::Max => {
            if args.len() != 1 {
                anyhow::bail!("Parse error: max bad number of arguments");
            }
            let expr = &args[0];
            let expr_reg = program.alloc_register();
            let _ = translate_expr(program, select, expr, expr_reg);
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                func: AggFunc::Max,
            });
            target_register
        }
        AggFunc::Min => {
            if args.len() != 1 {
                anyhow::bail!("Parse error: min bad number of arguments");
            }
            let expr = &args[0];
            let expr_reg = program.alloc_register();
            let _ = translate_expr(program, select, expr, expr_reg);
            program.emit_insn(Insn::AggStep {
                acc_reg: target_register,
                col: expr_reg,
                func: AggFunc::Min,
            });
            target_register
        }
        AggFunc::StringAgg => todo!(),
        AggFunc::Sum => {
            if args.len() != 1 {
                anyhow::bail!("Parse error: sum bad number of arguments");
            }
            let expr = &args[0];
            let expr_reg = program.alloc_register();
            let _ = translate_expr(program, select, expr, expr_reg)?;
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
                start_reg: pragma_result,
                count: pragma_result_end - pragma_result,
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

fn update_pragma(name: &str, value: i64, header: Rc<RefCell<DatabaseHeader>>, pager: Rc<Pager>) {
    match name {
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

fn maybe_apply_affinity(col: &Column, target_register: usize, program: &mut ProgramBuilder) {
    match col.ty {
        crate::schema::Type::Real => program.emit_insn(Insn::RealAffinity {
            register: target_register,
        }),
        _ => {}
    }
}
