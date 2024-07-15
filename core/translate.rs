use std::cell::RefCell;
use std::rc::Rc;

use crate::function::{AggFunc, Func, SingleRowFunc};
use crate::pager::Pager;
use crate::schema::{Column, Schema, Table};
use crate::sqlite3_ondisk::{DatabaseHeader, MIN_PAGE_CACHE_SIZE};
use crate::util::normalize_ident;
use crate::vdbe::{BranchOffset, Insn, Program, ProgramBuilder};
use anyhow::Result;
use sqlite3_parser::ast::{self, Expr, JoinOperator, Literal, UnaryOperator};

const HARDCODED_CURSOR_LEFT_TABLE: usize = 0;
const HARDCODED_CURSOR_RIGHT_TABLE: usize = 1;

struct Select<'a> {
    columns: &'a Vec<ast::ResultColumn>,
    column_info: Vec<ColumnInfo<'a>>,
    src_tables: Vec<SrcTable<'a>>, // Tables we use to get data from. This includes "from" and "joins"
    limit: &'a Option<ast::Limit>,
    exist_aggregation: bool,
    where_clause: &'a Option<ast::Expr>,
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
    rewind_offset: BranchOffset,
    rewind_label: BranchOffset,
    open_cursor: usize,
}

struct SrcTable<'a> {
    table: Table,
    alias: Option<&'a String>,
    join_info: Option<&'a ast::JoinedSelectTable>, // FIXME: preferably this should be a reference with lifetime == Select ast expr
}

struct ColumnInfo<'a> {
    func: Option<Func>,
    args: &'a Option<Vec<ast::Expr>>,
    columns_to_allocate: usize, /* number of result columns this col will result on */
}

impl<'a> ColumnInfo<'a> {
    pub fn new() -> Self {
        Self {
            func: None,
            args: &None,
            columns_to_allocate: 1,
        }
    }

    pub fn is_aggregation_function(&self) -> bool {
        matches!(self.func, Some(Func::Agg(_)))
    }
}

struct LimitInfo {
    limit_reg: usize,
    num: i64,
    goto_label: BranchOffset,
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
            let select = build_select(schema, &select)?;
            translate_select(select)
        }
        ast::Stmt::Pragma(name, body) => translate_pragma(&name, body, database_header, pager),
        _ => todo!(),
    }
}

fn build_select<'a>(schema: &Schema, select: &'a ast::Select) -> Result<Select<'a>> {
    match &select.body.select {
        ast::OneSelect::Select {
            columns,
            from: Some(from),
            where_clause,
            ..
        } => {
            let (table_name, maybe_alias) = match &from.select {
                Some(select_table) => match select_table.as_ref() {
                    ast::SelectTable::Table(name, alias, ..) => (
                        &name.name,
                        alias.as_ref().map(|als| match als {
                            ast::As::As(alias) => alias,     // users as u
                            ast::As::Elided(alias) => alias, // users u
                        }),
                    ),
                    _ => todo!(),
                },
                None => todo!(),
            };
            let table_name = &table_name.0;
            let maybe_alias = maybe_alias.map(|als| &als.0);
            let table = match schema.get_table(&table_name) {
                Some(table) => table,
                None => anyhow::bail!("Parse error: no such table: {}", table_name),
            };
            let mut joins = Vec::new();
            joins.push(SrcTable {
                table: Table::BTree(table.clone()),
                alias: maybe_alias,
                join_info: None,
            });
            if let Some(selected_joins) = &from.joins {
                for join in selected_joins {
                    let (table_name, maybe_alias) = match &join.table {
                        ast::SelectTable::Table(name, alias, ..) => (
                            &name.name,
                            alias.as_ref().map(|als| match als {
                                ast::As::As(alias) => alias,     // users as u
                                ast::As::Elided(alias) => alias, // users u
                            }),
                        ),
                        _ => todo!(),
                    };
                    let table_name = &table_name.0;
                    let maybe_alias = maybe_alias.as_ref().map(|als| &als.0);
                    let table = match schema.get_table(table_name) {
                        Some(table) => table,
                        None => anyhow::bail!("Parse error: no such table: {}", table_name),
                    };
                    joins.push(SrcTable {
                        table: Table::BTree(table),
                        alias: maybe_alias,
                        join_info: Some(&join),
                    });
                }
            }

            let _table = Table::BTree(table);
            let column_info = analyze_columns(columns, &joins);
            let exist_aggregation = column_info
                .iter()
                .any(|info| info.is_aggregation_function());
            Ok(Select {
                columns,
                column_info,
                src_tables: joins,
                limit: &select.limit,
                exist_aggregation,
                where_clause,
                loops: Vec::new(),
            })
        }
        ast::OneSelect::Select {
            columns,
            from: None,
            where_clause,
            ..
        } => {
            let column_info = analyze_columns(columns, &Vec::new());
            let exist_aggregation = column_info
                .iter()
                .any(|info| info.is_aggregation_function());
            Ok(Select {
                columns,
                column_info,
                src_tables: Vec::new(),
                limit: &select.limit,
                where_clause,
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
    let init_label = program.allocate_label();
    program.emit_insn_with_label_dependency(
        Insn::Init {
            target_pc: init_label,
        },
        init_label,
    );
    let start_offset = program.offset();

    let limit_info = if let Some(limit) = &select.limit {
        assert!(limit.offset.is_none());
        let target_register = program.alloc_register();
        let limit_reg = translate_expr(&mut program, &select, &limit.expr, target_register)?;
        let num = if let ast::Expr::Literal(ast::Literal::Numeric(num)) = &limit.expr {
            num.parse::<i64>()?
        } else {
            todo!();
        };
        let goto_label = program.allocate_label();
        if num == 0 {
            program.emit_insn_with_label_dependency(
                Insn::Goto {
                    target_pc: goto_label,
                },
                goto_label,
            );
        }
        Some(LimitInfo {
            limit_reg,
            num,
            goto_label,
        })
    } else {
        None
    };

    if !select.src_tables.is_empty() {
        let constraint = translate_tables_begin(&mut program, &mut select)?;

        let (register_start, register_end) = translate_columns(&mut program, &select)?;

        if !select.exist_aggregation {
            program.emit_insn(Insn::ResultRow {
                start_reg: register_start,
                count: register_end - register_start,
            });
            emit_limit_insn(&limit_info, &mut program);
        }

        translate_tables_end(&mut program, &select, constraint);

        if select.exist_aggregation {
            let mut target = register_start;
            for info in &select.column_info {
                if let Some(Func::Agg(func)) = &info.func {
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
            emit_limit_insn(&limit_info, &mut program);
        }
    } else {
        assert!(!select.exist_aggregation);
        let where_maybe = translate_where(&select, &mut program)?;
        let (register_start, register_end) = translate_columns(&mut program, &select)?;
        if let Some(where_clause_label) = where_maybe {
            program.resolve_label(where_clause_label, program.offset() + 1);
        }
        program.emit_insn(Insn::ResultRow {
            start_reg: register_start,
            count: register_end - register_start,
        });
        emit_limit_insn(&limit_info, &mut program);
    };
    program.emit_insn(Insn::Halt);
    let halt_offset = program.offset() - 1;
    if let Some(limit_info) = limit_info {
        if limit_info.goto_label < 0 {
            program.resolve_label(limit_info.goto_label, halt_offset);
        }
    }
    program.resolve_label(init_label, program.offset());
    program.emit_insn(Insn::Transaction);
    program.emit_constant_insns();
    program.emit_insn(Insn::Goto {
        target_pc: start_offset,
    });
    program.resolve_deferred_labels();
    Ok(program.build())
}

fn emit_limit_insn(limit_info: &Option<LimitInfo>, program: &mut ProgramBuilder) {
    if limit_info.is_none() {
        return;
    }
    let limit_info = limit_info.as_ref().unwrap();
    if limit_info.num > 0 {
        program.emit_insn_with_label_dependency(
            Insn::DecrJumpZero {
                reg: limit_info.limit_reg,
                target_pc: limit_info.goto_label,
            },
            limit_info.goto_label,
        );
    }
}

fn translate_where(select: &Select, program: &mut ProgramBuilder) -> Result<Option<BranchOffset>> {
    if let Some(w) = &select.where_clause {
        let label = program.allocate_label();
        translate_condition_expr(program, select, w, label, false)?;
        Ok(Some(label))
    } else {
        Ok(None)
    }
}

fn introspect_expression_for_cursors(
    program: &ProgramBuilder,
    select: &Select,
    where_expr: &ast::Expr,
) -> Result<Vec<usize>> {
    let mut cursors = vec![];
    match where_expr {
        ast::Expr::Binary(e1, _, e2) => {
            cursors.extend(introspect_expression_for_cursors(program, select, e1)?);
            cursors.extend(introspect_expression_for_cursors(program, select, e2)?);
        }
        ast::Expr::Id(ident) => {
            let (_, _, cursor_id) = resolve_ident_table(program, &ident.0, select)?;
            cursors.push(cursor_id);
        }
        ast::Expr::Qualified(tbl, ident) => {
            let (_, _, cursor_id) = resolve_ident_qualified(program, &tbl.0, &ident.0, select)?;
            cursors.push(cursor_id);
        }
        ast::Expr::Literal(_) => {}
        ast::Expr::Like {
            lhs,
            not,
            op,
            rhs,
            escape,
        } => {
            cursors.extend(introspect_expression_for_cursors(program, select, lhs)?);
            cursors.extend(introspect_expression_for_cursors(program, select, rhs)?);
        }
        other => {
            anyhow::bail!("Parse error: unsupported expression: {:?}", other);
        }
    }

    Ok(cursors)
}

fn get_no_match_target_cursor(
    program: &ProgramBuilder,
    select: &Select,
    expr: &ast::Expr,
) -> usize {
    // This is the hackiest part of the code. We are finding the cursor that should be advanced to the next row
    // when the condition is not met. This is done by introspecting the expression and finding the innermost cursor that is
    // used in the expression. This is a very naive approach and will not work in all cases.
    // Thankfully though it might be possible to just refine the logic contained here to make it work in all cases. Maybe.
    let cursors = introspect_expression_for_cursors(program, select, expr).unwrap_or_default();
    if cursors.is_empty() {
        HARDCODED_CURSOR_LEFT_TABLE
    } else {
        *cursors.iter().max().unwrap()
    }
}

fn evaluate_conditions(
    program: &mut ProgramBuilder,
    select: &Select,
) -> Result<Option<QueryConstraint>> {
    let join_constraints = select
        .src_tables
        .iter()
        .map(|v| v.join_info.clone())
        .filter_map(|v| v.map(|v| (v.constraint.clone(), v.operator)))
        .collect::<Vec<_>>();
    // TODO: only supports one JOIN; -> add support for multiple JOINs, e.g. SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id
    if join_constraints.len() > 1 {
        anyhow::bail!("Parse error: multiple JOINs not supported");
    }

    let join_maybe = join_constraints.first();

    let parsed_where_maybe = select.where_clause.as_ref().map(|where_clause| Where {
        constraint_expr: where_clause.clone(),
        no_match_jump_label: program.allocate_label(),
        no_match_target_cursor: get_no_match_target_cursor(program, select, &where_clause),
    });

    let parsed_join_maybe = join_maybe
        .map(|(constraint, _)| {
            if let Some(ast::JoinConstraint::On(expr)) = constraint {
                Some(Join {
                    constraint_expr: expr.clone(),
                    no_match_jump_label: program.allocate_label(),
                    no_match_target_cursor: get_no_match_target_cursor(program, select, expr),
                })
            } else {
                None
            }
        })
        .flatten();

    let constraint_maybe = match (parsed_where_maybe, parsed_join_maybe) {
        (None, None) => None,
        (Some(where_clause), None) => Some(QueryConstraint::Inner(Inner {
            where_clause: Some(where_clause),
            join_clause: None,
        })),
        (where_clause, Some(join_clause)) => {
            let (_, op) = join_maybe.unwrap();
            match op {
                JoinOperator::TypedJoin { natural, join_type } => {
                    if *natural {
                        todo!("Natural join not supported");
                    }
                    // default to inner join when no join type is specified
                    let join_type = join_type.unwrap_or(ast::JoinType::Inner);
                    match join_type {
                        ast::JoinType::Inner | ast::JoinType::Cross => {
                            // cross join with a condition is an inner join
                            Some(QueryConstraint::Inner(Inner {
                                where_clause,
                                join_clause: Some(join_clause),
                            }))
                        }
                        ast::JoinType::LeftOuter | ast::JoinType::Left => {
                            let left_join_match_flag = program.alloc_register();
                            let left_join_match_flag_hit_marker = program.allocate_label();
                            let left_join_found_match_next_row_label = program.allocate_label();

                            Some(QueryConstraint::Left(Left {
                                where_clause,
                                join_clause: Some(join_clause),
                                found_match_next_row_label: left_join_found_match_next_row_label,
                                match_flag: left_join_match_flag,
                                match_flag_hit_marker: left_join_match_flag_hit_marker,
                                left_cursor: HARDCODED_CURSOR_LEFT_TABLE, // FIXME: hardcoded
                                right_cursor: HARDCODED_CURSOR_RIGHT_TABLE, // FIXME: hardcoded
                            }))
                        }
                        ast::JoinType::RightOuter | ast::JoinType::Right => {
                            todo!();
                        }
                        ast::JoinType::FullOuter | ast::JoinType::Full => {
                            todo!();
                        }
                    }
                }
                JoinOperator::Comma => {
                    todo!();
                }
            }
        }
    };

    Ok(constraint_maybe)
}

fn translate_conditions(
    program: &mut ProgramBuilder,
    select: &Select,
    conditions: Option<QueryConstraint>,
) -> Result<Option<QueryConstraint>> {
    match conditions.as_ref() {
        Some(QueryConstraint::Left(Left {
            where_clause,
            join_clause,
            match_flag,
            match_flag_hit_marker,
            ..
        })) => {
            if let Some(where_clause) = where_clause {
                translate_condition_expr(
                    program,
                    select,
                    &where_clause.constraint_expr,
                    where_clause.no_match_jump_label,
                    false,
                )?;
            }
            if let Some(join_clause) = join_clause {
                translate_condition_expr(
                    program,
                    select,
                    &join_clause.constraint_expr,
                    join_clause.no_match_jump_label,
                    false,
                )?;
            }
            // Set match flag to 1 if we hit the marker (i.e. jump didn't happen to no_match_label as a result of the condition)
            program.emit_insn(Insn::Integer {
                value: 1,
                dest: *match_flag,
            });
            program.defer_label_resolution(*match_flag_hit_marker, (program.offset() - 1) as usize);
        }
        Some(QueryConstraint::Inner(inner_join)) => {
            if let Some(where_clause) = &inner_join.where_clause {
                translate_condition_expr(
                    program,
                    select,
                    &where_clause.constraint_expr,
                    where_clause.no_match_jump_label,
                    false,
                )?;
            }
            if let Some(join_clause) = &inner_join.join_clause {
                translate_condition_expr(
                    program,
                    select,
                    &join_clause.constraint_expr,
                    join_clause.no_match_jump_label,
                    false,
                )?;
            }
        }
        None => {}
    }

    Ok(conditions)
}

#[derive(Debug)]
struct Where {
    constraint_expr: ast::Expr,
    no_match_jump_label: BranchOffset,
    no_match_target_cursor: usize,
}

#[derive(Debug)]
struct Join {
    constraint_expr: ast::Expr,
    no_match_jump_label: BranchOffset,
    no_match_target_cursor: usize,
}

#[derive(Debug)]
struct Left {
    where_clause: Option<Where>,
    join_clause: Option<Join>,
    match_flag: usize,
    match_flag_hit_marker: BranchOffset,
    found_match_next_row_label: BranchOffset,
    left_cursor: usize,
    right_cursor: usize,
}

#[derive(Debug)]
struct Inner {
    where_clause: Option<Where>,
    join_clause: Option<Join>,
}

enum QueryConstraint {
    Left(Left),
    Inner(Inner),
}

fn translate_tables_begin(
    program: &mut ProgramBuilder,
    select: &mut Select,
) -> Result<Option<QueryConstraint>> {
    for join in &select.src_tables {
        let loop_info = translate_table_open_cursor(program, join);
        select.loops.push(loop_info);
    }

    let conditions = evaluate_conditions(program, select)?;

    for loop_info in &mut select.loops {
        let mut left_join_match_flag_maybe = None;
        if let Some(QueryConstraint::Left(Left {
            match_flag,
            right_cursor,
            ..
        })) = conditions.as_ref()
        {
            if loop_info.open_cursor == *right_cursor {
                left_join_match_flag_maybe = Some(*match_flag);
            }
        }
        translate_table_open_loop(program, loop_info, left_join_match_flag_maybe);
    }

    translate_conditions(program, select, conditions)
}

fn handle_skip_row(
    program: &mut ProgramBuilder,
    cursor_id: usize,
    next_row_instruction_offset: BranchOffset,
    constraint: &Option<QueryConstraint>,
) {
    match constraint {
        Some(QueryConstraint::Left(Left {
            where_clause,
            join_clause,
            match_flag,
            match_flag_hit_marker,
            found_match_next_row_label,
            left_cursor,
            right_cursor,
            ..
        })) => {
            if let Some(where_clause) = where_clause {
                if where_clause.no_match_target_cursor == cursor_id {
                    program.resolve_label(
                        where_clause.no_match_jump_label,
                        next_row_instruction_offset,
                    );
                }
            }
            if let Some(join_clause) = join_clause {
                if join_clause.no_match_target_cursor == cursor_id {
                    program.resolve_label(
                        join_clause.no_match_jump_label,
                        next_row_instruction_offset,
                    );
                }
            }
            if cursor_id == *right_cursor {
                // If the left join match flag has been set to 1, we jump to the next row (result row has been emitted already)
                program.emit_insn_with_label_dependency(
                    Insn::IfPos {
                        reg: *match_flag,
                        target_pc: *found_match_next_row_label,
                        decrement_by: 0,
                    },
                    *found_match_next_row_label,
                );
                // If not, we set the right table cursor's "pseudo null bit" on, which means any Insn::Column will return NULL
                program.emit_insn(Insn::NullRow {
                    cursor_id: *right_cursor,
                });
                // Jump to setting the left join match flag to 1 again, but this time the right table cursor will set everything to null
                program.emit_insn_with_label_dependency(
                    Insn::Goto {
                        target_pc: *match_flag_hit_marker,
                    },
                    *match_flag_hit_marker,
                );
            }
            if cursor_id == *left_cursor {
                program.resolve_label(*found_match_next_row_label, next_row_instruction_offset);
            }
        }
        Some(QueryConstraint::Inner(Inner {
            where_clause,
            join_clause,
            ..
        })) => {
            if let Some(join_clause) = join_clause {
                if cursor_id == join_clause.no_match_target_cursor {
                    program.resolve_label(
                        join_clause.no_match_jump_label,
                        next_row_instruction_offset,
                    );
                }
            }
            if let Some(where_clause) = where_clause {
                if cursor_id == where_clause.no_match_target_cursor {
                    program.resolve_label(
                        where_clause.no_match_jump_label,
                        next_row_instruction_offset,
                    );
                }
            }
        }
        None => {}
    }
}

fn translate_tables_end(
    program: &mut ProgramBuilder,
    select: &Select,
    constraint: Option<QueryConstraint>,
) {
    // iterate in reverse order as we open cursors in order
    for table_loop in select.loops.iter().rev() {
        let cursor_id = table_loop.open_cursor;
        let next_row_instruction_offset = program.offset();
        program.emit_insn(Insn::NextAsync { cursor_id });
        program.emit_insn(Insn::NextAwait {
            cursor_id,
            pc_if_next: table_loop.rewind_offset as BranchOffset,
        });
        program.resolve_label(table_loop.rewind_label, program.offset());
        handle_skip_row(program, cursor_id, next_row_instruction_offset, &constraint);
    }
}

fn translate_table_open_cursor(program: &mut ProgramBuilder, table: &SrcTable) -> LoopInfo {
    let table_identifier = match table.alias {
        Some(alias) => alias.clone(),
        None => table.table.get_name().to_string(),
    };
    let cursor_id = program.alloc_cursor_id(table_identifier, table.table.clone());
    let root_page = match &table.table {
        Table::BTree(btree) => btree.root_page,
        Table::Pseudo(_) => todo!(),
    };
    program.emit_insn(Insn::OpenReadAsync {
        cursor_id,
        root_page,
    });
    program.emit_insn(Insn::OpenReadAwait);
    LoopInfo {
        open_cursor: cursor_id,
        rewind_offset: 0,
        rewind_label: 0,
    }
}

fn translate_table_open_loop(
    program: &mut ProgramBuilder,
    loop_info: &mut LoopInfo,
    left_join_match_flag_maybe: Option<usize>,
) {
    if let Some(match_flag) = left_join_match_flag_maybe {
        // Initialize left join as not matched
        program.emit_insn(Insn::Integer {
            value: 0,
            dest: match_flag,
        });
    }
    program.emit_insn(Insn::RewindAsync {
        cursor_id: loop_info.open_cursor,
    });
    let rewind_await_label = program.allocate_label();
    program.emit_insn_with_label_dependency(
        Insn::RewindAwait {
            cursor_id: loop_info.open_cursor,
            pc_if_empty: rewind_await_label,
        },
        rewind_await_label,
    );
    loop_info.rewind_label = rewind_await_label;
    loop_info.rewind_offset = program.offset() - 1;
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
                translate_table_star(join, program, target_register);
                target_register += &join.table.columns().len();
            }
        }
        ast::ResultColumn::TableStar(_) => todo!(),
    }
    Ok(())
}

fn translate_table_star(table: &SrcTable, program: &mut ProgramBuilder, target_register: usize) {
    let table_identifier = match table.alias {
        Some(alias) => alias.clone(),
        None => table.table.get_name().to_string(),
    };
    let table_cursor = program.resolve_cursor_id(&table_identifier);
    let table = &table.table;
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

fn analyze_columns<'a>(
    columns: &'a Vec<ast::ResultColumn>,
    joins: &Vec<SrcTable>,
) -> Vec<ColumnInfo<'a>> {
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
fn analyze_column<'a>(column: &'a ast::ResultColumn, column_info_out: &mut ColumnInfo<'a>) {
    match column {
        ast::ResultColumn::Expr(expr, _) => analyze_expr(expr, column_info_out),
        ast::ResultColumn::Star => {}
        ast::ResultColumn::TableStar(_) => {}
    }
}

fn analyze_expr<'a>(expr: &'a Expr, column_info_out: &mut ColumnInfo<'a>) {
    match expr {
        ast::Expr::FunctionCall {
            name,
            distinctness: _,
            args,
            filter_over: _,
        } => {
            let func_type = match normalize_ident(name.0.as_str()).as_str().parse() {
                Ok(func) => Some(func),
                Err(_) => None,
            };
            if func_type.is_none() {
                let args = args.as_ref().unwrap();
                if !args.is_empty() {
                    analyze_expr(args.first().unwrap(), column_info_out);
                }
            } else {
                column_info_out.func = func_type;
                // TODO(pere): use lifetimes for args? Arenas would be lovely here :(
                column_info_out.args = args;
            }
        }
        ast::Expr::FunctionCallStar { .. } => todo!(),
        _ => {}
    }
}

fn translate_condition_expr(
    program: &mut ProgramBuilder,
    select: &Select,
    expr: &ast::Expr,
    target_jump: BranchOffset,
    jump_if_true: bool, // if true jump to target on op == true, if false invert op
) -> Result<()> {
    match expr {
        ast::Expr::Between { .. } => todo!(),
        ast::Expr::Binary(lhs, ast::Operator::And, rhs) => {
            if jump_if_true {
                let label = program.allocate_label();
                let _ = translate_condition_expr(program, select, lhs, label, false);
                let _ = translate_condition_expr(program, select, rhs, target_jump, true);
                program.resolve_label(label, program.offset());
            } else {
                let _ = translate_condition_expr(program, select, lhs, target_jump, false);
                let _ = translate_condition_expr(program, select, rhs, target_jump, false);
            }
        }
        ast::Expr::Binary(lhs, ast::Operator::Or, rhs) => {
            if jump_if_true {
                let _ = translate_condition_expr(program, select, lhs, target_jump, true);
                let _ = translate_condition_expr(program, select, rhs, target_jump, true);
            } else {
                let label = program.allocate_label();
                let _ = translate_condition_expr(program, select, lhs, label, true);
                let _ = translate_condition_expr(program, select, rhs, target_jump, false);
                program.resolve_label(label, program.offset());
            }
        }
        ast::Expr::Binary(lhs, op, rhs) => {
            let lhs_reg = program.alloc_register();
            let rhs_reg = program.alloc_register();
            let _ = translate_expr(program, select, lhs, lhs_reg);
            match lhs.as_ref() {
                ast::Expr::Literal(_) => program.mark_last_insn_constant(),
                _ => {}
            }
            let _ = translate_expr(program, select, rhs, rhs_reg);
            match rhs.as_ref() {
                ast::Expr::Literal(_) => program.mark_last_insn_constant(),
                _ => {}
            }
            match op {
                ast::Operator::Greater => {
                    if jump_if_true {
                        program.emit_insn_with_label_dependency(
                            Insn::Gt {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: target_jump,
                            },
                            target_jump,
                        )
                    } else {
                        program.emit_insn_with_label_dependency(
                            Insn::Le {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: target_jump,
                            },
                            target_jump,
                        )
                    }
                }
                ast::Operator::GreaterEquals => {
                    if jump_if_true {
                        program.emit_insn_with_label_dependency(
                            Insn::Ge {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: target_jump,
                            },
                            target_jump,
                        )
                    } else {
                        program.emit_insn_with_label_dependency(
                            Insn::Lt {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: target_jump,
                            },
                            target_jump,
                        )
                    }
                }
                ast::Operator::Less => {
                    if jump_if_true {
                        program.emit_insn_with_label_dependency(
                            Insn::Lt {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: target_jump,
                            },
                            target_jump,
                        )
                    } else {
                        program.emit_insn_with_label_dependency(
                            Insn::Ge {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: target_jump,
                            },
                            target_jump,
                        )
                    }
                }
                ast::Operator::LessEquals => {
                    if jump_if_true {
                        program.emit_insn_with_label_dependency(
                            Insn::Le {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: target_jump,
                            },
                            target_jump,
                        )
                    } else {
                        program.emit_insn_with_label_dependency(
                            Insn::Gt {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: target_jump,
                            },
                            target_jump,
                        )
                    }
                }
                ast::Operator::Equals => {
                    if jump_if_true {
                        program.emit_insn_with_label_dependency(
                            Insn::Eq {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: target_jump,
                            },
                            target_jump,
                        )
                    } else {
                        program.emit_insn_with_label_dependency(
                            Insn::Ne {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: target_jump,
                            },
                            target_jump,
                        )
                    }
                }
                ast::Operator::NotEquals => {
                    if jump_if_true {
                        program.emit_insn_with_label_dependency(
                            Insn::Ne {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: target_jump,
                            },
                            target_jump,
                        )
                    } else {
                        program.emit_insn_with_label_dependency(
                            Insn::Eq {
                                lhs: lhs_reg,
                                rhs: rhs_reg,
                                target_pc: target_jump,
                            },
                            target_jump,
                        )
                    }
                }
                ast::Operator::Is => todo!(),
                ast::Operator::IsNot => todo!(),
                _ => {
                    todo!("op {:?} not implemented", op);
                }
            }
        }
        ast::Expr::Literal(lit) => match lit {
            ast::Literal::Numeric(val) => {
                let maybe_int = val.parse::<i64>();
                if let Ok(int_value) = maybe_int {
                    let reg = program.alloc_register();
                    program.emit_insn(Insn::Integer {
                        value: int_value,
                        dest: reg,
                    });
                    if target_jump < 0 {
                        program.add_label_dependency(target_jump, program.offset());
                    }
                    program.emit_insn(Insn::IfNot {
                        reg,
                        target_pc: target_jump,
                        null_reg: reg,
                    });
                } else {
                    anyhow::bail!("Parse error: unsupported literal type in condition");
                }
            }
            _ => todo!(),
        },
        ast::Expr::InList { lhs, not, rhs } => {}
        ast::Expr::Like {
            lhs,
            not,
            op,
            rhs,
            escape,
        } => {
            let cur_reg = program.alloc_register();
            assert!(match rhs.as_ref() {
                ast::Expr::Literal(_) => true,
                _ => false,
            });
            match op {
                ast::LikeOperator::Like => {
                    let pattern_reg = program.alloc_register();
                    let column_reg = program.alloc_register();
                    // LIKE(pattern, column). We should translate the pattern first before the column
                    let _ = translate_expr(program, select, rhs, pattern_reg)?;
                    program.mark_last_insn_constant();
                    let _ = translate_expr(program, select, lhs, column_reg)?;
                    program.emit_insn(Insn::Function {
                        func: SingleRowFunc::Like,
                        start_reg: pattern_reg,
                        dest: cur_reg,
                    });
                }
                ast::LikeOperator::Glob => todo!(),
                ast::LikeOperator::Match => todo!(),
                ast::LikeOperator::Regexp => todo!(),
            }
            if jump_if_true ^ *not {
                program.emit_insn_with_label_dependency(
                    Insn::If {
                        reg: cur_reg,
                        target_pc: target_jump,
                        null_reg: cur_reg,
                    },
                    target_jump,
                )
            } else {
                program.emit_insn_with_label_dependency(
                    Insn::IfNot {
                        reg: cur_reg,
                        target_pc: target_jump,
                        null_reg: cur_reg,
                    },
                    target_jump,
                )
            }
        }
        _ => todo!("op {:?} not implemented", expr),
    }
    Ok(())
}

fn wrap_eval_jump_expr(
    program: &mut ProgramBuilder,
    insn: Insn,
    target_register: usize,
    if_true_label: BranchOffset,
) {
    program.emit_insn(Insn::Integer {
        value: 1, // emit True by default
        dest: target_register,
    });
    program.emit_insn_with_label_dependency(insn, if_true_label);
    program.emit_insn(Insn::Integer {
        value: 0, // emit False if we reach this point (no jump)
        dest: target_register,
    });
    program.preassign_label_to_next_insn(if_true_label);
}

fn translate_expr(
    program: &mut ProgramBuilder,
    select: &Select,
    expr: &ast::Expr,
    target_register: usize,
) -> Result<usize> {
    match expr {
        ast::Expr::Between { .. } => todo!(),
        ast::Expr::Binary(e1, op, e2) => {
            let e1_reg = program.alloc_register();
            let e2_reg = program.alloc_register();
            let _ = translate_expr(program, select, e1, e1_reg)?;
            let _ = translate_expr(program, select, e2, e2_reg)?;

            match op {
                ast::Operator::NotEquals => {
                    let if_true_label = program.allocate_label();
                    wrap_eval_jump_expr(
                        program,
                        Insn::Ne {
                            lhs: e1_reg,
                            rhs: e2_reg,
                            target_pc: if_true_label,
                        },
                        target_register,
                        if_true_label,
                    );
                }
                ast::Operator::Equals => {
                    let if_true_label = program.allocate_label();
                    wrap_eval_jump_expr(
                        program,
                        Insn::Eq {
                            lhs: e1_reg,
                            rhs: e2_reg,
                            target_pc: if_true_label,
                        },
                        target_register,
                        if_true_label,
                    );
                }
                ast::Operator::Less => {
                    let if_true_label = program.allocate_label();
                    wrap_eval_jump_expr(
                        program,
                        Insn::Lt {
                            lhs: e1_reg,
                            rhs: e2_reg,
                            target_pc: if_true_label,
                        },
                        target_register,
                        if_true_label,
                    );
                }
                ast::Operator::LessEquals => {
                    let if_true_label = program.allocate_label();
                    wrap_eval_jump_expr(
                        program,
                        Insn::Le {
                            lhs: e1_reg,
                            rhs: e2_reg,
                            target_pc: if_true_label,
                        },
                        target_register,
                        if_true_label,
                    );
                }
                ast::Operator::Greater => {
                    let if_true_label = program.allocate_label();
                    wrap_eval_jump_expr(
                        program,
                        Insn::Gt {
                            lhs: e1_reg,
                            rhs: e2_reg,
                            target_pc: if_true_label,
                        },
                        target_register,
                        if_true_label,
                    );
                }
                ast::Operator::GreaterEquals => {
                    let if_true_label = program.allocate_label();
                    wrap_eval_jump_expr(
                        program,
                        Insn::Ge {
                            lhs: e1_reg,
                            rhs: e2_reg,
                            target_pc: if_true_label,
                        },
                        target_register,
                        if_true_label,
                    );
                }
                ast::Operator::Add => {
                    program.emit_insn(Insn::Add {
                        lhs: e1_reg,
                        rhs: e2_reg,
                        dest: target_register,
                    });
                }
                other_unimplemented => todo!("{:?}", other_unimplemented),
            }
            Ok(target_register)
        }
        ast::Expr::Case { .. } => todo!(),
        ast::Expr::Cast { .. } => todo!(),
        ast::Expr::Collate(_, _) => todo!(),
        ast::Expr::DoublyQualified(_, _, _) => todo!(),
        ast::Expr::Exists(_) => todo!(),
        ast::Expr::FunctionCall {
            name,
            distinctness: _,
            args,
            filter_over: _,
        } => {
            let func_type: Option<Func> = match normalize_ident(name.0.as_str()).as_str().parse() {
                Ok(func) => Some(func),
                Err(_) => None,
            };
            match func_type {
                Some(Func::Agg(_)) => {
                    anyhow::bail!("Parse error: aggregation function in non-aggregation context")
                }
                Some(Func::SingleRow(srf)) => {
                    match srf {
                        SingleRowFunc::Coalesce => {
                            let args = if let Some(args) = args {
                                if args.len() < 2 {
                                    anyhow::bail!(
                                        "Parse error: coalesce function with less than 2 arguments"
                                    );
                                }
                                args
                            } else {
                                anyhow::bail!("Parse error: coalesce function with no arguments");
                            };

                            // coalesce function is implemented as a series of not null checks
                            // whenever a not null check succeeds, we jump to the end of the series
                            let label_coalesce_end = program.allocate_label();
                            for (index, arg) in args.iter().enumerate() {
                                let reg = translate_expr(program, select, arg, target_register)?;
                                if index < args.len() - 1 {
                                    program.emit_insn_with_label_dependency(
                                        Insn::NotNull {
                                            reg,
                                            target_pc: label_coalesce_end,
                                        },
                                        label_coalesce_end,
                                    );
                                }
                            }
                            program.preassign_label_to_next_insn(label_coalesce_end);

                            Ok(target_register)
                        }
                        SingleRowFunc::Like => {
                            let args = if let Some(args) = args {
                                if args.len() < 2 {
                                    anyhow::bail!(
                                        "Parse error: like function with less than 2 arguments"
                                    );
                                }
                                args
                            } else {
                                anyhow::bail!("Parse error: like function with no arguments");
                            };
                            for arg in args {
                                let reg = program.alloc_register();
                                let _ = translate_expr(program, select, arg, reg)?;
                                match arg {
                                    ast::Expr::Literal(_) => program.mark_last_insn_constant(),
                                    _ => {}
                                }
                            }
                            program.emit_insn(Insn::Function {
                                start_reg: target_register + 1,
                                dest: target_register,
                                func: SingleRowFunc::Like,
                            });
                            Ok(target_register)
                        }
                        SingleRowFunc::Abs => {
                            let args = if let Some(args) = args {
                                if args.len() != 1 {
                                    anyhow::bail!(
                                        "Parse error: abs function with not exactly 1 argument"
                                    );
                                }
                                args
                            } else {
                                anyhow::bail!("Parse error: abs function with no arguments");
                            };

                            let regs = program.alloc_register();
                            let _ = translate_expr(program, select, &args[0], regs)?;
                            program.emit_insn(Insn::Function {
                                start_reg: regs,
                                dest: target_register,
                                func: SingleRowFunc::Abs,
                            });

                            Ok(target_register)
                        }
                    }
                }
                None => {
                    anyhow::bail!("Parse error: unknown function {}", name.0);
                }
            }
        }
        ast::Expr::FunctionCallStar { .. } => todo!(),
        ast::Expr::Id(ident) => {
            // let (idx, col) = table.unwrap().get_column(&ident.0).unwrap();
            let (idx, col, cursor_id) = resolve_ident_table(program, &ident.0, select)?;
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
            ast::Literal::Null => {
                program.emit_insn(Insn::Null {
                    dest: target_register,
                });
                Ok(target_register)
            }
            ast::Literal::CurrentDate => todo!(),
            ast::Literal::CurrentTime => todo!(),
            ast::Literal::CurrentTimestamp => todo!(),
        },
        ast::Expr::Name(_) => todo!(),
        ast::Expr::NotNull(_) => todo!(),
        ast::Expr::Parenthesized(_) => todo!(),
        ast::Expr::Qualified(tbl, ident) => {
            let (idx, col, cursor_id) = resolve_ident_qualified(program, &tbl.0, &ident.0, select)?;
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
        ast::Expr::Raise(_, _) => todo!(),
        ast::Expr::Subquery(_) => todo!(),
        ast::Expr::Unary(op, expr) => match (op, expr.as_ref()) {
            (UnaryOperator::Negative, ast::Expr::Literal(ast::Literal::Numeric(numeric_value))) => {
                let maybe_int = numeric_value.parse::<i64>();
                if let Ok(value) = maybe_int {
                    program.emit_insn(Insn::Integer {
                        value: -value,
                        dest: target_register,
                    });
                } else {
                    program.emit_insn(Insn::Real {
                        value: -numeric_value.parse::<f64>()?,
                        dest: target_register,
                    });
                }
                Ok(target_register)
            }
            _ => todo!(),
        },
        ast::Expr::Variable(_) => todo!(),
    }
}

fn resolve_ident_qualified<'a>(
    program: &ProgramBuilder,
    table_name: &String,
    ident: &String,
    select: &'a Select,
) -> Result<(usize, &'a Column, usize)> {
    for join in &select.src_tables {
        match join.table {
            Table::BTree(ref table) => {
                let table_identifier = match join.alias {
                    Some(alias) => alias.clone(),
                    None => table.name.to_string(),
                };
                if table_identifier == *table_name {
                    let res = table
                        .columns
                        .iter()
                        .enumerate()
                        .find(|(_, col)| col.name == *ident);
                    if res.is_some() {
                        let (idx, col) = res.unwrap();
                        let cursor_id = program.resolve_cursor_id(&table_identifier);
                        return Ok((idx, col, cursor_id));
                    }
                }
            }
            Table::Pseudo(_) => todo!(),
        }
    }
    anyhow::bail!(
        "Parse error: column with qualified name {}.{} not found",
        table_name,
        ident
    );
}

fn resolve_ident_table<'a>(
    program: &ProgramBuilder,
    ident: &String,
    select: &'a Select,
) -> Result<(usize, &'a Column, usize)> {
    let mut found = Vec::new();
    for join in &select.src_tables {
        match join.table {
            Table::BTree(ref table) => {
                let table_identifier = match join.alias {
                    Some(alias) => alias.clone(),
                    None => table.name.to_string(),
                };
                let res = table
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(_, col)| col.name == *ident);
                if res.is_some() {
                    let (idx, col) = res.unwrap();
                    let cursor_id = program.resolve_cursor_id(&table_identifier);
                    found.push((idx, col, cursor_id));
                }
            }
            Table::Pseudo(_) => todo!(),
        }
    }
    if found.len() == 1 {
        return Ok(found[0]);
    }
    if found.is_empty() {
        anyhow::bail!("Parse error: column with name {} not found", ident.as_str());
    }

    anyhow::bail!("Parse error: ambiguous column name {}", ident.as_str());
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
        Func::SingleRow(_) => anyhow::bail!("Parse error: single row function in aggregation"),
        Func::Agg(agg_func) => match agg_func {
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
                    delimiter: 0,
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
                    delimiter: 0,
                    func: AggFunc::Count,
                });
                target_register
            }
            AggFunc::GroupConcat => {
                if args.len() != 1 && args.len() != 2 {
                    anyhow::bail!("Parse error: group_concat bad number of arguments");
                }

                let expr_reg = program.alloc_register();
                let delimiter_reg = program.alloc_register();

                let expr = &args[0];
                let delimiter_expr: ast::Expr;

                if args.len() == 2 {
                    match &args[1] {
                        ast::Expr::Id(ident) => {
                            if ident.0.starts_with('"') {
                                delimiter_expr =
                                    ast::Expr::Literal(Literal::String(ident.0.to_string()));
                            } else {
                                delimiter_expr = args[1].clone();
                            }
                        }
                        ast::Expr::Literal(Literal::String(s)) => {
                            delimiter_expr = ast::Expr::Literal(Literal::String(s.to_string()));
                        }
                        _ => anyhow::bail!("Incorrect delimiter parameter"),
                    };
                } else {
                    delimiter_expr = ast::Expr::Literal(Literal::String(String::from("\",\"")));
                }

                if let Err(error) = translate_expr(program, select, expr, expr_reg) {
                    anyhow::bail!(error);
                }
                if let Err(error) = translate_expr(program, select, &delimiter_expr, delimiter_reg)
                {
                    anyhow::bail!(error);
                }

                program.emit_insn(Insn::AggStep {
                    acc_reg: target_register,
                    col: expr_reg,
                    delimiter: delimiter_reg,
                    func: AggFunc::GroupConcat,
                });

                target_register
            }
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
                    delimiter: 0,
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
                    delimiter: 0,
                    func: AggFunc::Min,
                });
                target_register
            }
            AggFunc::StringAgg => {
                if args.len() != 2 {
                    anyhow::bail!("Parse error: string_agg bad number of arguments");
                }

                let expr_reg = program.alloc_register();
                let delimiter_reg = program.alloc_register();

                let expr = &args[0];
                let delimiter_expr: ast::Expr;

                match &args[1] {
                    ast::Expr::Id(ident) => {
                        if ident.0.starts_with('"') {
                            anyhow::bail!("Parse error: no such column: \",\" - should this be a string literal in single-quotes?");
                        } else {
                            delimiter_expr = args[1].clone();
                        }
                    }
                    ast::Expr::Literal(Literal::String(s)) => {
                        delimiter_expr = ast::Expr::Literal(Literal::String(s.to_string()));
                    }
                    _ => anyhow::bail!("Incorrect delimiter parameter"),
                };

                if let Err(error) = translate_expr(program, select, expr, expr_reg) {
                    anyhow::bail!(error);
                }
                if let Err(error) = translate_expr(program, select, &delimiter_expr, delimiter_reg)
                {
                    anyhow::bail!(error);
                }

                program.emit_insn(Insn::AggStep {
                    acc_reg: target_register,
                    col: expr_reg,
                    delimiter: delimiter_reg,
                    func: AggFunc::StringAgg,
                });

                target_register
            }
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
                    delimiter: 0,
                    func: AggFunc::Sum,
                });
                target_register
            }
            AggFunc::Total => {
                if args.len() != 1 {
                    anyhow::bail!("Parse error: total bad number of arguments");
                }
                let expr = &args[0];
                let expr_reg = program.alloc_register();
                let _ = translate_expr(program, select, expr, expr_reg)?;
                program.emit_insn(Insn::AggStep {
                    acc_reg: target_register,
                    col: expr_reg,
                    delimiter: 0,
                    func: AggFunc::Total,
                });
                target_register
            }
        },
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
    let init_label = program.allocate_label();
    program.emit_insn_with_label_dependency(
        Insn::Init {
            target_pc: init_label,
        },
        init_label,
    );
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
    program.resolve_label(init_label, program.offset());
    program.emit_insn(Insn::Transaction);
    program.emit_constant_insns();
    program.emit_insn(Insn::Goto {
        target_pc: start_offset,
    });
    program.resolve_deferred_labels();
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
    if col.ty == crate::schema::Type::Real {
        program.emit_insn(Insn::RealAffinity {
            register: target_register,
        })
    }
}
