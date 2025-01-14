use super::emitter::emit_program;
use super::expr::get_name;
use super::plan::SelectQueryType;
use crate::function::Func;
use crate::translate::optimizer::optimize_plan;
use crate::translate::plan::{Aggregate, Direction, GroupBy, Plan, ResultSetColumn, SelectPlan};
use crate::translate::planner::{
    bind_column_references, break_predicate_at_and_boundaries, parse_from, parse_limit,
    parse_where, resolve_aggregates, OperatorIdCounter,
};
use crate::util::normalize_ident;
use crate::SymbolTable;
use crate::{schema::Schema, vdbe::builder::ProgramBuilder, Result};
use sqlite3_parser::ast;
use sqlite3_parser::ast::ResultColumn;

pub fn translate_select(
    program: &mut ProgramBuilder,
    schema: &Schema,
    select: ast::Select,
    syms: &SymbolTable,
) -> Result<()> {
    let mut select_plan = prepare_select_plan(schema, select)?;
    optimize_plan(&mut select_plan)?;
    emit_program(program, select_plan, syms)
}

pub fn prepare_select_plan(schema: &Schema, select: ast::Select) -> Result<Plan> {
    match *select.body.select {
        ast::OneSelect::Select {
            mut columns,
            from,
            where_clause,
            group_by,
            ..
        } => {
            let col_count = columns.len();
            if col_count == 0 {
                crate::bail_parse_error!("SELECT without columns is not allowed");
            }

            let mut operator_id_counter = OperatorIdCounter::new();

            // Parse the FROM clause
            let (source, referenced_tables) = parse_from(schema, from, &mut operator_id_counter)?;

            let mut plan = SelectPlan {
                source,
                result_columns: vec![],
                where_clause: None,
                group_by: None,
                order_by: None,
                aggregates: vec![],
                limit: None,
                referenced_tables,
                available_indexes: schema.indexes.clone().into_values().flatten().collect(),
                contains_constant_false_condition: false,
                query_type: SelectQueryType::TopLevel,
            };

            // Parse the WHERE clause
            plan.where_clause = parse_where(where_clause, &plan.referenced_tables)?;

            let mut aggregate_expressions = Vec::new();
            for (result_column_idx, column) in columns.iter_mut().enumerate() {
                match column {
                    ast::ResultColumn::Star => {
                        plan.source.select_star(&mut plan.result_columns);
                    }
                    ast::ResultColumn::TableStar(name) => {
                        let name_normalized = normalize_ident(name.0.as_str());
                        let referenced_table = plan
                            .referenced_tables
                            .iter()
                            .find(|t| t.table_identifier == name_normalized);

                        if referenced_table.is_none() {
                            crate::bail_parse_error!("Table {} not found", name.0);
                        }
                        let table_reference = referenced_table.unwrap();
                        for (idx, col) in table_reference.columns().iter().enumerate() {
                            plan.result_columns.push(ResultSetColumn {
                                expr: ast::Expr::Column {
                                    database: None, // TODO: support different databases
                                    table: table_reference.table_index,
                                    column: idx,
                                    is_rowid_alias: col.is_rowid_alias,
                                },
                                name: col.name.clone(),
                                contains_aggregates: false,
                            });
                        }
                    }
                    ast::ResultColumn::Expr(ref mut expr, maybe_alias) => {
                        bind_column_references(expr, &plan.referenced_tables)?;
                        match expr {
                            ast::Expr::FunctionCall {
                                name,
                                distinctness: _,
                                args,
                                filter_over: _,
                                order_by: _,
                            } => {
                                let args_count = if let Some(args) = &args {
                                    args.len()
                                } else {
                                    0
                                };
                                match Func::resolve_function(
                                    normalize_ident(name.0.as_str()).as_str(),
                                    args_count,
                                ) {
                                    Ok(Func::Agg(f)) => {
                                        let agg = Aggregate {
                                            func: f,
                                            args: args.as_ref().unwrap().clone(),
                                            original_expr: expr.clone(),
                                        };
                                        aggregate_expressions.push(agg.clone());
                                        plan.result_columns.push(ResultSetColumn {
                                            name: get_name(
                                                maybe_alias.as_ref(),
                                                expr,
                                                &plan.referenced_tables,
                                                || format!("expr_{}", result_column_idx),
                                            ),
                                            expr: expr.clone(),
                                            contains_aggregates: true,
                                        });
                                    }
                                    Ok(_) => {
                                        let contains_aggregates =
                                            resolve_aggregates(expr, &mut aggregate_expressions);
                                        plan.result_columns.push(ResultSetColumn {
                                            name: get_name(
                                                maybe_alias.as_ref(),
                                                expr,
                                                &plan.referenced_tables,
                                                || format!("expr_{}", result_column_idx),
                                            ),
                                            expr: expr.clone(),
                                            contains_aggregates,
                                        });
                                    }
                                    _ => {}
                                }
                            }
                            ast::Expr::FunctionCallStar {
                                name,
                                filter_over: _,
                            } => {
                                if let Ok(Func::Agg(f)) = Func::resolve_function(
                                    normalize_ident(name.0.as_str()).as_str(),
                                    0,
                                ) {
                                    let agg = Aggregate {
                                        func: f,
                                        args: vec![ast::Expr::Literal(ast::Literal::Numeric(
                                            "1".to_string(),
                                        ))],
                                        original_expr: expr.clone(),
                                    };
                                    aggregate_expressions.push(agg.clone());
                                    plan.result_columns.push(ResultSetColumn {
                                        name: get_name(
                                            maybe_alias.as_ref(),
                                            expr,
                                            &plan.referenced_tables,
                                            || format!("expr_{}", result_column_idx),
                                        ),
                                        expr: expr.clone(),
                                        contains_aggregates: true,
                                    });
                                } else {
                                    crate::bail_parse_error!(
                                        "Invalid aggregate function: {}",
                                        name.0
                                    );
                                }
                            }
                            expr => {
                                let contains_aggregates =
                                    resolve_aggregates(&expr, &mut aggregate_expressions);
                                plan.result_columns.push(ResultSetColumn {
                                    name: get_name(
                                        maybe_alias.as_ref(),
                                        expr,
                                        &plan.referenced_tables,
                                        || format!("expr_{}", result_column_idx),
                                    ),
                                    expr: expr.clone(),
                                    contains_aggregates,
                                });
                            }
                        }
                    }
                }
            }
            if let Some(mut group_by) = group_by {
                for expr in group_by.exprs.iter_mut() {
                    bind_column_references(expr, &plan.referenced_tables)?;
                }

                plan.group_by = Some(GroupBy {
                    exprs: group_by.exprs,
                    having: if let Some(having) = group_by.having {
                        let mut predicates = vec![];
                        break_predicate_at_and_boundaries(having, &mut predicates);
                        for expr in predicates.iter_mut() {
                            bind_column_references(expr, &plan.referenced_tables)?;
                            let contains_aggregates =
                                resolve_aggregates(expr, &mut aggregate_expressions);
                            if !contains_aggregates {
                                // TODO: sqlite allows HAVING clauses with non aggregate expressions like
                                // HAVING id = 5. We should support this too eventually (I guess).
                                // sqlite3-parser does not support HAVING without group by though, so we'll
                                // need to either make a PR or add it to our vendored version.
                                crate::bail_parse_error!(
                                    "HAVING clause must contain an aggregate function"
                                );
                            }
                        }
                        Some(predicates)
                    } else {
                        None
                    },
                });
            }

            plan.aggregates = aggregate_expressions;

            // Parse the ORDER BY clause
            if let Some(order_by) = select.order_by {
                let mut key = Vec::new();

                for o in order_by {
                    // if the ORDER BY expression is a number, interpret it as an 1-indexed column number
                    // otherwise, interpret it normally as an expression
                    let mut expr = if let ast::Expr::Literal(ast::Literal::Numeric(num)) = &o.expr {
                        let column_number = num.parse::<usize>()?;
                        if column_number == 0 {
                            crate::bail_parse_error!("invalid column index: {}", column_number);
                        }
                        let maybe_result_column = columns.get(column_number - 1);
                        match maybe_result_column {
                            Some(ResultColumn::Expr(e, _)) => e.clone(),
                            None => {
                                crate::bail_parse_error!("invalid column index: {}", column_number)
                            }
                            _ => todo!(),
                        }
                    } else {
                        o.expr
                    };

                    bind_column_references(&mut expr, &plan.referenced_tables)?;
                    resolve_aggregates(&expr, &mut plan.aggregates);

                    key.push((
                        expr,
                        o.order.map_or(Direction::Ascending, |o| match o {
                            ast::SortOrder::Asc => Direction::Ascending,
                            ast::SortOrder::Desc => Direction::Descending,
                        }),
                    ));
                }
                plan.order_by = Some(key);
            }

            // Parse the LIMIT clause
            plan.limit = select.limit.and_then(|l| parse_limit(*l));

            // Return the unoptimized query plan
            Ok(Plan::Select(plan))
        }
        _ => todo!(),
    }
}
