use super::emitter::emit_program;
use super::plan::{select_star, Operation, Search, SelectQueryType};
use crate::function::{AggFunc, ExtFunc, Func};
use crate::translate::optimizer::optimize_plan;
use crate::translate::plan::{Aggregate, Direction, GroupBy, Plan, ResultSetColumn, SelectPlan};
use crate::translate::planner::{
    bind_column_references, break_predicate_at_and_boundaries, parse_from, parse_limit,
    parse_where, resolve_aggregates,
};
use crate::util::normalize_ident;
use crate::vdbe::builder::{ProgramBuilderOpts, QueryMode};
use crate::SymbolTable;
use crate::{schema::Schema, vdbe::builder::ProgramBuilder, Result};
use sqlite3_parser::ast::{self};
use sqlite3_parser::ast::{ResultColumn, SelectInner};

pub fn translate_select(
    query_mode: QueryMode,
    schema: &Schema,
    select: ast::Select,
    syms: &SymbolTable,
) -> Result<ProgramBuilder> {
    let mut select_plan = prepare_select_plan(schema, select, syms)?;
    optimize_plan(&mut select_plan, schema)?;
    let Plan::Select(ref select) = select_plan else {
        panic!("select_plan is not a SelectPlan");
    };

    let mut program = ProgramBuilder::new(ProgramBuilderOpts {
        query_mode,
        num_cursors: count_plan_required_cursors(select),
        approx_num_insns: estimate_num_instructions(select),
        approx_num_labels: estimate_num_labels(select),
    });
    emit_program(&mut program, select_plan, syms)?;
    Ok(program)
}

pub fn prepare_select_plan(
    schema: &Schema,
    select: ast::Select,
    syms: &SymbolTable,
) -> Result<Plan> {
    match *select.body.select {
        ast::OneSelect::Select(select_inner) => {
            let SelectInner {
                mut columns,
                from,
                where_clause,
                group_by,
                ..
            } = *select_inner;
            let col_count = columns.len();
            if col_count == 0 {
                crate::bail_parse_error!("SELECT without columns is not allowed");
            }

            let mut where_predicates = vec![];

            // Parse the FROM clause into a vec of TableReferences. Fold all the join conditions expressions into the WHERE clause.
            let table_references = parse_from(schema, from, syms, &mut where_predicates)?;

            // Preallocate space for the result columns
            let result_columns = Vec::with_capacity(
                columns
                    .iter()
                    .map(|c| match c {
                        // Allocate space for all columns in all tables
                        ResultColumn::Star => {
                            table_references.iter().map(|t| t.columns().len()).sum()
                        }
                        // Guess 5 columns if we can't find the table using the identifier (maybe it's in [brackets] or `tick_quotes`, or miXeDcAse)
                        ResultColumn::TableStar(n) => table_references
                            .iter()
                            .find(|t| t.identifier == n.0)
                            .map(|t| t.columns().len())
                            .unwrap_or(5),
                        // Otherwise allocate space for 1 column
                        ResultColumn::Expr(_, _) => 1,
                    })
                    .sum(),
            );

            let mut plan = SelectPlan {
                table_references,
                result_columns,
                where_clause: where_predicates,
                group_by: None,
                order_by: None,
                aggregates: vec![],
                limit: None,
                offset: None,
                contains_constant_false_condition: false,
                query_type: SelectQueryType::TopLevel,
            };

            let mut aggregate_expressions = Vec::new();
            for column in columns.iter_mut() {
                match column {
                    ResultColumn::Star => {
                        select_star(&plan.table_references, &mut plan.result_columns);
                    }
                    ResultColumn::TableStar(name) => {
                        let name_normalized = normalize_ident(name.0.as_str());
                        let referenced_table = plan
                            .table_references
                            .iter()
                            .enumerate()
                            .find(|(_, t)| t.identifier == name_normalized);

                        if referenced_table.is_none() {
                            crate::bail_parse_error!("Table {} not found", name.0);
                        }
                        let (table_index, table) = referenced_table.unwrap();
                        for (idx, col) in table.columns().iter().enumerate() {
                            plan.result_columns.push(ResultSetColumn {
                                expr: ast::Expr::Column {
                                    database: None, // TODO: support different databases
                                    table: table_index,
                                    column: idx,
                                    is_rowid_alias: col.is_rowid_alias,
                                },
                                alias: None,
                                contains_aggregates: false,
                            });
                        }
                    }
                    ResultColumn::Expr(ref mut expr, maybe_alias) => {
                        bind_column_references(
                            expr,
                            &plan.table_references,
                            Some(&plan.result_columns),
                        )?;
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
                                        let agg_args = match (args, &f) {
                                            (None, crate::function::AggFunc::Count0) => {
                                                // COUNT() case
                                                vec![ast::Expr::Literal(ast::Literal::Numeric(
                                                    "1".to_string(),
                                                ))]
                                            }
                                            (None, _) => crate::bail_parse_error!(
                                                "Aggregate function {} requires arguments",
                                                name.0
                                            ),
                                            (Some(args), _) => args.clone(),
                                        };

                                        let agg = Aggregate {
                                            func: f,
                                            args: agg_args.clone(),
                                            original_expr: expr.clone(),
                                        };
                                        aggregate_expressions.push(agg.clone());
                                        plan.result_columns.push(ResultSetColumn {
                                            alias: maybe_alias.as_ref().map(|alias| match alias {
                                                ast::As::Elided(alias) => alias.0.clone(),
                                                ast::As::As(alias) => alias.0.clone(),
                                            }),
                                            expr: expr.clone(),
                                            contains_aggregates: true,
                                        });
                                    }
                                    Ok(_) => {
                                        let contains_aggregates =
                                            resolve_aggregates(expr, &mut aggregate_expressions);
                                        plan.result_columns.push(ResultSetColumn {
                                            alias: maybe_alias.as_ref().map(|alias| match alias {
                                                ast::As::Elided(alias) => alias.0.clone(),
                                                ast::As::As(alias) => alias.0.clone(),
                                            }),
                                            expr: expr.clone(),
                                            contains_aggregates,
                                        });
                                    }
                                    Err(e) => {
                                        if let Some(f) = syms.resolve_function(&name.0, args_count)
                                        {
                                            if let ExtFunc::Scalar(_) = f.as_ref().func {
                                                let contains_aggregates = resolve_aggregates(
                                                    expr,
                                                    &mut aggregate_expressions,
                                                );
                                                plan.result_columns.push(ResultSetColumn {
                                                    alias: maybe_alias.as_ref().map(|alias| {
                                                        match alias {
                                                            ast::As::Elided(alias) => {
                                                                alias.0.clone()
                                                            }
                                                            ast::As::As(alias) => alias.0.clone(),
                                                        }
                                                    }),
                                                    expr: expr.clone(),
                                                    contains_aggregates,
                                                });
                                            } else {
                                                let agg = Aggregate {
                                                    func: AggFunc::External(f.func.clone().into()),
                                                    args: args.as_ref().unwrap().clone(),
                                                    original_expr: expr.clone(),
                                                };
                                                aggregate_expressions.push(agg.clone());
                                                plan.result_columns.push(ResultSetColumn {
                                                    alias: maybe_alias.as_ref().map(|alias| {
                                                        match alias {
                                                            ast::As::Elided(alias) => {
                                                                alias.0.clone()
                                                            }
                                                            ast::As::As(alias) => alias.0.clone(),
                                                        }
                                                    }),
                                                    expr: expr.clone(),
                                                    contains_aggregates: true,
                                                });
                                            }
                                            continue; // Continue with the normal flow instead of returning
                                        } else {
                                            return Err(e);
                                        }
                                    }
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
                                        alias: maybe_alias.as_ref().map(|alias| match alias {
                                            ast::As::Elided(alias) => alias.0.clone(),
                                            ast::As::As(alias) => alias.0.clone(),
                                        }),
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
                                    resolve_aggregates(expr, &mut aggregate_expressions);
                                plan.result_columns.push(ResultSetColumn {
                                    alias: maybe_alias.as_ref().map(|alias| match alias {
                                        ast::As::Elided(alias) => alias.0.clone(),
                                        ast::As::As(alias) => alias.0.clone(),
                                    }),
                                    expr: expr.clone(),
                                    contains_aggregates,
                                });
                            }
                        }
                    }
                }
            }

            // Parse the actual WHERE clause and add its conditions to the plan WHERE clause that already contains the join conditions.
            parse_where(
                where_clause,
                &plan.table_references,
                Some(&plan.result_columns),
                &mut plan.where_clause,
            )?;

            if let Some(mut group_by) = group_by {
                for expr in group_by.exprs.iter_mut() {
                    bind_column_references(
                        expr,
                        &plan.table_references,
                        Some(&plan.result_columns),
                    )?;
                }

                plan.group_by = Some(GroupBy {
                    exprs: group_by.exprs,
                    having: if let Some(having) = group_by.having {
                        let mut predicates = vec![];
                        break_predicate_at_and_boundaries(*having, &mut predicates);
                        for expr in predicates.iter_mut() {
                            bind_column_references(
                                expr,
                                &plan.table_references,
                                Some(&plan.result_columns),
                            )?;
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

                    bind_column_references(
                        &mut expr,
                        &plan.table_references,
                        Some(&plan.result_columns),
                    )?;
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

            // Parse the LIMIT/OFFSET clause
            (plan.limit, plan.offset) =
                select.limit.map_or(Ok((None, None)), |l| parse_limit(*l))?;

            // Return the unoptimized query plan
            Ok(Plan::Select(plan))
        }
        _ => todo!(),
    }
}

fn count_plan_required_cursors(plan: &SelectPlan) -> usize {
    let num_table_cursors: usize = plan
        .table_references
        .iter()
        .map(|t| match &t.op {
            Operation::Scan { .. } => 1,
            Operation::Search(search) => match search {
                Search::RowidEq { .. } | Search::RowidSearch { .. } => 1,
                Search::IndexSearch { .. } => 2, // btree cursor and index cursor
            },
            Operation::Subquery { plan, .. } => count_plan_required_cursors(plan),
        })
        .sum();
    let num_sorter_cursors = plan.group_by.is_some() as usize + plan.order_by.is_some() as usize;
    let num_pseudo_cursors = plan.group_by.is_some() as usize + plan.order_by.is_some() as usize;

    num_table_cursors + num_sorter_cursors + num_pseudo_cursors
}

fn estimate_num_instructions(select: &SelectPlan) -> usize {
    let table_instructions: usize = select
        .table_references
        .iter()
        .map(|t| match &t.op {
            Operation::Scan { .. } => 10,
            Operation::Search(_) => 15,
            Operation::Subquery { plan, .. } => 10 + estimate_num_instructions(plan),
        })
        .sum();

    let group_by_instructions = select.group_by.is_some() as usize * 10;
    let order_by_instructions = select.order_by.is_some() as usize * 10;
    let condition_instructions = select.where_clause.len() * 3;

    let num_instructions = 20
        + table_instructions
        + group_by_instructions
        + order_by_instructions
        + condition_instructions;

    num_instructions
}

fn estimate_num_labels(select: &SelectPlan) -> usize {
    let init_halt_labels = 2;
    // 3 loop labels for each table in main loop + 1 to signify end of main loop
    let table_labels = select
        .table_references
        .iter()
        .map(|t| match &t.op {
            Operation::Scan { .. } => 3,
            Operation::Search(_) => 3,
            Operation::Subquery { plan, .. } => 3 + estimate_num_labels(plan),
        })
        .sum::<usize>()
        + 1;

    let group_by_labels = select.group_by.is_some() as usize * 10;
    let order_by_labels = select.order_by.is_some() as usize * 10;
    let condition_labels = select.where_clause.len() * 2;

    let num_labels =
        init_halt_labels + table_labels + group_by_labels + order_by_labels + condition_labels;

    num_labels
}
