use super::plan::{Aggregate, Direction, Operator, Plan, ProjectionColumn};
use crate::{
    function::Func,
    schema::{BTreeTable, Schema},
    util::normalize_ident,
    Result,
};
use sqlite3_parser::ast::{self, FromClause, JoinType, ResultColumn};
use std::rc::Rc;

pub struct OperatorIdCounter {
    id: usize,
}

impl OperatorIdCounter {
    pub fn new() -> Self {
        Self { id: 0 }
    }
    pub fn get_next_id(&mut self) -> usize {
        let id = self.id;
        self.id += 1;
        id
    }
}

pub fn prepare_select_plan<'a>(schema: &Schema, select: ast::Select) -> Result<Plan> {
    match select.body.select {
        ast::OneSelect::Select {
            columns,
            from,
            where_clause,
            ..
        } => {
            let col_count = columns.len();
            if col_count == 0 {
                crate::bail_parse_error!("SELECT without columns is not allowed");
            }

            let mut operator_id_counter = OperatorIdCounter::new();

            // Parse the FROM clause
            let (mut operator, referenced_tables) =
                parse_from(schema, from, &mut operator_id_counter)?;

            // Parse the WHERE clause
            if let Some(w) = where_clause {
                let mut predicates = vec![];
                break_predicate_at_and_boundaries(w, &mut predicates);
                operator = Operator::Filter {
                    source: Box::new(operator),
                    predicates,
                    id: operator_id_counter.get_next_id(),
                };
            }

            // Parse the SELECT clause to either a projection or an aggregation
            // depending on the presence of aggregate functions.
            // Since GROUP BY is not supported yet, mixing aggregate and non-aggregate
            // columns is not allowed.
            //
            // If there are no aggregate functions, we can simply project the columns.
            // For a simple SELECT *, the projection operator is skipped.
            let is_select_star = col_count == 1 && matches!(columns[0], ast::ResultColumn::Star);
            if !is_select_star {
                let mut aggregate_expressions = Vec::new();
                let mut scalar_expressions = Vec::with_capacity(col_count);
                for column in columns.clone() {
                    match column {
                        ast::ResultColumn::Star => {
                            scalar_expressions.push(ProjectionColumn::Star);
                        }
                        ast::ResultColumn::TableStar(name) => {
                            let name_normalized = normalize_ident(name.0.as_str());
                            let referenced_table = referenced_tables
                                .iter()
                                .find(|(t, t_id)| *t_id == name_normalized);

                            if referenced_table.is_none() {
                                crate::bail_parse_error!("Table {} not found", name.0);
                            }
                            let (table, identifier) = referenced_table.unwrap();
                            scalar_expressions.push(ProjectionColumn::TableStar(
                                table.clone(),
                                identifier.clone(),
                            ));
                        }
                        ast::ResultColumn::Expr(expr, _) => match expr {
                            ast::Expr::FunctionCall {
                                name,
                                distinctness,
                                args,
                                filter_over,
                                order_by,
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
                                    Ok(Func::Agg(f)) => aggregate_expressions.push(Aggregate {
                                        func: f,
                                        args: args.unwrap(),
                                    }),
                                    Ok(_) => {
                                        scalar_expressions.push(ProjectionColumn::Column(
                                            ast::Expr::FunctionCall {
                                                name,
                                                distinctness,
                                                args,
                                                filter_over,
                                                order_by,
                                            },
                                        ));
                                    }
                                    _ => {}
                                }
                            }
                            ast::Expr::FunctionCallStar { name, filter_over } => {
                                match Func::resolve_function(
                                    normalize_ident(name.0.as_str()).as_str(),
                                    0,
                                ) {
                                    Ok(Func::Agg(f)) => aggregate_expressions.push(Aggregate {
                                        func: f,
                                        args: vec![],
                                    }),
                                    Ok(Func::Scalar(_)) => {
                                        scalar_expressions.push(ProjectionColumn::Column(
                                            ast::Expr::FunctionCallStar { name, filter_over },
                                        ));
                                    }
                                    _ => {}
                                }
                            }
                            _ => {
                                scalar_expressions.push(ProjectionColumn::Column(expr));
                            }
                        },
                    }
                }

                let mixing_aggregate_and_non_aggregate_columns =
                    !aggregate_expressions.is_empty() && aggregate_expressions.len() != col_count;

                if mixing_aggregate_and_non_aggregate_columns {
                    crate::bail_parse_error!(
                        "mixing aggregate and non-aggregate columns is not allowed (GROUP BY is not supported)"
                    );
                }
                if !aggregate_expressions.is_empty() {
                    operator = Operator::Aggregate {
                        source: Box::new(operator),
                        aggregates: aggregate_expressions,
                        id: operator_id_counter.get_next_id(),
                    }
                } else if !scalar_expressions.is_empty() {
                    operator = Operator::Projection {
                        source: Box::new(operator),
                        expressions: scalar_expressions,
                        id: operator_id_counter.get_next_id(),
                    };
                }
            }

            // Parse the ORDER BY clause
            if let Some(order_by) = select.order_by {
                let mut key = Vec::new();
                for o in order_by {
                    // if the ORDER BY expression is a number, interpret it as an 1-indexed column number
                    // otherwise, interpret it normally as an expression
                    let expr = if let ast::Expr::Literal(ast::Literal::Numeric(num)) = o.expr {
                        let column_number = num.parse::<usize>()?;
                        if column_number == 0 {
                            crate::bail_parse_error!("invalid column index: {}", column_number);
                        }
                        let maybe_result_column = columns.get(column_number - 1);
                        match maybe_result_column {
                            Some(ResultColumn::Expr(expr, _)) => expr.clone(),
                            None => {
                                crate::bail_parse_error!("invalid column index: {}", column_number)
                            }
                            _ => todo!(),
                        }
                    } else {
                        o.expr
                    };
                    key.push((
                        expr,
                        o.order.map_or(Direction::Ascending, |o| match o {
                            ast::SortOrder::Asc => Direction::Ascending,
                            ast::SortOrder::Desc => Direction::Descending,
                        }),
                    ));
                }
                operator = Operator::Order {
                    source: Box::new(operator),
                    key,
                    id: operator_id_counter.get_next_id(),
                };
            }

            // Parse the LIMIT clause
            if let Some(limit) = &select.limit {
                operator = match &limit.expr {
                    ast::Expr::Literal(ast::Literal::Numeric(n)) => {
                        let l = n.parse()?;
                        if l == 0 {
                            Operator::Nothing
                        } else {
                            Operator::Limit {
                                source: Box::new(operator),
                                limit: l,
                                id: operator_id_counter.get_next_id(),
                            }
                        }
                    }
                    _ => todo!(),
                }
            }

            // Return the unoptimized query plan
            return Ok(Plan {
                root_operator: operator,
                referenced_tables,
            });
        }
        _ => todo!(),
    };
}

fn parse_from(
    schema: &Schema,
    from: Option<FromClause>,
    operator_id_counter: &mut OperatorIdCounter,
) -> Result<(Operator, Vec<(Rc<BTreeTable>, String)>)> {
    if from.as_ref().and_then(|f| f.select.as_ref()).is_none() {
        return Ok((Operator::Nothing, vec![]));
    }

    let from = from.unwrap();

    let first_table = match *from.select.unwrap() {
        ast::SelectTable::Table(qualified_name, maybe_alias, _) => {
            let Some(table) = schema.get_table(&qualified_name.name.0) else {
                crate::bail_parse_error!("Table {} not found", qualified_name.name.0);
            };
            let alias = maybe_alias
                .map(|a| match a {
                    ast::As::As(id) => id,
                    ast::As::Elided(id) => id,
                })
                .map(|a| a.0);

            (table, alias.unwrap_or(qualified_name.name.0))
        }
        _ => todo!(),
    };

    let mut operator = Operator::Scan {
        table: first_table.0.clone(),
        predicates: None,
        table_identifier: first_table.1.clone(),
        id: operator_id_counter.get_next_id(),
    };

    let mut tables = vec![first_table];

    for join in from.joins.unwrap_or_default().into_iter() {
        let (right, outer, predicates) =
            parse_join(schema, join, operator_id_counter, &mut tables)?;
        operator = Operator::Join {
            left: Box::new(operator),
            right: Box::new(right),
            predicates,
            outer,
            id: operator_id_counter.get_next_id(),
        }
    }

    return Ok((operator, tables));
}

fn parse_join(
    schema: &Schema,
    join: ast::JoinedSelectTable,
    operator_id_counter: &mut OperatorIdCounter,
    tables: &mut Vec<(Rc<BTreeTable>, String)>,
) -> Result<(Operator, bool, Option<Vec<ast::Expr>>)> {
    let ast::JoinedSelectTable {
        operator,
        table,
        constraint,
    } = join;

    let table = match table {
        ast::SelectTable::Table(qualified_name, maybe_alias, _) => {
            let Some(table) = schema.get_table(&qualified_name.name.0) else {
                crate::bail_parse_error!("Table {} not found", qualified_name.name.0);
            };
            let alias = maybe_alias
                .map(|a| match a {
                    ast::As::As(id) => id,
                    ast::As::Elided(id) => id,
                })
                .map(|a| a.0);

            (table, alias.unwrap_or(qualified_name.name.0))
        }
        _ => todo!(),
    };

    tables.push(table.clone());

    let outer = match operator {
        ast::JoinOperator::TypedJoin(Some(join_type)) => {
            if join_type == JoinType::LEFT | JoinType::OUTER {
                true
            } else if join_type == JoinType::RIGHT | JoinType::OUTER {
                true
            } else {
                false
            }
        }
        _ => false,
    };

    let predicates = constraint.map(|c| match c {
        ast::JoinConstraint::On(expr) => {
            let mut predicates = vec![];
            break_predicate_at_and_boundaries(expr, &mut predicates);
            predicates
        }
        ast::JoinConstraint::Using(_) => todo!("USING joins not supported yet"),
    });

    Ok((
        Operator::Scan {
            table: table.0.clone(),
            predicates: None,
            table_identifier: table.1.clone(),
            id: operator_id_counter.get_next_id(),
        },
        outer,
        predicates,
    ))
}

fn break_predicate_at_and_boundaries(predicate: ast::Expr, out_predicates: &mut Vec<ast::Expr>) {
    match predicate {
        ast::Expr::Binary(left, ast::Operator::And, right) => {
            break_predicate_at_and_boundaries(*left, out_predicates);
            break_predicate_at_and_boundaries(*right, out_predicates);
        }
        _ => {
            out_predicates.push(predicate);
        }
    }
}
