use super::plan::{
    Aggregate, BTreeTableReference, Direction, GroupBy, Plan, ResultSetColumn, SourceOperator,
};
use crate::{
    function::Func,
    schema::Schema,
    util::{exprs_are_equivalent, normalize_ident},
    Result,
};
use sqlite3_parser::ast::{self, FromClause, JoinType, ResultColumn};

pub struct OperatorIdCounter {
    id: usize,
}

impl OperatorIdCounter {
    pub fn new() -> Self {
        Self { id: 1 }
    }
    pub fn get_next_id(&mut self) -> usize {
        let id = self.id;
        self.id += 1;
        id
    }
}

fn resolve_aggregates(expr: &ast::Expr, aggs: &mut Vec<Aggregate>) -> bool {
    if aggs
        .iter()
        .any(|a| exprs_are_equivalent(&a.original_expr, expr))
    {
        return true;
    }
    match expr {
        ast::Expr::FunctionCall { name, args, .. } => {
            let args_count = if let Some(args) = &args {
                args.len()
            } else {
                0
            };
            match Func::resolve_function(normalize_ident(name.0.as_str()).as_str(), args_count) {
                Ok(Func::Agg(f)) => {
                    aggs.push(Aggregate {
                        func: f,
                        args: args.clone().unwrap_or_default(),
                        original_expr: expr.clone(),
                    });
                    true
                }
                _ => {
                    let mut contains_aggregates = false;
                    if let Some(args) = args {
                        for arg in args.iter() {
                            contains_aggregates |= resolve_aggregates(arg, aggs);
                        }
                    }
                    contains_aggregates
                }
            }
        }
        ast::Expr::FunctionCallStar { name, .. } => {
            if let Ok(Func::Agg(f)) =
                Func::resolve_function(normalize_ident(name.0.as_str()).as_str(), 0)
            {
                aggs.push(Aggregate {
                    func: f,
                    args: vec![],
                    original_expr: expr.clone(),
                });
                true
            } else {
                false
            }
        }
        ast::Expr::Binary(lhs, _, rhs) => {
            let mut contains_aggregates = false;
            contains_aggregates |= resolve_aggregates(lhs, aggs);
            contains_aggregates |= resolve_aggregates(rhs, aggs);
            contains_aggregates
        }
        ast::Expr::Unary(_, expr) => {
            let mut contains_aggregates = false;
            contains_aggregates |= resolve_aggregates(expr, aggs);
            contains_aggregates
        }
        // TODO: handle other expressions that may contain aggregates
        _ => false,
    }
}

/// Recursively resolve column references in an expression.
/// Id, Qualified and DoublyQualified are converted to Column.
fn bind_column_references(
    expr: &mut ast::Expr,
    referenced_tables: &[BTreeTableReference],
) -> Result<()> {
    match expr {
        ast::Expr::Id(id) => {
            // true and false are special constants that are effectively aliases for 1 and 0
            // and not identifiers of columns
            if id.0.eq_ignore_ascii_case("true") || id.0.eq_ignore_ascii_case("false") {
                return Ok(());
            }
            let mut match_result = None;
            let normalized_id = normalize_ident(id.0.as_str());
            for (tbl_idx, table) in referenced_tables.iter().enumerate() {
                let col_idx = table
                    .table
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(&normalized_id));
                if col_idx.is_some() {
                    if match_result.is_some() {
                        crate::bail_parse_error!("Column {} is ambiguous", id.0);
                    }
                    let col = table.table.columns.get(col_idx.unwrap()).unwrap();
                    match_result = Some((tbl_idx, col_idx.unwrap(), col.primary_key));
                }
            }
            if match_result.is_none() {
                crate::bail_parse_error!("Column {} not found", id.0);
            }
            let (tbl_idx, col_idx, is_primary_key) = match_result.unwrap();
            *expr = ast::Expr::Column {
                database: None, // TODO: support different databases
                table: tbl_idx,
                column: col_idx,
                is_rowid_alias: is_primary_key,
            };
            Ok(())
        }
        ast::Expr::Qualified(tbl, id) => {
            let normalized_table_name = normalize_ident(tbl.0.as_str());
            let matching_tbl_idx = referenced_tables.iter().position(|t| {
                t.table_identifier
                    .eq_ignore_ascii_case(&normalized_table_name)
            });
            if matching_tbl_idx.is_none() {
                crate::bail_parse_error!("Table {} not found", normalized_table_name);
            }
            let tbl_idx = matching_tbl_idx.unwrap();
            let normalized_id = normalize_ident(id.0.as_str());
            let col_idx = referenced_tables[tbl_idx]
                .table
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(&normalized_id));
            if col_idx.is_none() {
                crate::bail_parse_error!("Column {} not found", normalized_id);
            }
            let col = referenced_tables[tbl_idx]
                .table
                .columns
                .get(col_idx.unwrap())
                .unwrap();
            *expr = ast::Expr::Column {
                database: None, // TODO: support different databases
                table: tbl_idx,
                column: col_idx.unwrap(),
                is_rowid_alias: col.is_rowid_alias,
            };
            Ok(())
        }
        ast::Expr::Between {
            lhs,
            not: _,
            start,
            end,
        } => {
            bind_column_references(lhs, referenced_tables)?;
            bind_column_references(start, referenced_tables)?;
            bind_column_references(end, referenced_tables)?;
            Ok(())
        }
        ast::Expr::Binary(expr, _operator, expr1) => {
            bind_column_references(expr, referenced_tables)?;
            bind_column_references(expr1, referenced_tables)?;
            Ok(())
        }
        ast::Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            if let Some(base) = base {
                bind_column_references(base, referenced_tables)?;
            }
            for (when, then) in when_then_pairs {
                bind_column_references(when, referenced_tables)?;
                bind_column_references(then, referenced_tables)?;
            }
            if let Some(else_expr) = else_expr {
                bind_column_references(else_expr, referenced_tables)?;
            }
            Ok(())
        }
        ast::Expr::Cast { expr, type_name: _ } => bind_column_references(expr, referenced_tables),
        ast::Expr::Collate(expr, _string) => bind_column_references(expr, referenced_tables),
        ast::Expr::FunctionCall {
            name: _,
            distinctness: _,
            args,
            order_by: _,
            filter_over: _,
        } => {
            if let Some(args) = args {
                for arg in args {
                    bind_column_references(arg, referenced_tables)?;
                }
            }
            Ok(())
        }
        // Column references cannot exist before binding
        ast::Expr::Column { .. } => unreachable!(),
        ast::Expr::DoublyQualified(_, _, _) => todo!(),
        ast::Expr::Exists(_) => todo!(),
        ast::Expr::FunctionCallStar { .. } => Ok(()),
        ast::Expr::InList { lhs, not: _, rhs } => {
            bind_column_references(lhs, referenced_tables)?;
            if let Some(rhs) = rhs {
                for arg in rhs {
                    bind_column_references(arg, referenced_tables)?;
                }
            }
            Ok(())
        }
        ast::Expr::InSelect { .. } => todo!(),
        ast::Expr::InTable { .. } => todo!(),
        ast::Expr::IsNull(expr) => {
            bind_column_references(expr, referenced_tables)?;
            Ok(())
        }
        ast::Expr::Like { lhs, rhs, .. } => {
            bind_column_references(lhs, referenced_tables)?;
            bind_column_references(rhs, referenced_tables)?;
            Ok(())
        }
        ast::Expr::Literal(_) => Ok(()),
        ast::Expr::Name(_) => todo!(),
        ast::Expr::NotNull(expr) => {
            bind_column_references(expr, referenced_tables)?;
            Ok(())
        }
        ast::Expr::Parenthesized(expr) => {
            for e in expr.iter_mut() {
                bind_column_references(e, referenced_tables)?;
            }
            Ok(())
        }
        ast::Expr::Raise(_, _) => todo!(),
        ast::Expr::Subquery(_) => todo!(),
        ast::Expr::Unary(_, expr) => {
            bind_column_references(expr, referenced_tables)?;
            Ok(())
        }
        ast::Expr::Variable(_) => todo!(),
    }
}

#[allow(clippy::extra_unused_lifetimes)]
pub fn prepare_select_plan<'a>(schema: &Schema, select: ast::Select) -> Result<Plan> {
    match select.body.select {
        ast::OneSelect::Select {
            columns,
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

            let mut plan = Plan {
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
            };

            // Parse the WHERE clause
            if let Some(w) = where_clause {
                let mut predicates = vec![];
                break_predicate_at_and_boundaries(w, &mut predicates);
                for expr in predicates.iter_mut() {
                    bind_column_references(expr, &plan.referenced_tables)?;
                }
                plan.where_clause = Some(predicates);
            }

            let mut aggregate_expressions = Vec::new();
            for column in columns.clone() {
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
                        for (idx, col) in table_reference.table.columns.iter().enumerate() {
                            plan.result_columns.push(ResultSetColumn {
                                expr: ast::Expr::Column {
                                    database: None, // TODO: support different databases
                                    table: table_reference.table_index,
                                    column: idx,
                                    is_rowid_alias: col.is_rowid_alias,
                                },
                                contains_aggregates: false,
                            });
                        }
                    }
                    ast::ResultColumn::Expr(mut expr, _) => {
                        bind_column_references(&mut expr, &plan.referenced_tables)?;
                        match &expr {
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
                                            expr: expr.clone(),
                                            contains_aggregates: true,
                                        });
                                    }
                                    Ok(_) => {
                                        let contains_aggregates =
                                            resolve_aggregates(&expr, &mut aggregate_expressions);
                                        plan.result_columns.push(ResultSetColumn {
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
            if let Some(limit) = &select.limit {
                plan.limit = match &limit.expr {
                    ast::Expr::Literal(ast::Literal::Numeric(n)) => {
                        let l = n.parse()?;
                        Some(l)
                    }
                    _ => todo!(),
                }
            }

            // Return the unoptimized query plan
            Ok(plan)
        }
        _ => todo!(),
    }
}

#[allow(clippy::type_complexity)]
fn parse_from(
    schema: &Schema,
    from: Option<FromClause>,
    operator_id_counter: &mut OperatorIdCounter,
) -> Result<(SourceOperator, Vec<BTreeTableReference>)> {
    if from.as_ref().and_then(|f| f.select.as_ref()).is_none() {
        return Ok((SourceOperator::Nothing, vec![]));
    }

    let from = from.unwrap();

    let first_table = match *from.select.unwrap() {
        ast::SelectTable::Table(qualified_name, maybe_alias, _) => {
            let normalized_qualified_name = normalize_ident(qualified_name.name.0.as_str());
            let Some(table) = schema.get_table(&normalized_qualified_name) else {
                crate::bail_parse_error!("Table {} not found", normalized_qualified_name);
            };
            let alias = maybe_alias
                .map(|a| match a {
                    ast::As::As(id) => id,
                    ast::As::Elided(id) => id,
                })
                .map(|a| a.0);

            BTreeTableReference {
                table: table.clone(),
                table_identifier: alias.unwrap_or(normalized_qualified_name),
                table_index: 0,
            }
        }
        _ => todo!(),
    };

    let mut operator = SourceOperator::Scan {
        table_reference: first_table.clone(),
        predicates: None,
        id: operator_id_counter.get_next_id(),
        iter_dir: None,
    };

    let mut tables = vec![first_table];

    let mut table_index = 1;
    for join in from.joins.unwrap_or_default().into_iter() {
        let (right, outer, using, predicates) =
            parse_join(schema, join, operator_id_counter, &mut tables, table_index)?;
        operator = SourceOperator::Join {
            left: Box::new(operator),
            right: Box::new(right),
            predicates,
            outer,
            using,
            id: operator_id_counter.get_next_id(),
        };
        table_index += 1;
    }

    Ok((operator, tables))
}

fn parse_join(
    schema: &Schema,
    join: ast::JoinedSelectTable,
    operator_id_counter: &mut OperatorIdCounter,
    tables: &mut Vec<BTreeTableReference>,
    table_index: usize,
) -> Result<(
    SourceOperator,
    bool,
    Option<ast::DistinctNames>,
    Option<Vec<ast::Expr>>,
)> {
    let ast::JoinedSelectTable {
        operator,
        table,
        constraint,
    } = join;

    let table = match table {
        ast::SelectTable::Table(qualified_name, maybe_alias, _) => {
            let normalized_name = normalize_ident(qualified_name.name.0.as_str());
            let Some(table) = schema.get_table(&normalized_name) else {
                crate::bail_parse_error!("Table {} not found", normalized_name);
            };
            let alias = maybe_alias
                .map(|a| match a {
                    ast::As::As(id) => id,
                    ast::As::Elided(id) => id,
                })
                .map(|a| a.0);
            BTreeTableReference {
                table: table.clone(),
                table_identifier: alias.unwrap_or(normalized_name),
                table_index,
            }
        }
        _ => todo!(),
    };

    tables.push(table.clone());

    let (outer, natural) = match operator {
        ast::JoinOperator::TypedJoin(Some(join_type)) => {
            let is_outer = join_type.contains(JoinType::OUTER);
            let is_natural = join_type.contains(JoinType::NATURAL);
            (is_outer, is_natural)
        }
        _ => (false, false),
    };

    let mut using = None;
    let mut predicates = None;

    if natural && constraint.is_some() {
        crate::bail_parse_error!("NATURAL JOIN cannot be combined with ON or USING clause");
    }

    let constraint = if natural {
        // NATURAL JOIN is first transformed into a USING join with the common columns
        let left_tables = &tables[..table_index];
        assert!(!left_tables.is_empty());
        let right_table = &tables[table_index];
        let right_cols = &right_table.table.columns;
        let mut distinct_names = None;
        // TODO: O(n^2) maybe not great for large tables or big multiway joins
        for right_col in right_cols.iter() {
            let mut found_match = false;
            for left_table in left_tables.iter() {
                for left_col in left_table.table.columns.iter() {
                    if left_col.name == right_col.name {
                        if distinct_names.is_none() {
                            distinct_names =
                                Some(ast::DistinctNames::new(ast::Name(left_col.name.clone())));
                        } else {
                            distinct_names
                                .as_mut()
                                .unwrap()
                                .insert(ast::Name(left_col.name.clone()))
                                .unwrap();
                        }
                        found_match = true;
                        break;
                    }
                }
                if found_match {
                    break;
                }
            }
        }
        if distinct_names.is_none() {
            crate::bail_parse_error!("No columns found to NATURAL join on");
        }
        Some(ast::JoinConstraint::Using(distinct_names.unwrap()))
    } else {
        constraint
    };

    if let Some(constraint) = constraint {
        match constraint {
            ast::JoinConstraint::On(expr) => {
                let mut preds = vec![];
                break_predicate_at_and_boundaries(expr, &mut preds);
                for predicate in preds.iter_mut() {
                    bind_column_references(predicate, tables)?;
                }
                predicates = Some(preds);
            }
            ast::JoinConstraint::Using(distinct_names) => {
                // USING join is replaced with a list of equality predicates
                let mut using_predicates = vec![];
                for distinct_name in distinct_names.iter() {
                    let name_normalized = normalize_ident(distinct_name.0.as_str());
                    let left_tables = &tables[..table_index];
                    assert!(!left_tables.is_empty());
                    let right_table = &tables[table_index];
                    let mut left_col = None;
                    for (left_table_idx, left_table) in left_tables.iter().enumerate() {
                        left_col = left_table
                            .table
                            .columns
                            .iter()
                            .enumerate()
                            .find(|(_, col)| col.name == name_normalized)
                            .map(|(idx, col)| (left_table_idx, idx, col));
                        if left_col.is_some() {
                            break;
                        }
                    }
                    if left_col.is_none() {
                        crate::bail_parse_error!(
                            "cannot join using column {} - column not present in all tables",
                            distinct_name.0
                        );
                    }
                    let right_col = right_table
                        .table
                        .columns
                        .iter()
                        .enumerate()
                        .find(|(_, col)| col.name == name_normalized);
                    if right_col.is_none() {
                        crate::bail_parse_error!(
                            "cannot join using column {} - column not present in all tables",
                            distinct_name.0
                        );
                    }
                    let (left_table_idx, left_col_idx, left_col) = left_col.unwrap();
                    let (right_col_idx, right_col) = right_col.unwrap();
                    using_predicates.push(ast::Expr::Binary(
                        Box::new(ast::Expr::Column {
                            database: None,
                            table: left_table_idx,
                            column: left_col_idx,
                            is_rowid_alias: left_col.is_rowid_alias,
                        }),
                        ast::Operator::Equals,
                        Box::new(ast::Expr::Column {
                            database: None,
                            table: right_table.table_index,
                            column: right_col_idx,
                            is_rowid_alias: right_col.is_rowid_alias,
                        }),
                    ));
                }
                predicates = Some(using_predicates);
                using = Some(distinct_names);
            }
        }
    }

    Ok((
        SourceOperator::Scan {
            table_reference: table.clone(),
            predicates: None,
            id: operator_id_counter.get_next_id(),
            iter_dir: None,
        },
        outer,
        using,
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
