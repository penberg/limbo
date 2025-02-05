use super::{
    plan::{
        Aggregate, JoinInfo, Operation, Plan, ResultSetColumn, SelectQueryType, TableReference,
        WhereTerm,
    },
    select::prepare_select_plan,
    SymbolTable,
};
use crate::{
    function::Func,
    schema::{Schema, Table},
    util::{exprs_are_equivalent, normalize_ident},
    vdbe::BranchOffset,
    Result,
};
use sqlite3_parser::ast::{self, Expr, FromClause, JoinType, Limit, UnaryOperator};

pub const ROWID: &str = "rowid";

pub fn resolve_aggregates(expr: &Expr, aggs: &mut Vec<Aggregate>) -> bool {
    if aggs
        .iter()
        .any(|a| exprs_are_equivalent(&a.original_expr, expr))
    {
        return true;
    }
    match expr {
        Expr::FunctionCall { name, args, .. } => {
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
        Expr::FunctionCallStar { name, .. } => {
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
        Expr::Binary(lhs, _, rhs) => {
            let mut contains_aggregates = false;
            contains_aggregates |= resolve_aggregates(lhs, aggs);
            contains_aggregates |= resolve_aggregates(rhs, aggs);
            contains_aggregates
        }
        Expr::Unary(_, expr) => {
            let mut contains_aggregates = false;
            contains_aggregates |= resolve_aggregates(expr, aggs);
            contains_aggregates
        }
        // TODO: handle other expressions that may contain aggregates
        _ => false,
    }
}

pub fn bind_column_references(
    expr: &mut Expr,
    referenced_tables: &[TableReference],
    result_columns: Option<&[ResultSetColumn]>,
) -> Result<()> {
    match expr {
        Expr::Id(id) => {
            // true and false are special constants that are effectively aliases for 1 and 0
            // and not identifiers of columns
            if id.0.eq_ignore_ascii_case("true") || id.0.eq_ignore_ascii_case("false") {
                return Ok(());
            }
            let normalized_id = normalize_ident(id.0.as_str());

            if !referenced_tables.is_empty() {
                if let Some(row_id_expr) =
                    parse_row_id(&normalized_id, 0, || referenced_tables.len() != 1)?
                {
                    *expr = row_id_expr;

                    return Ok(());
                }
            }
            let mut match_result = None;
            for (tbl_idx, table) in referenced_tables.iter().enumerate() {
                let col_idx = table.columns().iter().position(|c| {
                    c.name
                        .as_ref()
                        .map_or(false, |name| name.eq_ignore_ascii_case(&normalized_id))
                });
                if col_idx.is_some() {
                    if match_result.is_some() {
                        crate::bail_parse_error!("Column {} is ambiguous", id.0);
                    }
                    let col = table.columns().get(col_idx.unwrap()).unwrap();
                    match_result = Some((tbl_idx, col_idx.unwrap(), col.is_rowid_alias));
                }
            }
            if let Some((tbl_idx, col_idx, is_rowid_alias)) = match_result {
                *expr = Expr::Column {
                    database: None, // TODO: support different databases
                    table: tbl_idx,
                    column: col_idx,
                    is_rowid_alias,
                };
                return Ok(());
            }

            if let Some(result_columns) = result_columns {
                for result_column in result_columns.iter() {
                    if result_column
                        .name(referenced_tables)
                        .map_or(false, |name| name == &normalized_id)
                    {
                        *expr = result_column.expr.clone();
                        return Ok(());
                    }
                }
            }
            crate::bail_parse_error!("Column {} not found", id.0);
        }
        Expr::Qualified(tbl, id) => {
            let normalized_table_name = normalize_ident(tbl.0.as_str());
            let matching_tbl_idx = referenced_tables
                .iter()
                .position(|t| t.identifier.eq_ignore_ascii_case(&normalized_table_name));
            if matching_tbl_idx.is_none() {
                crate::bail_parse_error!("Table {} not found", normalized_table_name);
            }
            let tbl_idx = matching_tbl_idx.unwrap();
            let normalized_id = normalize_ident(id.0.as_str());

            if let Some(row_id_expr) = parse_row_id(&normalized_id, tbl_idx, || false)? {
                *expr = row_id_expr;

                return Ok(());
            }
            let col_idx = referenced_tables[tbl_idx].columns().iter().position(|c| {
                c.name
                    .as_ref()
                    .map_or(false, |name| name.eq_ignore_ascii_case(&normalized_id))
            });
            if col_idx.is_none() {
                crate::bail_parse_error!("Column {} not found", normalized_id);
            }
            let col = referenced_tables[tbl_idx]
                .columns()
                .get(col_idx.unwrap())
                .unwrap();
            *expr = Expr::Column {
                database: None, // TODO: support different databases
                table: tbl_idx,
                column: col_idx.unwrap(),
                is_rowid_alias: col.is_rowid_alias,
            };
            Ok(())
        }
        Expr::Between {
            lhs,
            not: _,
            start,
            end,
        } => {
            bind_column_references(lhs, referenced_tables, result_columns)?;
            bind_column_references(start, referenced_tables, result_columns)?;
            bind_column_references(end, referenced_tables, result_columns)?;
            Ok(())
        }
        Expr::Binary(expr, _operator, expr1) => {
            bind_column_references(expr, referenced_tables, result_columns)?;
            bind_column_references(expr1, referenced_tables, result_columns)?;
            Ok(())
        }
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            if let Some(base) = base {
                bind_column_references(base, referenced_tables, result_columns)?;
            }
            for (when, then) in when_then_pairs {
                bind_column_references(when, referenced_tables, result_columns)?;
                bind_column_references(then, referenced_tables, result_columns)?;
            }
            if let Some(else_expr) = else_expr {
                bind_column_references(else_expr, referenced_tables, result_columns)?;
            }
            Ok(())
        }
        Expr::Cast { expr, type_name: _ } => {
            bind_column_references(expr, referenced_tables, result_columns)
        }
        Expr::Collate(expr, _string) => {
            bind_column_references(expr, referenced_tables, result_columns)
        }
        Expr::FunctionCall {
            name: _,
            distinctness: _,
            args,
            order_by: _,
            filter_over: _,
        } => {
            if let Some(args) = args {
                for arg in args {
                    bind_column_references(arg, referenced_tables, result_columns)?;
                }
            }
            Ok(())
        }
        // Already bound earlier
        Expr::Column { .. } | Expr::RowId { .. } => Ok(()),
        Expr::DoublyQualified(_, _, _) => todo!(),
        Expr::Exists(_) => todo!(),
        Expr::FunctionCallStar { .. } => Ok(()),
        Expr::InList { lhs, not: _, rhs } => {
            bind_column_references(lhs, referenced_tables, result_columns)?;
            if let Some(rhs) = rhs {
                for arg in rhs {
                    bind_column_references(arg, referenced_tables, result_columns)?;
                }
            }
            Ok(())
        }
        Expr::InSelect { .. } => todo!(),
        Expr::InTable { .. } => todo!(),
        Expr::IsNull(expr) => {
            bind_column_references(expr, referenced_tables, result_columns)?;
            Ok(())
        }
        Expr::Like { lhs, rhs, .. } => {
            bind_column_references(lhs, referenced_tables, result_columns)?;
            bind_column_references(rhs, referenced_tables, result_columns)?;
            Ok(())
        }
        Expr::Literal(_) => Ok(()),
        Expr::Name(_) => todo!(),
        Expr::NotNull(expr) => {
            bind_column_references(expr, referenced_tables, result_columns)?;
            Ok(())
        }
        Expr::Parenthesized(expr) => {
            for e in expr.iter_mut() {
                bind_column_references(e, referenced_tables, result_columns)?;
            }
            Ok(())
        }
        Expr::Raise(_, _) => todo!(),
        Expr::Subquery(_) => todo!(),
        Expr::Unary(_, expr) => {
            bind_column_references(expr, referenced_tables, result_columns)?;
            Ok(())
        }
        Expr::Variable(_) => Ok(()),
    }
}

fn parse_from_clause_table(
    schema: &Schema,
    table: ast::SelectTable,
    cur_table_index: usize,
    syms: &SymbolTable,
) -> Result<TableReference> {
    match table {
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
            Ok(TableReference {
                op: Operation::Scan { iter_dir: None },
                table: Table::BTree(table.clone()),
                identifier: alias.unwrap_or(normalized_qualified_name),
                join_info: None,
            })
        }
        ast::SelectTable::Select(subselect, maybe_alias) => {
            let Plan::Select(mut subplan) = prepare_select_plan(schema, *subselect, syms)? else {
                unreachable!();
            };
            subplan.query_type = SelectQueryType::Subquery {
                yield_reg: usize::MAX, // will be set later in bytecode emission
                coroutine_implementation_start: BranchOffset::Placeholder, // will be set later in bytecode emission
            };
            let identifier = maybe_alias
                .map(|a| match a {
                    ast::As::As(id) => id.0.clone(),
                    ast::As::Elided(id) => id.0.clone(),
                })
                .unwrap_or(format!("subquery_{}", cur_table_index));
            let table_reference = TableReference::new_subquery(identifier, subplan, None);
            Ok(table_reference)
        }
        _ => todo!(),
    }
}

pub fn parse_from(
    schema: &Schema,
    mut from: Option<FromClause>,
    syms: &SymbolTable,
    out_where_clause: &mut Vec<WhereTerm>,
) -> Result<Vec<TableReference>> {
    if from.as_ref().and_then(|f| f.select.as_ref()).is_none() {
        return Ok(vec![]);
    }

    let mut from_owned = std::mem::take(&mut from).unwrap();
    let select_owned = *std::mem::take(&mut from_owned.select).unwrap();
    let joins_owned = std::mem::take(&mut from_owned.joins).unwrap_or_default();
    let mut tables = vec![parse_from_clause_table(schema, select_owned, 0, syms)?];

    for join in joins_owned.into_iter() {
        parse_join(schema, join, syms, &mut tables, out_where_clause)?;
    }

    Ok(tables)
}

pub fn parse_where(
    where_clause: Option<Expr>,
    table_references: &[TableReference],
    result_columns: Option<&[ResultSetColumn]>,
    out_where_clause: &mut Vec<WhereTerm>,
) -> Result<()> {
    if let Some(where_expr) = where_clause {
        let mut predicates = vec![];
        break_predicate_at_and_boundaries(where_expr, &mut predicates);
        for expr in predicates.iter_mut() {
            bind_column_references(expr, table_references, result_columns)?;
        }
        for expr in predicates {
            let eval_at_loop = get_rightmost_table_referenced_in_expr(&expr)?;
            out_where_clause.push(WhereTerm {
                expr,
                from_outer_join: false,
                eval_at_loop,
            });
        }
        Ok(())
    } else {
        Ok(())
    }
}

/**
  Returns the rightmost table index that is referenced in the given AST expression.
  Rightmost = innermost loop.
  This is used to determine where we should evaluate a given condition expression,
  and it needs to be the rightmost table referenced in the expression, because otherwise
  the condition would be evaluated before a row is read from that table.
*/
fn get_rightmost_table_referenced_in_expr<'a>(predicate: &'a ast::Expr) -> Result<usize> {
    let mut max_table_idx = 0;
    match predicate {
        ast::Expr::Binary(e1, _, e2) => {
            max_table_idx = max_table_idx.max(get_rightmost_table_referenced_in_expr(e1)?);
            max_table_idx = max_table_idx.max(get_rightmost_table_referenced_in_expr(e2)?);
        }
        ast::Expr::Column { table, .. } => {
            max_table_idx = max_table_idx.max(*table);
        }
        ast::Expr::Id(_) => {
            /* Id referring to column will already have been rewritten as an Expr::Column */
            /* we only get here with literal 'true' or 'false' etc  */
        }
        ast::Expr::Qualified(_, _) => {
            unreachable!("Qualified should be resolved to a Column before optimizer")
        }
        ast::Expr::Literal(_) => {}
        ast::Expr::Like { lhs, rhs, .. } => {
            max_table_idx = max_table_idx.max(get_rightmost_table_referenced_in_expr(lhs)?);
            max_table_idx = max_table_idx.max(get_rightmost_table_referenced_in_expr(rhs)?);
        }
        ast::Expr::FunctionCall {
            args: Some(args), ..
        } => {
            for arg in args {
                max_table_idx = max_table_idx.max(get_rightmost_table_referenced_in_expr(arg)?);
            }
        }
        ast::Expr::InList { lhs, rhs, .. } => {
            max_table_idx = max_table_idx.max(get_rightmost_table_referenced_in_expr(lhs)?);
            if let Some(rhs_list) = rhs {
                for rhs_expr in rhs_list {
                    max_table_idx =
                        max_table_idx.max(get_rightmost_table_referenced_in_expr(rhs_expr)?);
                }
            }
        }
        _ => {}
    }

    Ok(max_table_idx)
}

fn parse_join(
    schema: &Schema,
    join: ast::JoinedSelectTable,
    syms: &SymbolTable,
    tables: &mut Vec<TableReference>,
    out_where_clause: &mut Vec<WhereTerm>,
) -> Result<()> {
    let ast::JoinedSelectTable {
        operator: join_operator,
        table,
        constraint,
    } = join;

    let cur_table_index = tables.len();
    let table = parse_from_clause_table(schema, table, cur_table_index, syms)?;
    tables.push(table);

    let (outer, natural) = match join_operator {
        ast::JoinOperator::TypedJoin(Some(join_type)) => {
            let is_outer = join_type.contains(JoinType::OUTER);
            let is_natural = join_type.contains(JoinType::NATURAL);
            (is_outer, is_natural)
        }
        _ => (false, false),
    };

    let mut using = None;

    if natural && constraint.is_some() {
        crate::bail_parse_error!("NATURAL JOIN cannot be combined with ON or USING clause");
    }

    let constraint = if natural {
        assert!(tables.len() >= 2);
        let rightmost_table = tables.last().unwrap();
        // NATURAL JOIN is first transformed into a USING join with the common columns
        let right_cols = rightmost_table.columns();
        let mut distinct_names: Option<ast::DistinctNames> = None;
        // TODO: O(n^2) maybe not great for large tables or big multiway joins
        for right_col in right_cols.iter() {
            let mut found_match = false;
            for left_table in tables.iter().take(tables.len() - 1) {
                for left_col in left_table.columns().iter() {
                    if left_col.name == right_col.name {
                        if let Some(distinct_names) = distinct_names.as_mut() {
                            distinct_names
                                .insert(ast::Name(
                                    left_col.name.clone().expect("column name is None"),
                                ))
                                .unwrap();
                        } else {
                            distinct_names = Some(ast::DistinctNames::new(ast::Name(
                                left_col.name.clone().expect("column name is None"),
                            )));
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
        if let Some(distinct_names) = distinct_names {
            Some(ast::JoinConstraint::Using(distinct_names))
        } else {
            crate::bail_parse_error!("No columns found to NATURAL join on");
        }
    } else {
        constraint
    };

    if let Some(constraint) = constraint {
        match constraint {
            ast::JoinConstraint::On(expr) => {
                let mut preds = vec![];
                break_predicate_at_and_boundaries(expr, &mut preds);
                for predicate in preds.iter_mut() {
                    bind_column_references(predicate, tables, None)?;
                }
                for pred in preds {
                    let cur_table_idx = tables.len() - 1;
                    let eval_at_loop = if outer {
                        cur_table_idx
                    } else {
                        get_rightmost_table_referenced_in_expr(&pred)?
                    };
                    out_where_clause.push(WhereTerm {
                        expr: pred,
                        from_outer_join: outer,
                        eval_at_loop,
                    });
                }
            }
            ast::JoinConstraint::Using(distinct_names) => {
                // USING join is replaced with a list of equality predicates
                for distinct_name in distinct_names.iter() {
                    let name_normalized = normalize_ident(distinct_name.0.as_str());
                    let cur_table_idx = tables.len() - 1;
                    let left_tables = &tables[..cur_table_idx];
                    assert!(!left_tables.is_empty());
                    let right_table = tables.last().unwrap();
                    let mut left_col = None;
                    for (left_table_idx, left_table) in left_tables.iter().enumerate() {
                        left_col = left_table
                            .columns()
                            .iter()
                            .enumerate()
                            .find(|(_, col)| {
                                col.name
                                    .as_ref()
                                    .map_or(false, |name| *name == name_normalized)
                            })
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
                    let right_col = right_table.columns().iter().enumerate().find(|(_, col)| {
                        col.name
                            .as_ref()
                            .map_or(false, |name| *name == name_normalized)
                    });
                    if right_col.is_none() {
                        crate::bail_parse_error!(
                            "cannot join using column {} - column not present in all tables",
                            distinct_name.0
                        );
                    }
                    let (left_table_idx, left_col_idx, left_col) = left_col.unwrap();
                    let (right_col_idx, right_col) = right_col.unwrap();
                    let expr = Expr::Binary(
                        Box::new(Expr::Column {
                            database: None,
                            table: left_table_idx,
                            column: left_col_idx,
                            is_rowid_alias: left_col.is_rowid_alias,
                        }),
                        ast::Operator::Equals,
                        Box::new(Expr::Column {
                            database: None,
                            table: cur_table_idx,
                            column: right_col_idx,
                            is_rowid_alias: right_col.is_rowid_alias,
                        }),
                    );
                    let eval_at_loop = if outer {
                        cur_table_idx
                    } else {
                        get_rightmost_table_referenced_in_expr(&expr)?
                    };
                    out_where_clause.push(WhereTerm {
                        expr,
                        from_outer_join: outer,
                        eval_at_loop,
                    });
                }
                using = Some(distinct_names);
            }
        }
    }

    assert!(tables.len() >= 2);
    let last_idx = tables.len() - 1;
    let rightmost_table = tables.get_mut(last_idx).unwrap();
    rightmost_table.join_info = Some(JoinInfo { outer, using });

    Ok(())
}

pub fn parse_limit(limit: Limit) -> Result<(Option<isize>, Option<isize>)> {
    let offset_val = match limit.offset {
        Some(offset_expr) => match offset_expr {
            Expr::Literal(ast::Literal::Numeric(n)) => n.parse().ok(),
            // If OFFSET is negative, the result is as if OFFSET is zero
            Expr::Unary(UnaryOperator::Negative, expr) => match *expr {
                Expr::Literal(ast::Literal::Numeric(n)) => n.parse::<isize>().ok().map(|num| -num),
                _ => crate::bail_parse_error!("Invalid OFFSET clause"),
            },
            _ => crate::bail_parse_error!("Invalid OFFSET clause"),
        },
        None => Some(0),
    };

    if let Expr::Literal(ast::Literal::Numeric(n)) = limit.expr {
        Ok((n.parse().ok(), offset_val))
    } else if let Expr::Unary(UnaryOperator::Negative, expr) = limit.expr {
        if let Expr::Literal(ast::Literal::Numeric(n)) = *expr {
            let limit_val = n.parse::<isize>().ok().map(|num| -num);
            Ok((limit_val, offset_val))
        } else {
            crate::bail_parse_error!("Invalid LIMIT clause");
        }
    } else if let Expr::Id(id) = limit.expr {
        if id.0.eq_ignore_ascii_case("true") {
            Ok((Some(1), offset_val))
        } else if id.0.eq_ignore_ascii_case("false") {
            Ok((Some(0), offset_val))
        } else {
            crate::bail_parse_error!("Invalid LIMIT clause");
        }
    } else {
        crate::bail_parse_error!("Invalid LIMIT clause");
    }
}

pub fn break_predicate_at_and_boundaries(predicate: Expr, out_predicates: &mut Vec<Expr>) {
    match predicate {
        Expr::Binary(left, ast::Operator::And, right) => {
            break_predicate_at_and_boundaries(*left, out_predicates);
            break_predicate_at_and_boundaries(*right, out_predicates);
        }
        _ => {
            out_predicates.push(predicate);
        }
    }
}

fn parse_row_id<F>(column_name: &str, table_id: usize, fn_check: F) -> Result<Option<Expr>>
where
    F: FnOnce() -> bool,
{
    if column_name.eq_ignore_ascii_case(ROWID) {
        if fn_check() {
            crate::bail_parse_error!("ROWID is ambiguous");
        }

        return Ok(Some(Expr::RowId {
            database: None, // TODO: support different databases
            table: table_id,
        }));
    }
    Ok(None)
}
