use super::{
    plan::{Aggregate, Plan, SelectQueryType, SourceOperator, TableReference, TableReferenceType},
    select::prepare_select_plan,
};
use crate::{
    function::Func,
    schema::{Schema, Table},
    util::{exprs_are_equivalent, normalize_ident},
    vdbe::BranchOffset,
    Result,
};
use sqlite3_parser::ast::{self, Expr, FromClause, JoinType, Limit};

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

pub fn resolve_aggregates(expr: &ast::Expr, aggs: &mut Vec<Aggregate>) -> bool {
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

pub fn bind_column_references(
    expr: &mut ast::Expr,
    referenced_tables: &[TableReference],
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
                    .columns()
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(&normalized_id));
                if col_idx.is_some() {
                    if match_result.is_some() {
                        crate::bail_parse_error!("Column {} is ambiguous", id.0);
                    }
                    let col = table.columns().get(col_idx.unwrap()).unwrap();
                    match_result = Some((tbl_idx, col_idx.unwrap(), col.is_rowid_alias));
                }
            }
            if match_result.is_none() {
                crate::bail_parse_error!("Column {} not found", id.0);
            }
            let (tbl_idx, col_idx, is_rowid_alias) = match_result.unwrap();
            *expr = ast::Expr::Column {
                database: None, // TODO: support different databases
                table: tbl_idx,
                column: col_idx,
                is_rowid_alias,
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
                .columns()
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(&normalized_id));
            if col_idx.is_none() {
                crate::bail_parse_error!("Column {} not found", normalized_id);
            }
            let col = referenced_tables[tbl_idx]
                .columns()
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
        // Already bound earlier
        ast::Expr::Column { .. } => Ok(()),
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

fn parse_from_clause_table(
    schema: &Schema,
    table: ast::SelectTable,
    operator_id_counter: &mut OperatorIdCounter,
    cur_table_index: usize,
) -> Result<(TableReference, SourceOperator)> {
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
            let table_reference = TableReference {
                table: Table::BTree(table.clone()),
                table_identifier: alias.unwrap_or(normalized_qualified_name),
                table_index: cur_table_index,
                reference_type: TableReferenceType::BTreeTable,
            };
            Ok((
                table_reference.clone(),
                SourceOperator::Scan {
                    table_reference,
                    predicates: None,
                    id: operator_id_counter.get_next_id(),
                    iter_dir: None,
                },
            ))
        }
        ast::SelectTable::Select(subselect, maybe_alias) => {
            let Plan::Select(mut subplan) = prepare_select_plan(schema, *subselect)? else {
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
            let table_reference =
                TableReference::new_subquery(identifier.clone(), cur_table_index, &subplan);
            Ok((
                table_reference.clone(),
                SourceOperator::Subquery {
                    id: operator_id_counter.get_next_id(),
                    table_reference,
                    plan: Box::new(subplan),
                    predicates: None,
                },
            ))
        }
        _ => todo!(),
    }
}

pub fn parse_from(
    schema: &Schema,
    mut from: Option<FromClause>,
    operator_id_counter: &mut OperatorIdCounter,
) -> Result<(SourceOperator, Vec<TableReference>)> {
    if from.as_ref().and_then(|f| f.select.as_ref()).is_none() {
        return Ok((
            SourceOperator::Nothing {
                id: operator_id_counter.get_next_id(),
            },
            vec![],
        ));
    }

    let mut table_index = 0;
    let mut tables = vec![];

    let mut from_owned = std::mem::take(&mut from).unwrap();
    let select_owned = *std::mem::take(&mut from_owned.select).unwrap();
    let joins_owned = std::mem::take(&mut from_owned.joins).unwrap_or_default();
    let (table_reference, mut operator) =
        parse_from_clause_table(schema, select_owned, operator_id_counter, table_index)?;

    tables.push(table_reference);
    table_index += 1;

    for join in joins_owned.into_iter() {
        let JoinParseResult {
            source_operator: right,
            is_outer_join: outer,
            using,
            predicates,
        } = parse_join(schema, join, operator_id_counter, &mut tables, table_index)?;
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

pub fn parse_where(
    where_clause: Option<Expr>,
    referenced_tables: &[TableReference],
) -> Result<Option<Vec<Expr>>> {
    if let Some(where_expr) = where_clause {
        let mut predicates = vec![];
        break_predicate_at_and_boundaries(where_expr, &mut predicates);
        for expr in predicates.iter_mut() {
            bind_column_references(expr, referenced_tables)?;
        }
        Ok(Some(predicates))
    } else {
        Ok(None)
    }
}

struct JoinParseResult {
    source_operator: SourceOperator,
    is_outer_join: bool,
    using: Option<ast::DistinctNames>,
    predicates: Option<Vec<ast::Expr>>,
}

fn parse_join(
    schema: &Schema,
    join: ast::JoinedSelectTable,
    operator_id_counter: &mut OperatorIdCounter,
    tables: &mut Vec<TableReference>,
    table_index: usize,
) -> Result<JoinParseResult> {
    let ast::JoinedSelectTable {
        operator: join_operator,
        table,
        constraint,
    } = join;

    let (table_reference, source_operator) =
        parse_from_clause_table(schema, table, operator_id_counter, table_index)?;

    tables.push(table_reference);

    let (outer, natural) = match join_operator {
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
        let right_cols = &right_table.columns();
        let mut distinct_names = None;
        // TODO: O(n^2) maybe not great for large tables or big multiway joins
        for right_col in right_cols.iter() {
            let mut found_match = false;
            for left_table in left_tables.iter() {
                for left_col in left_table.columns().iter() {
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
                            .columns()
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
                        .columns()
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

    Ok(JoinParseResult {
        source_operator,
        is_outer_join: outer,
        using,
        predicates,
    })
}

pub fn parse_limit(limit: Limit) -> Option<usize> {
    if let Expr::Literal(ast::Literal::Numeric(n)) = limit.expr {
        n.parse().ok()
    } else if let Expr::Id(id) = limit.expr {
        if id.0.eq_ignore_ascii_case("true") {
            Some(1)
        } else if id.0.eq_ignore_ascii_case("false") {
            Some(0)
        } else {
            None
        }
    } else {
        None
    }
}

pub fn break_predicate_at_and_boundaries(
    predicate: ast::Expr,
    out_predicates: &mut Vec<ast::Expr>,
) {
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
