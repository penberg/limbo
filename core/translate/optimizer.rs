use std::rc::Rc;

use sqlite3_parser::ast;

use crate::{schema::BTreeTable, util::normalize_ident, Result};

use super::plan::{
    get_table_ref_bitmask_for_ast_expr, get_table_ref_bitmask_for_operator, Operator, Plan,
};

/**
 * Make a few passes over the plan to optimize it.
 */
pub fn optimize_plan(mut select_plan: Plan) -> Result<Plan> {
    push_predicates(
        &mut select_plan.root_operator,
        &select_plan.referenced_tables,
    )?;
    eliminate_constants(&mut select_plan.root_operator)?;
    use_indexes(
        &mut select_plan.root_operator,
        &select_plan.referenced_tables,
    )?;
    Ok(select_plan)
}

/**
 * Use indexes where possible (currently just primary key lookups)
 */
fn use_indexes(
    operator: &mut Operator,
    referenced_tables: &[(Rc<BTreeTable>, String)],
) -> Result<()> {
    match operator {
        Operator::Scan {
            table,
            predicates: filter,
            table_identifier,
            id,
        } => {
            if filter.is_none() {
                return Ok(());
            }

            let fs = filter.as_mut().unwrap();
            let mut i = 0;
            let mut maybe_rowid_predicate = None;
            while i < fs.len() {
                let f = fs[i].take_ownership();
                let table_index = referenced_tables
                    .iter()
                    .position(|(t, t_id)| Rc::ptr_eq(t, table) && t_id == table_identifier)
                    .unwrap();
                let (can_use, expr) =
                    try_extract_rowid_comparison_expression(f, table_index, referenced_tables)?;
                if can_use {
                    maybe_rowid_predicate = Some(expr);
                    fs.remove(i);
                    break;
                } else {
                    fs[i] = expr;
                    i += 1;
                }
            }

            if let Some(rowid_predicate) = maybe_rowid_predicate {
                let predicates_owned = if fs.is_empty() {
                    None
                } else {
                    Some(fs.drain(..).collect())
                };
                *operator = Operator::SeekRowid {
                    table: table.clone(),
                    table_identifier: table_identifier.clone(),
                    rowid_predicate,
                    predicates: predicates_owned,
                    id: *id,
                }
            }

            return Ok(());
        }
        Operator::Aggregate { source, .. } => {
            use_indexes(source, referenced_tables)?;
            return Ok(());
        }
        Operator::Filter { source, .. } => {
            use_indexes(source, referenced_tables)?;
            return Ok(());
        }
        Operator::SeekRowid { .. } => {
            return Ok(());
        }
        Operator::Limit { source, .. } => {
            use_indexes(source, referenced_tables)?;
            return Ok(());
        }
        Operator::Join { left, right, .. } => {
            use_indexes(left, referenced_tables)?;
            use_indexes(right, referenced_tables)?;
            return Ok(());
        }
        Operator::Order { source, .. } => {
            use_indexes(source, referenced_tables)?;
            return Ok(());
        }
        Operator::Projection { source, .. } => {
            use_indexes(source, referenced_tables)?;
            return Ok(());
        }
        Operator::Nothing => {
            return Ok(());
        }
    }
}

// removes predicates that are always true
// returns false if there is an impossible predicate that is always false
fn eliminate_constants(operator: &mut Operator) -> Result<bool> {
    match operator {
        Operator::Filter {
            source, predicates, ..
        } => {
            let mut i = 0;
            while i < predicates.len() {
                let predicate = &predicates[i];
                if predicate.is_always_true()? {
                    predicates.remove(i);
                } else if predicate.is_always_false()? {
                    return Ok(false);
                } else {
                    i += 1;
                }
            }

            if predicates.is_empty() {
                *operator = source.take_ownership();
                eliminate_constants(operator)?;
            } else {
                eliminate_constants(source)?;
            }

            return Ok(true);
        }
        Operator::Join {
            left,
            right,
            predicates,
            outer,
            ..
        } => {
            if !eliminate_constants(left)? {
                return Ok(false);
            }
            if !eliminate_constants(right)? && !*outer {
                return Ok(false);
            }

            if predicates.is_none() {
                return Ok(true);
            }

            let predicates = predicates.as_mut().unwrap();

            let mut i = 0;
            while i < predicates.len() {
                let predicate = &predicates[i];
                if predicate.is_always_true()? {
                    predicates.remove(i);
                } else if predicate.is_always_false()? && !*outer {
                    return Ok(false);
                } else {
                    i += 1;
                }
            }

            return Ok(true);
        }
        Operator::Aggregate { source, .. } => {
            let ok = eliminate_constants(source)?;
            if !ok {
                *source = Box::new(Operator::Nothing);
            }
            return Ok(ok);
        }
        Operator::SeekRowid {
            rowid_predicate,
            predicates,
            ..
        } => {
            if let Some(predicates) = predicates {
                let mut i = 0;
                while i < predicates.len() {
                    let predicate = &predicates[i];
                    if predicate.is_always_true()? {
                        predicates.remove(i);
                    } else if predicate.is_always_false()? {
                        return Ok(false);
                    } else {
                        i += 1;
                    }
                }
            }

            if rowid_predicate.is_always_false()? {
                return Ok(false);
            }

            return Ok(true);
        }
        Operator::Limit { source, .. } => {
            let ok = eliminate_constants(source)?;
            if !ok {
                *operator = Operator::Nothing;
            }
            return Ok(ok);
        }
        Operator::Order { source, .. } => {
            let ok = eliminate_constants(source)?;
            if !ok {
                *operator = Operator::Nothing;
            }
            return Ok(true);
        }
        Operator::Projection { source, .. } => {
            let ok = eliminate_constants(source)?;
            if !ok {
                *operator = Operator::Nothing;
            }
            return Ok(ok);
        }
        Operator::Scan { predicates, .. } => {
            if let Some(ps) = predicates {
                let mut i = 0;
                while i < ps.len() {
                    let predicate = &ps[i];
                    if predicate.is_always_true()? {
                        ps.remove(i);
                    } else if predicate.is_always_false()? {
                        return Ok(false);
                    } else {
                        i += 1;
                    }
                }

                if ps.is_empty() {
                    *predicates = None;
                }
            }
            return Ok(true);
        }
        Operator::Nothing => return Ok(true),
    }
}

/**
  Recursively pushes predicates down the tree, as far as possible.
*/
fn push_predicates(
    operator: &mut Operator,
    referenced_tables: &Vec<(Rc<BTreeTable>, String)>,
) -> Result<()> {
    match operator {
        Operator::Filter {
            source, predicates, ..
        } => {
            let mut i = 0;
            while i < predicates.len() {
                // try to push the predicate to the source
                // if it succeeds, remove the predicate from the filter
                let predicate_owned = predicates[i].take_ownership();
                let Some(predicate) = push_predicate(source, predicate_owned, referenced_tables)?
                else {
                    predicates.remove(i);
                    continue;
                };
                predicates[i] = predicate;
                i += 1;
            }

            if predicates.is_empty() {
                *operator = source.take_ownership();
            }

            return Ok(());
        }
        Operator::Join {
            left,
            right,
            predicates,
            outer,
            ..
        } => {
            push_predicates(left, referenced_tables)?;
            push_predicates(right, referenced_tables)?;

            if predicates.is_none() {
                return Ok(());
            }

            let predicates = predicates.as_mut().unwrap();

            let mut i = 0;
            while i < predicates.len() {
                // try to push the predicate to the left side first, then to the right side

                // temporarily take ownership of the predicate
                let predicate_owned = predicates[i].take_ownership();
                // left join predicates cant be pushed to the left side
                let push_result = if *outer {
                    Some(predicate_owned)
                } else {
                    push_predicate(left, predicate_owned, referenced_tables)?
                };
                // if the predicate was pushed to a child, remove it from the list
                let Some(predicate) = push_result else {
                    predicates.remove(i);
                    continue;
                };
                // otherwise try to push it to the right side
                // if it was pushed to the right side, remove it from the list
                let Some(predicate) = push_predicate(right, predicate, referenced_tables)? else {
                    predicates.remove(i);
                    continue;
                };
                // otherwise keep the predicate in the list
                predicates[i] = predicate;
                i += 1;
            }

            return Ok(());
        }
        Operator::Aggregate { source, .. } => {
            push_predicates(source, referenced_tables)?;

            return Ok(());
        }
        Operator::SeekRowid { .. } => {
            return Ok(());
        }
        Operator::Limit { source, .. } => {
            push_predicates(source, referenced_tables)?;
            return Ok(());
        }
        Operator::Order { source, .. } => {
            push_predicates(source, referenced_tables)?;
            return Ok(());
        }
        Operator::Projection { source, .. } => {
            push_predicates(source, referenced_tables)?;
            return Ok(());
        }
        Operator::Scan { .. } => {
            return Ok(());
        }
        Operator::Nothing => {
            return Ok(());
        }
    }
}

/**
  Push a single predicate down the tree, as far as possible.
  Returns Ok(None) if the predicate was pushed, otherwise returns itself as Ok(Some(predicate))
*/
fn push_predicate(
    operator: &mut Operator,
    predicate: ast::Expr,
    referenced_tables: &Vec<(Rc<BTreeTable>, String)>,
) -> Result<Option<ast::Expr>> {
    match operator {
        Operator::Scan {
            predicates,
            table_identifier,
            ..
        } => {
            let table_index = referenced_tables
                .iter()
                .position(|(_, t_id)| t_id == table_identifier)
                .unwrap();

            let predicate_bitmask =
                get_table_ref_bitmask_for_ast_expr(referenced_tables, &predicate)?;

            // the expression is allowed to refer to tables on its left, i.e. the righter bits in the mask
            // e.g. if this table is 0010, and the table on its right in the join is 0100:
            // if predicate_bitmask is 0011, the predicate can be pushed (refers to this table and the table on its left)
            // if predicate_bitmask is 0001, the predicate can be pushed (refers to the table on its left)
            // if predicate_bitmask is 0101, the predicate can't be pushed (refers to this table and a table on its right)
            let next_table_on_the_right_in_join_bitmask = 1 << (table_index + 1);
            if predicate_bitmask >= next_table_on_the_right_in_join_bitmask {
                return Ok(Some(predicate));
            }

            if predicates.is_none() {
                predicates.replace(vec![predicate]);
            } else {
                predicates.as_mut().unwrap().push(predicate);
            }

            return Ok(None);
        }
        Operator::Filter {
            source,
            predicates: ps,
            ..
        } => {
            let push_result = push_predicate(source, predicate, referenced_tables)?;
            if push_result.is_none() {
                return Ok(None);
            }

            ps.push(push_result.unwrap());

            return Ok(None);
        }
        Operator::Join {
            left,
            right,
            predicates: join_on_preds,
            outer,
            ..
        } => {
            let push_result_left = push_predicate(left, predicate, referenced_tables)?;
            if push_result_left.is_none() {
                return Ok(None);
            }
            let push_result_right =
                push_predicate(right, push_result_left.unwrap(), referenced_tables)?;
            if push_result_right.is_none() {
                return Ok(None);
            }

            if *outer {
                return Ok(Some(push_result_right.unwrap()));
            }

            let pred = push_result_right.unwrap();

            let table_refs_bitmask = get_table_ref_bitmask_for_ast_expr(referenced_tables, &pred)?;

            let left_bitmask = get_table_ref_bitmask_for_operator(referenced_tables, left)?;
            let right_bitmask = get_table_ref_bitmask_for_operator(referenced_tables, right)?;

            if table_refs_bitmask & left_bitmask == 0 || table_refs_bitmask & right_bitmask == 0 {
                return Ok(Some(pred));
            }

            if join_on_preds.is_none() {
                join_on_preds.replace(vec![pred]);
            } else {
                join_on_preds.as_mut().unwrap().push(pred);
            }

            return Ok(None);
        }
        Operator::Aggregate { source, .. } => {
            let push_result = push_predicate(source, predicate, referenced_tables)?;
            if push_result.is_none() {
                return Ok(None);
            }

            return Ok(Some(push_result.unwrap()));
        }
        Operator::SeekRowid { .. } => {
            return Ok(Some(predicate));
        }
        Operator::Limit { source, .. } => {
            let push_result = push_predicate(source, predicate, referenced_tables)?;
            if push_result.is_none() {
                return Ok(None);
            }

            return Ok(Some(push_result.unwrap()));
        }
        Operator::Order { source, .. } => {
            let push_result = push_predicate(source, predicate, referenced_tables)?;
            if push_result.is_none() {
                return Ok(None);
            }

            return Ok(Some(push_result.unwrap()));
        }
        Operator::Projection { source, .. } => {
            let push_result = push_predicate(source, predicate, referenced_tables)?;
            if push_result.is_none() {
                return Ok(None);
            }

            return Ok(Some(push_result.unwrap()));
        }
        Operator::Nothing => {
            return Ok(Some(predicate));
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConstantPredicate {
    AlwaysTrue,
    AlwaysFalse,
}

/**
  Helper trait for expressions that can be optimized
  Implemented for ast::Expr
*/
pub trait Optimizable {
    // if the expression is a constant expression e.g. '1', returns the constant condition
    fn check_constant(&self) -> Result<Option<ConstantPredicate>>;
    fn is_always_true(&self) -> Result<bool> {
        Ok(self
            .check_constant()?
            .map_or(false, |c| c == ConstantPredicate::AlwaysTrue))
    }
    fn is_always_false(&self) -> Result<bool> {
        Ok(self
            .check_constant()?
            .map_or(false, |c| c == ConstantPredicate::AlwaysFalse))
    }
    // if the expression is the primary key of a table, returns the index of the table
    fn check_primary_key(
        &self,
        referenced_tables: &[(Rc<BTreeTable>, String)],
    ) -> Result<Option<usize>>;
}

impl Optimizable for ast::Expr {
    fn check_primary_key(
        &self,
        referenced_tables: &[(Rc<BTreeTable>, String)],
    ) -> Result<Option<usize>> {
        match self {
            ast::Expr::Id(ident) => {
                let ident = normalize_ident(&ident.0);
                let tables = referenced_tables
                    .iter()
                    .enumerate()
                    .filter_map(|(i, (t, _))| {
                        if t.get_column(&ident).map_or(false, |(_, c)| c.primary_key) {
                            Some(i)
                        } else {
                            None
                        }
                    });

                let mut matches = 0;
                let mut matching_tbl = None;

                for tbl in tables {
                    matching_tbl = Some(tbl);
                    matches += 1;
                    if matches > 1 {
                        crate::bail_parse_error!("ambiguous column name {}", ident)
                    }
                }

                Ok(matching_tbl)
            }
            ast::Expr::Qualified(tbl, ident) => {
                let tbl = normalize_ident(&tbl.0);
                let ident = normalize_ident(&ident.0);
                let table = referenced_tables.iter().enumerate().find(|(_, (t, t_id))| {
                    *t_id == tbl && t.get_column(&ident).map_or(false, |(_, c)| c.primary_key)
                });

                if table.is_none() {
                    return Ok(None);
                }

                let table = table.unwrap();

                Ok(Some(table.0))
            }
            _ => Ok(None),
        }
    }
    fn check_constant(&self) -> Result<Option<ConstantPredicate>> {
        match self {
            ast::Expr::Literal(lit) => match lit {
                ast::Literal::Null => Ok(Some(ConstantPredicate::AlwaysFalse)),
                ast::Literal::Numeric(b) => {
                    if let Ok(int_value) = b.parse::<i64>() {
                        return Ok(Some(if int_value == 0 {
                            ConstantPredicate::AlwaysFalse
                        } else {
                            ConstantPredicate::AlwaysTrue
                        }));
                    }
                    if let Ok(float_value) = b.parse::<f64>() {
                        return Ok(Some(if float_value == 0.0 {
                            ConstantPredicate::AlwaysFalse
                        } else {
                            ConstantPredicate::AlwaysTrue
                        }));
                    }

                    Ok(None)
                }
                ast::Literal::String(s) => {
                    let without_quotes = s.trim_matches('\'');
                    if let Ok(int_value) = without_quotes.parse::<i64>() {
                        return Ok(Some(if int_value == 0 {
                            ConstantPredicate::AlwaysFalse
                        } else {
                            ConstantPredicate::AlwaysTrue
                        }));
                    }

                    if let Ok(float_value) = without_quotes.parse::<f64>() {
                        return Ok(Some(if float_value == 0.0 {
                            ConstantPredicate::AlwaysFalse
                        } else {
                            ConstantPredicate::AlwaysTrue
                        }));
                    }

                    Ok(Some(ConstantPredicate::AlwaysFalse))
                }
                _ => Ok(None),
            },
            ast::Expr::Unary(op, expr) => {
                if *op == ast::UnaryOperator::Not {
                    let trivial = expr.check_constant()?;
                    return Ok(trivial.map(|t| match t {
                        ConstantPredicate::AlwaysTrue => ConstantPredicate::AlwaysFalse,
                        ConstantPredicate::AlwaysFalse => ConstantPredicate::AlwaysTrue,
                    }));
                }

                if *op == ast::UnaryOperator::Negative {
                    let trivial = expr.check_constant()?;
                    return Ok(trivial);
                }

                Ok(None)
            }
            ast::Expr::InList { lhs: _, not, rhs } => {
                if rhs.is_none() {
                    return Ok(Some(if *not {
                        ConstantPredicate::AlwaysTrue
                    } else {
                        ConstantPredicate::AlwaysFalse
                    }));
                }
                let rhs = rhs.as_ref().unwrap();
                if rhs.is_empty() {
                    return Ok(Some(if *not {
                        ConstantPredicate::AlwaysTrue
                    } else {
                        ConstantPredicate::AlwaysFalse
                    }));
                }

                Ok(None)
            }
            ast::Expr::Binary(lhs, op, rhs) => {
                let lhs_trivial = lhs.check_constant()?;
                let rhs_trivial = rhs.check_constant()?;
                match op {
                    ast::Operator::And => {
                        if lhs_trivial == Some(ConstantPredicate::AlwaysFalse)
                            || rhs_trivial == Some(ConstantPredicate::AlwaysFalse)
                        {
                            return Ok(Some(ConstantPredicate::AlwaysFalse));
                        }
                        if lhs_trivial == Some(ConstantPredicate::AlwaysTrue)
                            && rhs_trivial == Some(ConstantPredicate::AlwaysTrue)
                        {
                            return Ok(Some(ConstantPredicate::AlwaysTrue));
                        }

                        Ok(None)
                    }
                    ast::Operator::Or => {
                        if lhs_trivial == Some(ConstantPredicate::AlwaysTrue)
                            || rhs_trivial == Some(ConstantPredicate::AlwaysTrue)
                        {
                            return Ok(Some(ConstantPredicate::AlwaysTrue));
                        }
                        if lhs_trivial == Some(ConstantPredicate::AlwaysFalse)
                            && rhs_trivial == Some(ConstantPredicate::AlwaysFalse)
                        {
                            return Ok(Some(ConstantPredicate::AlwaysFalse));
                        }

                        Ok(None)
                    }
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }
}

pub fn try_extract_rowid_comparison_expression(
    expr: ast::Expr,
    table_index: usize,
    referenced_tables: &[(Rc<BTreeTable>, String)],
) -> Result<(bool, ast::Expr)> {
    match expr {
        ast::Expr::Binary(lhs, ast::Operator::Equals, rhs) => {
            if let Some(lhs_table_index) = lhs.check_primary_key(referenced_tables)? {
                if lhs_table_index == table_index {
                    return Ok((true, *rhs));
                }
            }

            if let Some(rhs_table_index) = rhs.check_primary_key(referenced_tables)? {
                if rhs_table_index == table_index {
                    return Ok((true, *lhs));
                }
            }

            Ok((false, ast::Expr::Binary(lhs, ast::Operator::Equals, rhs)))
        }
        _ => Ok((false, expr)),
    }
}

trait TakeOwnership {
    fn take_ownership(&mut self) -> Self;
}

impl TakeOwnership for ast::Expr {
    fn take_ownership(&mut self) -> Self {
        std::mem::replace(self, ast::Expr::Literal(ast::Literal::Null))
    }
}

impl TakeOwnership for Operator {
    fn take_ownership(&mut self) -> Self {
        std::mem::replace(self, Operator::Nothing)
    }
}

fn replace_with<T: TakeOwnership>(expr: &mut T, mut replacement: T) {
    *expr = replacement.take_ownership();
}
