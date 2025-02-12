use std::{collections::HashMap, rc::Rc};

use limbo_sqlite3_parser::ast;

use crate::{
    schema::{Index, Schema},
    Result,
};

use super::plan::{
    DeletePlan, Direction, IterationDirection, Operation, Plan, Search, SelectPlan, TableReference,
    WhereTerm,
};

pub fn optimize_plan(plan: &mut Plan, schema: &Schema) -> Result<()> {
    match plan {
        Plan::Select(plan) => optimize_select_plan(plan, schema),
        Plan::Delete(plan) => optimize_delete_plan(plan, schema),
    }
}

/**
 * Make a few passes over the plan to optimize it.
 * TODO: these could probably be done in less passes,
 * but having them separate makes them easier to understand
 */
fn optimize_select_plan(plan: &mut SelectPlan, schema: &Schema) -> Result<()> {
    optimize_subqueries(plan, schema)?;
    rewrite_exprs_select(plan)?;
    if let ConstantConditionEliminationResult::ImpossibleCondition =
        eliminate_constant_conditions(&mut plan.where_clause)?
    {
        plan.contains_constant_false_condition = true;
        return Ok(());
    }

    use_indexes(
        &mut plan.table_references,
        &schema.indexes,
        &mut plan.where_clause,
    )?;

    eliminate_unnecessary_orderby(plan, schema)?;

    Ok(())
}

fn optimize_delete_plan(plan: &mut DeletePlan, schema: &Schema) -> Result<()> {
    rewrite_exprs_delete(plan)?;
    if let ConstantConditionEliminationResult::ImpossibleCondition =
        eliminate_constant_conditions(&mut plan.where_clause)?
    {
        plan.contains_constant_false_condition = true;
        return Ok(());
    }

    use_indexes(
        &mut plan.table_references,
        &schema.indexes,
        &mut plan.where_clause,
    )?;

    Ok(())
}

fn optimize_subqueries(plan: &mut SelectPlan, schema: &Schema) -> Result<()> {
    for table in plan.table_references.iter_mut() {
        if let Operation::Subquery { plan, .. } = &mut table.op {
            optimize_select_plan(&mut *plan, schema)?;
        }
    }

    Ok(())
}

fn query_is_already_ordered_by(
    table_references: &[TableReference],
    key: &mut ast::Expr,
    available_indexes: &HashMap<String, Vec<Rc<Index>>>,
) -> Result<bool> {
    let first_table = table_references.first();
    if first_table.is_none() {
        return Ok(false);
    }
    let table_reference = first_table.unwrap();
    match &table_reference.op {
        Operation::Scan { .. } => Ok(key.is_rowid_alias_of(0)),
        Operation::Search(search) => match search {
            Search::RowidEq { .. } => Ok(key.is_rowid_alias_of(0)),
            Search::RowidSearch { .. } => Ok(key.is_rowid_alias_of(0)),
            Search::IndexSearch { index, .. } => {
                let index_rc = key.check_index_scan(0, &table_reference, available_indexes)?;
                let index_is_the_same =
                    index_rc.map(|irc| Rc::ptr_eq(index, &irc)).unwrap_or(false);
                Ok(index_is_the_same)
            }
        },
        _ => Ok(false),
    }
}

fn eliminate_unnecessary_orderby(plan: &mut SelectPlan, schema: &Schema) -> Result<()> {
    if plan.order_by.is_none() {
        return Ok(());
    }
    if plan.table_references.len() == 0 {
        return Ok(());
    }

    let o = plan.order_by.as_mut().unwrap();

    if o.len() != 1 {
        // TODO: handle multiple order by keys
        return Ok(());
    }

    let (key, direction) = o.first_mut().unwrap();

    let already_ordered =
        query_is_already_ordered_by(&plan.table_references, key, &schema.indexes)?;

    if already_ordered {
        push_scan_direction(&mut plan.table_references[0], direction);
        plan.order_by = None;
    }

    Ok(())
}

/**
 * Use indexes where possible.
 * Right now we make decisions about using indexes ONLY based on condition expressions, not e.g. ORDER BY or others.
 * This is just because we are WIP.
 *
 * When this function is called, condition expressions from both the actual WHERE clause and the JOIN clauses are in the where_clause vector.
 * If we find a condition that can be used to index scan, we pop it off from the where_clause vector and put it into a Search operation.
 * We put it there simply because it makes it a bit easier to track during translation.
 */
fn use_indexes(
    table_references: &mut [TableReference],
    available_indexes: &HashMap<String, Vec<Rc<Index>>>,
    where_clause: &mut Vec<WhereTerm>,
) -> Result<()> {
    if where_clause.is_empty() {
        return Ok(());
    }

    'outer: for (table_index, table_reference) in table_references.iter_mut().enumerate() {
        if let Operation::Scan { .. } = &mut table_reference.op {
            let mut i = 0;
            while i < where_clause.len() {
                let cond = where_clause.get_mut(i).unwrap();
                if let Some(index_search) = try_extract_index_search_expression(
                    cond,
                    table_index,
                    &table_reference,
                    available_indexes,
                )? {
                    where_clause.remove(i);
                    table_reference.op = Operation::Search(index_search);
                    continue 'outer;
                }
                i += 1;
            }
        }
    }

    Ok(())
}

#[derive(Debug, PartialEq, Clone)]
enum ConstantConditionEliminationResult {
    Continue,
    ImpossibleCondition,
}

/// Removes predicates that are always true.
/// Returns a ConstantEliminationResult indicating whether any predicates are always false.
/// This is used to determine whether the query can be aborted early.
fn eliminate_constant_conditions(
    where_clause: &mut Vec<WhereTerm>,
) -> Result<ConstantConditionEliminationResult> {
    let mut i = 0;
    while i < where_clause.len() {
        let predicate = &where_clause[i];
        if predicate.expr.is_always_true()? {
            // true predicates can be removed since they don't affect the result
            where_clause.remove(i);
        } else if predicate.expr.is_always_false()? {
            // any false predicate in a list of conjuncts (AND-ed predicates) will make the whole list false,
            // except an outer join condition, because that just results in NULLs, not skipping the whole loop
            if predicate.from_outer_join {
                i += 1;
                continue;
            }
            where_clause.truncate(0);
            return Ok(ConstantConditionEliminationResult::ImpossibleCondition);
        } else {
            i += 1;
        }
    }

    Ok(ConstantConditionEliminationResult::Continue)
}

fn push_scan_direction(table: &mut TableReference, direction: &Direction) {
    if let Operation::Scan {
        ref mut iter_dir, ..
    } = table.op
    {
        if iter_dir.is_none() {
            match direction {
                Direction::Ascending => *iter_dir = Some(IterationDirection::Forwards),
                Direction::Descending => *iter_dir = Some(IterationDirection::Backwards),
            }
        }
    }
}

fn rewrite_exprs_select(plan: &mut SelectPlan) -> Result<()> {
    for rc in plan.result_columns.iter_mut() {
        rewrite_expr(&mut rc.expr)?;
    }
    for agg in plan.aggregates.iter_mut() {
        rewrite_expr(&mut agg.original_expr)?;
    }
    for cond in plan.where_clause.iter_mut() {
        rewrite_expr(&mut cond.expr)?;
    }
    if let Some(group_by) = &mut plan.group_by {
        for expr in group_by.exprs.iter_mut() {
            rewrite_expr(expr)?;
        }
    }
    if let Some(order_by) = &mut plan.order_by {
        for (expr, _) in order_by.iter_mut() {
            rewrite_expr(expr)?;
        }
    }

    Ok(())
}

fn rewrite_exprs_delete(plan: &mut DeletePlan) -> Result<()> {
    for cond in plan.where_clause.iter_mut() {
        rewrite_expr(&mut cond.expr)?;
    }
    Ok(())
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
    fn is_rowid_alias_of(&self, table_index: usize) -> bool;
    fn check_index_scan(
        &mut self,
        table_index: usize,
        table_reference: &TableReference,
        available_indexes: &HashMap<String, Vec<Rc<Index>>>,
    ) -> Result<Option<Rc<Index>>>;
}

impl Optimizable for ast::Expr {
    fn is_rowid_alias_of(&self, table_index: usize) -> bool {
        match self {
            Self::Column {
                table,
                is_rowid_alias,
                ..
            } => *is_rowid_alias && *table == table_index,
            _ => false,
        }
    }
    fn check_index_scan(
        &mut self,
        table_index: usize,
        table_reference: &TableReference,
        available_indexes: &HashMap<String, Vec<Rc<Index>>>,
    ) -> Result<Option<Rc<Index>>> {
        match self {
            Self::Column { table, column, .. } => {
                if *table != table_index {
                    return Ok(None);
                }
                let Some(available_indexes_for_table) =
                    available_indexes.get(table_reference.table.get_name())
                else {
                    return Ok(None);
                };
                let Some(column) = table_reference.table.get_column_at(*column) else {
                    return Ok(None);
                };
                for index in available_indexes_for_table.iter() {
                    if let Some(name) = column.name.as_ref() {
                        if &index.columns.first().unwrap().name == name {
                            return Ok(Some(index.clone()));
                        }
                    }
                }
                Ok(None)
            }
            Self::Binary(lhs, op, rhs) => {
                // Only consider index scans for binary ops that are comparisons.
                // e.g. "t1.id = t2.id" is a valid index scan, but "t1.id + 1" is not.
                //
                // TODO/optimization: consider detecting index scan on e.g. table t1 in
                // "WHERE t1.id + 1 = t2.id"
                // here the Expr could be rewritten to "t1.id = t2.id - 1"
                // and then t1.id could be used as an index key.
                if !matches!(
                    *op,
                    ast::Operator::Equals
                        | ast::Operator::Greater
                        | ast::Operator::GreaterEquals
                        | ast::Operator::Less
                        | ast::Operator::LessEquals
                ) {
                    return Ok(None);
                }
                let lhs_index =
                    lhs.check_index_scan(table_index, &table_reference, available_indexes)?;
                if lhs_index.is_some() {
                    return Ok(lhs_index);
                }
                let rhs_index =
                    rhs.check_index_scan(table_index, &table_reference, available_indexes)?;
                if rhs_index.is_some() {
                    // swap lhs and rhs
                    let swapped_operator = match *op {
                        ast::Operator::Equals => ast::Operator::Equals,
                        ast::Operator::Greater => ast::Operator::Less,
                        ast::Operator::GreaterEquals => ast::Operator::LessEquals,
                        ast::Operator::Less => ast::Operator::Greater,
                        ast::Operator::LessEquals => ast::Operator::GreaterEquals,
                        _ => unreachable!(),
                    };
                    let lhs_new = rhs.take_ownership();
                    let rhs_new = lhs.take_ownership();
                    *self = Self::Binary(Box::new(lhs_new), swapped_operator, Box::new(rhs_new));
                    return Ok(rhs_index);
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }
    fn check_constant(&self) -> Result<Option<ConstantPredicate>> {
        match self {
            Self::Literal(lit) => match lit {
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
            Self::Unary(op, expr) => {
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
            Self::InList { lhs: _, not, rhs } => {
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
            Self::Binary(lhs, op, rhs) => {
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

fn opposite_cmp_op(op: ast::Operator) -> ast::Operator {
    match op {
        ast::Operator::Equals => ast::Operator::Equals,
        ast::Operator::Greater => ast::Operator::Less,
        ast::Operator::GreaterEquals => ast::Operator::LessEquals,
        ast::Operator::Less => ast::Operator::Greater,
        ast::Operator::LessEquals => ast::Operator::GreaterEquals,
        _ => panic!("unexpected operator: {:?}", op),
    }
}

pub fn try_extract_index_search_expression(
    cond: &mut WhereTerm,
    table_index: usize,
    table_reference: &TableReference,
    available_indexes: &HashMap<String, Vec<Rc<Index>>>,
) -> Result<Option<Search>> {
    if cond.eval_at_loop != table_index {
        return Ok(None);
    }
    match &mut cond.expr {
        ast::Expr::Binary(lhs, operator, rhs) => {
            if lhs.is_rowid_alias_of(table_index) {
                match operator {
                    ast::Operator::Equals => {
                        let rhs_owned = rhs.take_ownership();
                        return Ok(Some(Search::RowidEq {
                            cmp_expr: WhereTerm {
                                expr: rhs_owned,
                                from_outer_join: cond.from_outer_join,
                                eval_at_loop: cond.eval_at_loop,
                            },
                        }));
                    }
                    ast::Operator::Greater
                    | ast::Operator::GreaterEquals
                    | ast::Operator::Less
                    | ast::Operator::LessEquals => {
                        let rhs_owned = rhs.take_ownership();
                        return Ok(Some(Search::RowidSearch {
                            cmp_op: *operator,
                            cmp_expr: WhereTerm {
                                expr: rhs_owned,
                                from_outer_join: cond.from_outer_join,
                                eval_at_loop: cond.eval_at_loop,
                            },
                        }));
                    }
                    _ => {}
                }
            }

            if rhs.is_rowid_alias_of(table_index) {
                match operator {
                    ast::Operator::Equals => {
                        let lhs_owned = lhs.take_ownership();
                        return Ok(Some(Search::RowidEq {
                            cmp_expr: WhereTerm {
                                expr: lhs_owned,
                                from_outer_join: cond.from_outer_join,
                                eval_at_loop: cond.eval_at_loop,
                            },
                        }));
                    }
                    ast::Operator::Greater
                    | ast::Operator::GreaterEquals
                    | ast::Operator::Less
                    | ast::Operator::LessEquals => {
                        let lhs_owned = lhs.take_ownership();
                        return Ok(Some(Search::RowidSearch {
                            cmp_op: opposite_cmp_op(*operator),
                            cmp_expr: WhereTerm {
                                expr: lhs_owned,
                                from_outer_join: cond.from_outer_join,
                                eval_at_loop: cond.eval_at_loop,
                            },
                        }));
                    }
                    _ => {}
                }
            }

            if let Some(index_rc) =
                lhs.check_index_scan(table_index, &table_reference, available_indexes)?
            {
                match operator {
                    ast::Operator::Equals
                    | ast::Operator::Greater
                    | ast::Operator::GreaterEquals
                    | ast::Operator::Less
                    | ast::Operator::LessEquals => {
                        let rhs_owned = rhs.take_ownership();
                        return Ok(Some(Search::IndexSearch {
                            index: index_rc,
                            cmp_op: *operator,
                            cmp_expr: WhereTerm {
                                expr: rhs_owned,
                                from_outer_join: cond.from_outer_join,
                                eval_at_loop: cond.eval_at_loop,
                            },
                        }));
                    }
                    _ => {}
                }
            }

            if let Some(index_rc) =
                rhs.check_index_scan(table_index, &table_reference, available_indexes)?
            {
                match operator {
                    ast::Operator::Equals
                    | ast::Operator::Greater
                    | ast::Operator::GreaterEquals
                    | ast::Operator::Less
                    | ast::Operator::LessEquals => {
                        let lhs_owned = lhs.take_ownership();
                        return Ok(Some(Search::IndexSearch {
                            index: index_rc,
                            cmp_op: opposite_cmp_op(*operator),
                            cmp_expr: WhereTerm {
                                expr: lhs_owned,
                                from_outer_join: cond.from_outer_join,
                                eval_at_loop: cond.eval_at_loop,
                            },
                        }));
                    }
                    _ => {}
                }
            }

            Ok(None)
        }
        _ => Ok(None),
    }
}

fn rewrite_expr(expr: &mut ast::Expr) -> Result<()> {
    match expr {
        ast::Expr::Id(id) => {
            // Convert "true" and "false" to 1 and 0
            if id.0.eq_ignore_ascii_case("true") {
                *expr = ast::Expr::Literal(ast::Literal::Numeric(1.to_string()));
                return Ok(());
            }
            if id.0.eq_ignore_ascii_case("false") {
                *expr = ast::Expr::Literal(ast::Literal::Numeric(0.to_string()));
                return Ok(());
            }
            Ok(())
        }
        ast::Expr::Between {
            lhs,
            not,
            start,
            end,
        } => {
            // Convert `y NOT BETWEEN x AND z` to `x > y OR y > z`
            let (lower_op, upper_op) = if *not {
                (ast::Operator::Greater, ast::Operator::Greater)
            } else {
                // Convert `y BETWEEN x AND z` to `x <= y AND y <= z`
                (ast::Operator::LessEquals, ast::Operator::LessEquals)
            };

            rewrite_expr(start)?;
            rewrite_expr(lhs)?;
            rewrite_expr(end)?;

            let start = start.take_ownership();
            let lhs = lhs.take_ownership();
            let end = end.take_ownership();

            let lower_bound = ast::Expr::Binary(Box::new(start), lower_op, Box::new(lhs.clone()));
            let upper_bound = ast::Expr::Binary(Box::new(lhs), upper_op, Box::new(end));

            if *not {
                *expr = ast::Expr::Binary(
                    Box::new(lower_bound),
                    ast::Operator::Or,
                    Box::new(upper_bound),
                );
            } else {
                *expr = ast::Expr::Binary(
                    Box::new(lower_bound),
                    ast::Operator::And,
                    Box::new(upper_bound),
                );
            }
            Ok(())
        }
        ast::Expr::Parenthesized(ref mut exprs) => {
            for subexpr in exprs.iter_mut() {
                rewrite_expr(subexpr)?;
            }
            let exprs = std::mem::take(exprs);
            *expr = ast::Expr::Parenthesized(exprs);
            Ok(())
        }
        // Process other expressions recursively
        ast::Expr::Binary(lhs, _, rhs) => {
            rewrite_expr(lhs)?;
            rewrite_expr(rhs)?;
            Ok(())
        }
        ast::Expr::FunctionCall { args, .. } => {
            if let Some(args) = args {
                for arg in args.iter_mut() {
                    rewrite_expr(arg)?;
                }
            }
            Ok(())
        }
        ast::Expr::Unary(_, arg) => {
            rewrite_expr(arg)?;
            Ok(())
        }
        _ => Ok(()),
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
