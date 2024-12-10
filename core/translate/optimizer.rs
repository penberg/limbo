use std::rc::Rc;

use sqlite3_parser::ast;

use crate::{schema::Index, Result};

use super::plan::{
    get_table_ref_bitmask_for_ast_expr, get_table_ref_bitmask_for_operator, BTreeTableReference,
    Direction, IterationDirection, Plan, Search, SourceOperator,
};

/**
 * Make a few passes over the plan to optimize it.
 * TODO: these could probably be done in less passes,
 * but having them separate makes them easier to understand
 */
pub fn optimize_plan(mut select_plan: Plan) -> Result<Plan> {
    push_predicates(
        &mut select_plan.source,
        &mut select_plan.where_clause,
        &select_plan.referenced_tables,
    )?;
    eliminate_constants(&mut select_plan.source)?;
    use_indexes(
        &mut select_plan.source,
        &select_plan.referenced_tables,
        &select_plan.available_indexes,
    )?;
    eliminate_unnecessary_orderby(
        &mut select_plan.source,
        &mut select_plan.order_by,
        &select_plan.referenced_tables,
        &select_plan.available_indexes,
    )?;
    Ok(select_plan)
}

fn _operator_is_already_ordered_by(
    operator: &mut SourceOperator,
    key: &mut ast::Expr,
    referenced_tables: &[BTreeTableReference],
    available_indexes: &Vec<Rc<Index>>,
) -> Result<bool> {
    match operator {
        SourceOperator::Scan {
            table_reference, ..
        } => Ok(key.is_rowid_alias_of(table_reference.table_index)),
        SourceOperator::Search {
            table_reference,
            search,
            ..
        } => match search {
            Search::PrimaryKeyEq { .. } => Ok(key.is_rowid_alias_of(table_reference.table_index)),
            Search::PrimaryKeySearch { .. } => {
                Ok(key.is_rowid_alias_of(table_reference.table_index))
            }
            Search::IndexSearch { index, .. } => {
                let index_idx = key.check_index_scan(
                    table_reference.table_index,
                    referenced_tables,
                    available_indexes,
                )?;
                let index_is_the_same = index_idx
                    .map(|i| Rc::ptr_eq(&available_indexes[i], index))
                    .unwrap_or(false);
                Ok(index_is_the_same)
            }
        },
        SourceOperator::Join { left, .. } => {
            _operator_is_already_ordered_by(left, key, referenced_tables, available_indexes)
        }
        _ => Ok(false),
    }
}

fn eliminate_unnecessary_orderby(
    operator: &mut SourceOperator,
    order_by: &mut Option<Vec<(ast::Expr, Direction)>>,
    referenced_tables: &[BTreeTableReference],
    available_indexes: &Vec<Rc<Index>>,
) -> Result<()> {
    if order_by.is_none() {
        return Ok(());
    }

    let o = order_by.as_mut().unwrap();

    if o.len() != 1 {
        // TODO: handle multiple order by keys
        return Ok(());
    }

    let (key, direction) = o.first_mut().unwrap();

    let already_ordered =
        _operator_is_already_ordered_by(operator, key, referenced_tables, available_indexes)?;

    if already_ordered {
        push_scan_direction(operator, direction);
        *order_by = None;
    }

    Ok(())
}

/**
 * Use indexes where possible
 */
fn use_indexes(
    operator: &mut SourceOperator,
    referenced_tables: &[BTreeTableReference],
    available_indexes: &[Rc<Index>],
) -> Result<()> {
    match operator {
        SourceOperator::Search { .. } => Ok(()),
        SourceOperator::Scan {
            table_reference,
            predicates: filter,
            id,
            ..
        } => {
            if filter.is_none() {
                return Ok(());
            }

            let fs = filter.as_mut().unwrap();
            for i in 0..fs.len() {
                let f = fs[i].take_ownership();
                let table_index = referenced_tables
                    .iter()
                    .position(|t| {
                        Rc::ptr_eq(&t.table, &table_reference.table)
                            && t.table_identifier == table_reference.table_identifier
                    })
                    .unwrap();
                match try_extract_index_search_expression(
                    f,
                    table_index,
                    referenced_tables,
                    available_indexes,
                )? {
                    Either::Left(non_index_using_expr) => {
                        fs[i] = non_index_using_expr;
                    }
                    Either::Right(index_search) => {
                        fs.remove(i);
                        *operator = SourceOperator::Search {
                            id: *id,
                            table_reference: table_reference.clone(),
                            predicates: Some(fs.clone()),
                            search: index_search,
                        };

                        return Ok(());
                    }
                }
            }

            Ok(())
        }
        SourceOperator::Join { left, right, .. } => {
            use_indexes(left, referenced_tables, available_indexes)?;
            use_indexes(right, referenced_tables, available_indexes)?;
            Ok(())
        }
        SourceOperator::Nothing => Ok(()),
    }
}

#[derive(Debug, PartialEq, Clone)]
enum ConstantConditionEliminationResult {
    Continue,
    ImpossibleCondition,
}

// removes predicates that are always true
// returns a ConstantEliminationResult indicating whether any predicates are always false
fn eliminate_constants(
    operator: &mut SourceOperator,
) -> Result<ConstantConditionEliminationResult> {
    match operator {
        SourceOperator::Join {
            left,
            right,
            predicates,
            outer,
            ..
        } => {
            if eliminate_constants(left)? == ConstantConditionEliminationResult::ImpossibleCondition
            {
                return Ok(ConstantConditionEliminationResult::ImpossibleCondition);
            }
            if eliminate_constants(right)?
                == ConstantConditionEliminationResult::ImpossibleCondition
                && !*outer
            {
                return Ok(ConstantConditionEliminationResult::ImpossibleCondition);
            }

            if predicates.is_none() {
                return Ok(ConstantConditionEliminationResult::Continue);
            }

            let predicates = predicates.as_mut().unwrap();

            let mut i = 0;
            while i < predicates.len() {
                let predicate = &predicates[i];
                if predicate.is_always_true()? {
                    predicates.remove(i);
                } else if predicate.is_always_false()? && !*outer {
                    return Ok(ConstantConditionEliminationResult::ImpossibleCondition);
                } else {
                    i += 1;
                }
            }

            Ok(ConstantConditionEliminationResult::Continue)
        }
        SourceOperator::Scan { predicates, .. } => {
            if let Some(ps) = predicates {
                let mut i = 0;
                while i < ps.len() {
                    let predicate = &ps[i];
                    if predicate.is_always_true()? {
                        ps.remove(i);
                    } else if predicate.is_always_false()? {
                        return Ok(ConstantConditionEliminationResult::ImpossibleCondition);
                    } else {
                        i += 1;
                    }
                }

                if ps.is_empty() {
                    *predicates = None;
                }
            }
            Ok(ConstantConditionEliminationResult::Continue)
        }
        SourceOperator::Search { predicates, .. } => {
            if let Some(predicates) = predicates {
                let mut i = 0;
                while i < predicates.len() {
                    let predicate = &predicates[i];
                    if predicate.is_always_true()? {
                        predicates.remove(i);
                    } else if predicate.is_always_false()? {
                        return Ok(ConstantConditionEliminationResult::ImpossibleCondition);
                    } else {
                        i += 1;
                    }
                }
            }

            Ok(ConstantConditionEliminationResult::Continue)
        }
        SourceOperator::Nothing => Ok(ConstantConditionEliminationResult::Continue),
    }
}

/**
  Recursively pushes predicates down the tree, as far as possible.
*/
fn push_predicates(
    operator: &mut SourceOperator,
    where_clause: &mut Option<Vec<ast::Expr>>,
    referenced_tables: &Vec<BTreeTableReference>,
) -> Result<()> {
    if let Some(predicates) = where_clause {
        let mut i = 0;
        while i < predicates.len() {
            let predicate = predicates[i].take_ownership();
            let Some(predicate) = push_predicate(operator, predicate, referenced_tables)? else {
                predicates.remove(i);
                continue;
            };
            predicates[i] = predicate;
            i += 1;
        }
        if predicates.is_empty() {
            *where_clause = None;
        }
    }
    match operator {
        SourceOperator::Join {
            left,
            right,
            predicates,
            outer,
            ..
        } => {
            push_predicates(left, where_clause, referenced_tables)?;
            push_predicates(right, where_clause, referenced_tables)?;

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

            Ok(())
        }
        SourceOperator::Scan { .. } => Ok(()),
        SourceOperator::Search { .. } => Ok(()),
        SourceOperator::Nothing => Ok(()),
    }
}

/**
  Push a single predicate down the tree, as far as possible.
  Returns Ok(None) if the predicate was pushed, otherwise returns itself as Ok(Some(predicate))
*/
fn push_predicate(
    operator: &mut SourceOperator,
    predicate: ast::Expr,
    referenced_tables: &Vec<BTreeTableReference>,
) -> Result<Option<ast::Expr>> {
    match operator {
        SourceOperator::Scan {
            predicates,
            table_reference,
            ..
        } => {
            let table_index = referenced_tables
                .iter()
                .position(|t| t.table_identifier == table_reference.table_identifier)
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

            Ok(None)
        }
        SourceOperator::Search { .. } => Ok(Some(predicate)),
        SourceOperator::Join {
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

            Ok(None)
        }
        SourceOperator::Nothing => Ok(Some(predicate)),
    }
}

fn push_scan_direction(operator: &mut SourceOperator, direction: &Direction) {
    match operator {
        SourceOperator::Scan { iter_dir, .. } => {
            if iter_dir.is_none() {
                match direction {
                    Direction::Ascending => *iter_dir = Some(IterationDirection::Forwards),
                    Direction::Descending => *iter_dir = Some(IterationDirection::Backwards),
                }
            }
        }
        _ => todo!(),
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
    fn is_rowid_alias_of(&self, table_index: usize) -> bool;
    fn check_index_scan(
        &mut self,
        table_index: usize,
        referenced_tables: &[BTreeTableReference],
        available_indexes: &[Rc<Index>],
    ) -> Result<Option<usize>>;
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
        referenced_tables: &[BTreeTableReference],
        available_indexes: &[Rc<Index>],
    ) -> Result<Option<usize>> {
        match self {
            Self::Column { table, column, .. } => {
                for (idx, index) in available_indexes.iter().enumerate() {
                    if index.table_name == referenced_tables[*table].table.name {
                        let column = referenced_tables[*table]
                            .table
                            .columns
                            .get(*column)
                            .unwrap();
                        if index.columns.first().unwrap().name == column.name {
                            return Ok(Some(idx));
                        }
                    }
                }
                Ok(None)
            }
            Self::Binary(lhs, op, rhs) => {
                let lhs_index =
                    lhs.check_index_scan(table_index, referenced_tables, available_indexes)?;
                if lhs_index.is_some() {
                    return Ok(lhs_index);
                }
                let rhs_index =
                    rhs.check_index_scan(table_index, referenced_tables, available_indexes)?;
                if rhs_index.is_some() {
                    // swap lhs and rhs
                    let lhs_new = rhs.take_ownership();
                    let rhs_new = lhs.take_ownership();
                    *self = Self::Binary(Box::new(lhs_new), *op, Box::new(rhs_new));
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

pub enum Either<T, U> {
    Left(T),
    Right(U),
}

pub fn try_extract_index_search_expression(
    expr: ast::Expr,
    table_index: usize,
    referenced_tables: &[BTreeTableReference],
    available_indexes: &[Rc<Index>],
) -> Result<Either<ast::Expr, Search>> {
    match expr {
        ast::Expr::Binary(mut lhs, operator, mut rhs) => {
            if lhs.is_rowid_alias_of(table_index) {
                match operator {
                    ast::Operator::Equals => {
                        return Ok(Either::Right(Search::PrimaryKeyEq { cmp_expr: *rhs }));
                    }
                    ast::Operator::Greater
                    | ast::Operator::GreaterEquals
                    | ast::Operator::Less
                    | ast::Operator::LessEquals => {
                        return Ok(Either::Right(Search::PrimaryKeySearch {
                            cmp_op: operator,
                            cmp_expr: *rhs,
                        }));
                    }
                    _ => {}
                }
            }

            if rhs.is_rowid_alias_of(table_index) {
                match operator {
                    ast::Operator::Equals => {
                        return Ok(Either::Right(Search::PrimaryKeyEq { cmp_expr: *lhs }));
                    }
                    ast::Operator::Greater
                    | ast::Operator::GreaterEquals
                    | ast::Operator::Less
                    | ast::Operator::LessEquals => {
                        return Ok(Either::Right(Search::PrimaryKeySearch {
                            cmp_op: operator,
                            cmp_expr: *lhs,
                        }));
                    }
                    _ => {}
                }
            }

            if let Some(index_index) =
                lhs.check_index_scan(table_index, referenced_tables, available_indexes)?
            {
                match operator {
                    ast::Operator::Equals
                    | ast::Operator::Greater
                    | ast::Operator::GreaterEquals
                    | ast::Operator::Less
                    | ast::Operator::LessEquals => {
                        return Ok(Either::Right(Search::IndexSearch {
                            index: available_indexes[index_index].clone(),
                            cmp_op: operator,
                            cmp_expr: *rhs,
                        }));
                    }
                    _ => {}
                }
            }

            if let Some(index_index) =
                rhs.check_index_scan(table_index, referenced_tables, available_indexes)?
            {
                match operator {
                    ast::Operator::Equals
                    | ast::Operator::Greater
                    | ast::Operator::GreaterEquals
                    | ast::Operator::Less
                    | ast::Operator::LessEquals => {
                        return Ok(Either::Right(Search::IndexSearch {
                            index: available_indexes[index_index].clone(),
                            cmp_op: operator,
                            cmp_expr: *lhs,
                        }));
                    }
                    _ => {}
                }
            }

            Ok(Either::Left(ast::Expr::Binary(lhs, operator, rhs)))
        }
        _ => Ok(Either::Left(expr)),
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

impl TakeOwnership for SourceOperator {
    fn take_ownership(&mut self) -> Self {
        std::mem::replace(self, SourceOperator::Nothing)
    }
}
