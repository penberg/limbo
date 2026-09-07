use crate::sync::Arc;
use crate::{turso_assert, turso_assert_greater_than_or_equal};

use super::plan::NamedWindowBound;
use super::{
    expr::{walk_expr, walk_expr_mut},
    plan::{
        Aggregate, ColumnMask, ColumnUsedMask, Distinctness, EvalAt, IterationDirection, JoinInfo,
        JoinOrderMember, JoinType as PlanJoinType, JoinedTable, Operation, OuterQueryReference,
        Plan, QueryDestination, ResultSetColumn, Scan, TableReferences, WhereTerm,
    },
    select::{prepare_select_plan, prepare_select_plan_from_arms},
};
use crate::translate::plan::BitSet;
use crate::translate::{
    emitter::Resolver,
    expr::{
        expr_contains_nondeterministic_scalar_function, expr_vector_size, unwrap_parens,
        BindingBehavior, WalkControl,
    },
    plan::{NonFromClauseSubquery, SubqueryState},
};
use crate::{
    ast::Limit,
    function::Func,
    schema::Table,
    util::{exprs_are_equivalent, normalize_ident},
    Result,
};
use crate::{
    function::{AccumulatorFunc, AggFunc, ExtFunc, WindowFunc},
    translate::expr::bind_and_rewrite_expr,
};
use crate::{
    translate::plan::{Window, WindowFunction},
    vdbe::builder::ProgramBuilder,
};
use smallvec::SmallVec;
use turso_parser::ast::{
    self, As, Expr, FromClause, JoinType, Materialized, Over, QualifiedName, Select,
    TableInternalId, With,
};

/// Data needed to plan each reference to a CTE separately.
///
/// Separate plans prevent two references from accidentally sharing table IDs
/// or cursor IDs unless the CTE is deliberately materialized once.
struct CteDefinition {
    /// Identifies materialized results shared by references to this CTE.
    cte_id: usize,
    /// Normalized CTE name.
    name: String,
    /// SELECT syntax used to build a plan for each reference.
    select: Select,
    /// Explicit column names from `WITH t(a, b) AS (...)`.
    explicit_columns: Vec<String>,
    /// Other CTE definitions referenced by this CTE.
    referenced_cte_indices: SmallVec<[usize; 2]>,
    /// True when `AS MATERIALIZED` requires one stored result.
    materialize_hint: bool,
    /// The CTE body contains a reference to its own name.
    references_itself: bool,
}

fn collect_cte_definitions(with: With, program: &mut ProgramBuilder) -> Result<Vec<CteDefinition>> {
    let mut definitions = Vec::with_capacity(with.ctes.len());
    let mut referenced_table_names_by_cte = Vec::with_capacity(with.ctes.len());

    for cte in with.ctes {
        let name = normalize_ident(cte.tbl_name.as_str());
        if definitions
            .iter()
            .any(|definition: &CteDefinition| definition.name == name)
        {
            crate::bail_parse_error!("duplicate WITH table name: {}", cte.tbl_name.as_str());
        }

        let mut referenced_table_names = Vec::new();
        collect_from_clause_table_refs(&cte.select, &mut referenced_table_names);
        let references_itself = RecursiveRefCounter { cte_name: &name }
            .count_select(&cte.select, &mut RecursiveRefScope::new())
            > 0;
        referenced_table_names_by_cte.push(referenced_table_names);
        definitions.push(CteDefinition {
            cte_id: program.alloc_cte_id(),
            name,
            select: cte.select,
            explicit_columns: cte
                .columns
                .iter()
                .map(|column| normalize_ident(column.col_name.as_str()))
                .collect(),
            referenced_cte_indices: SmallVec::new(),
            materialize_hint: cte.materialized == Materialized::Yes,
            references_itself,
        });
    }

    for (cte_index, referenced_table_names) in referenced_table_names_by_cte.iter().enumerate() {
        definitions[cte_index].referenced_cte_indices = definitions
            .iter()
            .enumerate()
            .filter(|(candidate_index, definition)| {
                *candidate_index != cte_index && referenced_table_names.contains(&definition.name)
            })
            .map(|(candidate_index, _)| candidate_index)
            .collect();
    }
    Ok(definitions)
}

/// Collect all table names referenced in a SELECT's FROM clause.
/// Used to determine which earlier CTEs a CTE directly depends on.
fn collect_from_clause_table_refs(select: &Select, out: &mut Vec<String>) {
    collect_from_select_body(&select.body, out);
    collect_subquery_table_refs_in_select_exprs(select, out);
}

fn collect_from_select_body(body: &ast::SelectBody, out: &mut Vec<String>) {
    collect_from_one_select(&body.select, out);
    for compound in &body.compounds {
        collect_from_one_select(&compound.select, out);
    }
}

fn collect_from_one_select(one: &ast::OneSelect, out: &mut Vec<String>) {
    match one {
        ast::OneSelect::Select { from, .. } => {
            if let Some(from_clause) = from {
                collect_from_select_table(&from_clause.select, out);
                for join in &from_clause.joins {
                    collect_from_select_table(&join.table, out);
                }
            }
        }
        ast::OneSelect::Values(_) => {}
    }
}

/// Counts references to a recursive CTE's own name the way SQLite's name
/// resolution does:
/// - a nested `WITH` that redefines the name shadows it for that subtree, and
/// - references inside a nested CTE body only count when that CTE is itself
///   referenced (an unused nested CTE that mentions the recursive table does
///   not make the query recursive), weighted by how many references its body
///   contains.
struct RecursiveRefCounter<'a> {
    cte_name: &'a str,
}

/// Names visible at the current point, innermost last. Each entry carries the
/// number of recursive references that using the name implies: 0 for a name
/// that shadows the recursive table, and the body's own reference count for
/// any other nested CTE.
type RecursiveRefScope = Vec<(String, usize)>;

impl RecursiveRefCounter<'_> {
    /// The number of recursive references implied by referring to `name`.
    fn name_weight(&self, name: &str, scope: &RecursiveRefScope) -> usize {
        for (scope_name, weight) in scope.iter().rev() {
            if scope_name == name {
                return *weight;
            }
        }
        usize::from(name == self.cte_name)
    }

    /// Brings the CTEs of a nested `WITH` into scope with their reference
    /// weights. The caller is responsible for truncating `scope` afterwards.
    fn push_nested_ctes(&self, with: Option<&With>, scope: &mut RecursiveRefScope) {
        let Some(with) = with else {
            return;
        };
        for cte in &with.ctes {
            let name = normalize_ident(cte.tbl_name.as_str());
            // The CTE's own name is visible inside its body, where it refers
            // to the nested CTE itself rather than the recursive table.
            scope.push((name, 0));
            let weight = self.count_select(&cte.select, scope);
            scope.last_mut().expect("scope entry pushed above").1 = weight;
        }
    }

    fn count_select(&self, select: &Select, scope: &mut RecursiveRefScope) -> usize {
        let scope_base = scope.len();
        self.push_nested_ctes(select.with.as_ref(), scope);
        let mut count = self.count_one_select(&select.body.select, scope);
        for compound in &select.body.compounds {
            count += self.count_one_select(&compound.select, scope);
        }
        for sorted in &select.order_by {
            count += self.count_expr(&sorted.expr, scope);
        }
        if let Some(limit) = &select.limit {
            count += self.count_expr(&limit.expr, scope);
            if let Some(offset) = &limit.offset {
                count += self.count_expr(offset, scope);
            }
        }
        scope.truncate(scope_base);
        count
    }

    fn count_one_select(&self, one: &ast::OneSelect, scope: &mut RecursiveRefScope) -> usize {
        match one {
            ast::OneSelect::Select {
                columns,
                from,
                where_clause,
                group_by,
                window_clause,
                ..
            } => {
                let mut count = 0;
                if let Some(from) = from {
                    count += self.count_from_table(&from.select, scope);
                    for join in &from.joins {
                        count += self.count_from_table(&join.table, scope);
                        if let Some(ast::JoinConstraint::On(expr)) = &join.constraint {
                            count += self.count_expr(expr, scope);
                        }
                    }
                }
                for column in columns {
                    if let ast::ResultColumn::Expr(expr, _) = column {
                        count += self.count_expr(expr, scope);
                    }
                }
                if let Some(expr) = where_clause {
                    count += self.count_expr(expr, scope);
                }
                if let Some(group_by) = group_by {
                    for expr in &group_by.exprs {
                        count += self.count_expr(expr, scope);
                    }
                    if let Some(having) = &group_by.having {
                        count += self.count_expr(having, scope);
                    }
                }
                for window_def in window_clause {
                    count += self.count_window(&window_def.window, scope);
                }
                count
            }
            ast::OneSelect::Values(rows) => rows
                .iter()
                .flatten()
                .map(|expr| self.count_expr(expr, scope))
                .sum(),
        }
    }

    fn count_from_table(&self, table: &ast::SelectTable, scope: &mut RecursiveRefScope) -> usize {
        match table {
            ast::SelectTable::Table(name, _, _) => {
                if name.db_name.is_none() {
                    self.name_weight(&normalize_ident(name.name.as_str()), scope)
                } else {
                    0
                }
            }
            ast::SelectTable::TableCall(name, args, _) => {
                let mut count = if name.db_name.is_none() {
                    self.name_weight(&normalize_ident(name.name.as_str()), scope)
                } else {
                    0
                };
                for arg in args {
                    count += self.count_expr(arg, scope);
                }
                count
            }
            ast::SelectTable::Select(subselect, _) => self.count_select(subselect, scope),
            ast::SelectTable::Sub(from, _) => {
                let mut count = self.count_from_table(&from.select, scope);
                for join in &from.joins {
                    count += self.count_from_table(&join.table, scope);
                    if let Some(ast::JoinConstraint::On(expr)) = &join.constraint {
                        count += self.count_expr(expr, scope);
                    }
                }
                count
            }
        }
    }

    fn count_window(&self, window: &ast::Window, scope: &mut RecursiveRefScope) -> usize {
        let mut count = 0;
        for expr in &window.partition_by {
            count += self.count_expr(expr, scope);
        }
        for sorted in &window.order_by {
            count += self.count_expr(&sorted.expr, scope);
        }
        if let Some(frame_clause) = &window.frame_clause {
            for bound in std::iter::once(&frame_clause.start).chain(frame_clause.end.as_ref()) {
                if let ast::FrameBound::Following(expr) | ast::FrameBound::Preceding(expr) = bound {
                    count += self.count_expr(expr, scope);
                }
            }
        }
        count
    }

    fn count_expr(&self, expr: &Expr, scope: &mut RecursiveRefScope) -> usize {
        let mut count = 0;
        let _ = walk_expr(expr, &mut |node: &Expr| -> Result<WalkControl> {
            match node {
                Expr::Exists(select) | Expr::Subquery(select) => {
                    count += self.count_select(select, scope);
                    Ok(WalkControl::SkipChildren)
                }
                Expr::InSelect { rhs, .. } => {
                    count += self.count_select(rhs, scope);
                    // The walker does not descend into the subquery, only the
                    // left-hand side expression.
                    Ok(WalkControl::Continue)
                }
                _ => Ok(WalkControl::Continue),
            }
        });
        count
    }

    /// Returns `(top_level_from_count, total_count)` for one arm of a
    /// recursive CTE body: direct references to the recursive table in the
    /// arm's FROM clause, and all references reachable from the arm.
    fn count_arm(&self, one: &ast::OneSelect, scope: &mut RecursiveRefScope) -> (usize, usize) {
        fn count_direct_in_from_table(
            counter: &RecursiveRefCounter,
            table: &ast::SelectTable,
            scope: &RecursiveRefScope,
        ) -> usize {
            match table {
                ast::SelectTable::Table(name, _, _) | ast::SelectTable::TableCall(name, _, _) => {
                    if name.db_name.is_some() {
                        return 0;
                    }
                    let name = normalize_ident(name.name.as_str());
                    // A direct reference only counts when nothing shadows the
                    // recursive table's name.
                    usize::from(
                        name == counter.cte_name
                            && !scope.iter().any(|(scope_name, _)| *scope_name == name),
                    )
                }
                ast::SelectTable::Select(_, _) => 0,
                ast::SelectTable::Sub(from, _) => {
                    count_direct_in_from_table(counter, &from.select, scope)
                        + from
                            .joins
                            .iter()
                            .map(|join| count_direct_in_from_table(counter, &join.table, scope))
                            .sum::<usize>()
                }
            }
        }

        let top_level_from_count = if let ast::OneSelect::Select {
            from: Some(from), ..
        } = one
        {
            count_direct_in_from_table(self, &from.select, scope)
                + from
                    .joins
                    .iter()
                    .map(|join| count_direct_in_from_table(self, &join.table, scope))
                    .sum::<usize>()
        } else {
            0
        };
        let total_count = self.count_one_select(one, scope);
        (top_level_from_count, total_count)
    }
}

fn collect_from_select_table(table: &ast::SelectTable, out: &mut Vec<String>) {
    match table {
        ast::SelectTable::Table(qualified_name, _, _) => {
            if qualified_name.db_name.is_none() {
                out.push(normalize_ident(qualified_name.name.as_str()));
            }
        }
        ast::SelectTable::TableCall(qualified_name, args, _) => {
            if qualified_name.db_name.is_none() {
                out.push(normalize_ident(qualified_name.name.as_str()));
            }
            for arg in args {
                collect_subquery_table_refs_in_expr(arg, out);
            }
        }
        ast::SelectTable::Select(subselect, _) => {
            collect_from_clause_table_refs(subselect, out);
        }
        ast::SelectTable::Sub(from_clause, _) => {
            collect_from_select_table(&from_clause.select, out);
            for join in &from_clause.joins {
                collect_from_select_table(&join.table, out);
                if let Some(ast::JoinConstraint::On(expr)) = &join.constraint {
                    collect_subquery_table_refs_in_expr(expr, out);
                }
            }
        }
    }
}

/// Collect table references from subqueries embedded in expressions.
fn collect_subquery_table_refs_in_select_exprs(select: &Select, out: &mut Vec<String>) {
    collect_subquery_table_refs_in_one_select(&select.body.select, out);
    for compound in &select.body.compounds {
        collect_subquery_table_refs_in_one_select(&compound.select, out);
    }

    for sorted in &select.order_by {
        collect_subquery_table_refs_in_expr(&sorted.expr, out);
    }

    if let Some(limit) = &select.limit {
        collect_subquery_table_refs_in_expr(&limit.expr, out);
        if let Some(offset) = &limit.offset {
            collect_subquery_table_refs_in_expr(offset, out);
        }
    }
}

fn collect_subquery_table_refs_in_one_select(one: &ast::OneSelect, out: &mut Vec<String>) {
    match one {
        ast::OneSelect::Select {
            columns,
            where_clause,
            group_by,
            ..
        } => {
            for column in columns {
                if let ast::ResultColumn::Expr(expr, _) = column {
                    collect_subquery_table_refs_in_expr(expr, out);
                }
            }
            if let Some(expr) = where_clause {
                collect_subquery_table_refs_in_expr(expr, out);
            }
            if let Some(group_by) = group_by {
                for expr in &group_by.exprs {
                    collect_subquery_table_refs_in_expr(expr, out);
                }
                if let Some(having) = &group_by.having {
                    collect_subquery_table_refs_in_expr(having, out);
                }
            }
            if let ast::OneSelect::Select {
                from: Some(from), ..
            } = one
            {
                for join in &from.joins {
                    if let Some(ast::JoinConstraint::On(expr)) = &join.constraint {
                        collect_subquery_table_refs_in_expr(expr, out);
                    }
                }
            }
        }
        ast::OneSelect::Values(rows) => {
            for row in rows {
                for expr in row {
                    collect_subquery_table_refs_in_expr(expr, out);
                }
            }
        }
    }
}

fn collect_subquery_table_refs_in_expr(expr: &Expr, out: &mut Vec<String>) {
    let _ = walk_expr(expr, &mut |node: &Expr| -> Result<WalkControl> {
        match node {
            Expr::Exists(select) | Expr::Subquery(select) => {
                collect_from_clause_table_refs(select, out);
                Ok(WalkControl::SkipChildren)
            }
            Expr::InSelect { rhs, .. } => {
                collect_from_clause_table_refs(rhs, out);
                Ok(WalkControl::SkipChildren)
            }
            _ => Ok(WalkControl::Continue),
        }
    });
}

/// Valid ways to refer to the rowid of a btree table.
pub const ROWID_STRS: [&str; 3] = ["rowid", "_rowid_", "oid"];

/// This function walks the expression tree and identifies aggregate
/// and window functions.
///
/// # Window functions
/// - If `windows` is `Some`, window functions will be resolved against the
///   provided set of windows or added to it if not present.
/// - If `windows` is `None`, any encountered window function is treated
///   as a misuse and results in a parse error.
///
/// # Aggregates
/// Aggregate functions are always allowed. They are collected in `aggs`.
///
/// # Returns
/// - `Ok(true)` if at least one aggregate function was found.
/// - `Ok(false)` if no aggregates were found.
/// - `Err(..)` if an invalid function usage is detected (e.g., window
///   function encountered while `windows` is `None`).
pub fn resolve_window_and_aggregate_functions(
    top_level_expr: &Expr,
    resolver: &Resolver,
    aggs: &mut Vec<Aggregate>,
    mut windows: Option<&mut Vec<Window>>,
    named_windows: &mut [crate::translate::plan::NamedWindowDef],
) -> Result<bool> {
    let mut contains_aggregates = false;
    walk_expr(top_level_expr, &mut |expr: &Expr| -> Result<WalkControl> {
        match expr {
            Expr::FunctionCall {
                name,
                args,
                distinctness,
                filter_over,
                order_by,
                within_group,
            } => {
                let ordered_set_func = ordered_set_agg_func(&normalize_ident(name.as_str()));

                if !within_group.is_empty() {
                    let new_agg = build_ordered_set_aggregate(
                        name,
                        args,
                        distinctness.as_ref(),
                        order_by,
                        within_group,
                        filter_over,
                        ordered_set_func,
                    )?;
                    add_aggregate_if_not_exists(
                        aggs,
                        expr,
                        &new_agg.args,
                        Distinctness::NonDistinct,
                        new_agg.func,
                        filter_over.filter_clause.as_deref().cloned(),
                    )?;
                    contains_aggregates = true;
                    return Ok(WalkControl::SkipChildren);
                }

                // Ordered-set aggregates are only meaningful with WITHIN GROUP. mode() in
                // particular has no plain-aggregate form, so reject it early with a clear error.
                if matches!(ordered_set_func, Some(AggFunc::Mode)) {
                    crate::bail_parse_error!(
                        "mode() requires a WITHIN GROUP (ORDER BY ...) clause"
                    );
                }

                if !order_by.is_empty() {
                    crate::bail_parse_error!(
                        "ORDER BY clause is not supported yet in aggregate functions"
                    );
                }
                let args_count = args.len();
                let distinctness = Distinctness::from_ast(distinctness.as_ref());

                match Func::resolve_function(name.as_str(), args_count)? {
                    Some(Func::Agg(f)) => {
                        if let Some(over_clause) = filter_over.over_clause.as_ref() {
                            link_with_window(
                                windows.as_deref_mut(),
                                named_windows,
                                resolver,
                                expr,
                                AccumulatorFunc::Agg(f),
                                over_clause,
                                filter_over.filter_clause.as_deref(),
                                distinctness,
                            )?;
                        } else {
                            add_aggregate_if_not_exists(
                                aggs,
                                expr,
                                args,
                                distinctness,
                                f,
                                filter_over.filter_clause.as_deref().cloned(),
                            )?;
                            contains_aggregates = true;
                        }
                        return Ok(WalkControl::SkipChildren);
                    }
                    Some(Func::Window(f)) => {
                        if let Some(over_clause) = filter_over.over_clause.as_ref() {
                            link_with_window(
                                windows.as_deref_mut(),
                                named_windows,
                                resolver,
                                expr,
                                AccumulatorFunc::Window(f),
                                over_clause,
                                filter_over.filter_clause.as_deref(),
                                distinctness,
                            )?;
                        } else {
                            crate::bail_parse_error!("misuse of window function {}()", f);
                        }
                        return Ok(WalkControl::SkipChildren);
                    }
                    None => {
                        if let Some(f) = resolver
                            .symbol_table
                            .resolve_function(name.as_str(), args_count)
                        {
                            let func = AggFunc::External(f.func.clone().into());
                            if let ExtFunc::Aggregate { .. } = f.as_ref().func {
                                if let Some(over_clause) = filter_over.over_clause.as_ref() {
                                    link_with_window(
                                        windows.as_deref_mut(),
                                        named_windows,
                                        resolver,
                                        expr,
                                        AccumulatorFunc::Agg(func),
                                        over_clause,
                                        filter_over.filter_clause.as_deref(),
                                        distinctness,
                                    )?;
                                } else {
                                    add_aggregate_if_not_exists(
                                        aggs,
                                        expr,
                                        args,
                                        distinctness,
                                        func,
                                        filter_over.filter_clause.as_deref().cloned(),
                                    )?;
                                    contains_aggregates = true;
                                }
                                return Ok(WalkControl::SkipChildren);
                            }
                        }
                    }
                    _ => {
                        if filter_over.over_clause.is_some() {
                            crate::bail_parse_error!(
                                "{} may not be used as a window function",
                                name.as_str()
                            );
                        }
                    }
                }
            }
            Expr::FunctionCallStar { name, filter_over } => {
                match Func::resolve_function(name.as_str(), 0)? {
                    Some(Func::Agg(f)) => {
                        if let Some(over_clause) = filter_over.over_clause.as_ref() {
                            link_with_window(
                                windows.as_deref_mut(),
                                named_windows,
                                resolver,
                                expr,
                                AccumulatorFunc::Agg(f),
                                over_clause,
                                filter_over.filter_clause.as_deref(),
                                Distinctness::NonDistinct,
                            )?;
                        } else {
                            add_aggregate_if_not_exists(
                                aggs,
                                expr,
                                &[],
                                Distinctness::NonDistinct,
                                f,
                                filter_over.filter_clause.as_deref().cloned(),
                            )?;
                            contains_aggregates = true;
                        }
                        return Ok(WalkControl::SkipChildren);
                    }
                    Some(Func::Window(f)) => {
                        if let Some(over_clause) = filter_over.over_clause.as_ref() {
                            link_with_window(
                                windows.as_deref_mut(),
                                named_windows,
                                resolver,
                                expr,
                                AccumulatorFunc::Window(f),
                                over_clause,
                                filter_over.filter_clause.as_deref(),
                                Distinctness::NonDistinct,
                            )?;
                        } else {
                            crate::bail_parse_error!("misuse of window function {}()", f);
                        }
                        return Ok(WalkControl::SkipChildren);
                    }
                    Some(func) => {
                        if filter_over.over_clause.is_some() {
                            crate::bail_parse_error!(
                                "{} may not be used as a window function",
                                name.as_str()
                            );
                        }

                        // Check if the function supports (*) syntax using centralized logic
                        if func.supports_star_syntax() {
                            return Ok(WalkControl::Continue);
                        } else {
                            crate::bail_parse_error!(
                                "wrong number of arguments to function {}()",
                                name.as_str()
                            );
                        }
                    }
                    None => {
                        if let Some(f) = resolver.symbol_table.resolve_function(name.as_str(), 0) {
                            let func = AggFunc::External(f.func.clone().into());
                            if let ExtFunc::Aggregate { .. } = f.as_ref().func {
                                if let Some(over_clause) = filter_over.over_clause.as_ref() {
                                    link_with_window(
                                        windows.as_deref_mut(),
                                        named_windows,
                                        resolver,
                                        expr,
                                        AccumulatorFunc::Agg(func),
                                        over_clause,
                                        filter_over.filter_clause.as_deref(),
                                        Distinctness::NonDistinct,
                                    )?;
                                } else {
                                    add_aggregate_if_not_exists(
                                        aggs,
                                        expr,
                                        &[],
                                        Distinctness::NonDistinct,
                                        func,
                                        filter_over.filter_clause.as_deref().cloned(),
                                    )?;
                                    contains_aggregates = true;
                                }
                                return Ok(WalkControl::SkipChildren);
                            }
                        } else {
                            crate::bail_parse_error!("no such function: {}", name.as_str());
                        }
                    }
                }
            }
            _ => {}
        }

        Ok(WalkControl::Continue)
    })?;

    Ok(contains_aggregates)
}

#[allow(clippy::too_many_arguments)]
fn link_with_window(
    windows: Option<&mut Vec<Window>>,
    named_windows: &mut [crate::translate::plan::NamedWindowDef],
    resolver: &Resolver,
    expr: &Expr,
    func: AccumulatorFunc,
    over_clause: &Over,
    filter_clause: Option<&Expr>,
    distinctness: Distinctness,
) -> Result<()> {
    if distinctness.is_distinct() {
        crate::bail_parse_error!("DISTINCT is not supported for window functions");
    }
    // FILTER decides which input rows contribute to a running aggregate, so
    // it is only meaningful for aggregating window functions. Non-aggregate
    // ones (`row_number`/`lag`/`lead`) have nothing to filter.
    if matches!(func, AccumulatorFunc::Window(_)) && filter_clause.is_some() {
        crate::bail_parse_error!("FILTER clause may only be used with aggregate window functions");
    }
    expr_vector_size(expr)?;
    if let Some(windows) = windows {
        // An inline `OVER (base ...)` chains to a named window from the
        // SELECT's WINDOW clause (sqlite3WindowUpdate, window.c:676). Any
        // named window is a candidate here, and a missing base is an error
        // — unlike WINDOW-clause chaining, which only sees earlier defs.
        let over_effective: std::borrow::Cow<'_, ast::Over> = match over_clause {
            ast::Over::Window(window) if window.base.is_some() => std::borrow::Cow::Owned(
                ast::Over::Window(chain_inline_window_base(window, named_windows, windows)?),
            ),
            _ => std::borrow::Cow::Borrowed(over_clause),
        };
        let over_clause: &ast::Over = &over_effective;
        // Pick each function's effective frame (sqlite3WindowUpdate,
        // window.c:687-725):
        // - Built-ins with a coerced frame (rank, row_number, ...) ignore
        //   the user clause entirely — including its validation, so a
        //   user EXCLUDE or unsupported shape is dropped, not rejected.
        // - Everything else honors the user clause, defaulting to
        //   RANGE UNBOUNDED PRECEDING TO CURRENT ROW.
        // Functions whose effective frames differ can't share one buffer
        // pass (window.c:1679), so they land in separate `Window` entries.
        let has_coerced_frame = matches!(
            &func,
            AccumulatorFunc::Window(w) if w.coerced_frame().is_some()
        );
        let user_frame = if has_coerced_frame {
            None
        } else {
            match over_clause {
                ast::Over::Window(window) => crate::translate::plan::validate_frame_clause(
                    &window.frame_clause,
                    window.order_by.len(),
                )?,
                ast::Over::Name(name) => {
                    // Named windows store their FRAME clause on the
                    // `NamedWindowDef`. Look it up and validate the
                    // clause as this function's user_frame.
                    let window_name = normalize_ident(name.as_str());
                    let def = named_windows
                        .iter()
                        .rfind(|d| d.name == window_name)
                        .ok_or_else(|| {
                            crate::LimboError::ParseError(format!("no such window: {window_name}"))
                        })?;
                    // `bound` may already be `None` (taken on an earlier
                    // attachment), but the def's order_by length we
                    // need for validation lives on `bound`. After take
                    // we know a sister resolved Window exists with the
                    // same order_by length.
                    let order_by_len = match def.bound.as_ref() {
                        Some(b) => b.order_by.len(),
                        None => windows
                            .iter()
                            .rfind(|w| w.name.as_ref() == Some(&window_name))
                            .expect("sister Window exists after def bound was taken")
                            .order_by
                            .len(),
                    };
                    crate::translate::plan::validate_frame_clause(
                        &def.user_frame_clause,
                        order_by_len,
                    )?
                }
            }
        };
        let effective_frame = resolve_effective_frame(&func, user_frame)?;
        let window = resolve_window(windows, named_windows, over_clause, effective_frame)?;
        // Two equivalent window expressions can share one `WindowFunction`
        // entry unless they contain nondeterministic calls like `random()`,
        // which SQLite evaluates separately at each SQL occurrence.
        let deduplicate = !expr_contains_nondeterministic_scalar_function(expr, resolver)?;
        if deduplicate
            && window
                .functions
                .iter()
                .any(|f| exprs_are_equivalent(&f.original_expr, expr))
        {
            return Ok(());
        }
        window.functions.push(WindowFunction {
            func,
            original_expr: expr.clone(),
            rewritten: None,
        });
    } else {
        let func_name = match &func {
            AccumulatorFunc::Agg(f) => f.as_str().to_string(),
            AccumulatorFunc::Window(f) => f.to_string(),
        };
        crate::bail_parse_error!("misuse of window function {}()", func_name);
    }
    Ok(())
}

/// Decide the frame a function actually runs with, given any user-written
/// FRAME clause. Mirrors SQLite's `sqlite3WindowUpdate` policy at
/// `window.c:687-725`: the eight listed built-ins (row_number, rank,
/// dense_rank, percent_rank, cume_dist, ntile, lead, lag) ignore the
/// user clause and run with their coerced frame; aggregates and
/// first_value / last_value / nth_value honor whatever the user wrote
/// and default to RANGE UNBOUNDED PRECEDING TO CURRENT ROW.
fn resolve_effective_frame(
    func: &AccumulatorFunc,
    user_frame: Option<crate::translate::plan::Frame>,
) -> Result<crate::translate::plan::Frame> {
    use crate::translate::plan::{Frame, FrameBoundary};
    use turso_parser::ast::FrameMode;

    // Built-ins with a coerced frame ignore the user clause entirely
    // (`Some(_)` from `coerced_frame()`). The rest honor it.
    if let AccumulatorFunc::Window(w) = func {
        if let Some(coerced) = w.coerced_frame() {
            return Ok(coerced);
        }
    }

    let Some(frame) = user_frame else {
        return Ok(Frame {
            mode: FrameMode::Range,
            start: FrameBoundary::UnboundedPreceding,
            end: FrameBoundary::CurrentRow,
            exclude: None,
        });
    };
    // min/max use SQLite's per-function sorted-index strategy.
    // group_concat and json_group_* maintain removable-prefix state in
    // their aggregate payloads, matching their SQLite xInverse behavior.
    // first_value / nth_value don't need xInverse — they're WINDOWFUNCNOOP
    // (window.c:591) and read positionally from csr_app at output time, so
    // any frame shape works as long as the frame-index counters are
    // tracked. last_value's inverse is the frame-row count decrement
    // (window.c:505-522).
    let supports_sliding = matches!(
        func,
        AccumulatorFunc::Agg(
            AggFunc::Sum
                | AggFunc::Total
                | AggFunc::Count
                | AggFunc::Count0
                | AggFunc::Avg
                | AggFunc::Min
                | AggFunc::Max
                | AggFunc::GroupConcat
                | AggFunc::StringAgg,
        ) | AccumulatorFunc::Window(
            WindowFunc::FirstValue | WindowFunc::NthValue | WindowFunc::LastValue
        )
    );
    #[cfg(feature = "json")]
    let supports_sliding = supports_sliding
        || matches!(
            func,
            AccumulatorFunc::Agg(
                AggFunc::JsonGroupObject
                    | AggFunc::JsonbGroupObject
                    | AggFunc::JsonGroupArray
                    | AggFunc::JsonbGroupArray
            )
        );
    // Anything but an UNBOUNDED PRECEDING start makes the frame shrink
    // from the left, firing xInverse.
    let moving_start = !matches!(
        frame.start,
        crate::translate::plan::FrameBoundary::UnboundedPreceding
    );
    if moving_start && frame.exclude.is_none() && !supports_sliding {
        crate::bail_parse_error!(
            "{}() does not yet support window frames with a moving start; \
             use a frame with UNBOUNDED PRECEDING start",
            func.as_str()
        );
    }
    Ok(frame)
}

/// Resolve the `Window` a function call should be attached to, given the
/// function's coerced frame. Two functions can share a `Window` only when
/// their OVER clauses are equivalent AND their coerced frames match —
/// functions with the same OVER but conflicting frames get separate
/// `Window` entries so each compiles to its own ephemeral-table pass.
/// Merge an inline `OVER (base ...)` window spec with its named base
/// window (sqlite3WindowChain, window.c:1276). The spec may not:
/// - have its own PARTITION BY,
/// - add ORDER BY when the base already has one,
/// - chain to a base that carries an explicit frame.
///
/// The result inherits the base's PARTITION BY (and its ORDER BY when the
/// spec has none), keeps its own frame clause, and drops the base ref.
fn chain_inline_window_base(
    window: &ast::Window,
    named_windows: &[crate::translate::plan::NamedWindowDef],
    windows: &[Window],
) -> Result<ast::Window> {
    let base = window.base.as_ref().expect("caller checked base.is_some()");
    let base_name = normalize_ident(base.as_str());
    let Some(base_def) = named_windows.iter().rfind(|d| d.name == base_name) else {
        crate::bail_parse_error!("no such window: {}", base_name);
    };
    if !window.partition_by.is_empty() {
        crate::bail_parse_error!("cannot override PARTITION clause of window: {}", base_name);
    }
    // The base's bound clauses move into the first `Window` attached under
    // its name, so read them from wherever they currently live.
    let (base_partition, base_order) = match base_def.bound.as_ref() {
        Some(bound) => (bound.partition_by.clone(), bound.order_by.clone()),
        None => {
            let sister = windows
                .iter()
                .rfind(|w| w.name.as_deref() == Some(base_name.as_str()))
                .expect("sister Window must exist after the named def's bound was taken");
            (sister.partition_by.clone(), sister.order_by.clone())
        }
    };
    if !base_order.is_empty() && !window.order_by.is_empty() {
        crate::bail_parse_error!("cannot override ORDER BY clause of window: {}", base_name);
    }
    if base_def.user_frame_clause.is_some() {
        crate::bail_parse_error!(
            "cannot override frame specification of window: {}",
            base_name
        );
    }
    let mut merged = window.clone();
    merged.base = None;
    merged.partition_by = base_partition.into_iter().map(Box::new).collect();
    if merged.order_by.is_empty() {
        merged.order_by = base_order
            .into_iter()
            .map(|(expr, order, nulls)| ast::SortedColumn {
                expr: Box::new(expr),
                order: Some(order),
                nulls,
            })
            .collect();
    }
    Ok(merged)
}

fn resolve_window<'a>(
    windows: &'a mut Vec<Window>,
    named_windows: &mut [crate::translate::plan::NamedWindowDef],
    over_clause: &Over,
    frame: crate::translate::plan::Frame,
) -> Result<&'a mut Window> {
    match over_clause {
        Over::Window(window) => {
            if let Some(idx) = windows.iter().position(|w| w.is_equivalent(window, &frame)) {
                return Ok(&mut windows[idx]);
            }
            windows.push(Window::new_unnamed(window, frame)?);
            Ok(windows.last_mut().expect("just pushed, so must exist"))
        }
        Over::Name(name) => {
            let window_name = normalize_ident(name.as_str());
            // Reuse an existing resolved entry with the same name AND
            // frame so functions sharing one coerced frame fold into one
            // ephemeral-table pass. SQLite uses the most recent
            // definition when names collide, so iterate in reverse.
            if let Some(idx) = windows
                .iter()
                .rposition(|w| w.name.as_ref() == Some(&window_name) && w.frame == frame)
            {
                return Ok(&mut windows[idx]);
            }
            // Need a new resolved entry. Verify the name exists.
            let def = named_windows
                .iter_mut()
                .rfind(|d| d.name == window_name)
                .ok_or_else(|| {
                    crate::LimboError::ParseError(format!("no such window: {window_name}"))
                })?;
            // First attachment under this name takes ownership of the
            // bound exprs. Subsequent distinct-frame
            // attachments deep-clone from a sister resolved Window —
            // the first attachment guarantees one exists.
            let bound = match def.bound.take() {
                Some(bound) => bound,
                None => {
                    let sister = windows
                        .iter()
                        .rfind(|w| w.name.as_ref() == Some(&window_name))
                        .expect("sister Window must exist after the named def was taken");
                    NamedWindowBound {
                        partition_by: sister.partition_by.clone(),
                        order_by: sister.order_by.clone(),
                    }
                }
            };
            windows.push(Window::from_named_bound(window_name, bound, frame));
            Ok(windows.last_mut().expect("just pushed, so must exist"))
        }
    }
}

fn add_aggregate_if_not_exists(
    aggs: &mut Vec<Aggregate>,
    expr: &Expr,
    args: &[Box<Expr>],
    distinctness: Distinctness,
    func: AggFunc,
    filter_expr: Option<ast::Expr>,
) -> Result<()> {
    if distinctness.is_distinct() && args.len() != 1 {
        crate::bail_parse_error!("DISTINCT aggregate functions must have exactly one argument");
    }
    if aggs
        .iter()
        .all(|a| !exprs_are_equivalent(&a.original_expr, expr))
    {
        aggs.push(Aggregate::new(func, args, expr, distinctness, filter_expr));
    }
    Ok(())
}

/// Maps a normalized function name to the ordered-set [`AggFunc`] it implements, if any.
/// Ordered-set aggregates are written `f(direct_args) WITHIN GROUP (ORDER BY x)`.
fn ordered_set_agg_func(normalized_name: &str) -> Option<AggFunc> {
    match normalized_name {
        "mode" => Some(AggFunc::Mode),
        "percentile_cont" => Some(AggFunc::PercentileCont),
        "percentile_disc" => Some(AggFunc::PercentileDisc),
        _ => None,
    }
}

struct OrderedSetAggregate {
    func: AggFunc,
    // Matches `add_aggregate_if_not_exists`, which consumes `&[Box<Expr>]`.
    #[allow(clippy::vec_box)]
    args: Vec<Box<Expr>>,
}

/// Validates a `WITHIN GROUP (ORDER BY ...)` ordered-set aggregate and rewrites it into a
/// uniform argument list for the rest of the pipeline: `[value]` for `mode`, or
/// `[value, fraction]` for the percentile functions. `value` is the single ORDER BY
/// expression; `fraction` is the direct argument.
fn build_ordered_set_aggregate(
    name: &ast::Name,
    args: &[Box<Expr>],
    distinctness: Option<&ast::Distinctness>,
    order_by: &[ast::SortedColumn],
    within_group: &[ast::SortedColumn],
    filter_over: &ast::FunctionTail,
    ordered_set_func: Option<AggFunc>,
) -> Result<OrderedSetAggregate> {
    let Some(func) = ordered_set_func else {
        crate::bail_parse_error!(
            "WITHIN GROUP is not supported for function {}()",
            name.as_str()
        );
    };
    if filter_over.over_clause.is_some() {
        crate::bail_parse_error!(
            "ordered-set aggregate {}() may not be used as a window function",
            name.as_str()
        );
    }
    if distinctness.is_some() {
        crate::bail_parse_error!(
            "DISTINCT is not supported for ordered-set aggregate {}()",
            name.as_str()
        );
    }
    if !order_by.is_empty() {
        crate::bail_parse_error!(
            "{}() does not accept an argument ORDER BY together with WITHIN GROUP",
            name.as_str()
        );
    }
    if within_group.len() != 1 {
        crate::bail_parse_error!(
            "WITHIN GROUP for {}() must specify exactly one ORDER BY expression",
            name.as_str()
        );
    }
    let sort_col = &within_group[0];
    if matches!(sort_col.order, Some(ast::SortOrder::Desc)) || sort_col.nulls.is_some() {
        crate::bail_parse_error!(
            "DESC and NULLS ordering inside WITHIN GROUP are not supported yet"
        );
    }
    let expected_direct_args = match func {
        AggFunc::Mode => 0,
        _ => 1, // percentile_cont / percentile_disc take the fraction
    };
    if args.len() != expected_direct_args {
        crate::bail_parse_error!("wrong number of arguments to function {}()", name.as_str());
    }
    let mut new_args: Vec<Box<Expr>> = Vec::with_capacity(args.len() + 1);
    new_args.push(sort_col.expr.clone());
    new_args.extend(args.iter().cloned());
    Ok(OrderedSetAggregate {
        func,
        args: new_args,
    })
}

/// Plans one use of a CTE.
///
/// Each use gets separate table and cursor IDs unless it shares materialized rows.
#[allow(clippy::too_many_arguments)]
fn plan_cte(
    cte_definition_index: usize,
    cte_definitions: &[CteDefinition],
    base_outer_query_refs: &[OuterQueryReference],
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
    validate_explicit_column_count: bool,
) -> Result<JoinedTable> {
    let cte_definition = &cte_definitions[cte_definition_index];
    if program.is_cte_being_defined(&cte_definition.name) {
        crate::bail_parse_error!("circular reference: {}", cte_definition.name);
    }

    // Plan only the other CTEs this definition names. Planning every preceding
    // CTE here would repeatedly plan the same transitive dependencies.
    program.push_cte_being_defined(cte_definition.name.clone());
    let outer_query_refs: Result<Vec<OuterQueryReference>> = (|| {
        let mut outer_query_refs = base_outer_query_refs.to_vec();
        for &referenced_cte_index in &cte_definition.referenced_cte_indices {
            let referenced_cte_name = &cte_definitions[referenced_cte_index].name;
            if outer_query_refs
                .iter()
                .any(|reference| &reference.identifier == referenced_cte_name)
            {
                continue;
            }
            let referenced_cte_table = plan_cte(
                referenced_cte_index,
                cte_definitions,
                base_outer_query_refs,
                resolver,
                program,
                connection,
                false,
            )?;
            outer_query_refs.push(OuterQueryReference {
                identifier: referenced_cte_table.identifier.clone(),
                internal_id: referenced_cte_table.internal_id,
                table: referenced_cte_table.table.clone(),
                using_dedup_hidden_cols: referenced_cte_table.using_dedup_hidden_cols()?,
                col_used_mask: ColumnUsedMask::default(),
                cte_select: None,
                cte_explicit_columns: vec![],
                cte_id: Some(cte_definitions[referenced_cte_index].cte_id),
                // This entry only lets the body's FROM clause find the sibling
                // CTE by name; its columns become visible when a FROM clause
                // actually adds the table.
                cte_definition_only: true,
                rowid_referenced: false,
                scope_depth: 0,
            });
        }
        Ok(outer_query_refs)
    })();
    program.pop_cte_being_defined();
    let outer_query_refs = outer_query_refs?;

    let cte_query_plan = if cte_definition.references_itself {
        prepare_recursive_cte_plan(
            cte_definition,
            resolver,
            program,
            &outer_query_refs,
            connection,
        )?
    } else {
        // A non-recursive CTE cannot read a table or view with the same name.
        // Keep that name hidden while planning so the lookup reports a circular reference.
        program.push_cte_being_defined(cte_definition.name.clone());
        let plan = prepare_select_plan(
            cte_definition.select.clone(),
            resolver,
            program,
            &outer_query_refs,
            QueryDestination::placeholder_for_subquery(),
            connection,
        );
        program.pop_cte_being_defined();
        plan?
    };

    let explicit_columns = if cte_definition.explicit_columns.is_empty() {
        None
    } else {
        Some(cte_definition.explicit_columns.as_slice())
    };

    // SQLite defers explicit column-count validation until the CTE is actually
    // referenced, so preplanned visibility-only copies must not raise here.
    if validate_explicit_column_count {
        if let Some(columns) = explicit_columns {
            let result_column_count = cte_query_plan.select_result_columns().len();
            if columns.len() != result_column_count {
                crate::bail_parse_error!(
                    "table {} has {} columns but {} column names were provided",
                    cte_definition.name,
                    result_column_count,
                    columns.len()
                );
            }
        }
    }

    match cte_query_plan {
        Plan::Select(_) | Plan::CompoundSelect { .. } | Plan::RecursiveCte(_) => {
            JoinedTable::new_subquery_from_plan(
                cte_definition.name.clone(),
                cte_query_plan,
                None,
                program.table_reference_counter.next(),
                explicit_columns,
                Some(cte_definition.cte_id),
                cte_definition.materialize_hint,
            )
        }
        Plan::Delete(_) | Plan::Update(_) => {
            crate::bail_parse_error!("DELETE/UPDATE queries are not supported in CTEs")
        }
    }
}

fn prepare_recursive_cte_plan(
    cte_definition: &CteDefinition,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    outer_query_refs: &[OuterQueryReference],
    connection: &Arc<crate::Connection>,
) -> Result<Plan> {
    let select = &cte_definition.select;
    let mut first_recursive_query_index = None;
    let ref_counter = RecursiveRefCounter {
        cte_name: &cte_definition.name,
    };
    // Nested CTEs defined at the body level are visible in every arm; bring
    // them into scope so shadowing and use-through-nested-CTE references are
    // counted the way name resolution will see them.
    let mut ref_scope = RecursiveRefScope::new();
    ref_counter.push_nested_ctes(select.with.as_ref(), &mut ref_scope);
    for (query_index, query) in std::iter::once(&select.body.select)
        .chain(
            select
                .body
                .compounds
                .iter()
                .map(|compound| &compound.select),
        )
        .enumerate()
    {
        let (top_level_from_count, total_count) = ref_counter.count_arm(query, &mut ref_scope);
        if first_recursive_query_index.is_none() && total_count == 0 {
            continue;
        }
        if query_index == 0 {
            crate::bail_parse_error!("circular reference: {}", cte_definition.name);
        }
        first_recursive_query_index.get_or_insert(query_index);
        if top_level_from_count == 0 {
            crate::bail_parse_error!("circular reference: {}", cte_definition.name);
        }
        if top_level_from_count > 1 {
            crate::bail_parse_error!(
                "multiple references to recursive table: {}",
                cte_definition.name
            );
        }
        if total_count > top_level_from_count {
            crate::bail_parse_error!("multiple recursive references: {}", cte_definition.name);
        }
    }
    let Some(first_recursive_query_index) = first_recursive_query_index else {
        return Err(crate::LimboError::InternalError(format!(
            "recursive CTE {} has no recursive query",
            cte_definition.name
        )));
    };

    let recursive_compound_operator =
        select.body.compounds[first_recursive_query_index - 1].operator;
    let union_all = match recursive_compound_operator {
        ast::CompoundOperator::UnionAll => true,
        ast::CompoundOperator::Union => false,
        ast::CompoundOperator::Except | ast::CompoundOperator::Intersect => {
            crate::bail_parse_error!(
                "recursive CTEs must use UNION ALL or UNION between the initial and recursive queries"
            );
        }
    };
    for compound in select
        .body
        .compounds
        .iter()
        .skip(first_recursive_query_index)
    {
        if compound.operator != recursive_compound_operator {
            crate::bail_parse_error!("recursive CTE queries must use the same UNION operator");
        }
    }

    let initial_query = prepare_select_plan_from_arms(
        select.body.select.clone(),
        select.body.compounds[..first_recursive_query_index - 1]
            .iter()
            .cloned(),
        select.with.clone(),
        vec![],
        None,
        resolver,
        program,
        outer_query_refs,
        QueryDestination::placeholder_for_subquery(),
        connection,
    )?;

    let explicit_columns = (!cte_definition.explicit_columns.is_empty())
        .then_some(cte_definition.explicit_columns.as_slice());
    if let Some(columns) = explicit_columns {
        let result_column_count = initial_query.select_result_columns().len();
        if columns.len() != result_column_count {
            crate::bail_parse_error!(
                "table {} has {} values for {} columns",
                cte_definition.name,
                result_column_count,
                columns.len()
            );
        }
    }

    let input_table = JoinedTable::new_recursive_cte_input(
        cte_definition.name.clone(),
        &initial_query,
        program.table_reference_counter.next(),
        explicit_columns,
    )?;
    let input_table_id = input_table.internal_id;

    let mut recursive_query_outer_refs = outer_query_refs.to_vec();
    recursive_query_outer_refs.push(OuterQueryReference {
        identifier: cte_definition.name.clone(),
        internal_id: input_table.internal_id,
        table: input_table.table,
        using_dedup_hidden_cols: ColumnMask::default(),
        col_used_mask: ColumnUsedMask::default(),
        cte_select: None,
        cte_explicit_columns: cte_definition.explicit_columns.clone(),
        cte_id: None,
        cte_definition_only: false,
        rowid_referenced: false,
        scope_depth: 0,
    });

    let recursive_query = prepare_select_plan_from_arms(
        select.body.compounds[first_recursive_query_index - 1]
            .select
            .clone(),
        select.body.compounds[first_recursive_query_index..]
            .iter()
            .map(|compound| ast::CompoundSelect {
                operator: ast::CompoundOperator::UnionAll,
                select: compound.select.clone(),
            }),
        select.with.clone(),
        vec![],
        None,
        resolver,
        program,
        &recursive_query_outer_refs,
        QueryDestination::placeholder_for_subquery(),
        connection,
    )?;

    if initial_query.select_result_columns().len() != recursive_query.select_result_columns().len()
    {
        crate::bail_parse_error!(
            "SELECTs to the left and right of {} do not have the same number of result columns",
            recursive_compound_operator
        );
    }
    reject_aggregates_and_windows_in_recursive_query(&recursive_query)?;

    let queue_order = super::select::resolve_recursive_cte_queue_order(
        &select.order_by,
        &initial_query,
        &recursive_query,
        resolver,
    )?;
    let (limit, offset) = select
        .limit
        .clone()
        .map_or(Ok((None, None)), |limit| parse_limit(limit, resolver))?;

    Ok(Plan::RecursiveCte(Box::new(
        super::plan::RecursiveCtePlan {
            name: cte_definition.name.clone(),
            initial_query: Box::new(initial_query),
            recursive_query: Box::new(recursive_query),
            input_table_id,
            union_all,
            limit,
            offset,
            queue_order,
            query_destination: QueryDestination::placeholder_for_subquery(),
        },
    )))
}

fn reject_aggregates_and_windows_in_recursive_query(query: &Plan) -> Result<()> {
    match query {
        Plan::Select(select) => {
            if !select.aggregates.is_empty() || select.group_by.is_some() {
                crate::bail_parse_error!("recursive aggregate queries not supported");
            }
            if select.window.is_some() {
                crate::bail_parse_error!("cannot use window functions in recursive queries");
            }
        }
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            if left
                .iter()
                .any(|(select, _)| !select.aggregates.is_empty() || select.group_by.is_some())
                || !right_most.aggregates.is_empty()
                || right_most.group_by.is_some()
            {
                crate::bail_parse_error!("recursive aggregate queries not supported");
            }
            if left.iter().any(|(select, _)| select.window.is_some()) || right_most.window.is_some()
            {
                crate::bail_parse_error!("cannot use window functions in recursive queries");
            }
        }
        Plan::RecursiveCte(_) | Plan::Delete(_) | Plan::Update(_) => {
            return Err(crate::LimboError::InternalError(
                "recursive CTE query is not a SELECT".to_string(),
            ));
        }
    }
    Ok(())
}

/// Plan CTEs from a WITH clause and add them as outer query references.
/// This is used by DML statements (DELETE, UPDATE) to make CTEs available
/// for subqueries in WHERE and SET clauses.
#[turso_macros::trace_stack]
pub fn plan_ctes_as_outer_refs(
    with: Option<With>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    table_references: &mut TableReferences,
    connection: &Arc<crate::Connection>,
) -> Result<()> {
    let Some(with) = with else {
        return Ok(());
    };

    let cte_definitions = collect_cte_definitions(with, program)?;

    let base_outer_query_refs =
        base_outer_refs_for_cte_planning(table_references.outer_query_refs(), &cte_definitions);
    for (cte_definition_index, cte_definition) in cte_definitions.iter().enumerate() {
        let joined_table = plan_cte(
            cte_definition_index,
            &cte_definitions,
            &base_outer_query_refs,
            resolver,
            program,
            connection,
            false,
        )?;
        table_references.add_outer_query_reference(OuterQueryReference {
            identifier: cte_definition.name.clone(),
            internal_id: joined_table.internal_id,
            table: joined_table.table,
            using_dedup_hidden_cols: ColumnMask::default(),
            col_used_mask: ColumnUsedMask::default(),
            cte_select: (!cte_definition.references_itself).then(|| cte_definition.select.clone()),
            cte_explicit_columns: cte_definition.explicit_columns.clone(),
            cte_id: Some(cte_definition.cte_id),
            cte_definition_only: true,
            rowid_referenced: false,
            scope_depth: 0,
        });
    }

    Ok(())
}

fn parse_from_clause_table(
    table: ast::SelectTable,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    table_references: &mut TableReferences,
    vtab_predicates: &mut Vec<Expr>,
    cte_definitions: &[CteDefinition],
    connection: &Arc<crate::Connection>,
) -> Result<()> {
    match table {
        ast::SelectTable::Table(qualified_name, maybe_alias, indexed) => parse_table(
            table_references,
            resolver,
            program,
            cte_definitions,
            vtab_predicates,
            &qualified_name,
            maybe_alias.as_ref(),
            &[],
            indexed,
            connection,
        ),
        ast::SelectTable::Select(subselect, maybe_alias) => {
            // Make the parent's CTEs visible while planning this inline subquery.
            let mut outer_query_refs_for_subquery = table_references.outer_query_refs().to_vec();
            let base_outer_query_refs_for_subquery = base_outer_refs_for_cte_planning(
                table_references.outer_query_refs(),
                cte_definitions,
            );
            for (cte_definition_index, cte_definition) in cte_definitions.iter().enumerate() {
                if outer_query_refs_for_subquery
                    .iter()
                    .any(|reference| reference.identifier == cte_definition.name)
                {
                    continue;
                }
                // This plan only makes the name visible. A later FROM lookup performs
                // the real CTE use and validates its explicit column list.
                let cte_table = plan_cte(
                    cte_definition_index,
                    cte_definitions,
                    &base_outer_query_refs_for_subquery,
                    resolver,
                    program,
                    connection,
                    false,
                )?;
                outer_query_refs_for_subquery.push(OuterQueryReference {
                    identifier: cte_definition.name.clone(),
                    internal_id: cte_table.internal_id,
                    table: cte_table.table,
                    using_dedup_hidden_cols: ColumnMask::default(),
                    col_used_mask: ColumnUsedMask::default(),
                    cte_select: (!cte_definition.references_itself)
                        .then(|| cte_definition.select.clone()),
                    cte_explicit_columns: cte_definition.explicit_columns.clone(),
                    cte_id: Some(cte_definition.cte_id),
                    cte_definition_only: false,
                    rowid_referenced: false,
                    scope_depth: 0,
                });
            }

            let subplan = prepare_select_plan(
                subselect,
                resolver,
                program,
                &outer_query_refs_for_subquery,
                QueryDestination::placeholder_for_subquery(),
                connection,
            )?;
            match &subplan {
                Plan::Select(_) | Plan::CompoundSelect { .. } | Plan::RecursiveCte(_) => {}
                Plan::Delete(_) | Plan::Update(_) => {
                    crate::bail_parse_error!(
                        "DELETE/UPDATE queries are not supported in FROM clause subqueries"
                    );
                }
            }
            let cur_table_index = table_references.joined_tables().len();
            let identifier = maybe_alias
                .map(|a| normalize_ident(a.name().as_str()))
                .unwrap_or_else(|| format!("(subquery-{cur_table_index})"));
            table_references.add_joined_table(JoinedTable::new_subquery_from_plan(
                identifier,
                subplan,
                None,
                program.table_reference_counter.next(),
                None,  // No explicit columns for regular subqueries
                None,  // Regular inline subqueries don't have a CTE identity
                false, // No materialize hint for inline subqueries
            )?);
            Ok(())
        }
        ast::SelectTable::TableCall(qualified_name, args, maybe_alias) => parse_table(
            table_references,
            resolver,
            program,
            cte_definitions,
            vtab_predicates,
            &qualified_name,
            maybe_alias.as_ref(),
            &args,
            None, // table-valued functions don't support INDEXED BY
            connection,
        ),
        ast::SelectTable::Sub(..) => {
            crate::bail_parse_error!("Parenthesized FROM clause subqueries are not supported")
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn parse_table(
    table_references: &mut TableReferences,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    cte_definitions: &[CteDefinition],
    vtab_predicates: &mut Vec<Expr>,
    qualified_name: &QualifiedName,
    maybe_alias: Option<&As>,
    args: &[Box<Expr>],
    indexed: Option<ast::Indexed>,
    connection: &Arc<crate::Connection>,
) -> Result<()> {
    let normalized_qualified_name = normalize_ident(qualified_name.name.as_str());
    let database_id = resolver.resolve_existing_table_database_id_qualified(qualified_name)?;
    let table_name = &qualified_name.name;

    if qualified_name.db_name.is_none() {
        // Each CTE use gets its own table and cursor IDs unless it shares materialized rows.
        if let Some(cte_definition_index) = cte_definitions
            .iter()
            .position(|definition| definition.name == normalized_qualified_name)
        {
            if !args.is_empty() {
                crate::bail_parse_error!("'{}' is not a function", table_name.as_str());
            }
            let planning_outer_query_refs = base_outer_refs_for_cte_planning(
                table_references.outer_query_refs(),
                cte_definitions,
            );
            // Only a real FROM/JOIN use should report an explicit column-count mismatch.
            let mut cte_table = plan_cte(
                cte_definition_index,
                cte_definitions,
                &planning_outer_query_refs,
                resolver,
                program,
                connection,
                true,
            )?;

            // If there's an alias provided, update the identifier to use that alias
            if let Some(a) = maybe_alias {
                cte_table.identifier = normalize_ident(a.name().as_str());
            }

            // Mark the pre-planned outer_query_ref as "CTE definition only" so it is
            // still available for CTE lookup in subquery FROM clauses (e.g.
            // EXISTS (SELECT 1 FROM <cte_name> ...)), but no longer participates in
            // column resolution. Column resolution now goes through the joined_table
            // which has the alias (if any) or the original name.
            table_references.mark_outer_query_ref_cte_definition_only(&normalized_qualified_name);

            table_references.add_joined_table(cte_table);
            return Ok(());
        }

        // A non-recursive CTE cannot read its own name, even when a table or view
        // with that name exists.
        if program.is_cte_being_defined(&normalized_qualified_name) {
            crate::bail_parse_error!("circular reference: {}", table_name.as_str());
        }

        // A CTE can read another CTE defined by the surrounding WITH clause.
        if let Some(outer_ref) =
            table_references.find_outer_query_ref_by_identifier(&normalized_qualified_name)
        {
            if !args.is_empty() {
                if matches!(outer_ref.table, Table::RecursiveCteInput(_)) {
                    // SQLite resolves the recursive self-reference as a plain
                    // table with zero table-valued-function parameters.
                    crate::bail_parse_error!(
                        "too many arguments on {}() - max 0",
                        table_name.as_str()
                    );
                }
                crate::bail_parse_error!("'{}' is not a function", table_name.as_str());
            }
            let alias = maybe_alias.map(|a| normalize_ident(a.name().as_str()));
            let cte_select_syntax = outer_ref.cte_select.clone();
            let cte_explicit_columns = outer_ref.cte_explicit_columns.clone();
            let cte_id = outer_ref.cte_id;
            let outer_table = outer_ref.table.clone();
            let materialize_hint = match &outer_table {
                Table::FromClauseSubquery(subquery) => subquery.materialize_hint(),
                _ => false,
            };

            if let Some(cte_select) = cte_select_syntax {
                // Plan each use separately so two uses do not receive the same
                // table or cursor IDs.
                let cte_query_plan = prepare_select_plan(
                    cte_select,
                    resolver,
                    program,
                    table_references.outer_query_refs(),
                    QueryDestination::placeholder_for_subquery(),
                    connection,
                )?;
                let explicit_columns = if cte_explicit_columns.is_empty() {
                    None
                } else {
                    Some(cte_explicit_columns.as_slice())
                };
                // SQLite reports an explicit column-count mismatch only when the CTE is used.
                if let Some(columns) = explicit_columns {
                    let result_column_count = cte_query_plan.select_result_columns().len();
                    if columns.len() != result_column_count {
                        crate::bail_parse_error!(
                            "table {} has {} columns but {} column names were provided",
                            normalized_qualified_name,
                            result_column_count,
                            columns.len()
                        );
                    }
                }
                let mut joined_table = JoinedTable::new_subquery_from_plan(
                    normalized_qualified_name.clone(),
                    cte_query_plan,
                    None,
                    program.table_reference_counter.next(),
                    explicit_columns,
                    cte_id,
                    materialize_hint,
                )?;
                if let Some(alias) = alias {
                    joined_table.identifier = alias;
                }
                joined_table.database_id = database_id;
                table_references.add_joined_table(joined_table);
            } else {
                // All recursive arms read the same one-row pseudo-cursor.
                let internal_id = if matches!(outer_table, Table::RecursiveCteInput(_)) {
                    outer_ref.internal_id
                } else {
                    program.table_reference_counter.next()
                };
                table_references.add_joined_table(JoinedTable {
                    op: Operation::default_scan_for(&outer_table),
                    table: outer_table,
                    identifier: alias.unwrap_or(normalized_qualified_name),
                    internal_id,
                    join_info: None,
                    col_used_mask: ColumnUsedMask::default(),
                    column_use_counts: Vec::new(),
                    expression_index_usages: Vec::new(),
                    database_id,
                    indexed: None,
                });
            }
            return Ok(());
        }
    }

    // Resolve table using connection's with_schema method
    let table = resolver.with_schema(database_id, |schema| schema.get_table(table_name.as_str()));

    if let Some(table) = table {
        let alias = maybe_alias.map(|a| normalize_ident(a.name().as_str()));
        let internal_id = program.table_reference_counter.next();
        let tbl_ref = if let Table::Virtual(tbl) = table.as_ref() {
            transform_args_into_where_terms(args, internal_id, vtab_predicates, table.as_ref())?;
            Table::Virtual(tbl.clone())
        } else if let Table::BTree(table) = table.as_ref() {
            if !args.is_empty() {
                crate::bail_parse_error!("'{}' is not a function", table_name.as_str());
            }
            Table::BTree(table.clone())
        } else {
            return Err(crate::LimboError::InvalidArgument(
                "Table type not supported".to_string(),
            ));
        };
        table_references.add_joined_table(JoinedTable {
            op: Operation::default_scan_for(&tbl_ref),
            table: tbl_ref,
            identifier: alias.unwrap_or(normalized_qualified_name),
            internal_id,
            join_info: None,
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id,
            indexed,
        });
        return Ok(());
    };

    let regular_view =
        resolver.with_schema(database_id, |schema| schema.get_view(table_name.as_str()));
    if let Some(view) = regular_view {
        // Views are essentially query aliases, so just Expand the view as a subquery
        view.process()?;
        let mut view_select = view.select_stmt.clone();
        if let ast::OneSelect::Select {
            ref mut columns, ..
        } = view_select.body.select
        {
            for (col, result_col) in view.columns.iter().zip(columns.iter_mut()) {
                if let (Some(name_str), ast::ResultColumn::Expr(_, ref mut alias)) =
                    (&col.name, result_col)
                {
                    *alias = Some(ast::As::As(ast::Name::exact(name_str.clone())));
                }
            }
        }
        let subselect = Box::new(view_select);

        // Use the view name as alias if no explicit alias was provided
        let view_alias = maybe_alias
            .cloned()
            .or_else(|| Some(ast::As::As(table_name.clone())));

        // Views are pre-defined definitions — their body resolves against the
        // schema only, not against CTEs from the calling query context.
        // Pass empty cte_definitions and temporarily clear the ctes_being_defined
        // stack so that e.g. `WITH t AS (...) SELECT * FROM v` where view v
        // references table t will correctly use the real table, not the CTE.
        let saved_ctes = program.take_ctes_being_defined();
        let result = parse_from_clause_table(
            ast::SelectTable::Select(*subselect, view_alias),
            resolver,
            program,
            table_references,
            vtab_predicates,
            &[],
            connection,
        );
        program.restore_ctes_being_defined(saved_ctes);
        view.done();
        return result;
    }

    let view = resolver.with_schema(database_id, |schema| {
        schema.get_materialized_view(table_name.as_str())
    });
    if let Some(view) = view {
        // First check if the DBSP state table exists with the correct version
        let has_compatible_state = resolver.with_schema(database_id, |schema| {
            schema.has_compatible_dbsp_state_table(table_name.as_str())
        });

        if !has_compatible_state {
            use crate::incremental::compiler::DBSP_CIRCUIT_VERSION;
            return Err(crate::LimboError::InternalError(format!(
                "Materialized view '{table_name}' has an incompatible version. \n\
                 The current version is {DBSP_CIRCUIT_VERSION}, but the view was created with a different version. \n\
                 Please DROP and recreate the view to use it."
            )));
        }

        // Check if this materialized view has persistent storage
        let view_guard = view.lock();
        let root_page = view_guard.get_root_page();

        if root_page == 0 {
            drop(view_guard);
            return Err(crate::LimboError::InternalError(
                "Materialized view has no storage allocated".to_string(),
            ));
        }

        // This is a materialized view with storage - treat it as a regular BTree table
        // Create a BTreeTable from the view's metadata
        let columns = view_guard.column_schema.flat_columns();
        let btree_table = Arc::new(crate::schema::BTreeTable::new(
            root_page,
            view_guard.name().to_string(),
            crate::alloc::vec![],
            columns,
            crate::schema::BTreeCharacteristics::HAS_ROWID,
            crate::alloc::vec![],
            crate::alloc::vec![],
            crate::alloc::vec![],
            None,
        ));
        drop(view_guard);

        let alias = maybe_alias.map(|a| normalize_ident(a.name().as_str()));

        table_references.add_joined_table(JoinedTable {
            op: Operation::Scan(Scan::BTreeTable {
                iter_dir: IterationDirection::Forwards,
                index: None,
            }),
            table: Table::BTree(btree_table),
            identifier: alias.unwrap_or(normalized_qualified_name),
            internal_id: program.table_reference_counter.next(),
            join_info: None,
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id,
            indexed: None,
        });
        return Ok(());
    }

    // Query-backed CTEs become FROM-clause subqueries.
    // For other types of tables in the outer query references, we do not add them as joined tables,
    // because the query can simply _reference_ them in e.g. the SELECT columns or the WHERE clause,
    // but it's not part of the join order.
    if qualified_name.db_name.is_none() {
        if let Some(outer_ref) =
            table_references.find_outer_query_ref_by_identifier(&normalized_qualified_name)
        {
            if matches!(outer_ref.table, Table::FromClauseSubquery(_)) {
                table_references.add_joined_table(JoinedTable {
                    op: Operation::default_scan_for(&outer_ref.table),
                    table: outer_ref.table.clone(),
                    identifier: outer_ref.identifier.clone(),
                    internal_id: program.table_reference_counter.next(),
                    join_info: None,
                    col_used_mask: ColumnUsedMask::default(),
                    column_use_counts: Vec::new(),
                    expression_index_usages: Vec::new(),
                    database_id,
                    indexed: None,
                });
                return Ok(());
            }
        }
    }

    // Check if this is an incompatible view
    let is_incompatible = resolver.with_schema(database_id, |schema| {
        schema
            .incompatible_views
            .contains(&normalized_qualified_name)
    });

    if is_incompatible {
        use crate::incremental::compiler::DBSP_CIRCUIT_VERSION;
        crate::bail_parse_error!(
            "Materialized view '{}' has an incompatible version. \n\
             The view was created with a different DBSP version than the current version ({}). \n\
             Please DROP and recreate the view to use it.",
            normalized_qualified_name,
            DBSP_CIRCUIT_VERSION
        );
    }

    // A view row whose stored SQL failed to parse at schema load
    let is_broken_view = resolver.with_schema(database_id, |schema| {
        schema.broken_views.contains(&normalized_qualified_name)
    });

    if is_broken_view {
        crate::bail_parse_error!(
            "view '{}' could not be loaded: its SQL in sqlite_schema does not parse. \n\
             Use DROP VIEW to remove it, then recreate it.",
            normalized_qualified_name
        );
    }

    crate::bail_parse_error!(
        "no such table: {}",
        crate::util::table_name_for_error(qualified_name)
    );
}

fn transform_args_into_where_terms(
    args: &[Box<Expr>],
    internal_id: TableInternalId,
    predicates: &mut Vec<Expr>,
    table: &Table,
) -> Result<()> {
    let mut args_iter = args.iter();
    let mut hidden_count = 0;
    for (i, col) in table.columns().iter().enumerate() {
        if !col.hidden() {
            continue;
        }
        hidden_count += 1;

        if let Some(arg_expr) = args_iter.next() {
            let column_expr = Expr::Column {
                database: None,
                table: internal_id,
                column: i,
                is_rowid_alias: col.is_rowid_alias(),
            };
            // SQLite always uses equality, including for NULL. Unary plus keeps
            // the hidden column's affinity from changing the argument.
            let expr = Expr::Binary(
                column_expr.into(),
                ast::Operator::Equals,
                Expr::Unary(ast::UnaryOperator::Positive, arg_expr.clone()).into(),
            );
            predicates.push(expr);
        }
    }

    if args_iter.next().is_some() {
        return Err(crate::LimboError::ParseError(format!(
            "Too many arguments for {}: expected at most {}, got {}",
            table.get_name(),
            hidden_count,
            hidden_count + 1 + args_iter.count()
        )));
    }

    Ok(())
}

/// Build a stable outer-scope reference set for CTE planning.
/// Current WITH-scope CTE entries are excluded to avoid cloning/replanning cascades.
fn base_outer_refs_for_cte_planning(
    refs: &[OuterQueryReference],
    cte_definitions: &[CteDefinition],
) -> Vec<OuterQueryReference> {
    refs.iter()
        .filter(|r| !cte_definitions.iter().any(|cte| cte.name == r.identifier))
        .cloned()
        .map(|mut r| {
            if matches!(r.table, Table::FromClauseSubquery(_)) {
                r.cte_select = None;
            }
            r
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
#[turso_macros::trace_stack]
pub fn parse_from(
    from: Option<FromClause>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    with: Option<With>,
    preplan_ctes_for_non_from_subqueries: bool,
    out_where_clause: &mut Vec<WhereTerm>,
    vtab_predicates: &mut Vec<Expr>,
    table_references: &mut TableReferences,
    connection: &Arc<crate::Connection>,
) -> Result<()> {
    let mut cte_definitions = Vec::new();
    let mut shadowed_outer_ctes = Vec::new();

    if let Some(with) = with {
        cte_definitions = collect_cte_definitions(with, program)?;

        // This WITH clause's definitions shadow same-named CTEs from outer
        // scopes that are still being planned, so references to those names
        // here are not circular.
        let shadowing_names = cte_definitions
            .iter()
            .map(|definition| definition.name.clone())
            .collect::<Vec<_>>();
        shadowed_outer_ctes = program.mask_shadowed_ctes_being_defined(&shadowing_names);

        if preplan_ctes_for_non_from_subqueries {
            // Make these CTE names available to subqueries outside the FROM clause.
            let base_outer_query_refs = base_outer_refs_for_cte_planning(
                table_references.outer_query_refs(),
                &cte_definitions,
            );
            for (cte_definition_index, cte_definition) in cte_definitions.iter().enumerate() {
                let cte_table = plan_cte(
                    cte_definition_index,
                    &cte_definitions,
                    &base_outer_query_refs,
                    resolver,
                    program,
                    connection,
                    false,
                )?;
                table_references.add_outer_query_reference(OuterQueryReference {
                    identifier: cte_definition.name.clone(),
                    internal_id: cte_table.internal_id,
                    table: cte_table.table,
                    using_dedup_hidden_cols: ColumnMask::default(),
                    col_used_mask: ColumnUsedMask::default(),
                    cte_select: (!cte_definition.references_itself)
                        .then(|| cte_definition.select.clone()),
                    cte_explicit_columns: cte_definition.explicit_columns.clone(),
                    cte_id: Some(cte_definition.cte_id),
                    // This entry only lets a nested FROM clause find the CTE name.
                    cte_definition_only: true,
                    rowid_referenced: false,
                    scope_depth: 0,
                });
            }
        }
    }

    // Process FROM clause if present
    if let Some(from_owned) = from {
        let select_owned = from_owned.select;
        let joins_owned = from_owned.joins;
        parse_from_clause_table(
            *select_owned,
            resolver,
            program,
            table_references,
            vtab_predicates,
            &cte_definitions,
            connection,
        )?;

        for join in joins_owned.into_iter() {
            parse_join(
                join,
                resolver,
                program,
                &cte_definitions,
                out_where_clause,
                vtab_predicates,
                table_references,
                connection,
            )?;
        }
    }

    program.unmask_shadowed_ctes_being_defined(shadowed_outer_ctes);
    Ok(())
}

#[turso_macros::trace_stack]
pub fn parse_where(
    where_clause: Option<&Expr>,
    table_references: &mut TableReferences,
    result_columns: Option<&[ResultSetColumn]>,
    out_where_clause: &mut Vec<WhereTerm>,
    resolver: &Resolver,
) -> Result<()> {
    if let Some(where_expr) = where_clause {
        let start_idx = out_where_clause.len();
        break_predicate_at_and_boundaries(where_expr, out_where_clause);
        for expr in out_where_clause[start_idx..].iter_mut() {
            bind_and_rewrite_expr(
                &mut expr.expr,
                Some(table_references),
                result_columns,
                resolver,
                BindingBehavior::TryCanonicalColumnsFirst,
            )?;
            rewrite_between_exprs(&mut expr.expr)?;
        }
        // BETWEEN in WHERE is rewritten to binary terms here so each side can be
        // considered independently by constraint extraction and range planning.
        // Re-break any ANDs that were created so they become separate WhereTerms for
        // constraint extraction.
        let mut i = start_idx;
        while i < out_where_clause.len() {
            if matches!(
                &out_where_clause[i].expr,
                Expr::Binary(_, ast::Operator::And, _)
            ) {
                let term = out_where_clause.remove(i);
                let mut new_terms: Vec<WhereTerm> = Vec::new();
                break_predicate_at_and_boundaries(&term.expr, &mut new_terms);
                // Preserve from_outer_join from the original term
                for new_term in new_terms.iter_mut() {
                    new_term.from_outer_join = term.from_outer_join;
                }
                let count = new_terms.len();
                for (j, new_term) in new_terms.into_iter().enumerate() {
                    out_where_clause.insert(i + j, new_term);
                }
                i += count;
            } else {
                i += 1;
            }
        }
        Ok(())
    } else {
        Ok(())
    }
}

pub fn rewrite_between_exprs(expr: &mut Expr) -> Result<()> {
    walk_expr_mut(expr, &mut |e: &mut Expr| -> Result<WalkControl> {
        if let Expr::Between {
            lhs,
            not,
            start,
            end,
        } = e
        {
            let lhs_expr = std::mem::take(lhs.as_mut());
            let start_expr = std::mem::take(start.as_mut());
            let end_expr = std::mem::take(end.as_mut());

            let (lower, upper, combine_op) = if *not {
                (
                    Expr::Binary(
                        Box::new(lhs_expr.clone()),
                        ast::Operator::Less,
                        Box::new(start_expr),
                    ),
                    Expr::Binary(
                        Box::new(lhs_expr),
                        ast::Operator::Greater,
                        Box::new(end_expr),
                    ),
                    ast::Operator::Or,
                )
            } else {
                (
                    Expr::Binary(
                        Box::new(lhs_expr.clone()),
                        ast::Operator::GreaterEquals,
                        Box::new(start_expr),
                    ),
                    Expr::Binary(
                        Box::new(lhs_expr),
                        ast::Operator::LessEquals,
                        Box::new(end_expr),
                    ),
                    ast::Operator::And,
                )
            };
            *e = Expr::Binary(Box::new(lower), combine_op, Box::new(upper));
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

/**
  Returns the earliest point at which a WHERE term can be evaluated.
  For expressions referencing tables, this is the innermost loop that contains a row for each
  table referenced in the expression.
  For expressions not referencing any tables (e.g. constants), this is before the main loop is
  opened, because they do not need any table data.
*/
pub fn determine_where_to_eval_term(
    term: &WhereTerm,
    join_order: &[JoinOrderMember],
    subqueries: &[NonFromClauseSubquery],
    table_references: Option<&TableReferences>,
) -> Result<EvalAt> {
    if let Some(table_id) = term.from_outer_join {
        return Ok(EvalAt::Loop(
            join_order
                .iter()
                .position(|t| t.table_id == table_id)
                .unwrap_or(usize::MAX),
        ));
    }

    determine_where_to_eval_expr(&term.expr, join_order, subqueries, table_references)
}

/// A bitmask representing a set of tables in a query plan.
/// Tables are numbered by their index in [SelectPlan::joined_tables].
/// In the bitmask, the first bit is unused so that a mask with all zeros
/// can represent "no tables".
///
/// E.g. table 0 is represented by bit index 1, table 1 by bit index 2, etc.
///
/// Usage in Join Optimization
///
/// In join optimization, [TableMask] is used to:
/// - Generate subsets of tables for dynamic programming in join optimization
/// - Ensure tables are joined in valid orders (e.g., respecting LEFT JOIN order)
///
/// Usage with constraints (WHERE clause)
///
/// [TableMask] helps determine:
/// - Which tables are referenced in a constraint
/// - When a constraint can be applied as a join condition (all referenced tables must be on the left side of the table being joined)
///
/// Note that although [TableReference]s contain an internal ID as well, in join order optimization
/// the [TableMask] refers to the index of the table in the original join order, not the internal ID.
/// This is simply because we want to represent the tables as a contiguous set of bits, and the internal ID
/// might not be contiguous after e.g. subquery unnesting or other transformations.
pub type TableMask = BitSet;

/// Returns a [TableMask] representing the tables referenced in the given expression.
///
/// This includes outer references from subqueries, even if the subquery plan has
/// already been consumed, by relying on the cached outer reference ids.
/// Used in the optimizer for constraint analysis.
pub fn table_mask_from_expr(
    top_level_expr: &Expr,
    table_references: &TableReferences,
    subqueries: &[NonFromClauseSubquery],
) -> Result<TableMask> {
    let mut mask = TableMask::default();
    walk_expr(top_level_expr, &mut |expr: &Expr| -> Result<WalkControl> {
        match expr {
            Expr::Column { table, .. } | Expr::RowId { table, .. } => {
                if let Some(table_idx) = table_references
                    .joined_tables()
                    .iter()
                    .position(|t| t.internal_id == *table)
                {
                    mask.set(table_idx)?;
                } else if table_references
                    .find_outer_query_ref_by_internal_id(*table)
                    .is_none()
                {
                    // Tables from outer query scopes are guaranteed to be 'in scope' for this query,
                    // so they don't need to be added to the table mask. However, if the table is not found
                    // in the outer scope either, then it's an invalid reference.
                    crate::bail_parse_error!("table not found in joined_tables");
                }
            }
            // Given something like WHERE t.a = (SELECT ...), we can only evaluate that expression
            // when all both table 't' and all outer scope tables referenced by the subquery OR its nested subqueries are in scope.
            // Hence, the tables referenced in subqueries must be added to the table mask.
            Expr::SubqueryResult { subquery_id, .. } => {
                let Some(subquery) = subqueries.iter().find(|s| s.internal_id == *subquery_id)
                else {
                    crate::bail_parse_error!("subquery not found");
                };
                match &subquery.state {
                    SubqueryState::Unevaluated { plan } => {
                        let outer_ref_ids = plan.as_ref().unwrap().used_outer_query_ref_ids();
                        for outer_ref_id in &outer_ref_ids {
                            if let Some(table_idx) = table_references
                                .joined_tables()
                                .iter()
                                .position(|t| t.internal_id == *outer_ref_id)
                            {
                                mask.set(table_idx)?;
                            }
                        }
                    }
                    SubqueryState::Evaluated { outer_ref_ids, .. } => {
                        // Now hash-join plans can now translate some correlated subqueries early, we
                        // still revisit those predicates even though the plan has already been consumed.
                        // Without this cache we'd panic or lose the knowledge that an outer table was required.
                        //
                        // Example: `SELECT t.a FROM t WHERE t.a = (SELECT MAX(x.a) FROM x WHERE x.b = t.b)`.
                        // The outer expression `x.b = t.b` is visited after the subquery is translated,
                        // so we need cached `outer_ref_ids` to realize that `t` must already be in scope.
                        for outer_ref_id in outer_ref_ids {
                            if let Some(table_idx) = table_references
                                .joined_tables()
                                .iter()
                                .position(|t| t.internal_id == *outer_ref_id)
                            {
                                mask.set(table_idx)?;
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;

    Ok(mask)
}

/// Determines the earliest loop where an expression can be safely evaluated.
///
/// When a referenced table is not found in `join_order`, we check if it's a hash-join
/// build table and map the condition to the probe loop where its rows are produced.
/// Subquery references are also respected, even after their plans are consumed.
pub fn determine_where_to_eval_expr(
    top_level_expr: &Expr,
    join_order: &[JoinOrderMember],
    subqueries: &[NonFromClauseSubquery],
    table_references: Option<&TableReferences>,
) -> Result<EvalAt> {
    // If the expression references no tables, it can be evaluated before any table loops are opened.
    let mut eval_at: EvalAt = EvalAt::BeforeLoop;
    walk_expr(top_level_expr, &mut |expr: &Expr| -> Result<WalkControl> {
        match expr {
            Expr::Column { table, .. } | Expr::RowId { table, .. } => {
                let Some(join_idx) = join_order.iter().position(|t| t.table_id == *table) else {
                    // Table not found in join_order. Check if it's a hash join build table.
                    // If so, we need to evaluate the condition at the probe table's loop position.
                    if let Some(tables) = table_references {
                        for (probe_idx, member) in join_order.iter().enumerate() {
                            let probe_table = &tables.joined_tables()[member.original_idx];
                            if let Operation::HashJoin(ref hj) = probe_table.op {
                                let build_table = &tables.joined_tables()[hj.build_table_idx];
                                if build_table.internal_id == *table {
                                    // This table is the build side of a hash join.
                                    // Evaluate the condition at the probe table's loop position.
                                    eval_at = eval_at.max(EvalAt::Loop(probe_idx));
                                    return Ok(WalkControl::Continue);
                                }
                            }
                        }
                    }
                    // Must be an outer query reference; in that case, the table is already in scope.
                    return Ok(WalkControl::Continue);
                };
                eval_at = eval_at.max(EvalAt::Loop(join_idx));
            }
            // Given something like WHERE t.a = (SELECT ...), we can only evaluate that expression
            // when all both table 't' and all outer scope tables referenced by the subquery OR its nested subqueries are in scope.
            Expr::SubqueryResult { subquery_id, .. } => {
                let Some(subquery) = subqueries.iter().find(|s| s.internal_id == *subquery_id)
                else {
                    crate::bail_parse_error!("subquery not found");
                };
                match &subquery.state {
                    SubqueryState::Evaluated { evaluated_at, .. } => {
                        eval_at = eval_at.max(*evaluated_at);
                    }
                    SubqueryState::Unevaluated { plan } => {
                        let outer_ref_ids = plan.as_ref().unwrap().used_outer_query_ref_ids();
                        for outer_ref_id in &outer_ref_ids {
                            let join_idx = join_order
                                .iter()
                                .position(|t| t.table_id == *outer_ref_id)
                                .or_else(|| {
                                    let tables = table_references?;
                                    for (probe_idx, member) in join_order.iter().enumerate() {
                                        let probe_table =
                                            &tables.joined_tables()[member.original_idx];
                                        if let Operation::HashJoin(ref hj) = probe_table.op {
                                            let build_table =
                                                &tables.joined_tables()[hj.build_table_idx];
                                            if build_table.internal_id == *outer_ref_id {
                                                return Some(probe_idx);
                                            }
                                        }
                                    }
                                    None
                                });
                            if let Some(join_idx) = join_idx {
                                eval_at = eval_at.max(EvalAt::Loop(join_idx));
                            }
                        }
                        return Ok(WalkControl::Continue);
                    }
                }
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;

    Ok(eval_at)
}

#[allow(clippy::too_many_arguments)]
fn parse_join(
    join: ast::JoinedSelectTable,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    cte_definitions: &[CteDefinition],
    out_where_clause: &mut Vec<WhereTerm>,
    vtab_predicates: &mut Vec<Expr>,
    table_references: &mut TableReferences,
    connection: &Arc<crate::Connection>,
) -> Result<()> {
    let ast::JoinedSelectTable {
        operator: join_operator,
        table,
        constraint,
    } = join;

    parse_from_clause_table(
        table.as_ref().clone(),
        resolver,
        program,
        table_references,
        vtab_predicates,
        cte_definitions,
        connection,
    )?;

    let is_cross = matches!(join_operator, ast::JoinOperator::TypedJoin(Some(jt)) if jt.contains(JoinType::CROSS));

    let (outer, natural, full_outer) = match join_operator {
        ast::JoinOperator::TypedJoin(Some(join_type)) => {
            let is_right = join_type.contains(JoinType::RIGHT);
            let is_left = join_type.contains(JoinType::LEFT);
            let is_outer = join_type.contains(JoinType::OUTER);
            let is_natural = join_type.contains(JoinType::NATURAL);
            // FULL OUTER: LEFT+RIGHT or bare OUTER
            let is_full = (is_left && is_right) || (is_outer && !is_left && !is_right);

            if is_right && !is_left && !is_full {
                // RIGHT JOIN: swap the last two tables, then treat as LEFT JOIN.
                let len = table_references.joined_tables().len();
                // Only valid for a two-table FROM clause; with prior joins the swap
                // would break ON clause column references.
                if len > 2 {
                    crate::bail_parse_error!(
                        "RIGHT JOIN following another join is not yet supported. \
                         Try rewriting as LEFT JOIN or using a subquery."
                    );
                }
                table_references.joined_tables_mut().swap(len - 2, len - 1);
                table_references.set_right_join_swapped();
                // outer flag goes on the originally-left table (now rightmost after swap).
                (true, is_natural, false)
            } else if is_full {
                (true, is_natural, true)
            } else {
                (is_outer || is_left, is_natural, false)
            }
        }
        _ => (false, false, false),
    };

    if natural && constraint.is_some() {
        crate::bail_parse_error!("a NATURAL join may not have an ON or USING clause");
    }

    // SQLite allows duplicate table names/aliases in FROM clauses.
    // Ambiguity is detected later during column resolution.
    let rightmost_table = table_references.joined_tables().last().unwrap();
    let constraint = if natural {
        turso_assert_greater_than_or_equal!(table_references.joined_tables().len(), 2);
        // NATURAL JOIN is first transformed into a USING join with the common columns
        let mut distinct_names: Vec<ast::Name> = vec![];
        // TODO: O(n^2) maybe not great for large tables or big multiway joins
        // SQLite doesn't use HIDDEN columns for NATURAL joins: https://www3.sqlite.org/src/info/ab09ef427181130b
        for right_col in rightmost_table.columns().iter().filter(|col| !col.hidden()) {
            let mut found_match = false;
            for left_table in table_references
                .joined_tables()
                .iter()
                .take(table_references.joined_tables().len() - 1)
            {
                for left_col in left_table.columns().iter().filter(|col| !col.hidden()) {
                    if left_col
                        .name
                        .as_deref()
                        .zip(right_col.name.as_deref())
                        .is_some_and(|(l, r)| l.eq_ignore_ascii_case(r))
                    {
                        distinct_names.push(ast::Name::exact(
                            left_col.name.clone().expect("column name is None"),
                        ));
                        found_match = true;
                        break;
                    }
                }
                if found_match {
                    break;
                }
            }
        }
        if distinct_names.is_empty() {
            None // No common columns = cross join
        } else {
            Some(ast::JoinConstraint::Using(distinct_names))
        }
    } else {
        constraint
    };

    let mut using = vec![];

    if let Some(constraint) = constraint {
        match constraint {
            ast::JoinConstraint::On(ref expr) => {
                let start_idx = out_where_clause.len();
                break_predicate_at_and_boundaries(expr, out_where_clause);
                for predicate in out_where_clause[start_idx..].iter_mut() {
                    predicate.from_outer_join = if outer {
                        Some(table_references.joined_tables().last().unwrap().internal_id)
                    } else {
                        None
                    };
                    bind_and_rewrite_expr(
                        &mut predicate.expr,
                        Some(table_references),
                        None,
                        resolver,
                        BindingBehavior::TryResultColumnsFirst,
                    )?;
                }
            }
            ast::JoinConstraint::Using(distinct_names) => {
                // USING join is replaced with a list of equality predicates
                for distinct_name in distinct_names.iter() {
                    let name_normalized = normalize_ident(distinct_name.as_str());
                    let cur_table_idx = table_references.joined_tables().len() - 1;
                    let left_tables = &table_references.joined_tables()[..cur_table_idx];
                    turso_assert!(!left_tables.is_empty());
                    let right_table = table_references.joined_tables().last().unwrap();
                    let mut left_col = None;
                    for (left_table_offset, left_table) in left_tables.iter().enumerate() {
                        left_col = left_table
                            .columns()
                            .iter()
                            .enumerate()
                            .filter(|(_, col)| !natural || !col.hidden())
                            .find(|(_, col)| {
                                col.name
                                    .as_deref()
                                    .is_some_and(|name| name.eq_ignore_ascii_case(&name_normalized))
                            })
                            .map(|(idx, col)| {
                                (left_table_offset, left_table.internal_id, idx, col)
                            });
                        if left_col.is_some() {
                            break;
                        }
                    }
                    if left_col.is_none() {
                        crate::bail_parse_error!(
                            "cannot join using column {} - column not present in both tables",
                            distinct_name.as_str()
                        );
                    }
                    let right_col = right_table.columns().iter().enumerate().find(|(_, col)| {
                        col.name
                            .as_deref()
                            .is_some_and(|name| name.eq_ignore_ascii_case(&name_normalized))
                    });
                    if right_col.is_none() {
                        crate::bail_parse_error!(
                            "cannot join using column {} - column not present in both tables",
                            distinct_name.as_str()
                        );
                    }
                    let (left_table_idx, left_table_id, left_col_idx, left_col) = left_col.unwrap();
                    let (right_col_idx, right_col) = right_col.unwrap();
                    let expr = Expr::Binary(
                        Box::new(Expr::Column {
                            database: None,
                            table: left_table_id,
                            column: left_col_idx,
                            is_rowid_alias: left_col.is_rowid_alias(),
                        }),
                        ast::Operator::Equals,
                        Box::new(Expr::Column {
                            database: None,
                            table: right_table.internal_id,
                            column: right_col_idx,
                            is_rowid_alias: right_col.is_rowid_alias(),
                        }),
                    );

                    let left_table: &mut JoinedTable = table_references
                        .joined_tables_mut()
                        .get_mut(left_table_idx)
                        .unwrap();
                    left_table.mark_column_used(left_col_idx);
                    let right_table: &mut JoinedTable = table_references
                        .joined_tables_mut()
                        .get_mut(cur_table_idx)
                        .unwrap();
                    right_table.mark_column_used(right_col_idx);
                    out_where_clause.push(WhereTerm {
                        expr,
                        from_outer_join: if outer {
                            Some(right_table.internal_id)
                        } else {
                            None
                        },
                        consumed: false,
                    });
                }
                using = distinct_names;
            }
        }
    }

    assert!(table_references.joined_tables().len() >= 2);
    let last_idx = table_references.joined_tables().len() - 1;
    let rightmost_table = table_references
        .joined_tables_mut()
        .get_mut(last_idx)
        .unwrap();
    let plan_join_type = if full_outer {
        PlanJoinType::FullOuter
    } else if outer {
        PlanJoinType::LeftOuter
    } else {
        PlanJoinType::Inner
    };
    rightmost_table.join_info = Some(JoinInfo {
        join_type: plan_join_type,
        using,
        no_reorder: is_cross,
    });

    Ok(())
}

pub(crate) fn append_vtab_predicates_to_where_clause(
    vtab_predicates: &mut Vec<Expr>,
    table_references: &mut TableReferences,
    result_columns: &[ResultSetColumn],
    out_where_clause: &mut Vec<WhereTerm>,
    resolver: &Resolver,
) -> Result<()> {
    for mut expr in vtab_predicates.drain(..) {
        bind_and_rewrite_expr(
            &mut expr,
            Some(table_references),
            Some(result_columns),
            resolver,
            BindingBehavior::TryCanonicalColumnsFirst,
        )?;

        // Virtual table argument predicates (e.g. the 't2' in pragma_table_info('t2'))
        // must be associated with the virtual table's outer join context if the table is
        // the RHS of a LEFT JOIN. Otherwise the optimizer may incorrectly simplify the
        // LEFT JOIN into an INNER JOIN, breaking NULL row emission for unmatched rows.
        let from_outer_join = vtab_predicate_table_id(&expr).and_then(|table_id| {
            table_references
                .find_joined_table_by_internal_id(table_id)
                .and_then(|table_ref| {
                    table_ref
                        .join_info
                        .as_ref()
                        .and_then(|join_info| join_info.is_outer().then_some(table_id))
                })
        });
        out_where_clause.push(WhereTerm {
            expr,
            from_outer_join,
            consumed: false,
        });
    }
    Ok(())
}

/// Extract the table internal_id from a virtual table argument predicate.
/// These are always of the form `Column { table, .. } = literal` or `IsNull(Column { table, .. })`.
fn vtab_predicate_table_id(expr: &Expr) -> Option<TableInternalId> {
    match expr {
        Expr::Binary(lhs, _, _) | Expr::IsNull(lhs) => match lhs.as_ref() {
            Expr::Column { table, .. } => Some(*table),
            _ => None,
        },
        _ => None,
    }
}
pub fn break_predicate_at_and_boundaries<T: From<Expr>>(
    predicate: &Expr,
    out_predicates: &mut Vec<T>,
) {
    // Unwrap single-element parenthesized expressions recursively: ((expr)) -> expr.
    // This is semantically equivalent since single-element Parenthesized is purely
    // syntactic grouping. Multi-element Parenthesized (row values like (x, y)) are
    // left as-is by unwrap_parens.
    let predicate = unwrap_parens(predicate).unwrap_or(predicate);
    match predicate {
        Expr::Binary(left, ast::Operator::And, right) => {
            break_predicate_at_and_boundaries(left, out_predicates);
            break_predicate_at_and_boundaries(right, out_predicates);
        }
        _ => {
            out_predicates.push(predicate.clone().into());
        }
    }
}

pub fn parse_row_id<F>(
    column_name: &str,
    table_id: TableInternalId,
    fn_check: F,
) -> Result<Option<Expr>>
where
    F: FnOnce() -> bool,
{
    if ROWID_STRS
        .iter()
        .any(|s| s.eq_ignore_ascii_case(column_name))
    {
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

#[allow(clippy::type_complexity)]
#[turso_macros::trace_stack]
pub fn parse_limit(
    mut limit: Limit,
    resolver: &Resolver,
) -> Result<(Option<Box<Expr>>, Option<Box<Expr>>)> {
    bind_and_rewrite_expr(
        &mut limit.expr,
        None,
        None,
        resolver,
        BindingBehavior::TryResultColumnsFirst,
    )?;
    if let Some(ref mut off_expr) = limit.offset {
        bind_and_rewrite_expr(
            off_expr,
            None,
            None,
            resolver,
            BindingBehavior::TryResultColumnsFirst,
        )?;
    }
    Ok((Some(limit.expr), limit.offset))
}
