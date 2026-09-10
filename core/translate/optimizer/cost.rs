use crate::schema::Index;
use crate::stats::AnalyzeStats;
use crate::sync::Arc;
use crate::translate::expr::{walk_expr, WalkControl};
use crate::translate::optimizer::constraints::RangeConstraintRef;
use crate::translate::plan::JoinedTable;
use turso_parser::ast;

use super::constraints::Constraint;
use super::cost_params::CostModelParams;

/// A simple newtype wrapper over a f64 that represents the cost of an operation.
///
/// This is used to estimate the cost of scans, seeks, and joins.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Cost(pub f64);

/// Count the operations needed to check one `WHERE` expression.
pub fn where_expr_steps(expr: &ast::Expr) -> usize {
    let mut steps = 0;
    walk_expr(expr, &mut |expr| {
        steps += where_node_steps(expr);
        Ok(WalkControl::Continue)
    })
    .expect("counting WHERE operations cannot fail");
    steps.max(1)
}

pub fn where_node_steps(expr: &ast::Expr) -> usize {
    match expr {
        ast::Expr::Between { .. } => 2,
        ast::Expr::InList { rhs, .. } => rhs.len().max(1),
        ast::Expr::Case {
            when_then_pairs, ..
        } => when_then_pairs.len().max(1),
        ast::Expr::Register(_)
        | ast::Expr::Collate(..)
        | ast::Expr::DoublyQualified(..)
        | ast::Expr::Id(_)
        | ast::Expr::Column { .. }
        | ast::Expr::RowId { .. }
        | ast::Expr::Literal(_)
        | ast::Expr::Name(_)
        | ast::Expr::Parenthesized(_)
        | ast::Expr::Qualified(..)
        | ast::Expr::Variable(_)
        | ast::Expr::Default => 0,
        _ => 1,
    }
}

impl std::ops::Add for Cost {
    type Output = Cost;

    fn add(self, other: Cost) -> Cost {
        Cost(self.0 + other.0)
    }
}

impl std::ops::Deref for Cost {
    type Target = f64;

    fn deref(&self) -> &f64 {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct IndexInfo {
    pub unique: bool,
    pub column_count: usize,
    /// Whether the index satisfies the query without table lookups.
    /// True for genuinely covering indexes and for multi-index branches
    /// that only harvest rowids into a RowSet.
    pub covering: bool,
    /// Estimated rows per index leaf page, derived from the column count
    /// ratio between the index and its parent table.
    pub rows_per_leaf_page: f64,
}

/// Estimate rows per index leaf page based on the index/table width ratio.
/// Narrower indexes have smaller entries, fitting more rows per page.
pub fn index_leaf_rows_per_page(
    index_column_count: usize,
    table_column_count: usize,
    has_rowid_alias: bool,
    rows_per_table_page: f64,
) -> f64 {
    // Table width: all columns + implicit rowid (unless one column IS the rowid alias).
    let table_width = table_column_count as f64 + if has_rowid_alias { 0.0 } else { 1.0 };
    // Index width: indexed columns + rowid suffix (always present).
    let index_width = index_column_count as f64 + 1.0;
    (rows_per_table_page * table_width / index_width).max(1.0)
}

/// Compute `rows_per_leaf_page` for an index on the given table.
pub fn rows_per_leaf_page_for_index(
    index_column_count: usize,
    rhs_table: &JoinedTable,
    rows_per_table_page: f64,
) -> f64 {
    let table_column_count = rhs_table.columns().len();
    let has_rowid_alias = rhs_table
        .btree()
        .is_some_and(|bt| bt.get_rowid_alias_column().is_some());
    index_leaf_rows_per_page(
        index_column_count,
        table_column_count,
        has_rowid_alias,
        rows_per_table_page,
    )
}

/// Estimate IO and CPU cost for a full table scan.
///
/// # Arguments
/// * `base_row_count` - Total rows in the table
/// * `num_scans` - Number of times we scan the table (e.g., from outer loop in nested loop join)
/// * `params` - Cost model parameters
fn estimate_scan_cost(base_row_count: f64, num_scans: f64, params: &CostModelParams) -> Cost {
    let table_pages = (base_row_count / params.rows_per_table_page).max(1.0);

    // First scan reads all pages; subsequent scans benefit from caching
    let io_cost = if num_scans <= 1.0 {
        table_pages
    } else {
        // First scan + discounted cost for subsequent scans
        table_pages + (num_scans - 1.0) * table_pages * params.cache_reuse_factor
    };

    // CPU cost for processing all rows on each scan
    let cpu_cost = num_scans * base_row_count * params.cpu_cost_per_row;

    Cost(io_cost + cpu_cost)
}

/// Estimate the work to add every row to a new in-memory index.
///
/// Each insert searches the part of the index that was already built. A
/// balanced index needs about log2(rows) comparisons per insert.
pub(super) fn estimate_ephemeral_index_build_cost(
    row_count: f64,
    params: &CostModelParams,
) -> Cost {
    let comparisons_per_row = row_count.max(2.0).log2();
    Cost(row_count * comparisons_per_row * params.cpu_cost_per_seek)
}

/// Estimate how many B-tree levels an index search reads.
pub(super) fn estimate_btree_depth(row_count: f64, rows_per_page: f64) -> f64 {
    if row_count <= 1.0 {
        1.0
    } else {
        (row_count.ln() / rows_per_page.ln()).ceil().max(1.0)
    }
}

/// Estimate IO and CPU cost for index-based access.
///
/// This properly separates the number of B-tree seeks from the number of rows
/// returned per seek. A range scan does ONE seek followed by sequential leaf
/// page reads, not one seek per row.
///
/// # Arguments
/// * `base_row_count` - Total rows in the table (for estimating tree depth and page counts)
/// * `tree_depth` - B-tree depth (number of pages to traverse per seek)
/// * `index_info` - Index properties (covering, unique, etc.)
/// * `num_seeks` - Number of B-tree traversals (typically = outer cardinality for joins)
/// * `rows_per_seek` - Expected rows returned per seek (1 for point lookup, more for range)
/// * `params` - Cost model parameters
pub fn estimate_index_cost(
    base_row_count: f64,
    tree_depth: f64,
    index_info: IndexInfo,
    input_cardinality: f64,
    rows_per_seek: f64,
    params: &CostModelParams,
) -> Cost {
    // Detect full index scan: when rows_per_seek equals base_row_count, we're scanning
    // the entire index, not seeking to specific positions.
    let is_full_scan = (rows_per_seek - base_row_count).abs() < 1.0;

    // Cost of B-tree traversals: each seek traverses tree_depth pages.
    let seek_cost = if is_full_scan {
        // Full scan: one seek to start, then sequential reads.
        // When re-scanned (nested loop inner), first scan is cold, rest are cached.
        if input_cardinality <= 1.0 {
            tree_depth
        } else {
            tree_depth + (input_cardinality - 1.0) * tree_depth * params.cache_reuse_factor
        }
    } else {
        input_cardinality * tree_depth
    };

    let index_leaf_pages_count = (rows_per_seek / index_info.rows_per_leaf_page).max(1.0);
    let leaf_scan_cost = if is_full_scan {
        // Full scan of all leaf pages. Repeated scans benefit from caching.
        if input_cardinality <= 1.0 {
            index_leaf_pages_count
        } else {
            index_leaf_pages_count
                + (input_cardinality - 1.0) * index_leaf_pages_count * params.cache_reuse_factor
        }
    } else if rows_per_seek <= 1.0 {
        // Point lookup: the leaf page is the last page of the B-tree traversal,
        // already counted in seek_cost.
        0.0
    } else if input_cardinality <= 1.0 {
        index_leaf_pages_count
    } else {
        // Range scan in a nested-loop join: after the first iteration the inner
        // table's pages are largely in the buffer pool.  Apply the same caching
        // discount as full scans.
        index_leaf_pages_count
            + (input_cardinality - 1.0) * index_leaf_pages_count * params.cache_reuse_factor
    };

    // For non-covering indexes, we need to fetch from the table for each row.
    let table_lookup_cost = if index_info.covering {
        0.0
    } else {
        let table_pages_count = (base_row_count / params.rows_per_table_page).max(1.0);
        let selectivity = rows_per_seek / base_row_count.max(1.0);
        input_cardinality * selectivity * table_pages_count
    };

    let io_cost = seek_cost + leaf_scan_cost + table_lookup_cost;

    // CPU cost: key comparisons during seeks + row processing
    let total_rows = input_cardinality * rows_per_seek;
    let cpu_cost =
        input_cardinality * params.cpu_cost_per_seek + total_rows * params.cpu_cost_per_row;

    Cost((io_cost + cpu_cost - params.index_bonus).max(0.001))
}

pub(crate) fn is_unique_point_lookup(
    index_info: IndexInfo,
    usable_constraint_refs: &[RangeConstraintRef],
) -> bool {
    // Only plain `=` equalities count. An `IS` equality matches NULL keys, and
    // a UNIQUE index can store many NULL keys, so a key with an `IS` component
    // can return many rows.
    let eq_count = usable_constraint_refs
        .iter()
        .take_while(|cref| cref.eq.as_ref().is_some_and(|eq| !eq.null_matching))
        .count();
    index_info.unique && eq_count >= index_info.column_count
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RowCountEstimate {
    HardcodedFallback(f64),
    AnalyzeStats(f64),
}

impl RowCountEstimate {
    /// Create a hardcoded fallback using the given params.
    pub fn hardcoded_fallback(params: &CostModelParams) -> Self {
        RowCountEstimate::HardcodedFallback(params.rows_per_table_fallback)
    }
}

impl std::ops::Deref for RowCountEstimate {
    type Target = f64;

    fn deref(&self) -> &f64 {
        match self {
            RowCountEstimate::HardcodedFallback(val) => val,
            RowCountEstimate::AnalyzeStats(val) => val,
        }
    }
}

/// ANALYZE-based context for cost estimation.
/// Uses sqlite_stat1 histogram data for row estimates when available,
/// otherwise falls through to heuristic selectivity multipliers.
pub struct AnalyzeCtx<'a> {
    pub rhs_table: &'a JoinedTable,
    pub index: Option<&'a Arc<Index>>,
    pub stats: &'a AnalyzeStats,
}

pub(crate) fn estimate_rows_per_seek(
    index_info: IndexInfo,
    constraints: &[Constraint],
    usable_constraint_refs: &[RangeConstraintRef],
    base_row_count: RowCountEstimate,
    analyze_ctx: Option<&AnalyzeCtx>,
) -> f64 {
    let join_probe_constant_selectivity =
        join_probe_constant_selectivity(constraints, usable_constraint_refs);
    if is_unique_point_lookup(index_info, usable_constraint_refs) {
        return join_probe_constant_selectivity;
    }

    if let Some(ctx) = analyze_ctx {
        if !usable_constraint_refs.is_empty() {
            if let Some(eq_prefix_rows) =
                estimate_rows_from_analyze_stats(ctx, usable_constraint_refs)
            {
                // Apply range selectivity for any trailing non-equality constraints
                // beyond the equality prefix. SQLite does this via whereRangeAdjust
                // which divides by ~4 per range bound.
                let eq_prefix_len = usable_constraint_refs
                    .iter()
                    .take_while(|cref| cref.eq.is_some())
                    .count();
                let range_selectivity: f64 = usable_constraint_refs[eq_prefix_len..]
                    .iter()
                    .map(|cref| {
                        let mut sel = 1.0;
                        if let Some(lb) = cref.lower_bound {
                            sel *= constraints[lb].selectivity;
                        }
                        if let Some(ub) = cref.upper_bound {
                            sel *= constraints[ub].selectivity;
                        }
                        sel
                    })
                    .product();
                return (eq_prefix_rows * range_selectivity).max(1.0)
                    * join_probe_constant_selectivity;
            }
        }
    }

    let selectivity_multiplier: f64 = usable_constraint_refs
        .iter()
        .map(|cref| {
            if let Some(ref eq) = cref.eq {
                return constraints[eq.constraint_pos].selectivity;
            }
            let mut selectivity = 1.0;
            if let Some(lower_bound) = cref.lower_bound {
                selectivity *= constraints[lower_bound].selectivity;
            }
            if let Some(upper_bound) = cref.upper_bound {
                selectivity *= constraints[upper_bound].selectivity;
            }
            selectivity
        })
        .product();

    (selectivity_multiplier * *base_row_count).max(1.0) * join_probe_constant_selectivity
}

fn join_probe_constant_selectivity(
    constraints: &[Constraint],
    usable_constraint_refs: &[RangeConstraintRef],
) -> f64 {
    let equality_constraints = usable_constraint_refs
        .iter()
        .take_while(|constraint| constraint.eq.is_some())
        .map(|constraint| &constraints[constraint.eq.as_ref().unwrap().constraint_pos]);
    if !equality_constraints
        .clone()
        .any(|constraint| !constraint.lhs_mask.is_empty())
    {
        return 1.0;
    }
    equality_constraints
        .filter(|constraint| constraint.lhs_mask.is_empty())
        .map(|constraint| constraint.selectivity)
        .product()
}

/// Estimate rows per seek using ANALYZE stats (sqlite_stat1 histogram data).
/// Returns `None` when no actual stats exist for this index, signaling the
/// caller to fall back to selectivity-based estimation.
fn estimate_rows_from_analyze_stats(
    ctx: &AnalyzeCtx,
    constraint_refs: &[RangeConstraintRef],
) -> Option<f64> {
    let index = ctx.index?;

    // Only count leading equality-constrained columns. ANALYZE stats give
    // avg rows per distinct prefix value, which is only meaningful for
    // equality lookups. A range constraint (>, <, >=, <=) scans a portion
    // of the index rather than fixing a prefix value, so it should not
    // consume a prefix position in the stats lookup.
    let eq_prefix_len = constraint_refs
        .iter()
        .take_while(|cref| cref.eq.is_some())
        .count();

    // The per-key average must not be applied to a key that can be NULL:
    // NULL keys pile up in one bucket (think `deleted_at IS NULL` matching
    // most of the table), so the average across all keys says nothing about
    // that bucket. Decline, so the caller falls back to the pessimistic
    // heuristic estimate for `IS`.
    if constraint_refs[..eq_prefix_len]
        .iter()
        .any(|cref| cref.eq.as_ref().is_some_and(|eq| eq.null_matching))
    {
        return None;
    }

    if eq_prefix_len == 0 {
        // Pure range scan — ANALYZE per-distinct-value stats don't apply.
        return None;
    }

    let table_name = ctx.rhs_table.table.get_name();

    let table_stats = ctx.stats.table_stats(table_name)?;
    let idx_stats = table_stats.index_stats.get(&index.name)?;

    if eq_prefix_len <= idx_stats.avg_rows_per_distinct_prefix.len() {
        Some(idx_stats.avg_rows_per_distinct_prefix[eq_prefix_len - 1] as f64)
    } else {
        // Stats exist for this index but don't cover the equality prefix length.
        // Return None to fall through to selectivity-based estimation.
        None
    }
}

/// Estimate the cost of a scan or seek operation.
#[expect(clippy::too_many_arguments)]
pub fn estimate_cost_for_scan_or_seek(
    index_info: Option<IndexInfo>,
    constraints: &[Constraint],
    usable_constraint_refs: &[RangeConstraintRef],
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    is_index_ordered: bool,
    params: &CostModelParams,
    analyze_ctx: Option<&AnalyzeCtx>,
) -> Cost {
    let base_row_count = *base_row_count;

    let tree_depth = estimate_btree_depth(base_row_count, params.rows_per_table_page);

    let Some(index_info) = index_info else {
        // Full table scan (no index)
        return estimate_scan_cost(base_row_count, input_cardinality, params);
    };

    if is_unique_point_lookup(index_info, usable_constraint_refs) {
        // Unique point lookup: 1 seek per input row, 1 row returned per seek
        return estimate_index_cost(
            base_row_count,
            tree_depth,
            index_info,
            input_cardinality, // num_seeks = outer cardinality
            1.0,               // rows_per_seek = 1 for unique point lookup
            params,
        );
    }

    let rows_per_seek = estimate_rows_per_seek(
        index_info,
        constraints,
        usable_constraint_refs,
        RowCountEstimate::AnalyzeStats(base_row_count),
        analyze_ctx,
    );

    let base_cost = estimate_index_cost(
        base_row_count,
        tree_depth,
        index_info,
        input_cardinality, // num_seeks = outer cardinality
        rows_per_seek,
        params,
    );

    let is_full_scan = usable_constraint_refs.is_empty();
    // Penalize non-covering indexes doing full scans when not ordered by the index.
    // Without ordering benefit, a full scan on a non-covering index requires random
    // table lookups for each row, which is expensive.
    if !index_info.covering && is_full_scan && !is_index_ordered {
        // Full index scan without ordering benefit - prefer table scan instead
        Cost(base_cost.0 * 2.0)
    } else {
        base_cost
    }
}
