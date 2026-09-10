# Join optimizer

The join optimizer chooses a table order and an access method for each table.
Its main goal is to reduce query execution work.

Planning time is not a target for current join work.
Execution usually costs much more than planning.

## Main flow

The optimizer processes a query in this order:

1. It plans supported correlated subqueries in their written and unnested forms.
2. It keeps the form with the lower estimated cost.
3. It removes constant conditions and rewrites supported expressions.
4. It changes a left join to an inner join when a later filter rejects null rows.
5. It builds equality classes for compatible inner-join columns.
6. It extracts index constraints from `WHERE` and `ON` terms.
7. It searches for a table order and access methods.
8. It includes sort work when an order can remove a later sort.
9. It writes the selected order, methods, costs, and row estimates into the plan.

## Equality classes

An equality class links columns that must have equal values.
For example, `a.x = b.x AND b.x = c.x` also gives `a.x = c.x`.

The optimizer selects one column as the class representative.
It adds missing links from that column to other class members.
This star shape avoids a quadratic set of links.

The optimizer only infers links for plain column equalities in inner joins.
The columns must have the same affinity and supported collation.
The original terms still verify each result during execution.

## Join search

Queries with at most 12 tables use dynamic programming.
The memo keeps the best plan for each table set and final table.
This retains useful alternatives for later joins and hash joins.

Each prefix finishes an available connected component before it starts a cross join.
This rule prevents repeated scans of unrelated small tables.
The rule still permits a cross join after the current component is complete.

Larger queries use a greedy search.
The greedy search uses the same connected-component rule.

Outer joins, semi-joins, anti-joins, and `CROSS JOIN` can restrict table movement.
The search applies these restrictions before it compares plans.

## Cost and row estimates

Each table access has these estimates:

- Input rows from the current join prefix.
- Output rows for each input row.
- Output rows after the access and ready filters.
- Cost of the table access.
- Total cost through the table access.

The model includes page reads, seeks, row work, filter work, hash work, and sort work.
It uses `sqlite_stat1` data after `ANALYZE`.

Without statistics, the model uses fixed fallback values.
The main fallback assumes one million rows per table.
Fixed selectivity values estimate equality, range, and other filters.

A compound lookup can contain a join key and a constant filter.
The row estimate includes the constant filter even when the index consumes it.

For an outer join, the model estimates both matched and unmatched rows.
It uses a Poisson model when only the average match count is known.
This estimate handles common `LEFT JOIN ... WHERE right.key IS NULL` anti-joins.

## Observability

Use `EXPLAIN QUERY PLAN FORMAT=JSON` to inspect the selected plan.
Each table access can contain an `estimate` object.
See [`docs/eqp-json.md`](../../../docs/eqp-json.md) for the field definitions.

Use statement metrics to measure actual execution work.
Important counters include rows read, VM steps, seeks, scans, sorts, and hash-table work.

Use [`perf/join-benchmark`](../../../perf/join-benchmark) to collect plans and execution metrics.
The runner consumes result rows instead of printing them.
This keeps large query output out of benchmark logs.

## Source files

- `mod.rs` runs the main optimizer flow and writes the selected plan.
- `constraints.rs` builds equality classes and index constraints.
- `access_method.rs` compares scans, seeks, custom indexes, and hash joins.
- `cost.rs` estimates rows and execution work.
- `join.rs` searches table orders and estimates join prefixes.
- `order.rs` finds plans that can remove sorting.
- `unnest.rs` changes supported subqueries into joins.
