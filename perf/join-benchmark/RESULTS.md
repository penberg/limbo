# Join optimizer study: 2026-09-06

This study uses debug builds because repository rules prohibit release builds.
Execution work is the primary measure.
Planning time is outside the study.

## Measures

The runner consumes every result row and records statement metrics.
The main measures are VM steps and rows read.
These counters are more stable than elapsed time on a shared machine.

Elapsed time remains useful for large changes.
Each TPC-H query uses the same database and warm cache unless noted.

The study also saves the JSON query plan.
Each access node reports input rows, output rows, local cost, and total prefix cost.

## Baseline

The baseline used commit `b04d329547` and the analyzed benchmark databases.

| Query | VM steps | Rows read | Time |
|---|---:|---:|---:|
| TPC-H 5 | 19,424,408 | 3,555,780 | 26.37 s |
| TPC-H 9 | 58,993,989 | 6,649,413 | 107.76 s |
| Graph `a_cooccurrence` | 427,457 | 40,171 | 0.88 s |
| Graph `c_edge_counts` | 2,396,740 | 161,336 | 0.43 s |

The other six graph queries provide guard coverage.
Their baseline VM counts range from 1,060 to 289,758.

## Hypotheses

### Finish connected components first

TPC-H 5 placed `region` after `orders` without a join condition.
This order scanned the small table once for each order row.

A forced connected order reduced VM steps to 16,100,394.
It reduced rows read to 2,445,401.
The result validated connected-prefix pruning.

### Build equality classes

TPC-H 5 contains `customer.nation = supplier.nation` and `supplier.nation = nation.id`.
The old optimizer could not infer `customer.nation = nation.id`.

A forced query with that implied equality used 12,447,183 VM steps.
It read 1,898,579 rows.
The result justified equality classes.

The implementation uses one representative column for each class.
It adds missing links in a star shape.
This shape avoids unnecessary links and unstable symmetric plans.

### Count constant filters inside compound lookups

The `region` lookup used both its join key and `region.name`.
The old estimate treated every joined key as a hit.
It therefore missed the tenfold reduction from the name filter.

The new estimate applies a consumed constant filter when a lookup also uses a join key.
This estimate moved `nation` and `region` before `lineitem`.

### Count unmatched outer rows

The old model counted matches for a left join but did not count preserved unmatched rows.
It also mispriced `WHERE right.key IS NULL` anti-joins.

The new model estimates the chance of no match from the average match count.
It uses that value for null tests on non-null right-side columns.

### Keep looking after an unavailable duplicate

An early equality-class version exposed a compound-index bug in TPC-H 9.
An unavailable duplicate on one index column stopped the next usable column.

The search now skips the unavailable duplicate.
It only stops when a later index column creates a real prefix gap.

## Final results

| Query | Baseline VM | Final VM | VM change | Baseline rows | Final rows | Row change |
|---|---:|---:|---:|---:|---:|---:|
| TPC-H 5 | 19,424,408 | 12,447,165 | -35.9% | 3,555,780 | 1,898,579 | -46.6% |
| TPC-H 9 | 58,993,989 | 58,993,419 | 0.0% | 6,649,413 | 6,649,413 | 0.0% |
| Graph `a_cooccurrence` | 427,457 | 427,457 | 0.0% | 40,171 | 40,171 | 0.0% |
| Graph `c_edge_counts` | 2,396,740 | 101,243 | -95.8% | 161,336 | 8,077 | -95.0% |

TPC-H 5 measured time fell from 26.37 seconds to 9.82 seconds.
TPC-H 9 kept the same work and completed in 100.63 seconds.

All other graph queries kept the same VM steps and rows read.
The graph suite found no deterministic work regression.

## Commands

Print plans:

```bash
cargo run -p turso-join-benchmark -- \
  --database perf/tpc-h/TPC-H.db \
  --query-dir perf/tpc-h/queries \
  --query 5 --query 9 --plans
```

Measure TPC-H execution:

```bash
cargo run -p turso-join-benchmark -- \
  --database perf/tpc-h/TPC-H.db \
  --query-dir perf/tpc-h/queries \
  --query 5 --query 9 \
  --warmups 1 --repetitions 3 \
  --timeout-seconds 300
```

Measure the analyzed graph suite:

```bash
cargo run -p turso-join-benchmark -- \
  --database perf/graph-queries/graph-queries-analyzed.db \
  --query-dir perf/graph-queries/queries \
  --warmups 1 --repetitions 3
```
