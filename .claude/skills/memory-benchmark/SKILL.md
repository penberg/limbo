---
name: memory-benchmark
description: How to benchmark and analyze memory usage in Turso using the memory-benchmark crate and dhat heap profiler. Use this skill whenever the user mentions memory usage, memory profiling, allocation tracking, heap analysis, memory regression, memory benchmarking, dhat, or wants to understand where memory is being allocated during SQL workloads. Also use when investigating memory growth in WAL or MVCC mode. IMPORTANT - If you modify the perf/memory crate (add profiles, change CLI flags, change output format, etc.), update this skill document to reflect those changes so it stays accurate for future agents.
---

# Memory Benchmarking & Analysis

The `perf/memory` crate benchmarks memory usage of SQL workloads under WAL and MVCC journal modes. It uses `dhat` as the global allocator to track every heap allocation, and `memory-stats` for process-level RSS snapshots.

It also contains a `stack-report` helper binary for stack-usage investigations.
That binary runs a SQL payload with the `stacker` feature enabled and captures
`turso_stack` tracing events in-process, aggregating structured tracing fields
instead of parsing stderr log text.

## Location

- Benchmark crate: `perf/memory/`
- CodSpeed bench crate: `perf/memory/codspeed/` (CI allocation regression tracking)
- Analysis script: `perf/memory/analyze-dhat.py`
- dhat output: `dhat-heap.json` (written to CWD after each run)

The crate is split into a library and binaries. The workload engine lives in
`memory_benchmark::workload` (`run_workload`, `WorkloadConfig`,
`WorkloadObserver`, the `JournalMode`/`WorkloadProfile` enums and
`create_profile`); the `memory-benchmark` bin is a thin CLI over it that adds
dhat/RSS measurement. Randomized profiles (`read-heavy`, `mixed`) use a fixed
RNG seed (`profile::WORKLOAD_RNG_SEED`) so workloads are identical across runs.

## FTS Query Allocations

The `fts-memory` binary and `perf/memory/codspeed/benches/fts_queries.rs` share
`memory_benchmark::fts`, independently of the existing SQL profile runner.
See `perf/memory/README.md` for commands, corpus details, and measurement limits.

```bash
cargo run -p memory-benchmark --features fts --bin fts-memory -- \
  --query common --state warm --documents 10000 --queries 100 \
  --dhat-file /tmp/fts-warm.json > /tmp/fts-warm-report.json
python3 perf/memory/analyze-dhat.py /tmp/fts-warm.json --modules --top 20
cargo test -p memory-benchmark-codspeed --features fts --bench fts_queries -- --test
```

Use debug runs for correctness only; use `--profile bench-profile` for optimized
dhat investigations with debug information and no LTO. Query cases are `rare`,
`common`, `and`, `or`, `phrase`, and `ranked`. State `first` requires one query
(the default); `warm` defaults to 100 queries on one persistent connection after
one unmeasured warm-up. The default corpus has 10000 documents; 1000 and 100000
are useful local comparison sizes. WAL and one connection remain the defaults.

For full transaction lifecycles, pass `--mode mvcc --connections 2
--transactions-per-connection 10 --queries-per-transaction 2`. This measures 20
transactions and 40 queries, including `BEGIN CONCURRENT` and `COMMIT`. WAL uses
`BEGIN`. Explicit transaction counts conflict with `--queries`; query counts are
otherwise per connection. All transactions begin before query workers start,
and all workers finish before commits. Connections persist across rounds.
One connection covers repeated transactions; two/four cover overlapping snapshots.
Warm-up runs one complete transaction with one query per connection when explicit
transactions are selected. Reports include completed transaction/query counts and
`max_active_transactions`; heap totals aggregate all connections and scheduling.

`FtsWorkload` emits `setup`, `open`, `warmup`, `run`, `cleanup`, and `done` through
`FtsObserver`. The CLI's `DhatObserver` starts profiling on `run` and stops on
`cleanup`, before connections are dropped. Each transition emits a JSON phase
event to stderr, also saved in the final report's `phases` array. stderr includes
dhat diagnostics; filter phase events with `jq -Rc 'fromjson? | select(.event == "phase")'`.
Elapsed event times include profiler overhead; check exit status for success.
`after_batch` samples live heap after a query per connection or after all commits
in one transaction round. Worker errors are joined before rollback and cleanup.

Setup, database opening, warm-up, and cleanup are excluded. Heap totals, peaks,
retained bytes, and per-query live-byte samples track query-phase allocations,
not caches allocated before profiling. RSS after profiling includes dhat's own
stack/report overhead. JSON goes to stdout and allocation stacks to `--dhat-file`.

The Divan target uses the workspace's CodSpeed-compatible Divan dependency,
without dhat's allocator, and runs in the `fts-queries` memory CI shard. Build
with `cargo codspeed build -m memory -p memory-benchmark-codspeed --features codspeed,fts`
and run with `cargo codspeed run -m memory -p memory-benchmark-codspeed --bench fts_queries`.
Each query has first/warm variants at 1000 and 10000 documents and extra 10/100-query
warm variants at 10000 documents. Names encode `(state, documents, queries)`.
The 24 extra `transactions` cases use 1000 documents, ten transactions per
connection, and two queries per transaction: WAL/one connection and MVCC/one,
two, or four connections for all six query cases. Names encode `(mode,
connections, transactions_per_connection, queries_per_transaction)`. Divan uses
the same `FtsWorkload::prepare` and `run` methods with a no-op observer.
Local builds without the `codspeed` feature install Divan's `AllocProfiler` over
the system allocator. Run `cargo bench --profile bench-profile -p memory-benchmark-codspeed
--features fts --bench fts_queries -- transactions --sample-count 3 --sample-size 1`
for local allocation output. Divan counts only its measured threads, not Tokio
worker threads: multi-connection allocation figures are incomplete. Use the dhat
CLI for process-wide concurrent totals and peaks. `alloc` counts allocation calls;
realloc growth is separate under `grow`. `max alloc` is peak live memory for the
whole measured sequence, not per query.
The existing Criterion memory profiles and their setup-inclusive metrics are unchanged.

For above-cache investigations, use `--documents 20000 --extra-tokens 1024
--cache-pages 200 --min-index-bytes 268435456` with the transaction flags above.
Extra tokens use a fixed seed and do not change query matches. The minimum is
checked against stored FTS chunk bytes, not corpus or database size. Setup uses
the core test-helper backing-row reader, then closes inspection connections.
The JSON `index` reports `segment_bytes`, `segments`, `largest_segment_bytes`,
`page_size`, and `configured_cache_pages`; `corpus` records the three options.
An `event: "index"` stderr record reports sizes before measurement or rejection.
Cache pages must be at least 200; omitted means engine default. Zero extra tokens
and zero minimum preserve the small corpus. A 256 MiB index exceeds the current
192 MiB retained-segment budget, but cached searchers can retain segment data and
live cursors load every visible segment. This is not proof of cache eviction or
a memory ceiling. Read-only transactions do not exercise the four-searcher limit
across changing snapshots. Writer arena (64 MiB) and document flush (1000) limits
apply during setup, outside query profiling. See the README for all caveats.
The matching Divan case is opt-in: `--features fts-stress --bench fts_queries --
oversized_index`, with one/two MVCC connections, three transactions, two queries
per transaction, and one sample. CI builds with `codspeed,fts`, excluding the
large cases at compile time, and runs the smaller cases in `fts-queries`.
The large case took about 18 minutes per configuration under CodSpeed; positional
filters did not isolate Divan cases. Use local Divan or dhat for above-cache runs.
The build job also runs `cargo test -p memory-benchmark --features fts --locked`.

## Running Stack Reports

Use this when investigating stack usage from SQL translation/execution probes.
Run stack reports in release mode with `--features stacker` when comparing
against server logs or CI stack-size output. Debug builds can materially
overstate stack deltas and should only be used for quick local iteration.

```bash
cargo run --release -q -p memory-benchmark --features stacker --bin stack-report -- \
  --sql path/to/payload.sql \
  --top 40
```

Useful options:

```bash
--sql FILE|-             # SQL payload, or stdin with -
--format human|json|csv  # output format
--top N                  # aggregate/span rows per statement in human output
--statement N[,N...]     # only include reports for 1-based statement indexes
--sql-contains TEXT      # only include reports for statements containing TEXT, ASCII case-insensitive
```

The report is statement-oriented. For each SQL statement, it records the
remaining stack before execution, the minimum remaining stack sampled while that
statement ran, and `stack_used = baseline_remaining_stack - min_remaining_stack`.
Statements are sorted by `stack_used` descending so the worst SQL statements are
first. The human report also prints global and per-statement span aggregates
sorted by `total_inclusive_stack_used` descending. These aggregate rows group by
`label` plus `detail` and include call count, total/max self stack, total/max
inclusive stack, max cumulative stack at span entry, and `peak_path_hits` for
spans that were active at the statement's minimum remaining-stack sample.

Within each statement, raw span rows are still sorted by `stack_used`
descending, with the original tracing emission sequence kept in the
`trace_sequence` field (`seq` in human output). Raw span rows include
`inclusive_stack_used`, which is measured from the span's parent stack level down
to the deepest sampled remaining stack while the span was active. This is an
inclusive profiler-style metric, so nested spans intentionally overlap; use it
for ranking likely contributors, not for summing to statement total stack.

JSON and CSV formats are deterministic and intended for comparing runs. CSV
uses a `row_type` column with `global_aggregate`, `statement_aggregate`, `span`,
and `statement` rows.

Statement filters affect reporting only. The runner still executes the full SQL
payload in order so schema/data setup and earlier statements remain visible to
later selected statements. Multiple `--statement` and `--sql-contains` filters
are allowed; when both are present, a statement must match both kinds.

`stack-report` splits payloads with `turso_parser::parser::Parser::next_cmd()`.
It then executes statements with no result columns, and queries and drains
row-producing statements. Do not change binding `execute_batch` semantics for
stack reports.

The runner currently uses a fixed in-memory database and enables generated
columns, custom types, and materialized views internally. There are no stack
report CLI flags for selecting the database path or toggling those experimental
features.

## Running Benchmarks

Always run in release mode — debug builds have wildly different allocation patterns and the results are not representative of real-world usage.

```bash
# Basic: single connection, WAL mode, insert-heavy workload
cargo run --release -p memory-benchmark -- --mode wal --workload insert-heavy -i 100 -b 100

# MVCC with concurrent connections
cargo run --release -p memory-benchmark -- --mode mvcc --workload mixed -i 100 -b 100 --connections 4

# Run a final checkpoint after the workload
cargo run --release -p memory-benchmark -- --mode wal --workload read-heavy --checkpoint

# Exercise recursive queues at a 10k-row target cardinality
cargo run --release -p memory-benchmark -- --mode wal --workload recursive-cte -i 20 -b 10000

# Guarantee automatic MVCC checkpoints during the run by lowering the
# logical-log threshold (default is ~4 MB, more than small workloads write)
cargo run --release -p memory-benchmark -- --mode mvcc --workload insert-heavy --mvcc-checkpoint-threshold 16384

# All CLI options
cargo run --release -p memory-benchmark -- \
  --mode wal|mvcc \
  --workload insert-heavy|read-heavy|mixed|scan-heavy|recursive-cte|series-blob|update-churn \
  -i <iterations> \
  -b <batch-size> \
  --connections <N> \
  --checkpoint \
  --timeout <ms> \
  --cache-size <pages> \
  --mvcc-checkpoint-threshold <bytes>   # MVCC only; -1 disables auto-checkpoint
  --mvcc-gc-threshold <versions>        # MVCC only; -1 disables inline GC
  --format human|json|csv
```

The two `--mvcc-*-threshold` flags set the corresponding PRAGMAs on the shared
mv_store before the run. They are the knobs for isolating MVCC GC behavior:
disable the checkpoint (`--mvcc-checkpoint-threshold=-1`) so the only
reclamation is inline GC, then A/B the GC threshold (e.g. `-1` off vs `16384`
default vs a smaller, more aggressive value) on the `update-churn` workload.

Every run produces a `dhat-heap.json` in the current directory. This file contains per-allocation-site data for the entire run.

## Built-in Workload Profiles

| Profile | Description | Setup |
|---------|-------------|-------|
| `insert-heavy` | 100% INSERT statements | Creates table |
| `read-heavy` | 90% SELECT by id / 10% INSERT | Seeds 10k rows |
| `mixed` | 50% SELECT / 50% INSERT | Seeds 10k rows |
| `scan-heavy` | Full table scans with LIKE | Seeds 10k rows |
| `recursive-cte` | Repeated linear, priority-queue, UNION-distinct, and outer-LIMIT recursive CTE queries | No schema setup; `batch-size` is the target recursive result cardinality |
| `series-blob` | `INSERT INTO bench(data) SELECT zeroblob(2048) FROM generate_series(1, ?)` | Creates `bench`; `batch-size` is the series length |
| `update-churn` | Repeated UPDATEs to a fixed 10k-row set (key space partitioned per connection to avoid write-write conflicts) | Seeds 10k rows. Generates superseded versions — the MVCC GC accumulation case. |

Profiles implement the `Profile` trait in `perf/memory/src/profile/`. To add a new workload, create a new file implementing the trait and wire it into the `WorkloadProfile` enum in `main.rs`.

## Understanding the Output

The benchmark reports three categories of metrics:

### RSS (process-level)
Measured via `memory-stats` crate. Includes everything: heap, mmap'd files (WAL, DB pages pulled into OS page cache), tokio runtime, etc. Snapshots are taken at phase transitions (setup -> run) and after each batch.

- **Baseline**: RSS before any DB work (runtime overhead)
- **Peak**: Highest RSS observed during the run
- **Net growth**: Final RSS minus baseline — the memory attributable to the workload

### Heap (dhat)
Precise allocation tracking via the `dhat` global allocator. Only counts explicit heap allocations (malloc/alloc), not mmap.

- **Current**: Bytes still allocated at measurement time
- **Peak**: Highest simultaneous live allocation during the entire run
- **Total allocs**: Number of individual allocation calls
- **Total bytes**: Cumulative bytes allocated (includes freed memory) — measures allocation pressure

### Disk
File sizes after the benchmark completes:
- **DB file**: The `.db` file
- **WAL file**: The `.db-wal` file (WAL mode only)
- **Log file**: The `.db-log` file (MVCC logical log only)

## Analyzing dhat Output

After running a benchmark, use the analysis script to produce a readable report from `dhat-heap.json`:

```bash
# Overview: top allocation sites by bytes live at global peak
python3 perf/memory/analyze-dhat.py dhat-heap.json --top 15 --modules

# Focus on a specific subsystem
python3 perf/memory/analyze-dhat.py dhat-heap.json --filter mvcc --stacks
python3 perf/memory/analyze-dhat.py dhat-heap.json --filter btree --stacks
python3 perf/memory/analyze-dhat.py dhat-heap.json --filter page_cache --stacks

# Sort by different metrics
python3 perf/memory/analyze-dhat.py dhat-heap.json --sort-by eb  # bytes at exit (leaks)
python3 perf/memory/analyze-dhat.py dhat-heap.json --sort-by tb  # total bytes (pressure)
python3 perf/memory/analyze-dhat.py dhat-heap.json --sort-by mb  # max live bytes per site

# JSON output for programmatic use
python3 perf/memory/analyze-dhat.py dhat-heap.json --json
```

### Sort Metrics

| Flag | Metric | Use when |
|------|--------|----------|
| `gb` | Bytes live at global peak (default) | Finding what dominates memory at the high-water mark |
| `eb` | Bytes live at exit | Finding memory leaks or things that never get freed |
| `tb` | Total bytes allocated | Finding allocation pressure hotspots (GC churn) |
| `mb` | Max bytes live per site | Finding per-site high-water marks |
| `tbk` | Total allocation count | Finding chatty allocators (many small allocs) |

### Analysis Flags

- `--top N` — Show top N sites (default 15)
- `--filter PATTERN` — Filter to sites/stacks containing substring (e.g. `mvcc`, `btree`, `wal`, `pager`)
- `--stacks` — Show full callstacks for top allocation sites
- `--modules` — Aggregate by crate/module for a high-level breakdown
- `--json` — Machine-readable aggregated output

## Typical Workflow

When investigating memory usage or a suspected regression:

1. **Run the benchmark** with parameters matching the scenario:
   ```bash
   cargo run -p memory-benchmark -- --mode mvcc --workload mixed -i 500 -b 100 --connections 4
   ```

2. **Get the high-level picture** — which modules use the most memory:
   ```bash
   python3 perf/memory/analyze-dhat.py dhat-heap.json --modules --top 20
   ```

3. **Drill into the hot module** — e.g. if `turso_core` dominates:
   ```bash
   python3 perf/memory/analyze-dhat.py dhat-heap.json --filter turso_core --stacks --top 10
   ```

4. **Check for leaks** — anything still alive at exit that shouldn't be:
   ```bash
   python3 perf/memory/analyze-dhat.py dhat-heap.json --sort-by eb --top 10
   ```

5. **Compare modes** — run the same workload under WAL and MVCC and compare the reports to see the memory cost of MVCC versioning.

## Concurrency Details

When `--connections > 1`:
- Setup phase (schema creation, seeding) always runs on a single connection sequentially
- Run phase spawns one tokio task per connection, each executing its batch concurrently
- `--checkpoint` adds a final single-connection `PRAGMA wal_checkpoint(TRUNCATE)` phase after the run phase
- Each connection gets `busy_timeout` set (default 30s, configurable via `--timeout`)
- WAL mode uses `BEGIN`, MVCC uses `BEGIN CONCURRENT`
- The `Profile` trait's `next_batch(connections)` returns one batch per connection with non-overlapping row IDs

## CodSpeed Allocation Tracking in CI

`.github/workflows/codspeed-memory.yml` runs every workload profile under both
journal modes with CodSpeed's memory instrument (eBPF-based malloc tracking:
peak memory, total allocated, allocation count) so allocation regressions show
up on PRs. The bench harness is the separate crate `perf/memory/codspeed/`
(criterion benchmarks named `<mode>/<workload>/<total-ops>`, e.g.
`mvcc/insert-heavy/2000`, with much smaller iteration counts than the CLI
defaults). Each (mode, workload) pair runs at 1x/2x/4x scale — same batch
size, more iterations — so comparing the sizes shows how memory grows with
workload volume, plus an 8x `<ops>-checkpoint` variant that guarantees
checkpointing is part of the measurement: it lowers
`mvcc_checkpoint_threshold` to 16 KiB so MVCC auto-checkpoints fire mid-run
(WAL's 1000-frame threshold is hardcoded in `core/storage/wal.rs`) and ends
with an explicit `PRAGMA wal_checkpoint(TRUNCATE)`. The workflow builds
the bench binary once, then fans out one CI job per workload profile, each
filtering benchmarks by name — the sharding pattern from CodSpeed's
sharded-benchmarks docs.

The bench crate must stay free of `[[bin]]` targets: cargo builds a package's
bins (panic=abort under the release profile) alongside its benches
(panic=unwind), and the duplicated `turso_sdk_kit` cdylib/staticlib units then
collide on unhashed output filenames and break the build. That is why the
bench does not live in `perf/memory` itself.

Run locally:

```bash
# Quick correctness pass (runs each benchmark once)
cargo bench -p memory-benchmark-codspeed --bench memory_profiles -- --test

# What CI runs (requires cargo-codspeed; uninstrumented outside the CodSpeed runner)
cargo codspeed build -m memory -p memory-benchmark-codspeed --features codspeed
cargo codspeed run -m memory -p memory-benchmark-codspeed --bench memory_profiles "insert-heavy"
```

Do NOT run plain `cargo bench -p memory-benchmark-codspeed` without `-- --test`
unless you want full criterion sampling — each sample executes an entire
workload.

## Adding a New Profile

1. Create `perf/memory/src/profile/your_profile.rs` implementing the `Profile` trait
2. Add `pub mod your_profile;` to `perf/memory/src/profile/mod.rs`
3. Add a variant to `WorkloadProfile` enum in `src/workload.rs`
4. Wire it into `create_profile()` in `src/workload.rs`
5. Add it to `WORKLOADS` (and `base_workload_size`) in
   `perf/memory/codspeed/benches/memory_profiles.rs` and to the `workload`
   matrix in `.github/workflows/codspeed-memory.yml` so CI tracks it

The `Profile` trait:
```rust
pub trait Profile {
    fn name(&self) -> &str;
    fn next_batch(&mut self, connections: usize) -> (Phase, Vec<Vec<WorkItem>>);
}
```

Return `Phase::Setup` for schema/seeding (single batch), `Phase::Run` for measured work (one batch per connection), `Phase::Done` when finished.

## Keeping This Skill Up to Date

This skill document is the source of truth for how agents use the memory benchmark tooling. If you modify the `perf/memory` crate — adding profiles, changing CLI flags, altering output format, updating the analysis script, changing the `Profile` trait, etc. — update this SKILL.md to match. Specifically:

- New CLI flags: add to the "Running Benchmarks" section
- New profiles: add to the "Built-in Workload Profiles" table
- Changed output metrics: update the "Understanding the Output" section
- New analyze-dhat.py flags or sort metrics: update the "Analyzing dhat Output" section
- Changed `Profile` trait signature: update "Adding a New Profile"

Future agents rely on this document being accurate. Stale instructions cause wasted work.
