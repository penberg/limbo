# FTS query memory benchmarks

`fts-memory` and the `fts_queries` Divan/CodSpeed target share the corpus and
workload runner in `src/fts.rs`. They use a temporary on-disk database, with WAL
and one connection by default. MVCC and overlapping explicit transactions are
also supported. Existing SQL memory profiles are unchanged.

## Investigate with dhat

```bash
cargo run -p memory-benchmark --features fts --bin fts-memory -- \
  --query common --state warm --documents 10000 --queries 100 \
  --dhat-file /tmp/fts-common-warm.json > /tmp/fts-common-warm-report.json

python3 perf/memory/analyze-dhat.py /tmp/fts-common-warm.json --modules --top 20
python3 perf/memory/analyze-dhat.py /tmp/fts-common-warm.json --filter tantivy --stacks
python3 perf/memory/analyze-dhat.py /tmp/fts-common-warm.json --sort-by eb --top 20
```

The command above uses a debug build for correctness checks. For representative
allocation investigations, use `cargo run --profile bench-profile` with the same
arguments. This workspace profile enables optimization and debug information
without LTO, preserving more useful allocation stacks. Do not compare debug
allocation numbers with optimized CI results.

Each invocation runs one query case and prints a JSON report to stdout. dhat
writes allocation stacks to `--dhat-file` (default `dhat-heap.json`). Use distinct
paths to preserve reports from multiple runs. The temporary database is removed
when the process exits normally.

| Option | Values / default |
|--------|------------------|
| `--query` | `rare`, `common` (default), `and`, `or`, `phrase`, `ranked` |
| `--state` | `first`, `warm` (default) |
| `--documents` | Positive document count; default 10000; try 1000, 10000, 100000 |
| `--queries` | Queries per connection in autocommit mode; defaults to 1 for first-query runs, 100 for warmed runs |
| `--mode` | `wal` (default), `mvcc` |
| `--connections` | Positive connection count; default 1 |
| `--transactions-per-connection` | Enable explicit transactions and repeat this many per connection; conflicts with `--queries` |
| `--queries-per-transaction` | Queries inside each explicit transaction; default 1; requires `--transactions-per-connection` |
| `--dhat-file` | Output allocation profile path; default `dhat-heap.json` |

Setup inserts the corpus in 500-document transactions, builds the index over
the populated table, and checks that every query plan uses the FTS index method.
Each session reopens the database.
Additional connections share that reopened database handle.
`first` measures exactly one query per connection without warm-up; explicit
transactions must also have a count of one per connection in this state.
`warm` executes the selected query once per connection before measurement,
inside a complete unmeasured transaction when explicit transactions are selected.
Measured work reuses those connections. Neither state claims a cold OS cache.

Both runners include SQL preparation, execution, and draining the result rows.
They accumulate a row count and ID sum, not a result vector. Database opening,
indexing, warm-up, connection destruction, and database cleanup are outside the
measured region. The corpus is synthetic: common terms occur in every document,
rare terms in 1%, and alpha/beta terms in overlapping subsets. The phrase case
distinguishes ordered from reversed words. Ranked queries score alpha/beta
matches and return the best ten.

The JSON report contains total allocation bytes/count, per-query averages,
peak live query bytes, and query bytes retained while the session is still open.
`query_heap` records live tracked bytes after each batch: one query per connection
in autocommit mode, or one complete transaction per connection in explicit mode.
Samples include cumulative completed query and transaction counts. Compare
`--queries 1`, `10`, and `100` at a fixed corpus size to investigate accumulation.

Heap measurement starts after setup and warm-up. These numbers describe only
allocations tracked from that point, not the full live database/cache footprint.
If a pre-existing allocation is resized during measurement, dhat records the
resized allocation. Retained bytes are not automatically leaks: they can be
caches that remain live until the connection closes.

`rss_before_query_bytes` and `rss_after_profiler_bytes` are supporting process
snapshots, not a query peak. The latter includes dhat stack collection and report
generation overhead, which can be large. Use the heap metrics for query analysis.

## Measure concurrent transaction lifecycles

```bash
cargo run -p memory-benchmark --features fts --bin fts-memory -- \
  --mode mvcc --connections 2 --query common --documents 1000 \
  --transactions-per-connection 10 --queries-per-transaction 2 \
  --dhat-file /tmp/fts-transactions.json \
  > /tmp/fts-transactions-report.json 2> /tmp/fts-transactions-events.log
```

This measures 20 transactions and 40 FTS queries. Every transaction includes
`BEGIN CONCURRENT`, all its queries, and `COMMIT`. WAL uses `BEGIN` instead.
The original autocommit benchmarks remain separate.

Within each round, the runner begins all transactions and verifies that every
connection is out of autocommit before starting query workers. Query workers
run concurrently; all workers finish before any connection commits. Begin and
commit statements run sequentially across connections, inside measurement.
This guarantees overlapping transactions even when queries complete immediately.
Connections persist across rounds. On a query error, workers are stopped and
joined before active transactions are rolled back.

The report includes actual completed `transactions`, total `queries`, and
`max_active_transactions` observed before query execution. Counts exclude warm-up.
Heap metrics aggregate all connections, including worker scheduling and transaction
overhead; per-query averages therefore include amortized begin/commit costs.
Concurrent allocation order can vary between runs, especially peak live bytes;
compare repeated runs rather than treating one peak as deterministic.
Use one connection to compare repeated transactions without overlap, and two or
four to compare simultaneous snapshot retention. These workloads remain read-only.

## Investigate indexes larger than the caches

The large corpus adds `--extra-tokens` deterministic pseudorandom terms per
document. They enlarge the term dictionaries and postings without changing the
existing query matches. This is a high-vocabulary stress case, not a natural-language
corpus. `--cache-pages` sets the page cache on every measured connection (minimum
200 pages). `--min-index-bytes` rejects an undersized index before profiling.
Defaults are zero extra tokens, the engine's page-cache default, and no size minimum.

```bash
cargo run --profile bench-profile -p memory-benchmark --features fts --bin fts-memory -- \
  --documents 20000 --extra-tokens 1024 --cache-pages 200 \
  --min-index-bytes 268435456 --mode mvcc --connections 2 \
  --query common --transactions-per-connection 3 --queries-per-transaction 2 \
  --dhat-file /tmp/fts-large.json > /tmp/fts-large-report.json
```

Setup uses the core `test_helper` backing-row reader to measure actual stored
segment chunk bytes, not database size or source-text size. It reports
`index.segment_bytes`, `segments`, `largest_segment_bytes`, `page_size`, and
`configured_cache_pages`, both in stdout and an `event: "index"` stderr record.
Inspection connections close before measured connections open. This inspection,
including byte checksums, is outside profiling and can take time for large indexes.

The minimum above is 256 MiB, exceeding the current 192 MiB shared retained-segment
budget, the 64 MiB writer arena, and the configured 200-page cache (800 KiB with
4096-byte pages). The writer arena and 1000-document flush threshold apply during
setup, not query measurement. The report's segment count and maximum size distinguish
many small segments from one oversized segment.

An index above 192 MiB does **not** prove query memory is bounded or that every
query reloads the index. The engine retains the newest segment even when it exceeds
the budget, live cursors load all visible segments, and cached searchers may keep
segment data alive. The four-searcher limit counts distinct visible segment/tombstone
sets; repeated read-only transactions do not exercise snapshot churn. Query-string
size/depth limits are validation limits and are not part of this memory workload.
Compare first-query and warmed runs; warm dhat metrics exclude cache allocations
made during warm-up, even when those allocations remain live.

The same large workload is available to Divan/CodSpeed behind a separate feature:

```bash
cargo bench --profile bench-profile -p memory-benchmark-codspeed \
  --features fts-stress --bench fts_queries -- oversized_index
```

It runs one and two MVCC connections, three transactions each, two queries per
transaction, with one sample by default. To use CodSpeed, build with
`--features codspeed,fts-stress` and filter its runner to `oversized_index`.
CI enables this feature and runs it in the separate `fts-large-index` shard;
the `fts-queries` shard runs only the smaller cases. The build job also runs the
FTS workload and CLI tests. Local multi-connection Divan
allocation counts remain incomplete; use dhat for all-thread totals.

## Consume explicit workload phases

`FtsWorkload` owns the transitions, and `DhatObserver` controls the profiler:

```text
setup -> open -> warmup -> run -> cleanup -> done
                          |       |
                      start dhat  stop dhat before dropping connections
```

Each transition emits a JSON object to stderr, for example:

```json
{"event":"phase","phase":"run","elapsed_ms":123}
```

The final stdout report repeats these events in `phases`. Elapsed times include
profiler overhead and are not query timing measurements. `warmup` is emitted even
when `--state first` skips warm-up work. `done` means cleanup finished; check the
process exit status for success. Phase logging and report serialization are
outside the measured region.

stderr also contains dhat diagnostics and, with `cargo run`, build messages.
Extract just phase events without parsing those messages:

```bash
jq -Rc 'fromjson? | select(.event == "phase")' /tmp/fts-transactions-events.log
```

The observer interface also provides `after_batch` for in-process measurement.
It runs after workers finish and, for explicit transactions, after all commits.
Divan uses the same preparation and run methods with a no-op observer; setup
and input destruction remain outside its measured closure.

## Track regressions with Divan and CodSpeed

```bash
cargo test -p memory-benchmark --features fts
cargo test -p memory-benchmark-codspeed --features fts --bench fts_queries -- --test

cargo codspeed build -m memory -p memory-benchmark-codspeed --features codspeed,fts
cargo codspeed run -m memory -p memory-benchmark-codspeed --bench fts_queries
```

The first two commands check correctness without collecting representative
performance numbers. CodSpeed measurement requires its runner; the GitHub
`CodSpeed Memory` workflow includes an `fts-queries` shard. It does not install
the dhat allocator in the Divan executable.

For local allocation output, the target installs Divan's system-backed
`AllocProfiler` when the `codspeed` feature is off:

```bash
cargo bench --profile bench-profile -p memory-benchmark-codspeed \
  --features fts --bench fts_queries -- transactions --sample-count 3 --sample-size 1
```

Divan's `alloc` row counts allocation calls; realloc growth is reported separately
under `grow`. Add their bytes for growth-inclusive allocation pressure. `max alloc`
is Divan's peak live bytes/count on the measured thread. These values cover a whole
benchmark sequence, not one query. Divan does not collect allocations on Tokio worker
threads: its figures for two/four connections are incomplete. Use `fts-memory`
with the same configuration for process-wide concurrent heap metrics. Do not
compare partial local Divan numbers with CodSpeed's memory instrument as though
they have the same scope. Single-connection queries run on the measured thread.

Each query case has six configurations, named `(state, documents, queries)`:
first and warmed queries at 1000 and 10000 documents, plus 10-query and 100-query
warmed runs at 10000 documents. CodSpeed reports allocations for the entire
configuration, so divide by its query count for a per-query comparison.

There are also 24 `transactions` cases: every query case at 1000 documents, ten
transactions per connection, and two queries per transaction, using WAL with
one connection or MVCC with one, two, and four connections. Names encode
`(mode, connections, transactions_per_connection, queries_per_transaction)`.

Divan's input generator creates the fixture, opens connections, and performs any
warm-up outside the measured closure. Each input is used for one measured query
or transaction sequence and dropped after measurement. The same boundaries apply
under CodSpeed instrumentation.
Existing Criterion profiles retain their separate harness and measurement rules.
