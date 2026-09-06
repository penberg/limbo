# Join execution benchmark

This tool measures query execution without printing result rows. It reports one JSON record for each measured execution.

The records include elapsed time and stable work counters.
Use release builds for runtime tests so the engine uses its production settings.

Run the graph query set with analyzed statistics:

```bash
cargo run --release -p turso-join-benchmark -- \
  --database perf/graph-queries/graph-queries-analyzed.db \
  --query-dir perf/graph-queries/queries
```

Use `--filter cooccurrence` to select a small query set. Use `--repetitions 1` for a fast diagnostic run.

Use `--query 5 --query 9` to select exact file stems. This option prevents `--filter 1` from also selecting `10`.

Print the structured query plans without query execution:

```bash
cargo run --release -p turso-join-benchmark -- \
  --database perf/graph-queries/graph-queries-analyzed.db \
  --query-dir perf/graph-queries/queries \
  --plans
```

The tool uses warm-cache execution by default. Set `--warmups 0` to include the first execution.

Each query has a 30-second timeout by default. Set `--timeout-seconds 0` to disable the timeout.

Run selected TPC-H queries with a longer timeout:

```bash
cargo run --release -p turso-join-benchmark -- \
  --database perf/tpc-h/TPC-H.db \
  --query-dir perf/tpc-h/queries \
  --query 5 --query 9 \
  --timeout-seconds 300
```

See [RESULTS.md](RESULTS.md) for the first join optimizer study.
