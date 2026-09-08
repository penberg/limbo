# TPC-H Benchmarks

Performance comparison of Turso vs SQLite using the [TPC-H](http://www.tpc.org/tpch/) benchmark queries against a 1.2 GB database.

## Quickstart

```console
./scripts/run.sh
```

That builds `tursodb` in release mode, installs a local `sqlite3`,
downloads the 1.2 GB TPC-H database if it is not there yet, times every
query on both engines with the page cache dropped before each one, and
draws the figure. It takes about ten minutes and asks for sudo once.
You need Rust, `uv` for the plot, and `wget` or `curl` for the download.

## What it writes

Everything goes into `plot/`:

- `results_<timestamp>.txt`, the raw timings as one CSV block.
- `results.csv`, the same block converted by `plot/results2csv.sh` for
  the plot script.
- `tpch.png`, `tpch.pdf` and `tpch.tikz`, a grouped bar chart of
  per-query runtime for Limbo and SQLite on a log scale, in the same style
  as the `perf/latency` and `perf/throughput` plots. The `.tikz` is a
  pgfplots picture to `\input` into a LaTeX document that loads pgfplots
  with `\pgfplotsset{compat=1.18}`. A query an engine did not run is
  marked `n/a` in the table under the bars.

## Running the benchmark on its own

`benchmark.sh` builds, downloads and times the queries without drawing
anything, which is what CI does:

```bash
./perf/tpc-h/benchmark.sh
```

It writes `perf/tpc-h/results_<timestamp>.txt`, and unlike
`scripts/run.sh` it times every query a second time after `ANALYZE`,
which takes as long again, and appends those timings and the difference
between the two passes. `ANALYZE=0` skips the second pass and
`RESULTS_FILE` names the output file.

To draw a figure from an existing results file:

```bash
cd perf/tpc-h/plot
./results2csv.sh ../results_20260216_143000.txt > results.csv
uv run plot-tpch.py results.csv
```

Pass `analyze` after the file name to convert the `ANALYZE` block, and
`--name limbo=Turso` to the plot script to change an engine's legend name.

## Comparing two builds

`compare.sh` times every query on two `tursodb` binaries and prints the
difference, for checking a change against main:

```bash
./perf/tpc-h/compare.sh /path/to/main/tursodb
```
