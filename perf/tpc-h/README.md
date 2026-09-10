# TPC-H Benchmarks

Performance comparison of Turso vs SQLite using the [TPC-H](http://www.tpc.org/tpch/) benchmark queries against a 1.2 GB database.

## Quickstart

```console
./scripts/run.sh
```

That builds `tursodb` in release mode, installs a local `sqlite3`,
downloads the 1.2 GB TPC-H database if it is not there yet, times every
query on both engines with the page cache dropped before each one, five
passes over all the queries, and draws the figure. `tursodb` runs with
the io_uring backend, since the CLI defaults to plain syscalls. It takes
about half an hour and asks for sudo once. You need Rust, `uv` for the plot, and `wget`
or `curl` for the download. `REPEATS=1 ./scripts/run.sh` makes one pass
for a quick look.

## What it writes

Everything goes into `plot/`:

- `results_<timestamp>-r<pass>.txt`, the raw timings of one pass as one
  CSV block.
- `results-r<pass>.csv`, the same block converted by `plot/results2csv.sh`
  for the plot script.
- `tpch.png`, `tpch.pdf` and `tpch.tikz`, a grouped bar chart of
  per-query runtime for Limbo and SQLite on a log scale, in the same style
  as the `perf/latency` and `perf/throughput` plots. Each bar is the
  median over the passes and its whiskers reach the fastest and the
  slowest one. The `.tikz` is a pgfplots picture to `\input` into a LaTeX
  document that loads pgfplots with `\pgfplotsset{compat=1.18}`. A query
  an engine did not run is marked `n/a` in the table under the bars.

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

To draw a figure from existing results files, convert each one and give
the plot script all of them; with a single file the bars have no whiskers:

```bash
cd perf/tpc-h/plot
./results2csv.sh ../results_20260216_143000.txt > results-r1.csv
./results2csv.sh ../results_20260216_151200.txt > results-r2.csv
uv run plot-tpch.py results-r1.csv results-r2.csv
```

Pass `analyze` after the file name to convert the `ANALYZE` block, and
`--name limbo=Turso` to the plot script to change an engine's legend name.

## Comparing two builds

`compare.sh` times every query on two `tursodb` binaries and prints the
difference, for checking a change against main:

```bash
./perf/tpc-h/compare.sh /path/to/main/tursodb
```
