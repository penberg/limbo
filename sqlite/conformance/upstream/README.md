# SQLite TCL Compatibility Tests

This directory contains TCL-based tests that verify Turso's compatibility with SQLite behavior. The tests use a native TCL extension (`libturso_tcl`) that provides an in-process `sqlite3` command backed by the Turso engine.

## Prerequisites

- **TCL** (`tclsh`) installed on your system
- **TCL dev headers** (e.g., `tcl-dev` on Debian/Ubuntu, `tcl-tk` via Homebrew on macOS)
- **Rust toolchain** (for building the native extension)

## Building the Native Extension

Before running tests, build the `libturso_tcl` shared library:

```bash
make -C bindings/tcl
```

This will:
1. Build `turso_sqlite3` via Cargo
2. Compile `turso_tcl.c` into a shared library (`libturso_tcl.dylib` on macOS, `libturso_tcl.so` on Linux)

On Linux without local TCL dev headers, you can build inside Docker:

```bash
make -C bindings/tcl docker-build
```

## Running Tests

Run all tests:

```bash
./all.test
```

Run a single test file:

```bash
tclsh select1.test
```

## Test Structure

- `tester.tcl` — Test framework (loaded by all test files). Provides `do_test`, `do_execsql_test`, `do_catchsql_test`, and other helpers.
- `all.test` — Runner that sources all individual test files.
- `*.test` — Individual test files organized by SQL feature (e.g., `select1.test`, `insert.test`, `join.test`, `func.test`, `alter.test`).

## How a File Runs

`all.test` starts one `tclsh` process per file through `run_file.tcl`.
That wrapper sources the file and always prints the file's summary, even
when the file stops on a TCL error outside of a `do_test` (a missing
harness command, a setup statement the engine rejects). Such a stop is
reported as one extra failure named `FILE-ABORTED`, the tests that ran
before it are counted, and the file shows as `XABORT` in the table. The
run also lists every aborted file with the line and error at the end.

A file that runs longer than `file_timeout_seconds` (180 by default) is
killed and shows as `XHANG`; a file whose process dies on a signal shows
as `XCRASH`. Neither takes down the rest of the run.

The full output of each file is written to `logs/NAME.log`; the console
shows only each file's summary, failed-test list, and any abort, crash
or hang line, because the complete output of a run is tens of megabytes.
Set `TURSO_TCL_VERBOSE=1` to print everything. CI uploads the `logs`
directory as the `sqlite-tcl-logs` artifact.

`lock_common.tcl`, `malloc_common.tcl`, `bc_common.tcl`, `fuzz_common.tcl`
and `wal_common.tcl` are the upstream helper files. The one change is in
`lock_common.tcl`: the child processes it starts for multi-connection
tests are plain `tclsh`, so it makes them source `tester.tcl` (with
`TURSO_CHILD_PROCESS` set so they leave the parent's database alone). The
testfixture-only commands that upstream provides from C (`sqlite3_test_control`,
`testvfs`, `load_static_extension`, ...) are defined as no-ops in
`tester.tcl` so files that call them keep running; tests that depend on
their effect fail on their own assertions.

## Blessed and Known-Bad Files

`all.test` runs every test file on every invocation. Each file is listed in
the `test_files` table at the top of `all.test` with one of three statuses:

- `pass` — blessed: every test in the file must pass. Any failure fails the
  run.
- `fail` — known-bad: the file runs and its failures are printed, but they do
  not fail the run. If a known-bad file becomes fully green, the run fails
  with a request to bless it, so the known-bad list only ever shrinks.
- `hang` — known to run past the timeout. The file still runs, with the
  short `hang_timeout_seconds` limit (30 by default) instead of
  `file_timeout_seconds`, and shows as `XHANG`. If it completes, the run
  fails with a request to move it to `fail`.
- `skip` — not run at all. The meta-runner files (`full`, `quick`,
  `veryquick`), which would source `permutations.test` and run the whole
  suite again, and the fuzz family (`fuzz`, `fuzz-oss1`, `fuzz2`, `fuzz3`,
  `fuzz4`, `fuzzer1`, `fuzzer2`) until the CI failure they cause is
  understood. Every other test file runs; hangs and crashes are contained
  per file.

To bless a file after fixing its remaining failures, change its status from
`fail` to `pass` in `all.test`. A TCL error while sourcing a known-bad file
is contained and reported as `XCRASH` instead of aborting the whole run.
