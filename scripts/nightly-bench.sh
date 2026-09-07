#!/bin/bash
# nightly-bench.sh - run the nightly Criterion suite one benchmark binary per
# workflow step, so that a benchmark that takes the runner down can be named.
#
# Usage:
#   scripts/nightly-bench.sh build             # compile every bench binary
#   scripts/nightly-bench.sh check <workflow>  # every built bench has a step
#   scripts/nightly-bench.sh run <bench>       # run one bench, append output.txt
#
# The nightly runner has 4 CPUs and 8 GiB of RAM and no swap. A benchmark that
# grows past that starts thrashing, and the GitHub runner agent stops answering
# the server before the kernel gets around to killing the benchmark. GitHub
# then reports "the self-hosted runner lost communication with the server" and
# throws the whole job log away, so nothing says which benchmark it was.
#
# Two things here make that failure honest instead of silent:
#   - `run` puts an address-space limit on the benchmark, so a benchmark that
#     outgrows the machine dies with an allocation failure while the runner is
#     still healthy enough to upload the log.
#   - Each benchmark is its own workflow step. GitHub records when every step
#     started and finished even when it loses the log, so the step that never
#     finished is the culprit.
#
# `check` keeps the step list in the workflow file in sync with the bench
# targets cargo actually builds: a new bench with no step fails the job.
set -euo pipefail

cd "$(dirname "$0")/.."
target_dir=${CARGO_TARGET_DIR:-target}
# The bench job runs on a fresh checkout with no cargo cache, so the target
# directory the bookkeeping files below are written to does not exist yet.
mkdir -p "$target_dir"

# Same target list as `make bench-exclude-tpc-h`.
bench_targets() {
    make -s codspeed-benchmarks-exclude-tpc-h
}

# Bench binaries cargo built. Feature unification across the workspace can
# turn on features like `fts`, so this is the real list, not Cargo.toml's.
built_benches() {
    jq -r 'select(.reason == "compiler-artifact" and .executable != null
                  and (.target.kind | index("bench")))
           | .target.name' "$target_dir/nightly-bench-build.json" | sort -u
}

case "${1:-}" in
build)
    args=()
    while read -r name; do
        args+=(--bench "$name")
    done < <(bench_targets)
    cargo bench --no-run --features bench "${args[@]}" \
        --message-format=json-render-diagnostics > "$target_dir/nightly-bench-build.json"
    echo "Built bench binaries:"
    built_benches | sed 's/^/  /'
    ;;
check)
    workflow=$2
    grep -oE 'nightly-bench\.sh run [A-Za-z0-9_]+' "$workflow" | awk '{print $3}' | sort -u > "$target_dir/nightly-bench-steps.txt"
    built_benches | grep -vx tpc_h_benchmark > "$target_dir/nightly-bench-built.txt"
    if ! diff -u "$target_dir/nightly-bench-steps.txt" "$target_dir/nightly-bench-built.txt"; then
        echo "error: the bench steps in $workflow do not match the bench binaries cargo built (see diff above)." >&2
        echo "Add a 'scripts/nightly-bench.sh run <name>' step for every '+' line and drop the steps for every '-' line." >&2
        exit 1
    fi
    ;;
run)
    name=$2
    # Address-space cap in KiB. Well under the 8 GiB runner, well above the
    # ~2.5 GiB the biggest benchmark reaches.
    cap_kb=${NIGHTLY_BENCH_MEMORY_CAP_KB:-6291456}
    echo "free -m before $name:"
    free -m
    ulimit -v "$cap_kb"
    cargo bench --bench "$name" --features bench 2>&1 | tee -a output.txt
    echo "free -m after $name:"
    free -m
    ;;
*)
    echo "usage: $0 build | check <workflow-file> | run <bench-name>" >&2
    exit 2
    ;;
esac
