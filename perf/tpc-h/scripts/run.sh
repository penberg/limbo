#!/bin/sh
# Runs the whole benchmark with its defaults and draws the figure. Takes
# about ten minutes and asks for sudo once, to drop the page cache before
# every query. The second pass after ANALYZE that benchmark.sh makes on
# its own is skipped, since the figure only shows the first one.
set -eu

HERE="$(cd "$(dirname "$0")/.." && pwd)"
OUT=${OUT:-"$HERE/plot"}
mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"
RESULTS_FILE=${RESULTS_FILE:-"$OUT/results_$(date +%Y%m%d_%H%M%S).txt"}
ANALYZE=0
export RESULTS_FILE ANALYZE

"$HERE/benchmark.sh"

command -v uv > /dev/null || {
  echo "uv is needed to draw the figure: https://docs.astral.sh/uv/" >&2
  exit 1
}
cd "$OUT"
"$HERE/plot/results2csv.sh" "$RESULTS_FILE" > results.csv
uv run "$HERE/plot/plot-tpch.py" results.csv
