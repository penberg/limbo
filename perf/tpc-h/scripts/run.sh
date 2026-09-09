#!/bin/sh
# Runs the whole benchmark with its defaults and draws the figure: five
# passes over every query on both engines, so the figure gets a whisker on
# each bar from the fastest to the slowest run. Takes about half an hour
# and asks for sudo once, to drop the page cache before every query. The
# second pass after ANALYZE that benchmark.sh makes on its own is skipped,
# since the figure only shows the first one. `REPEATS=1 ./scripts/run.sh`
# for a quick look.
set -eu

HERE="$(cd "$(dirname "$0")/.." && pwd)"
OUT=${OUT:-"$HERE/plot"}
mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"
REPEATS=${REPEATS:-5}
SESSION="$(date +%Y%m%d_%H%M%S)"
ANALYZE=0
export ANALYZE

# Every pass runs all the queries before the next starts, so a query's runs
# are spread over the session instead of sitting back to back.
for run in $(seq 1 "$REPEATS"); do
  echo "pass $run of $REPEATS" >&2
  RESULTS_FILE="$OUT/results_$SESSION-r$run.txt" "$HERE/benchmark.sh"
done

command -v uv > /dev/null || {
  echo "uv is needed to draw the figure: https://docs.astral.sh/uv/" >&2
  exit 1
}
cd "$OUT"
csvs=""
for run in $(seq 1 "$REPEATS"); do
  "$HERE/plot/results2csv.sh" "results_$SESSION-r$run.txt" > "results-r$run.csv"
  csvs="$csvs results-r$run.csv"
done
uv run "$HERE/plot/plot-tpch.py" $csvs
