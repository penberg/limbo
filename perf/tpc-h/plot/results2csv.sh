#!/bin/bash
# Convert a TPC-H results_*.txt file to results.csv for plotting.
# Usage: ./results2csv.sh <results_file> [mode]
# mode: "no-analyze" (default) or "analyze"

set -e

if [ -z "$1" ]; then
    echo "Usage: $0 <results_file> [analyze|no-analyze]" >&2
    exit 1
fi

RESULTS_FILE="$1"
MODE="${2:-no-analyze}"

if [ "$MODE" = "analyze" ]; then
    SECTION="MODE: WITH ANALYZE"
else
    SECTION="MODE: WITHOUT ANALYZE"
fi

echo "Query,Limbo,SQLite"

in_section=false
while IFS= read -r line; do
    if [[ "$line" == "$SECTION" ]]; then
        in_section=true
        continue
    fi
    # Stop at the next section
    if $in_section && [[ "$line" == MODE:* || "$line" == DIFF:* ]]; then
        break
    fi
    if $in_section && [[ "$line" == *.sql,* ]]; then
        query=$(echo "$line" | cut -d, -f1 | sed 's/\.sql$//' | sed 's/^/Q/')
        turso=$(echo "$line" | cut -d, -f2)
        sqlite=$(echo "$line" | cut -d, -f3)
        echo "$query,$turso,$sqlite"
    fi
done < "$RESULTS_FILE"
