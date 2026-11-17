#!/usr/bin/env bash
# Query load generator — sequential loop, 1 query every 500ms.
# Each query targets a different subsystem.
# Usage: load-query.sh [duration_sec]
set -euo pipefail

ADDR="${LYNXDB_ADDR:-localhost:3100}"
DURATION="${1:-60}"

QUERIES=(
    # Filter and grouped aggregate in the columnar executor
    '{"q": "source=main status>=500 | stats count() as n, avg(duration) as avg_ms by path | sort - n | head 10"}'
    # Phrase search through the token filters
    '{"q": "source=main \"cache miss\" | head 20"}'
    # Time buckets
    '{"q": "source=main | stats count() by span(_time, 5m)"}'
    # Several aggregates, sketches included
    '{"q": "source=main | stats count(), avg(duration), p99(duration), dc(path) by source"}'
    # Early stop on a limit
    '{"q": "source=main | head 5"}'
    # Regex extraction in the row operators
    '{"q": "source=main | rex field=path \"/api/v1/(?<resource>[a-z]+)\" | stats count() by resource"}'
    # Computed columns over an aggregate
    '{"q": "source=main | eval is_error = if(status >= 500, 1, 0) | stats sum(is_error) as errors, count() as total | eval error_rate = round(errors * 100.0 / total, 2)"}'
)

NUM_QUERIES=${#QUERIES[@]}
END=$(($(date +%s) + DURATION))
COUNT=0
ERRORS=0

echo "Query load: ${NUM_QUERIES} queries cycling, duration=${DURATION}s, interval=500ms"
echo "Target: http://$ADDR/api/v1/query"

while [ "$(date +%s)" -lt "$END" ]; do
    IDX=$((COUNT % NUM_QUERIES))
    QUERY="${QUERIES[$IDX]}"

    HTTP_CODE=$(curl -sf -o /dev/null -w "%{http_code}" \
        -X POST "http://$ADDR/api/v1/query" \
        -H "Content-Type: application/json" \
        -d "$QUERY" 2>/dev/null || echo "000")

    if [ "$HTTP_CODE" != "200" ]; then
        ERRORS=$((ERRORS + 1))
    fi

    COUNT=$((COUNT + 1))
    sleep 0.5
done

echo "Query load complete: ${COUNT} queries, ${ERRORS} errors"
