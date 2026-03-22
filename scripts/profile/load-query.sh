#!/usr/bin/env bash
# Query load generator — sequential loop, 1 query every 500ms.
# Each query targets a different subsystem.
# Usage: load-query.sh [duration_sec]
set -euo pipefail

ADDR="${LYNXDB_ADDR:-localhost:3100}"
DURATION="${1:-60}"

QUERIES=(
    # Bloom filter + column scan path
    '{"q": "source=api-gw status>=500 | stats count, avg(duration_ms) by path | sort -count | head 10"}'
    # Full-text inverted index path
    '{"q": "search \"connection refused\" | head 20"}'
    # Time bucketing + aggregation
    '{"q": "| bin _time span=5m | stats count by _time"}'
    # Heavy multi-aggregation (tests aggregation operator memory)
    '{"q": "| stats count, avg(duration_ms), p99(duration_ms), dc(path) by source"}'
    # Short-circuit (tests head pushdown)
    '{"q": "| head 5"}'
    # Rex extraction (regex engine hot path)
    '{"q": "| rex field=_raw \"status=(?P<code>\\\\d+)\" | stats count by code"}'
    # Eval + aggregate combo
    '{"q": "| eval is_error=if(status>=500,1,0) | stats sum(is_error) as errors, count as total | eval error_rate=round(errors/total*100,2)"}'
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
