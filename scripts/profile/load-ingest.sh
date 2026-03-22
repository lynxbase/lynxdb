#!/usr/bin/env bash
# Ingest load generator — POST json_100k.log to /api/v1/ingest/raw.
# Usage: load-ingest.sh [duration_sec] [concurrency]
set -euo pipefail

ADDR="${LYNXDB_ADDR:-localhost:3100}"
DURATION="${1:-60}"
CONCURRENCY="${2:-4}"
FIXTURE="${3:-testdata/bench/json_100k.log}"

if [ ! -f "$FIXTURE" ]; then
    echo "ERROR: fixture not found: $FIXTURE" >&2
    echo "Run: make bench-fixtures" >&2
    exit 1
fi

echo "Ingest load: file=$FIXTURE, duration=${DURATION}s, concurrency=$CONCURRENCY"
echo "Target: http://$ADDR/api/v1/ingest/raw"

if command -v hey &>/dev/null; then
    hey -c "$CONCURRENCY" -z "${DURATION}s" \
        -m POST \
        -H "X-Source: bench" \
        -H "X-Source-Type: json" \
        -D "$FIXTURE" \
        "http://$ADDR/api/v1/ingest/raw"
else
    echo "hey not found, falling back to curl loop"
    END=$(($(date +%s) + DURATION))
    while [ "$(date +%s)" -lt "$END" ]; do
        for _ in $(seq 1 "$CONCURRENCY"); do
            curl -sf -X POST "http://$ADDR/api/v1/ingest/raw" \
                -H "X-Source: bench" -H "X-Source-Type: json" \
                --data-binary @"$FIXTURE" -o /dev/null &
        done
        wait
    done
fi
