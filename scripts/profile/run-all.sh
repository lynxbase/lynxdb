#!/usr/bin/env bash
# Master profiling orchestrator.
# Builds, starts server, seeds data, runs load, collects profiles, analyzes.
# Usage: scripts/profile/run-all.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

ADDR="${LYNXDB_ADDR:-localhost:3100}"
BASE="http://$ADDR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTDIR="artifacts/profiles/$TIMESTAMP"
mkdir -p "$OUTDIR"

cleanup() {
    if [ -n "${SERVER_PID:-}" ]; then
        echo "Stopping server (PID=$SERVER_PID)..."
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    # Kill any remaining background jobs
    jobs -p | xargs kill 2>/dev/null || true
}
trap cleanup EXIT

echo "========================================="
echo "  LynxDB Profiling Run — $TIMESTAMP"
echo "========================================="
echo ""

# Kill any stale server on the same port
if curl -sf "$BASE/health" >/dev/null 2>&1; then
    echo "WARNING: Server already running on $ADDR. Killing it..."
    pkill -f "lynxdb server" 2>/dev/null || true
    sleep 2
fi

echo "=== Build ==="
make build
echo ""

# Generate fixtures if missing
if [ ! -f testdata/bench/json_100k.log ]; then
    echo "=== Generate bench fixtures ==="
    make bench-fixtures
    echo ""
fi

# Start server
echo "=== Start server (profile-runtime, no-ui) ==="
GODEBUG=gctrace=1 ./lynxdb server \
    --addr "$ADDR" \
    --no-ui \
    --log-level info \
    --profile-runtime \
    >"$OUTDIR/server-stdout.log" \
    2>"$OUTDIR/gctrace.log" &
SERVER_PID=$!
echo "  Server PID=$SERVER_PID"

# Wait for health
echo "  Waiting for server..."
for i in $(seq 1 30); do
    if curl -sf "$BASE/health" >/dev/null 2>&1; then
        echo "  Server ready."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "ERROR: Server failed to start within 30s" >&2
        cat "$OUTDIR/server-stdout.log" >&2
        exit 1
    fi
    sleep 1
done
echo ""

# Seed 2M events
echo "=== Seed 2M events (20 x 100K) ==="
for i in $(seq 1 20); do
    curl -sf -X POST "$BASE/api/v1/ingest/raw" \
        -H "X-Source: bench" -H "X-Source-Type: json" \
        --data-binary @testdata/bench/json_100k.log -o /dev/null
    printf "  Batch %d/20\r" "$i"
done
echo "  Seed complete: 2M events ingested."
echo ""

# Wait for compaction
echo "=== Wait for segments + compaction ==="
for i in $(seq 1 60); do
    SEGMENTS=$(curl -sf "$BASE/api/v1/stats" | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['segment_count'])" 2>/dev/null || echo "0")
    if [ "$SEGMENTS" -gt 3 ] 2>/dev/null; then
        echo "  Segments: $SEGMENTS (>3, compaction likely ran)"
        break
    fi
    if [ "$i" -eq 60 ]; then
        echo "  Warning: Only $SEGMENTS segments after 60s. Continuing anyway."
    fi
    sleep 1
done
echo ""

# Heap baseline
echo "=== Heap baseline (before load) ==="
curl -sf -o "$OUTDIR/heap_before.pb.gz" "$BASE/api/v1/debug/pprof/heap"
echo "  Saved heap_before.pb.gz"
echo ""

# Start mixed load
echo "=== Start mixed load (60s) ==="
echo "  Ingest: hey -c 4 -z 60s (or curl fallback)"
echo "  Query:  sequential, 1 per 500ms"

bash "$SCRIPT_DIR/load-ingest.sh" 60 4 >"$OUTDIR/load-ingest.log" 2>&1 &
INGEST_PID=$!

bash "$SCRIPT_DIR/load-query.sh" 60 >"$OUTDIR/load-query.log" 2>&1 &
QUERY_PID=$!

# Collect profiles (after 5s stabilization)
sleep 5
echo ""
echo "=== Collect profiles ==="
bash "$SCRIPT_DIR/collect-profiles.sh" "$OUTDIR"
echo ""

# Wait for load to finish
echo "  Waiting for load generators..."
wait $INGEST_PID 2>/dev/null || true
wait $QUERY_PID 2>/dev/null || true
echo "  Load complete."
echo ""

# Heap snapshot after load
echo "=== Heap snapshot (after load) ==="
curl -sf -o "$OUTDIR/heap_after.pb.gz" "$BASE/api/v1/debug/pprof/heap"
echo "  Saved heap_after.pb.gz"
echo ""

# Analyze profiles
echo "=== Analyze profiles ==="
bash "$SCRIPT_DIR/analyze.sh" "$OUTDIR"
echo ""

# --- Done ---
echo "========================================="
echo "  Profiling complete: $OUTDIR"
echo "========================================="
echo ""
echo "Files:"
ls -lh "$OUTDIR/" | grep -v '^total'
echo ""
echo "Interactive exploration:"
echo "  go tool pprof -http=:8080 $OUTDIR/cpu.pb.gz"
echo "  go tool pprof -http=:8081 $OUTDIR/heap.pb.gz"
echo "  go tool pprof -http=:8082 -sample_index=alloc_objects $OUTDIR/allocs.pb.gz"
echo "  go tool trace $OUTDIR/trace.out"
echo "  cat $OUTDIR/FINDINGS.md"
