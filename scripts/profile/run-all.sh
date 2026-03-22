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

# --- Step 0: Kill any stale server on the same port ---
if curl -sf "$BASE/health" >/dev/null 2>&1; then
    echo "WARNING: Server already running on $ADDR. Killing it..."
    pkill -f "lynxdb server" 2>/dev/null || true
    sleep 2
fi

# --- Step 1: Build ---
echo "=== Step 1: Build ==="
make build
echo ""

# --- Step 2: Generate fixtures if missing ---
if [ ! -f testdata/bench/json_100k.log ]; then
    echo "=== Step 2: Generate bench fixtures ==="
    make bench-fixtures
    echo ""
fi

# --- Step 3: Start server ---
echo "=== Step 3: Start server (profile-runtime, no-ui) ==="
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

# --- Step 4: Seed 2M events ---
echo "=== Step 4: Seed 2M events (20 x 100K) ==="
for i in $(seq 1 20); do
    curl -sf -X POST "$BASE/api/v1/ingest/raw" \
        -H "X-Source: bench" -H "X-Source-Type: json" \
        --data-binary @testdata/bench/json_100k.log -o /dev/null
    printf "  Batch %d/20\r" "$i"
done
echo "  Seed complete: 2M events ingested."
echo ""

# --- Step 5: Wait for compaction ---
echo "=== Step 5: Wait for segments + compaction ==="
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

# --- Step 6: Heap baseline ---
echo "=== Step 6: Heap baseline (before load) ==="
curl -sf -o "$OUTDIR/heap_before.pb.gz" "$BASE/api/v1/debug/pprof/heap"
echo "  Saved heap_before.pb.gz"
echo ""

# --- Step 7: Start mixed load ---
echo "=== Step 7: Start mixed load (60s) ==="
echo "  Ingest: hey -c 4 -z 60s (or curl fallback)"
echo "  Query:  sequential, 1 per 500ms"

bash "$SCRIPT_DIR/load-ingest.sh" 60 4 >"$OUTDIR/load-ingest.log" 2>&1 &
INGEST_PID=$!

bash "$SCRIPT_DIR/load-query.sh" 60 >"$OUTDIR/load-query.log" 2>&1 &
QUERY_PID=$!

# --- Step 8: Collect profiles (after 5s stabilization) ---
sleep 5
echo ""
echo "=== Step 8: Collect profiles ==="
bash "$SCRIPT_DIR/collect-profiles.sh" "$OUTDIR"
echo ""

# Wait for load to finish
echo "  Waiting for load generators..."
wait $INGEST_PID 2>/dev/null || true
wait $QUERY_PID 2>/dev/null || true
echo "  Load complete."
echo ""

# --- Step 9: Heap after load ---
echo "=== Step 9: Heap snapshot (after load) ==="
curl -sf -o "$OUTDIR/heap_after.pb.gz" "$BASE/api/v1/debug/pprof/heap"
echo "  Saved heap_after.pb.gz"
echo ""

# --- Step 10: Analyze ---
echo "=== Step 10: Analyze profiles ==="
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
