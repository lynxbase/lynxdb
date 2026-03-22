#!/usr/bin/env bash
# Collect all profile types from a running LynxDB server.
# Run WHILE load is active for meaningful CPU/mutex/block profiles.
# Usage: collect-profiles.sh [output_dir]
set -euo pipefail

ADDR="${LYNXDB_ADDR:-localhost:3100}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTDIR="${1:-artifacts/profiles/$TIMESTAMP}"
mkdir -p "$OUTDIR"

BASE="http://$ADDR/api/v1/debug/pprof"

echo "Collecting profiles to $OUTDIR"

# 1. CPU profile (30 seconds)
echo "  [1/8] CPU profile (30s)..."
curl -sf -o "$OUTDIR/cpu.pb.gz" "$BASE/profile?seconds=30" &
CPU_PID=$!

# While CPU is collecting, grab instant profiles:

# 2. Heap profile (current live allocations)
echo "  [2/8] Heap profile..."
curl -sf -o "$OUTDIR/heap.pb.gz" "$BASE/heap"

# 3. Allocs profile (cumulative allocations since start)
echo "  [3/8] Allocs profile..."
curl -sf -o "$OUTDIR/allocs.pb.gz" "$BASE/allocs"

# 4. Goroutine profile (binary, for pprof)
echo "  [4/8] Goroutine profile..."
curl -sf -o "$OUTDIR/goroutine.pb.gz" "$BASE/goroutine"

# 5. Goroutine full dump (debug=2, text, for manual inspection)
echo "  [5/8] Goroutine full dump (debug=2)..."
curl -sf -o "$OUTDIR/goroutine_full.txt" "$BASE/goroutine?debug=2"

# 6. Mutex profile
echo "  [6/8] Mutex profile..."
curl -sf -o "$OUTDIR/mutex.pb.gz" "$BASE/mutex"

# 7. Block profile
echo "  [7/8] Block profile..."
curl -sf -o "$OUTDIR/block.pb.gz" "$BASE/block"

# Wait for CPU profile to finish
wait $CPU_PID
echo "  [8/8] CPU profile collected."

# 8. Execution trace (5 seconds)
echo "  [+] Execution trace (5s)..."
curl -sf -o "$OUTDIR/trace.out" "$BASE/trace?seconds=5"

# Prometheus metrics snapshot
echo "  [+] Prometheus metrics..."
curl -sf -o "$OUTDIR/metrics.txt" "http://$ADDR/metrics"

# Storage metrics snapshot
echo "  [+] Storage metrics..."
curl -sf -o "$OUTDIR/storage-metrics.json" "http://$ADDR/api/v1/metrics"

# Server stats
echo "  [+] Server stats..."
curl -sf -o "$OUTDIR/stats.json" "http://$ADDR/api/v1/stats"

echo ""
echo "Done. Profiles saved to: $OUTDIR"
echo ""
echo "Quick analysis:"
echo "  go tool pprof -http=:8080 $OUTDIR/cpu.pb.gz"
echo "  go tool pprof -http=:8081 $OUTDIR/heap.pb.gz"
echo "  go tool trace $OUTDIR/trace.out"

# Output the directory path for callers
echo "$OUTDIR"
