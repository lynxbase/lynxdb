#!/usr/bin/env bash
# Analyze collected profiles and produce FINDINGS.md.
# Usage: analyze.sh <profile-dir>
set -euo pipefail

DIR="${1:?Usage: analyze.sh <profile-dir>}"
FINDINGS="$DIR/FINDINGS.md"

cat > "$FINDINGS" << 'HEADER'
# LynxDB Performance Profile Findings

HEADER

echo "**Date**: $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$FINDINGS"
echo "**Go**: $(go version)" >> "$FINDINGS"
echo "**OS**: $(uname -srm)" >> "$FINDINGS"

# CPU info (macOS vs Linux)
if command -v sysctl &>/dev/null; then
    echo "**CPU**: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo unknown)" >> "$FINDINGS"
else
    echo "**CPU**: $(nproc) cores" >> "$FINDINGS"
fi
echo "" >> "$FINDINGS"

# --- CPU Profile ---
echo "## CPU Profile — Top 15" >> "$FINDINGS"
echo '```' >> "$FINDINGS"
if [ -f "$DIR/cpu.pb.gz" ] && [ -s "$DIR/cpu.pb.gz" ]; then
    go tool pprof -top -nodecount=15 "$DIR/cpu.pb.gz" 2>/dev/null >> "$FINDINGS" || echo "(empty or invalid)" >> "$FINDINGS"
else
    echo "(no data)" >> "$FINDINGS"
fi
echo '```' >> "$FINDINGS"
echo "" >> "$FINDINGS"

# --- Allocations: alloc_objects (GC pressure — many small objects) ---
echo "## Allocations — alloc_objects (GC pressure)" >> "$FINDINGS"
echo '```' >> "$FINDINGS"
if [ -f "$DIR/allocs.pb.gz" ] && [ -s "$DIR/allocs.pb.gz" ]; then
    go tool pprof -top -nodecount=15 -sample_index=alloc_objects "$DIR/allocs.pb.gz" 2>/dev/null >> "$FINDINGS" || echo "(empty or invalid)" >> "$FINDINGS"
else
    echo "(no data)" >> "$FINDINGS"
fi
echo '```' >> "$FINDINGS"
echo "" >> "$FINDINGS"

# --- Allocations: alloc_space (large allocations by bytes) ---
echo "## Allocations — alloc_space (bytes)" >> "$FINDINGS"
echo '```' >> "$FINDINGS"
if [ -f "$DIR/allocs.pb.gz" ] && [ -s "$DIR/allocs.pb.gz" ]; then
    go tool pprof -top -nodecount=15 -sample_index=alloc_space "$DIR/allocs.pb.gz" 2>/dev/null >> "$FINDINGS" || echo "(empty or invalid)" >> "$FINDINGS"
else
    echo "(no data)" >> "$FINDINGS"
fi
echo '```' >> "$FINDINGS"
echo "" >> "$FINDINGS"

# --- Heap (live memory) ---
echo "## Heap — inuse_space (live memory)" >> "$FINDINGS"
echo '```' >> "$FINDINGS"
if [ -f "$DIR/heap.pb.gz" ] && [ -s "$DIR/heap.pb.gz" ]; then
    go tool pprof -top -nodecount=15 -sample_index=inuse_space "$DIR/heap.pb.gz" 2>/dev/null >> "$FINDINGS" || echo "(empty or invalid)" >> "$FINDINGS"
else
    echo "(no data)" >> "$FINDINGS"
fi
echo '```' >> "$FINDINGS"
echo "" >> "$FINDINGS"

# --- Heap growth (before vs after) ---
echo "## Heap Growth (leak candidates)" >> "$FINDINGS"
if [ -f "$DIR/heap_before.pb.gz" ] && [ -f "$DIR/heap_after.pb.gz" ]; then
    echo '```' >> "$FINDINGS"
    echo "--- Before load ---" >> "$FINDINGS"
    go tool pprof -top -nodecount=5 -sample_index=inuse_space "$DIR/heap_before.pb.gz" 2>/dev/null >> "$FINDINGS" || true
    echo "" >> "$FINDINGS"
    echo "--- After load ---" >> "$FINDINGS"
    go tool pprof -top -nodecount=5 -sample_index=inuse_space "$DIR/heap_after.pb.gz" 2>/dev/null >> "$FINDINGS" || true
    echo '```' >> "$FINDINGS"
else
    echo "(heap_before/heap_after not collected)" >> "$FINDINGS"
fi
echo "" >> "$FINDINGS"

# --- Mutex Contention ---
echo "## Mutex Contention — Top 10" >> "$FINDINGS"
echo '```' >> "$FINDINGS"
if [ -f "$DIR/mutex.pb.gz" ] && [ -s "$DIR/mutex.pb.gz" ]; then
    go tool pprof -top -nodecount=10 "$DIR/mutex.pb.gz" 2>/dev/null >> "$FINDINGS" || echo "(empty — was --profile-runtime used?)" >> "$FINDINGS"
else
    echo "(no data)" >> "$FINDINGS"
fi
echo '```' >> "$FINDINGS"
echo "" >> "$FINDINGS"

# --- Block Profile ---
echo "## Block Profile — Top 10" >> "$FINDINGS"
echo '```' >> "$FINDINGS"
if [ -f "$DIR/block.pb.gz" ] && [ -s "$DIR/block.pb.gz" ]; then
    go tool pprof -top -nodecount=10 "$DIR/block.pb.gz" 2>/dev/null >> "$FINDINGS" || echo "(empty — was --profile-runtime used?)" >> "$FINDINGS"
else
    echo "(no data)" >> "$FINDINGS"
fi
echo '```' >> "$FINDINGS"
echo "" >> "$FINDINGS"

# --- Goroutine Count ---
echo "## Goroutine Summary" >> "$FINDINGS"
echo '```' >> "$FINDINGS"
if [ -f "$DIR/goroutine.pb.gz" ] && [ -s "$DIR/goroutine.pb.gz" ]; then
    go tool pprof -top -nodecount=10 "$DIR/goroutine.pb.gz" 2>/dev/null >> "$FINDINGS" || echo "(empty)" >> "$FINDINGS"
else
    echo "(no data)" >> "$FINDINGS"
fi
echo '```' >> "$FINDINGS"
echo "" >> "$FINDINGS"

# --- GC Pauses from gctrace ---
echo "## GC Pauses (from gctrace.log)" >> "$FINDINGS"
if [ -f "$DIR/gctrace.log" ] && [ -s "$DIR/gctrace.log" ]; then
    GC_COUNT=$(grep -c '^gc ' "$DIR/gctrace.log" 2>/dev/null || echo 0)
    echo "GC cycles: $GC_COUNT" >> "$FINDINGS"
    echo '```' >> "$FINDINGS"
    # Extract pause times (ms) — field after "pause"
    grep -oE 'pause [0-9.]+' "$DIR/gctrace.log" 2>/dev/null | awk '{print $2}' | sort -n | tail -5 | while read -r p; do
        echo "  pause: ${p}ms"
    done >> "$FINDINGS" || true
    echo '```' >> "$FINDINGS"
else
    echo "(no gctrace.log found)" >> "$FINDINGS"
fi
echo "" >> "$FINDINGS"

# --- Storage Metrics ---
echo "## Storage Metrics Snapshot" >> "$FINDINGS"
if [ -f "$DIR/storage-metrics.json" ]; then
    echo '```json' >> "$FINDINGS"
    python3 -m json.tool "$DIR/storage-metrics.json" >> "$FINDINGS" 2>/dev/null || \
        cat "$DIR/storage-metrics.json" >> "$FINDINGS"
    echo '```' >> "$FINDINGS"
else
    echo "(not collected)" >> "$FINDINGS"
fi
echo "" >> "$FINDINGS"

# --- Generate SVG flamegraphs ---
echo "## Flamegraph SVGs" >> "$FINDINGS"
for PROF in cpu heap allocs; do
    FILE="$DIR/${PROF}.pb.gz"
    if [ -f "$FILE" ] && [ -s "$FILE" ]; then
        SAMPLE_IDX=""
        case "$PROF" in
            heap) SAMPLE_IDX="-sample_index=inuse_space" ;;
            allocs) SAMPLE_IDX="-sample_index=alloc_objects" ;;
        esac
        go tool pprof -svg $SAMPLE_IDX "$FILE" > "$DIR/${PROF}-flamegraph.svg" 2>/dev/null && \
            echo "- \`${PROF}-flamegraph.svg\`" >> "$FINDINGS" || true
    fi
done
echo "" >> "$FINDINGS"

echo "Findings written to: $FINDINGS"
