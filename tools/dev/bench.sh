#!/bin/sh
# usage: bench.sh [<package>...]
# Runs the micro benchmarks of the row pipeline and the ingest pipeline, or of
# the packages given, five times each, and keeps the output for benchstat in
# artifacts/bench-micro.txt.
set -eu

cd "$(dirname -- "$0")/../.."

[ $# -gt 0 ] || set -- ./pkg/engine/pipeline/... ./pkg/ingest/pipeline/...

mkdir -p artifacts
status=artifacts/bench-micro.status
# The exit status of go test would be lost behind tee.
{
    go test -run '^$' -bench . -benchmem -count 5 -timeout 300s "$@" && echo 0 >"$status" ||
        echo $? >"$status"
} | tee artifacts/bench-micro.txt
code=$(cat "$status")
rm "$status"
exit "$code"
