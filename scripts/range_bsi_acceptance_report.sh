#!/usr/bin/env bash
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EVENTS="${LYNXDB_RANGE_BSI_BENCH_EVENTS:-200000}"
REPS="${LYNXDB_RANGE_BSI_BENCH_REPS:-3}"
GO_CACHE="${GOCACHE:-/tmp/go-build-cache-rbi}"
GO_MOD_CACHE="${GOMODCACHE:-/tmp/go-mod-cache-rbi}"
TMPDIR="${TMPDIR:-/tmp}"
REPORT_DIR="$(mktemp -d "$TMPDIR/range-bsi-acceptance.XXXXXX")"
FAILURES=0

cleanup() {
  rm -rf "$REPORT_DIR"
}
trap cleanup EXIT

run_test() {
  local name="$1"
  local pkg="$2"
  local test_re="$3"
  local tags="${4:-}"
  local out="$REPORT_DIR/${name}.txt"
  local status=0

  if [[ -n "$tags" ]]; then
    (
      cd "$ROOT" &&
        env GOCACHE="$GO_CACHE" GOMODCACHE="$GO_MOD_CACHE" \
          LYNXDB_RANGE_BSI_ACCEPTANCE=1 \
          LYNXDB_RANGE_BSI_BENCH_EVENTS="$EVENTS" \
          LYNXDB_RANGE_BSI_BENCH_REPS="$REPS" \
          go test -tags "$tags" "$pkg" -run "$test_re" -count=1 -v
    ) >"$out" 2>&1 || status=$?
  else
    (
      cd "$ROOT" &&
        env GOCACHE="$GO_CACHE" GOMODCACHE="$GO_MOD_CACHE" \
          LYNXDB_RANGE_BSI_ACCEPTANCE=1 \
          LYNXDB_RANGE_BSI_BENCH_EVENTS="$EVENTS" \
          LYNXDB_RANGE_BSI_BENCH_REPS="$REPS" \
          go test "$pkg" -run "$test_re" -count=1 -v
    ) >"$out" 2>&1 || status=$?
  fi

  local detail
  detail="$(grep -E 'ratio=|storage |writer BSI|PASS|FAIL' "$out" | tail -n 2 | tr '\n' ' ' | sed 's/[[:space:]]\+/ /g')"
  if [[ -z "$detail" ]]; then
    detail="$(tail -n 2 "$out" | tr '\n' ' ' | sed 's/[[:space:]]\+/ /g')"
  fi

  if [[ "$status" -eq 0 ]]; then
    printf "%-42s %-82s PASS\n" "$name" "$detail"
  else
    printf "%-42s %-82s FAIL\n" "$name" "$detail"
    FAILURES=$((FAILURES + 1))
  fi
}

run_bench() {
  local name="$1"
  local pkg="$2"
  local bench_re="$3"
  local out="$REPORT_DIR/${name}.txt"
  local status=0
  (
    cd "$ROOT" &&
      env GOCACHE="$GO_CACHE" GOMODCACHE="$GO_MOD_CACHE" \
        LYNXDB_RANGE_BSI_BENCH_EVENTS="$EVENTS" \
        go test "$pkg" -run '^$' -bench "$bench_re" -benchmem -benchtime=100ms -count=1
  ) >"$out" 2>&1 || status=$?

  local detail
  detail="$(grep -E '^BenchmarkRangeBSI_' "$out" | tail -n 4 | tr '\n' ' ' | sed 's/[[:space:]]\+/ /g')"
  if [[ -z "$detail" ]]; then
    detail="$(tail -n 2 "$out" | tr '\n' ' ' | sed 's/[[:space:]]\+/ /g')"
  fi

  if [[ "$status" -eq 0 ]]; then
    printf "%-42s %-82s PASS\n" "$name" "$detail"
  else
    printf "%-42s %-82s FAIL\n" "$name" "$detail"
    FAILURES=$((FAILURES + 1))
  fi
}

blocker() {
  local name="$1"
  local detail="$2"
  printf "%-42s %-82s FAIL\n" "$name" "$detail"
  FAILURES=$((FAILURES + 1))
}

printf "Range BSI acceptance report\n"
printf "machine: %s\n" "$(uname -a)"
printf "events: %s reps: %s\n\n" "$EVENTS" "$REPS"
printf "%-42s %-82s %s\n" "gate" "result" "status"
printf "%-42s %-82s %s\n" "----" "------" "------"

run_test "T6.1 query speedup status>=500" "./pkg/storage/segment" '^TestAcceptance_RangeBSIQuerySpeedup_StatusGE500$'
run_test "T6.1 query speedup duration between" "./pkg/storage/segment" '^TestAcceptance_RangeBSIQuerySpeedup_DurationBetween$'
run_test "T6.2 storage overhead" "./pkg/storage/segment" '^TestAcceptance_RangeBSIStorageOverhead$'
run_test "T6.4 writer BSI overhead" "./pkg/storage/segment" '^TestAcceptance_RangeBSIWriterOverhead$'
run_test "T6.7 correctness predicates" "./pkg/storage/segment" '^TestAcceptance_RangeBSICorrectness_RandomPredicatesMatchBruteScan$' "acceptance"

run_bench "bench query fixtures" "./pkg/storage/segment" '^BenchmarkRangeBSI_Query_'
run_bench "bench storage overhead" "./pkg/storage/segment" '^BenchmarkRangeBSI_StorageOverhead$'
run_bench "bench writer overhead" "./pkg/storage/segment" '^BenchmarkRangeBSI_Writer_'
run_bench "bench pipeline fixtures" "./pkg/engine/pipeline" '^BenchmarkRangeBSI_Pipeline_'

run_test "T6.3 ingest regression" "./cmd/lynxdb" '^TestAcceptance_RangeBSIIngestRegression_DefaultV2WithinTenPercentOfV1$'
run_test "T6.5 full pipeline e2e speedup" "./pkg/engine/pipeline" '^TestAcceptance_RangeBSIFullPipelineSpeedup_EquivalentV1V2Fixtures$'
run_test "T6.6 mixed-format pipeline speedup" "./pkg/engine/pipeline" '^TestAcceptance_RangeBSIMixedFormatPipelineSpeedup_MixedV1V2Fixtures$'

printf "\n"
if [[ "$FAILURES" -ne 0 ]]; then
  printf "FAIL: %d gate(s) failed or blocked\n" "$FAILURES"
  exit 1
fi

printf "PASS: all range BSI acceptance gates passed\n"
