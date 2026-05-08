package main

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/storage"
)

func TestAcceptance_RangeBSIIngestRegression_DefaultV2WithinTenPercentOfV1(t *testing.T) {
	if !benchRangeBSIAcceptanceEnabled() {
		t.Skip("set LYNXDB_RANGE_BSI_ACCEPTANCE=1 to run range BSI acceptance gates")
	}

	events := benchRangeBSIEventCount()
	reps := benchRangeBSIReps()
	lines := generateBenchLines(events)

	v1 := measureBenchIngestWithFormatMajor(t, lines, "1", reps)
	v2 := measureBenchIngestWithFormatMajor(t, lines, "", reps)
	ratio := float64(v1) / float64(v2)
	t.Logf("ingest V1=%s V2=%s ratio=%.2fx events=%d reps=%d", v1, v2, ratio, events, reps)
	if ratio < 0.90 {
		t.Fatalf("V2/V1 ingest throughput ratio = %.2fx, want >= 0.90x", ratio)
	}
}

func measureBenchIngestWithFormatMajor(t *testing.T, lines []string, formatMajor string, reps int) time.Duration {
	t.Helper()

	var total time.Duration
	for i := 0; i < reps; i++ {
		t.Setenv("LYNXDB_DEFAULT_FORMAT_MAJOR", formatMajor)
		restore, err := applyBenchFormatMajorEnv()
		if err != nil {
			t.Fatalf("applyBenchFormatMajorEnv(%q): %v", formatMajor, err)
		}

		eng := storage.NewEphemeralEngine()
		start := time.Now()
		count, err := eng.IngestLines(context.Background(), lines, storage.IngestOpts{
			Source:     "bench",
			SourceType: "bench",
		})
		elapsed := time.Since(start)
		if restore != nil {
			restore()
		}
		if closeErr := eng.Close(); closeErr != nil {
			t.Fatalf("Close: %v", closeErr)
		}
		if err != nil {
			t.Fatalf("IngestLines(format=%q): %v", formatMajor, err)
		}
		if count != len(lines) {
			t.Fatalf("IngestLines(format=%q) count = %d, want %d", formatMajor, count, len(lines))
		}
		total += elapsed
	}

	return total
}

func benchRangeBSIAcceptanceEnabled() bool {
	v := os.Getenv("LYNXDB_RANGE_BSI_ACCEPTANCE")
	return v == "1" || v == "true" || v == "TRUE"
}

func benchRangeBSIEventCount() int {
	if raw := os.Getenv("LYNXDB_RANGE_BSI_BENCH_EVENTS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err == nil && n > 0 {
			return n
		}
	}
	return 200_000
}

func benchRangeBSIReps() int {
	if raw := os.Getenv("LYNXDB_RANGE_BSI_BENCH_REPS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err == nil && n > 0 {
			return n
		}
	}
	return 3
}
