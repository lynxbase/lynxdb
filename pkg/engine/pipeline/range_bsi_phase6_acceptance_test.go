package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/memgov"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/storage/segment"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

func TestAcceptance_RangeBSIFullPipelineSpeedup_EquivalentV1V2Fixtures(t *testing.T) {
	if !pipelineRangeBSIAcceptanceEnabled() {
		t.Skip("set LYNXDB_RANGE_BSI_ACCEPTANCE=1 to run range BSI acceptance gates")
	}

	events := makePipelineRangeBSIEvents(pipelineRangeBSIEventCount(), 0)
	v1 := writePipelineRangeBSISource(t, events, "main", segment.LSG_FORMAT_MAJOR_V1, false, "v1")
	v2 := writePipelineRangeBSISource(t, events, "main", segment.LSG_FORMAT_MAJOR_V2, true, "v2")
	reps := pipelineRangeBSIReps()

	v1Count, v1Dur, v1Stats := measurePipelineRangeBSICount(t, []*SegmentSource{v1}, false, reps)
	v2Count, v2Dur, v2Stats := measurePipelineRangeBSICount(t, []*SegmentSource{v2}, true, reps)
	if v2Count != v1Count {
		t.Fatalf("V2 count = %d, want V1 count %d", v2Count, v1Count)
	}
	assertPipelineRangeBSIUsed(t, v2Stats)

	ratio := float64(v1Dur) / float64(v2Dur)
	t.Logf("full_pipeline V1=%s V2=%s ratio=%.2fx matches=%d v1_bsi_checks=%d v2_bsi_checks=%d v2_mask_bytes=%d",
		v1Dur, v2Dur, ratio, v1Count, v1Stats.RGRangeBSIChecks, v2Stats.RGRangeBSIChecks, v2Stats.RGRangeBSIMaskBytes)
	if ratio < 1.10 {
		t.Fatalf("full pipeline speedup = %.2fx, want >= 1.10x", ratio)
	}
}

func TestAcceptance_RangeBSIMixedFormatPipelineSpeedup_MixedV1V2Fixtures(t *testing.T) {
	if !pipelineRangeBSIAcceptanceEnabled() {
		t.Skip("set LYNXDB_RANGE_BSI_ACCEPTANCE=1 to run range BSI acceptance gates")
	}

	eventsPerHalf := max(pipelineRangeBSIEventCount()/2, 1)
	eventsA := makePipelineRangeBSIEvents(eventsPerHalf, 0)
	eventsB := makePipelineRangeBSIEvents(eventsPerHalf, eventsPerHalf)
	v1A := writePipelineRangeBSISource(t, eventsA, "main", segment.LSG_FORMAT_MAJOR_V1, false, "v1-a")
	v1B := writePipelineRangeBSISource(t, eventsB, "main", segment.LSG_FORMAT_MAJOR_V1, false, "v1-b")
	v2B := writePipelineRangeBSISource(t, eventsB, "main", segment.LSG_FORMAT_MAJOR_V2, true, "v2-b")
	reps := pipelineRangeBSIReps()

	v1Count, v1Dur, _ := measurePipelineRangeBSICount(t, []*SegmentSource{v1A, v1B}, false, reps)
	mixedCount, mixedDur, mixedStats := measurePipelineRangeBSICount(t, []*SegmentSource{v1A, v2B}, false, reps)
	if mixedCount != v1Count {
		t.Fatalf("mixed count = %d, want all-V1 count %d", mixedCount, v1Count)
	}
	assertPipelineRangeBSIUsed(t, mixedStats)

	ratio := float64(v1Dur) / float64(mixedDur)
	t.Logf("mixed_pipeline all_v1=%s mixed=%s ratio=%.2fx matches=%d bsi_checks=%d row_groups=%d mask_bytes=%d",
		v1Dur, mixedDur, ratio, v1Count, mixedStats.RGRangeBSIChecks, mixedStats.RowGroupsTotal, mixedStats.RGRangeBSIMaskBytes)
	if ratio < 1.05 {
		t.Fatalf("mixed pipeline speedup = %.2fx, want >= 1.05x", ratio)
	}
}

func BenchmarkRangeBSI_Pipeline_FullPipelineV1(b *testing.B) {
	events := makePipelineRangeBSIEvents(pipelineRangeBSIEventCount(), 0)
	src := writePipelineRangeBSISource(b, events, "main", segment.LSG_FORMAT_MAJOR_V1, false, "v1")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count, _, _ := measurePipelineRangeBSICount(b, []*SegmentSource{src}, false, 1)
		if count == 0 {
			b.Fatal("count = 0, want matches")
		}
	}
}

func BenchmarkRangeBSI_Pipeline_FullPipelineV2(b *testing.B) {
	events := makePipelineRangeBSIEvents(pipelineRangeBSIEventCount(), 0)
	src := writePipelineRangeBSISource(b, events, "main", segment.LSG_FORMAT_MAJOR_V2, true, "v2")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count, _, stats := measurePipelineRangeBSICount(b, []*SegmentSource{src}, true, 1)
		if count == 0 {
			b.Fatal("count = 0, want matches")
		}
		if stats.RGRangeBSIChecks == 0 {
			b.Fatal("RGRangeBSIChecks = 0, want BSI consultations")
		}
	}
}

func BenchmarkRangeBSI_Pipeline_MixedV1V2(b *testing.B) {
	eventsPerHalf := max(pipelineRangeBSIEventCount()/2, 1)
	eventsA := makePipelineRangeBSIEvents(eventsPerHalf, 0)
	eventsB := makePipelineRangeBSIEvents(eventsPerHalf, eventsPerHalf)
	v1 := writePipelineRangeBSISource(b, eventsA, "main", segment.LSG_FORMAT_MAJOR_V1, false, "v1")
	v2 := writePipelineRangeBSISource(b, eventsB, "main", segment.LSG_FORMAT_MAJOR_V2, true, "v2")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count, _, stats := measurePipelineRangeBSICount(b, []*SegmentSource{v1, v2}, false, 1)
		if count == 0 {
			b.Fatal("count = 0, want matches")
		}
		if stats.RGRangeBSIChecks == 0 {
			b.Fatal("RGRangeBSIChecks = 0, want BSI consultations")
		}
	}
}

func measurePipelineRangeBSICount(t testing.TB, sources []*SegmentSource, loweredToBSI bool, reps int) (int64, time.Duration, SegmentStreamStats) {
	t.Helper()

	var count int64
	var stats SegmentStreamStats
	start := time.Now()
	for i := 0; i < reps; i++ {
		next, nextStats := runPipelineRangeBSICount(t, sources, loweredToBSI)
		count = next
		stats.RGRangeBSIChecks += nextStats.RGRangeBSIChecks
		stats.RGRangeBSISkips += nextStats.RGRangeBSISkips
		stats.RGRangeBSIMaskBytes += nextStats.RGRangeBSIMaskBytes
		stats.RowGroupsTotal += nextStats.RowGroupsTotal
	}

	return count, time.Since(start), stats
}

func runPipelineRangeBSICount(t testing.TB, sources []*SegmentSource, loweredToBSI bool) (int64, SegmentStreamStats) {
	t.Helper()
	expr := &spl2.CompareExpr{
		Left:         &spl2.FieldExpr{Name: "status"},
		Op:           ">=",
		Right:        &spl2.LiteralExpr{Value: "500"},
		LoweredToBSI: loweredToBSI,
	}
	prog, err := vm.CompilePredicate(expr)
	if err != nil {
		t.Fatalf("CompilePredicate: %v", err)
	}
	hints := &SegmentStreamHints{
		RangePreds: []spl2.RangePredicate{{Field: "status", Min: "500", LoweredToBSI: loweredToBSI}},
		RequiredCols: []string{
			"status",
		},
	}
	scan := NewSegmentStreamIterator(
		sources,
		nil,
		hints,
		512,
		memgov.NewTestBudget("range-bsi-acceptance", 0).NewAccount("scan"),
	)
	scan.segPreds = nil
	defer scan.Close()

	filter := NewFilterIteratorWithExpr(scan, prog, expr)
	agg := NewAggregateIterator(filter, []AggFunc{{Name: "count", Alias: "count"}}, nil, nil)
	rows, err := CollectAll(context.Background(), agg)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("result rows = %d, want 1", len(rows))
	}

	return rows[0]["count"].AsInt(), scan.Stats()
}

func writePipelineRangeBSISource(t testing.TB, events []*event.Event, indexName string, formatMajor uint16, enableBSI bool, id string) *SegmentSource {
	t.Helper()
	restore, err := segment.SetDefaultFormatMajorForProcess(formatMajor)
	if err != nil {
		t.Fatalf("SetDefaultFormatMajorForProcess(%d): %v", formatMajor, err)
	}
	defer restore()

	var buf bytes.Buffer
	w := segment.NewWriter(&buf)
	w.SetRowGroupSize(8192)
	cfg := segment.IndexConfig{DisableBSI: !enableBSI}
	if enableBSI {
		cfg.ProfileOverrides = map[string]segment.IndexProfile{
			"status": segment.IndexProfileRangeBSI,
		}
		cfg.BSIMaxBitCount = 64
	}
	w.SetIndexConfig(cfg)
	if _, err := w.Write(events); err != nil {
		t.Fatalf("Write(format=%d, bsi=%v): %v", formatMajor, enableBSI, err)
	}
	reader, err := segment.OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment(format=%d): %v", formatMajor, err)
	}
	if enableBSI && !reader.HasRangeBSI() {
		t.Fatal("HasRangeBSI() = false, want true")
	}
	if !enableBSI && reader.IndexProfile("status") == segment.IndexProfileRangeBSI {
		t.Fatal("status IndexProfile = RangeBSI, want non-BSI fixture")
	}

	return &SegmentSource{
		Reader: reader,
		Index:  indexName,
		Meta: SegmentMeta{
			ID:         id,
			MinTime:    events[0].Time,
			MaxTime:    events[len(events)-1].Time,
			EventCount: int64(len(events)),
			SizeBytes:  int64(buf.Len()),
		},
	}
}

func makePipelineRangeBSIEvents(n, offset int) []*event.Event {
	base := time.Date(2026, 5, 8, 18, 0, 0, 0, time.UTC)
	events := make([]*event.Event, n)
	for i := 0; i < n; i++ {
		global := offset + i
		status := int64(200)
		if global%100 >= 97 {
			status = int64(500 + global%10)
		}
		ts := base.Add(time.Duration(global) * time.Millisecond)
		e := event.NewEvent(ts, fmt.Sprintf("status=%d row=%d", status, global))
		e.Source = "/var/log/range-bsi-pipeline.log"
		e.SourceType = "bench"
		e.Index = "main"
		e.SetField("status", event.IntValue(status))
		events[i] = e
	}

	return events
}

func assertPipelineRangeBSIUsed(t testing.TB, stats SegmentStreamStats) {
	t.Helper()
	if stats.RGRangeBSIChecks == 0 {
		t.Fatal("RGRangeBSIChecks = 0, want BSI consultations")
	}
	if stats.RGRangeBSIMaskBytes == 0 {
		t.Fatal("RGRangeBSIMaskBytes = 0, want BSI row masks")
	}
}

func pipelineRangeBSIAcceptanceEnabled() bool {
	v := os.Getenv("LYNXDB_RANGE_BSI_ACCEPTANCE")
	return v == "1" || v == "true" || v == "TRUE"
}

func pipelineRangeBSIEventCount() int {
	if raw := os.Getenv("LYNXDB_RANGE_BSI_BENCH_EVENTS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err == nil && n > 0 {
			return n
		}
	}
	return 200_000
}

func pipelineRangeBSIReps() int {
	if raw := os.Getenv("LYNXDB_RANGE_BSI_BENCH_REPS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err == nil && n > 0 {
			return n
		}
	}
	return 3
}
