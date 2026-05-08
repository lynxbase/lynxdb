package compaction

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/memgov"
	"github.com/lynxbase/lynxdb/pkg/model"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	storageformat "github.com/lynxbase/lynxdb/pkg/storage/format"
	"github.com/lynxbase/lynxdb/pkg/storage/part"
	"github.com/lynxbase/lynxdb/pkg/storage/segment"
)

func TestIntegration_Compaction_V1Inputs_EmitsV2WithRangeBSI(t *testing.T) {
	c := NewCompactor(testLogger())
	segA := compactionSegmentInfoFromFixture(t, "v1-a", L0, "v1.lsg")
	segB := compactionSegmentInfoFromFixture(t, "v1-b", L0, "v1_with_inverted.lsg")

	output, err := c.Execute(context.Background(), &Plan{
		InputSegments: []*SegmentInfo{segA, segB},
		OutputLevel:   L1,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	reader := openCompactionOutput(t, output.Data, segment.LSG_FORMAT_MAJOR_V2, true)
	if got := reader.EventCount(); got != segA.Meta.EventCount+segB.Meta.EventCount {
		t.Fatalf("EventCount = %d, want %d", got, segA.Meta.EventCount+segB.Meta.EventCount)
	}
	assertCompactionHasNonEmptyRangeSections(t, output.Data)
}

func TestIntegration_Compaction_V2Inputs_PreservesBSIValueSemantics(t *testing.T) {
	base := time.Date(2030, 1, 2, 4, 0, 0, 0, time.UTC)
	eventsA := makeCompactionBSIEvents(t, base, 700, 100, "v2-a")
	eventsB := makeCompactionBSIEvents(t, base.Add(700*time.Millisecond), 700, 1000, "v2-b")
	segA := compactionSegmentInfoFromData(t, "v2-a", L0, writeCompactionV2Segment(t, eventsA))
	segB := compactionSegmentInfoFromData(t, "v2-b", L0, writeCompactionV2Segment(t, eventsB))

	output, err := NewCompactor(testLogger()).Execute(context.Background(), &Plan{
		InputSegments: []*SegmentInfo{segA, segB},
		OutputLevel:   L1,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	openCompactionOutput(t, output.Data, segment.LSG_FORMAT_MAJOR_V2, true)
	assertCompactionStatusBSIMatchesRows(t, output.Data)
}

func TestIntegration_Compaction_MixedV1V2Inputs_RangePredicateMatchesBruteForce(t *testing.T) {
	v1 := compactionSegmentInfoFromFixture(t, "v1", L0, "v1_with_primary.lsg")
	v2Events := makeCompactionBSIEvents(t, time.Date(2030, 1, 2, 5, 0, 0, 0, time.UTC), 900, 300, "v2")
	v2 := compactionSegmentInfoFromData(t, "v2", L0, writeCompactionV2Segment(t, v2Events))

	result, err := NewCompactor(testLogger()).StreamingMerge(
		context.Background(),
		&Plan{InputSegments: []*SegmentInfo{v1, v2}, OutputLevel: L1},
		MergeWriterFunc(func([]*event.Event) error { return nil }),
	)
	if err != nil {
		t.Fatalf("StreamingMerge: %v", err)
	}
	if result.FormatV1Inputs != 1 || result.FormatV2Inputs != 1 {
		t.Fatalf("format mix = v1:%d v2:%d, want v1:1 v2:1", result.FormatV1Inputs, result.FormatV2Inputs)
	}

	output, err := NewCompactor(testLogger()).Execute(context.Background(), &Plan{
		InputSegments: []*SegmentInfo{v1, v2},
		OutputLevel:   L1,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	reader := openCompactionOutput(t, output.Data, segment.LSG_FORMAT_MAJOR_V2, true)
	merged, err := reader.ReadEvents()
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	got := countStatusAtLeast(merged, 500)
	want := countStatusAtLeast(append(readCompactionEvents(t, v1.Data), v2Events...), 500)
	if got != want {
		t.Fatalf("status>=500 count = %d, want %d", got, want)
	}
	assertCompactionStatusBSIMatchesRows(t, output.Data)
}

func TestIntegration_Compaction_PartialMigrationQuery_UsesV2BSIAndV1Fallback(t *testing.T) {
	v1A := compactionSegmentInfoFromFixture(t, "v1-a", L0, "v1.lsg")
	v1B := compactionSegmentInfoFromFixture(t, "v1-b", L0, "v1_with_primary.lsg")
	base := time.Date(2030, 1, 2, 5, 30, 0, 0, time.UTC)
	v2A := compactionSegmentInfoFromData(t, "v2-a", L0,
		writeCompactionV2Segment(t, makeCompactionBSIEvents(t, base, 512, 300, "v2-a")))
	v2B := compactionSegmentInfoFromData(t, "v2-b", L0,
		writeCompactionV2Segment(t, makeCompactionBSIEvents(t, base.Add(512*time.Millisecond), 512, 900, "v2-b")))
	compactedV2, err := NewCompactor(testLogger()).Execute(context.Background(), &Plan{
		InputSegments: []*SegmentInfo{v2A, v2B},
		OutputLevel:   L1,
	})
	if err != nil {
		t.Fatalf("Execute V2 compaction: %v", err)
	}
	openCompactionOutput(t, compactedV2.Data, segment.LSG_FORMAT_MAJOR_V2, true)

	sources := []*pipeline.SegmentSource{
		compactionSegmentSource(t, v1A),
		compactionSegmentSource(t, v1B),
		compactionSegmentSource(t, compactedV2),
	}
	hints := &pipeline.SegmentStreamHints{
		RangePreds: []spl2.RangePredicate{{Field: "status", Min: "500"}},
	}
	iter := pipeline.NewSegmentStreamIterator(
		sources,
		nil,
		hints,
		128,
		memgov.NewTestBudget("phase5-compaction", 0).NewAccount("scan"),
	)
	defer iter.Close()
	if err := iter.Init(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	rows, err := pipeline.CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}

	var allEvents []*event.Event
	allEvents = append(allEvents, readCompactionEvents(t, v1A.Data)...)
	allEvents = append(allEvents, readCompactionEvents(t, v1B.Data)...)
	allEvents = append(allEvents, readCompactionEvents(t, compactedV2.Data)...)
	if got, want := len(rows), countStatusAtLeast(allEvents, 500); got != want {
		t.Fatalf("status>=500 rows = %d, want %d", got, want)
	}

	stats := iter.Stats()
	if stats.RGRangeBSIChecks == 0 {
		t.Fatal("RGRangeBSIChecks = 0, want V2 segment to contribute BSI checks")
	}
	if stats.RGRangeBSIChecks >= stats.RowGroupsTotal {
		t.Fatalf("RGRangeBSIChecks = %d, RowGroupsTotal = %d; want V1 row groups served without BSI",
			stats.RGRangeBSIChecks, stats.RowGroupsTotal)
	}
}

func TestUnit_CompactionPlanning_MixedFormats_SelectsByPolicyNotFormat(t *testing.T) {
	c := NewCompactor(testLogger())
	base := time.Date(2030, 1, 2, 6, 0, 0, 0, time.UTC)
	inputs := []*SegmentInfo{
		compactionPlanSegment("v1-a", L1, base.Add(0*time.Minute), 100<<20),
		compactionPlanSegment("v2-a", L1, base.Add(1*time.Minute), 100<<20),
		compactionPlanSegment("v1-b", L1, base.Add(2*time.Minute), 100<<20),
		compactionPlanSegment("v2-b", L1, base.Add(3*time.Minute), 100<<20),
	}
	for _, seg := range inputs {
		c.AddSegment(seg)
	}

	plan := c.PlanCompaction("main")
	if plan == nil {
		t.Fatal("PlanCompaction = nil, want L1 merge plan")
	}
	if plan.OutputLevel != L2 {
		t.Fatalf("OutputLevel = %d, want %d", plan.OutputLevel, L2)
	}
	var v1, v2 int
	for _, seg := range plan.InputSegments {
		switch seg.Meta.ID[:2] {
		case "v1":
			v1++
		case "v2":
			v2++
		}
	}
	if v1 != 2 || v2 != 2 {
		t.Fatalf("selected format mix by ID = v1:%d v2:%d, want v1:2 v2:2", v1, v2)
	}
}

func TestIntegration_Compaction_DoesNotChangeDiskFormatMarker(t *testing.T) {
	root := t.TempDir()
	if err := storageformat.WriteMarker([]string{root}, segment.LSG_BINARY_MAX_MAJOR); err != nil {
		t.Fatalf("WriteMarker: %v", err)
	}
	before, err := os.ReadFile(filepath.Join(root, storageformat.MarkerFilename))
	if err != nil {
		t.Fatalf("ReadFile(FORMAT before): %v", err)
	}

	base := time.Date(2030, 1, 2, 7, 0, 0, 0, time.UTC)
	segA := compactionSegmentInfoFromData(t, "marker-a", L0,
		writeCompactionV2Segment(t, makeCompactionBSIEvents(t, base, 512, 100, "marker-a")))
	segB := compactionSegmentInfoFromData(t, "marker-b", L0,
		writeCompactionV2Segment(t, makeCompactionBSIEvents(t, base.Add(512*time.Millisecond), 512, 700, "marker-b")))

	layout := part.NewLayout(filepath.Join(root, "segments", "hot"))
	writer, err := part.NewPartStreamWriter(layout, "main", L1, part.WithFSync(false))
	if err != nil {
		t.Fatalf("NewPartStreamWriter: %v", err)
	}

	_, err = NewCompactor(testLogger()).StreamingMerge(
		context.Background(),
		&Plan{InputSegments: []*SegmentInfo{segA, segB}, OutputLevel: L1},
		MergeWriterFunc(func(batch []*event.Event) error {
			return writer.WriteRowGroup(context.Background(), batch)
		}),
	)
	if err != nil {
		_ = writer.Abort()
		t.Fatalf("StreamingMerge: %v", err)
	}
	if _, err := writer.Finalize(context.Background()); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	after, err := os.ReadFile(filepath.Join(root, storageformat.MarkerFilename))
	if err != nil {
		t.Fatalf("ReadFile(FORMAT after): %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatalf("FORMAT marker changed from %q to %q", before, after)
	}
}

func compactionSegmentInfoFromFixture(t *testing.T, id string, level int, name string) *SegmentInfo {
	t.Helper()
	path := filepath.Join("..", "..", "..", "testdata", "segments", name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", path, err)
	}
	return compactionSegmentInfoFromData(t, id, level, data)
}

func compactionSegmentInfoFromData(t *testing.T, id string, level int, data []byte) *SegmentInfo {
	t.Helper()
	events := readCompactionEvents(t, data)
	if len(events) == 0 {
		t.Fatalf("segment %s has no events", id)
	}
	return &SegmentInfo{
		Meta: model.SegmentMeta{
			ID:         id,
			Index:      "main",
			MinTime:    events[0].Time,
			MaxTime:    events[len(events)-1].Time,
			EventCount: int64(len(events)),
			SizeBytes:  int64(len(data)),
			Level:      level,
			CreatedAt:  time.Now(),
			Partition:  "2030-01-02",
		},
		Data: append([]byte(nil), data...),
	}
}

func readCompactionEvents(t *testing.T, data []byte) []*event.Event {
	t.Helper()
	reader, err := segment.OpenSegment(data)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	events, err := reader.ReadEvents()
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	return events
}

func writeCompactionV2Segment(t *testing.T, events []*event.Event) []byte {
	t.Helper()
	var buf bytes.Buffer
	writer := segment.NewWriter(&buf)
	writer.SetRowGroupSize(512)
	if _, err := writer.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}
	return append([]byte(nil), buf.Bytes()...)
}

func makeCompactionBSIEvents(t *testing.T, base time.Time, n int, statusBase int64, host string) []*event.Event {
	t.Helper()
	events := make([]*event.Event, n)
	for i := 0; i < n; i++ {
		status := statusBase + int64(i)
		ts := base.Add(time.Duration(i) * time.Millisecond)
		e := event.NewEvent(ts, fmt.Sprintf("status=%d duration_ms=%d host=%s row=%d", status, i*17, host, i))
		e.Host = host
		e.Source = "/var/log/compaction-bsi.log"
		e.SourceType = "json"
		e.Index = "main"
		e.SetField("status", event.IntValue(status))
		e.SetField("duration_ms", event.IntValue(int64(i*17)))
		events[i] = e
	}
	return events
}

func openCompactionOutput(t *testing.T, data []byte, wantMajor uint16, wantBSI bool) *segment.Reader {
	t.Helper()
	gotMajor, err := segment.SegmentHeaderMajor(data, int64(len(data)))
	if err != nil {
		t.Fatalf("SegmentHeaderMajor: %v", err)
	}
	if gotMajor != wantMajor {
		t.Fatalf("format major = %d, want %d", gotMajor, wantMajor)
	}
	reader, err := segment.OpenSegment(data)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	if reader.HasRangeBSI() != wantBSI {
		t.Fatalf("HasRangeBSI() = %v, want %v", reader.HasRangeBSI(), wantBSI)
	}
	if err := reader.VerifyAllRangeBSIs(); err != nil {
		t.Fatalf("VerifyAllRangeBSIs: %v", err)
	}
	return reader
}

func assertCompactionHasNonEmptyRangeSections(t *testing.T, data []byte) {
	t.Helper()
	footer, err := segment.DecodeFooter(data)
	if err != nil {
		t.Fatalf("DecodeFooter: %v", err)
	}
	columns, sectionBytes := footer.RangeBSIStats()
	if columns == 0 {
		t.Fatal("RangeBSIStats columns = 0, want at least one indexed column")
	}
	if sectionBytes == 0 {
		t.Fatal("RangeBSIStats sectionBytes = 0, want non-empty BSI sections")
	}
	for i, rg := range footer.RowGroups {
		if rg.PerColumnRangeLength == 0 {
			t.Fatalf("row group %d PerColumnRangeLength = 0, want non-empty range section", i)
		}
	}
}

func assertCompactionStatusBSIMatchesRows(t *testing.T, data []byte) {
	t.Helper()
	reader := openCompactionOutput(t, data, segment.LSG_FORMAT_MAJOR_V2, true)
	events, err := reader.ReadEvents()
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}

	globalStart := 0
	var checked int
	for rgIdx := 0; rgIdx < reader.RowGroupCount(); rgIdx++ {
		rowCount := reader.RowGroupRowCount(rgIdx)
		idx, err := reader.LoadRangeBSI(rgIdx, "status")
		if err != nil {
			t.Fatalf("LoadRangeBSI(%d, status): %v", rgIdx, err)
		}
		if idx == nil {
			globalStart += rowCount
			continue
		}
		meta, ok, err := reader.LoadRangeMeta(rgIdx, "status")
		if err != nil {
			t.Fatalf("LoadRangeMeta(%d, status): %v", rgIdx, err)
		}
		if !ok {
			t.Fatalf("LoadRangeMeta(%d, status) ok = false, want true", rgIdx)
		}
		for _, localRow := range sampleCompactionRows(rowCount, 16) {
			status, ok := events[globalStart+localRow].GetField("status").TryAsInt()
			if !ok {
				continue
			}
			offset, ok := idx.GetValue(uint64(localRow))
			if !ok {
				t.Fatalf("rg %d local row %d missing BSI value", rgIdx, localRow)
			}
			if got := meta.MinValue + offset; got != status {
				t.Fatalf("rg %d local row %d status BSI value = %d, want %d", rgIdx, localRow, got, status)
			}
			checked++
		}
		globalStart += rowCount
	}
	if checked == 0 {
		t.Fatal("checked no status BSI rows")
	}
}

func sampleCompactionRows(rowCount int, samples int) []int {
	if rowCount <= samples {
		rows := make([]int, rowCount)
		for i := range rows {
			rows[i] = i
		}
		return rows
	}
	rows := make([]int, 0, samples)
	for i := 0; i < samples; i++ {
		rows = append(rows, i*(rowCount-1)/(samples-1))
	}
	return rows
}

func countStatusAtLeast(events []*event.Event, min int64) int {
	var count int
	for _, e := range events {
		status, ok := e.GetField("status").TryAsInt()
		if ok && status >= min {
			count++
		}
	}
	return count
}

func compactionPlanSegment(id string, level int, min time.Time, sizeBytes int64) *SegmentInfo {
	return &SegmentInfo{
		Meta: model.SegmentMeta{
			ID:         id,
			Index:      "main",
			MinTime:    min,
			MaxTime:    min.Add(time.Hour),
			EventCount: 1,
			SizeBytes:  sizeBytes,
			Level:      level,
			Partition:  "2030-01-02",
		},
	}
}

func compactionSegmentSource(t *testing.T, seg *SegmentInfo) *pipeline.SegmentSource {
	t.Helper()
	reader, err := segment.OpenSegment(seg.Data)
	if err != nil {
		t.Fatalf("OpenSegment(%s): %v", seg.Meta.ID, err)
	}
	return &pipeline.SegmentSource{
		Reader: reader,
		Index:  seg.Meta.Index,
		Meta: pipeline.SegmentMeta{
			ID:         seg.Meta.ID,
			MinTime:    seg.Meta.MinTime,
			MaxTime:    seg.Meta.MaxTime,
			EventCount: seg.Meta.EventCount,
			SizeBytes:  seg.Meta.SizeBytes,
		},
	}
}
