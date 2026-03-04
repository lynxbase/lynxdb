package views

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/storage"
	"github.com/lynxbase/lynxdb/pkg/storage/segment"
)

func writeTestSegment(t *testing.T, dir string, events []*event.Event) string {
	t.Helper()
	segName := fmt.Sprintf("seg-test-L0-%d.lsg", time.Now().UnixNano())
	path := filepath.Join(dir, segName)
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create segment: %v", err)
	}
	sw := segment.NewWriter(f)
	if _, err := sw.Write(events); err != nil {
		f.Close()
		t.Fatalf("write segment: %v", err)
	}
	f.Close()

	return path
}

func TestMergeView_ProjectionMerge(t *testing.T) {
	dir := t.TempDir()
	layout := storage.NewLayout(dir)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	def := ViewDefinition{
		Name:    "mv_proj",
		Version: 1,
		Type:    ViewTypeProjection,
		Columns: []ColumnDef{
			{Name: "_time", Type: event.FieldTypeTimestamp},
			{Name: "uri", Type: event.FieldTypeString},
		},
		Status: ViewStatusActive,
	}

	segDir := layout.ViewSegmentDir("mv_proj")
	os.MkdirAll(segDir, 0o755)

	// Create 4 segments (enough to trigger merge).
	for i := 0; i < 4; i++ {
		e := makeTestEvent("nginx", fmt.Sprintf("/api/%d", i), "200")
		e.Index = "mv_proj"
		writeTestSegment(t, segDir, []*event.Event{e})
		time.Sleep(time.Millisecond)
	}

	entries, _ := os.ReadDir(segDir)
	if len(entries) != 4 {
		t.Fatalf("expected 4 segments, got %d", len(entries))
	}

	if err := MergeView(def, layout, logger); err != nil {
		t.Fatalf("MergeView: %v", err)
	}

	entries, _ = os.ReadDir(segDir)
	if len(entries) != 1 {
		t.Errorf("expected 1 segment after merge, got %d", len(entries))
	}

	segPath := filepath.Join(segDir, entries[0].Name())
	ms, err := segment.OpenSegmentFile(segPath)
	if err != nil {
		t.Fatalf("open merged segment: %v", err)
	}
	defer ms.Close()

	r := ms.Reader()
	if r.EventCount() != 4 {
		t.Errorf("merged segment events: got %d, want 4", r.EventCount())
	}
}

func TestMergeView_AggregationMerge(t *testing.T) {
	dir := t.TempDir()
	layout := storage.NewLayout(dir)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	def := ViewDefinition{
		Name:    "mv_agg",
		Version: 1,
		Type:    ViewTypeAggregation,
		GroupBy: []string{"_source"},
		Columns: []ColumnDef{
			{Name: "_time", Type: event.FieldTypeTimestamp},
			{Name: "_source", Type: event.FieldTypeString},
			{Name: "count", Type: event.FieldTypeFloat},
			{Name: "_sum_dur", Type: event.FieldTypeFloat},
			{Name: "_count_dur", Type: event.FieldTypeFloat},
		},
		Aggregations: []AggregationDef{
			{Name: "count", Type: "count", StateColumns: []string{"count"}},
			{Name: "avg(duration)", Type: "avg", StateColumns: []string{"_sum_dur", "_count_dur"}},
		},
		Status: ViewStatusActive,
	}

	segDir := layout.ViewSegmentDir("mv_agg")
	os.MkdirAll(segDir, 0o755)

	for i := 0; i < 4; i++ {
		e := event.NewEvent(time.Now(), "test")
		e.Source = "nginx"
		e.Index = "mv_agg"
		e.SetField("_source", event.StringValue("nginx"))
		e.SetField("count", event.FloatValue(float64(10+i)))
		e.SetField("_sum_dur", event.FloatValue(float64(100*(i+1))))
		e.SetField("_count_dur", event.FloatValue(float64(10+i)))
		writeTestSegment(t, segDir, []*event.Event{e})
		time.Sleep(time.Millisecond)
	}

	if err := MergeView(def, layout, logger); err != nil {
		t.Fatalf("MergeView: %v", err)
	}

	entries, _ := os.ReadDir(segDir)
	if len(entries) != 1 {
		t.Errorf("expected 1 segment after merge, got %d", len(entries))
	}

	segPath := filepath.Join(segDir, entries[0].Name())
	ms, err := segment.OpenSegmentFile(segPath)
	if err != nil {
		t.Fatalf("open merged segment: %v", err)
	}
	defer ms.Close()

	r := ms.Reader()
	if r.EventCount() != 1 {
		t.Errorf("merged aggregate events: got %d, want 1", r.EventCount())
	}

	events, err := r.ReadEvents()
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(events) > 0 {
		countVal, _ := valFloat(events[0], "count")
		if countVal != 46.0 {
			t.Errorf("merged count: got %v, want 46", countVal)
		}
	}
}

func TestMergeView_NotEnoughSegments(t *testing.T) {
	dir := t.TempDir()
	layout := storage.NewLayout(dir)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	def := ViewDefinition{
		Name:   "mv_few",
		Type:   ViewTypeProjection,
		Status: ViewStatusActive,
	}

	segDir := layout.ViewSegmentDir("mv_few")
	os.MkdirAll(segDir, 0o755)

	for i := 0; i < 2; i++ {
		e := event.NewEvent(time.Now(), "test")
		e.Index = "mv_few"
		writeTestSegment(t, segDir, []*event.Event{e})
		time.Sleep(time.Millisecond)
	}

	if err := MergeView(def, layout, logger); err != nil {
		t.Fatalf("MergeView: %v", err)
	}

	entries, _ := os.ReadDir(segDir)
	if len(entries) != 2 {
		t.Errorf("expected 2 segments (no merge), got %d", len(entries))
	}
}

func TestMergeAggregates_CountSum(t *testing.T) {
	def := ViewDefinition{
		GroupBy: []string{"src"},
		Aggregations: []AggregationDef{
			{Name: "count", Type: "count", StateColumns: []string{"cnt"}},
			{Name: "sum(x)", Type: "sum", StateColumns: []string{"sum_x"}},
		},
	}

	events := []*event.Event{
		makeAggEvent("nginx", 10, 100),
		makeAggEvent("nginx", 20, 200),
		makeAggEvent("api", 5, 50),
	}

	merged := mergeAggregates(events, def)
	if len(merged) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(merged))
	}

	for _, e := range merged {
		if e.GetField("src").String() == "nginx" {
			cnt, _ := valFloat(e, "cnt")
			if cnt != 30.0 {
				t.Errorf("nginx count: got %v, want 30", cnt)
			}
			sum, _ := valFloat(e, "sum_x")
			if sum != 300.0 {
				t.Errorf("nginx sum: got %v, want 300", sum)
			}
		}
	}
}

func TestMergeAggregates_MinMax(t *testing.T) {
	def := ViewDefinition{
		GroupBy: []string{"src"},
		Aggregations: []AggregationDef{
			{Name: "min(x)", Type: "min", StateColumns: []string{"min_x"}},
			{Name: "max(x)", Type: "max", StateColumns: []string{"max_x"}},
		},
	}

	events := []*event.Event{
		makeMinMaxEvent("nginx", 10, 100),
		makeMinMaxEvent("nginx", 5, 200),
		makeMinMaxEvent("nginx", 20, 50),
	}

	merged := mergeAggregates(events, def)
	if len(merged) != 1 {
		t.Fatalf("expected 1 group, got %d", len(merged))
	}

	e := merged[0]
	minVal, _ := valFloat(e, "min_x")
	if minVal != 5.0 {
		t.Errorf("min: got %v, want 5", minVal)
	}
	maxVal, _ := valFloat(e, "max_x")
	if maxVal != 200.0 {
		t.Errorf("max: got %v, want 200", maxVal)
	}
}

func TestMergeAggregates_Avg(t *testing.T) {
	def := ViewDefinition{
		GroupBy: []string{"src"},
		Aggregations: []AggregationDef{
			{Name: "avg(dur)", Type: "avg", StateColumns: []string{"_sum_dur", "_count_dur"}},
		},
	}

	e1 := event.NewEvent(time.Now(), "")
	e1.SetField("src", event.StringValue("nginx"))
	e1.SetField("_sum_dur", event.FloatValue(100))
	e1.SetField("_count_dur", event.FloatValue(10))

	e2 := event.NewEvent(time.Now(), "")
	e2.SetField("src", event.StringValue("nginx"))
	e2.SetField("_sum_dur", event.FloatValue(200))
	e2.SetField("_count_dur", event.FloatValue(20))

	merged := mergeAggregates([]*event.Event{e1, e2}, def)
	if len(merged) != 1 {
		t.Fatalf("expected 1 group, got %d", len(merged))
	}

	e := merged[0]
	sum, _ := valFloat(e, "_sum_dur")
	cnt, _ := valFloat(e, "_count_dur")
	if sum != 300.0 {
		t.Errorf("sum: got %v, want 300", sum)
	}
	if cnt != 30.0 {
		t.Errorf("count: got %v, want 30", cnt)
	}
}

func makeAggEvent(source string, count, sum float64) *event.Event {
	e := event.NewEvent(time.Now(), "")
	e.SetField("src", event.StringValue(source))
	e.SetField("cnt", event.FloatValue(count))
	e.SetField("sum_x", event.FloatValue(sum))

	return e
}

func makeMinMaxEvent(source string, minVal, maxVal float64) *event.Event {
	e := event.NewEvent(time.Now(), "")
	e.SetField("src", event.StringValue(source))
	e.SetField("min_x", event.FloatValue(minVal))
	e.SetField("max_x", event.FloatValue(maxVal))

	return e
}
