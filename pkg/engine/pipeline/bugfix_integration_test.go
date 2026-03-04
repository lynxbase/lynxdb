package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

// makeEDGAREvents creates test events mimicking EDGAR CSV access log lines.
// Each event has _time (parsed timestamp), _raw (CSV line), and uri_path field.
func makeEDGAREvents() []*event.Event {
	// 20 events across 2 time buckets (~300ms apart), various CIK, file types
	baseTime := time.Date(2025, 6, 30, 23, 59, 57, 0, time.FixedZone("EDT", -4*3600))
	type row struct {
		offsetMs  int
		cik       string
		accession string
		ext       string
		uriPath   string
	}
	rows := []row{
		{0, "1234", "0001234567-25-000001", "txt", "/archives/edgar/data/1234/0001234567-25-000001.txt"},
		{10, "1234", "0001234567-25-000002", "xml", "/archives/edgar/data/1234/0001234567-25-000002.xml"},
		{20, "5678", "0005678901-25-000001", "htm", "/archives/edgar/data/5678/0005678901-25-000001.htm"},
		{30, "5678", "0005678901-25-000002", "txt", "/archives/edgar/data/5678/0005678901-25-000002.txt"},
		{40, "9012", "0009012345-25-000001", "pdf", "/archives/edgar/data/9012/0009012345-25-000001.pdf"},
		{50, "9012", "0009012345-25-000001", "txt", "/archives/edgar/data/9012/0009012345-25-000001.txt"},
		{60, "1234", "0001234567-25-000003", "xml", "/archives/edgar/data/1234/0001234567-25-000003.xml"},
		{70, "3456", "0003456789-25-000001", "htm", "/archives/edgar/data/3456/0003456789-25-000001.htm"},
		{80, "3456", "0003456789-25-000002", "jpg", "/archives/edgar/data/3456/0003456789-25-000002.jpg"},
		{90, "7890", "0007890123-25-000001", "txt", "/archives/edgar/data/7890/0007890123-25-000001.txt"},
		// Second bucket: 300ms+ later
		{300, "1234", "0001234567-25-000004", "txt", "/archives/edgar/data/1234/0001234567-25-000004.txt"},
		{310, "5678", "0005678901-25-000003", "xml", "/archives/edgar/data/5678/0005678901-25-000003.xml"},
		{320, "9012", "0009012345-25-000002", "htm", "/archives/edgar/data/9012/0009012345-25-000002.htm"},
		{330, "3456", "0003456789-25-000003", "txt", "/archives/edgar/data/3456/0003456789-25-000003.txt"},
		{340, "7890", "0007890123-25-000002", "pdf", "/archives/edgar/data/7890/0007890123-25-000002.pdf"},
		{350, "1234", "0001234567-25-000005", "xml", "/archives/edgar/data/1234/0001234567-25-000005.xml"},
		{360, "5678", "0005678901-25-000004", "txt", "/archives/edgar/data/5678/0005678901-25-000004.txt"},
		{370, "9012", "0009012345-25-000003", "htm", "/other/path/not-edgar.htm"},
		{380, "3456", "0003456789-25-000004", "jpg", "/archives/edgar/data/3456/0003456789-25-000004.jpg"},
		{390, "7890", "0007890123-25-000003", "txt", "/archives/edgar/data/7890/0007890123-25-000003.txt"},
	}

	events := make([]*event.Event, len(rows))
	for i, r := range rows {
		ts := baseTime.Add(time.Duration(r.offsetMs) * time.Millisecond)
		rawCSV := ts.Format("2006-01-02T15:04:05.000-0700") + "," + r.cik + "," + r.uriPath
		e := event.NewEvent(ts, rawCSV)
		e.SetField("uri_path", event.StringValue(r.uriPath))
		events[i] = e
	}

	return events
}

func TestIntegration_RexCikStats(t *testing.T) {
	// REX to extract cik from uri_path, then STATS count, dc BY cik
	events := makeEDGAREvents()
	scan := NewScanIterator(events, 1024)

	// REX field=uri_path /archives/edgar/data/(?P<cik>\d+)/
	rex, err := NewRexIterator(scan, "uri_path", `/archives/edgar/data/(?P<cik>\d+)/`)
	if err != nil {
		t.Fatal(err)
	}

	aggs := []AggFunc{
		{Name: "count", Alias: "count"},
		{Name: "dc", Field: "uri_path", Alias: "dc_uri"},
	}
	agg := NewAggregateIterator(rex, aggs, []string{"cik"}, nil)

	ctx := context.Background()
	rows, err := CollectAll(ctx, agg)
	if err != nil {
		t.Fatal(err)
	}

	// Should have 5 distinct CIKs (1234, 5678, 9012, 3456, 7890) + 1 null group (event 17 doesn't match)
	if len(rows) < 5 {
		t.Errorf("expected at least 5 groups, got %d", len(rows))
	}

	// Verify null cik group uses "" not null
	for _, row := range rows {
		cik := row["cik"]
		if cik.IsNull() {
			t.Error("cik should not be null in result — should be empty string")
		}
	}
}

func TestIntegration_RexFileExtStats(t *testing.T) {
	// REX to extract file extension, STATS count BY ext
	events := makeEDGAREvents()
	scan := NewScanIterator(events, 1024)

	rex, err := NewRexIterator(scan, "uri_path", `\.(?P<file_ext>\w+)$`)
	if err != nil {
		t.Fatal(err)
	}

	aggs := []AggFunc{{Name: "count", Alias: "count"}}
	agg := NewAggregateIterator(rex, aggs, []string{"file_ext"}, nil)

	ctx := context.Background()
	rows, err := CollectAll(ctx, agg)
	if err != nil {
		t.Fatal(err)
	}

	// Expected extensions: txt, xml, htm, pdf, jpg
	extCounts := make(map[string]int64)
	for _, row := range rows {
		ext := row["file_ext"]
		extCounts[ext.AsString()] = row["count"].AsInt()
	}
	if len(extCounts) < 5 {
		t.Errorf("expected at least 5 extensions, got %d: %v", len(extCounts), extCounts)
	}

	// Verify no null keys
	for _, row := range rows {
		if row["file_ext"].IsNull() {
			t.Error("file_ext should not be null in output")
		}
	}
}

func TestIntegration_BinTimeBuckets(t *testing.T) {
	// BIN _time span=100ms + STATS count BY _time → should have 2+ buckets
	events := makeEDGAREvents()
	scan := NewScanIterator(events, 1024)
	bin := NewBinIterator(scan, "_time", "_time", 100*time.Millisecond)

	aggs := []AggFunc{{Name: "count", Alias: "count"}}
	agg := NewAggregateIterator(bin, aggs, []string{"_time"}, nil)

	ctx := context.Background()
	rows, err := CollectAll(ctx, agg)
	if err != nil {
		t.Fatal(err)
	}

	if len(rows) < 2 {
		t.Errorf("expected at least 2 time buckets, got %d", len(rows))
		for _, row := range rows {
			t.Logf("  bucket: %v count: %v", row["_time"], row["count"])
		}
	}

	// Total count should equal 20
	total := int64(0)
	for _, row := range rows {
		total += row["count"].AsInt()
	}
	if total != 20 {
		t.Errorf("total count: got %d, want 20", total)
	}
}

func TestIntegration_NullPropagation(t *testing.T) {
	// Use static batch with explicit null so columnar layout preserves nulls
	batch := NewBatch(2)
	batch.AddRow(map[string]event.Value{
		"_raw": event.StringValue("test"),
		"x":    event.StringValue("hello"),
	})
	batch.AddRow(map[string]event.Value{
		"_raw": event.StringValue("test"),
		"x":    event.NullValue(),
	})
	child := &staticIterator{batches: []*Batch{batch}}

	// EVAL y = x . " world" — should be null when x is null
	concatProg := &vm.Program{}
	xIdx := concatProg.AddFieldName("x")
	worldIdx := concatProg.AddConstant(event.StringValue(" world"))
	concatProg.EmitOp(vm.OpLoadField, xIdx)
	concatProg.EmitOp(vm.OpConstStr, worldIdx)
	concatProg.EmitOp(vm.OpConcat)
	concatProg.EmitOp(vm.OpReturn)

	eval := NewEvalIterator(child, []EvalAssignment{
		{Field: "y", Program: concatProg},
	})

	ctx := context.Background()
	rows, err := CollectAll(ctx, eval)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	// Row 0: x="hello", y should be "hello world"
	if rows[0]["y"].AsString() != "hello world" {
		t.Errorf("row 0 y: got %v, want 'hello world'", rows[0]["y"])
	}
	// Row 1: x=null, y should be null
	if !rows[1]["y"].IsNull() {
		t.Errorf("row 1 y: got %v, want null", rows[1]["y"])
	}
}

func TestIntegration_CoalesceNull(t *testing.T) {
	// Test coalesce(null, null, "default") → "default"
	events := []*event.Event{
		{Time: time.Now(), Raw: "test", Fields: map[string]event.Value{}},
	}
	scan := NewScanIterator(events, 1024)

	prog := &vm.Program{}
	aIdx := prog.AddFieldName("a")
	bIdx := prog.AddFieldName("b")
	defIdx := prog.AddConstant(event.StringValue("default"))
	prog.EmitOp(vm.OpLoadField, aIdx) // null
	prog.EmitOp(vm.OpLoadField, bIdx) // null
	prog.EmitOp(vm.OpConstStr, defIdx)
	prog.EmitOp(vm.OpCoalesce, 3)
	prog.EmitOp(vm.OpReturn)

	eval := NewEvalIterator(scan, []EvalAssignment{
		{Field: "result", Program: prog},
	})

	ctx := context.Background()
	rows, err := CollectAll(ctx, eval)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0]["result"].AsString() != "default" {
		t.Errorf("coalesce: got %v, want 'default'", rows[0]["result"])
	}
}

func TestIntegration_MultiStage(t *testing.T) {
	// Multi-stage: REX cik → STATS count BY cik → verify results
	events := makeEDGAREvents()
	scan := NewScanIterator(events, 1024)

	// REX to extract cik
	rex, err := NewRexIterator(scan, "uri_path", `/archives/edgar/data/(?P<cik>\d+)/`)
	if err != nil {
		t.Fatal(err)
	}

	// Filter: WHERE isnotnull(cik) using OpIsNotNull
	filterProg := &vm.Program{}
	cikIdx := filterProg.AddFieldName("cik")
	filterProg.EmitOp(vm.OpLoadField, cikIdx)
	filterProg.EmitOp(vm.OpIsNotNull)
	filterProg.EmitOp(vm.OpReturn)
	filter := NewFilterIterator(rex, filterProg)

	// STATS count BY cik
	aggs := []AggFunc{{Name: "count", Alias: "count"}}
	agg := NewAggregateIterator(filter, aggs, []string{"cik"}, nil)

	// SORT count desc
	sort := NewSortIterator(agg, []SortField{{Name: "count", Desc: true}}, 1024)

	ctx := context.Background()
	rows, err := CollectAll(ctx, sort)
	if err != nil {
		t.Fatal(err)
	}

	// Should have 5 distinct CIKs (filtered out the non-matching event)
	if len(rows) != 5 {
		t.Errorf("expected 5 CIK groups, got %d", len(rows))
		for _, row := range rows {
			t.Logf("  cik=%v count=%v", row["cik"], row["count"])
		}
	}

	// Total should be 19 (20 events minus 1 non-matching)
	total := int64(0)
	for _, row := range rows {
		total += row["count"].AsInt()
	}
	if total != 19 {
		t.Errorf("total count: got %d, want 19", total)
	}

	// Verify sorted descending
	for i := 1; i < len(rows); i++ {
		prev := rows[i-1]["count"].AsInt()
		curr := rows[i]["count"].AsInt()
		if prev < curr {
			t.Errorf("not sorted desc: %d < %d at index %d", prev, curr, i)
		}
	}
}
