package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// makeEDGARRows creates test rows mimicking EDGAR CSV access log lines.
// Each row has _time (parsed timestamp), _raw (CSV line), and uri_path field.
func makeEDGARRows() []map[string]event.Value {
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

	out := make([]map[string]event.Value, len(rows))
	for i, r := range rows {
		ts := baseTime.Add(time.Duration(r.offsetMs) * time.Millisecond)
		rawCSV := ts.Format("2006-01-02T15:04:05.000-0700") + "," + r.cik + "," + r.uriPath
		out[i] = map[string]event.Value{
			"_time":    event.TimestampValue(ts),
			"_raw":     event.StringValue(rawCSV),
			"uri_path": event.StringValue(r.uriPath),
		}
	}

	return out
}

func TestIntegration_BinTimeBuckets(t *testing.T) {
	// BIN _time span=100ms + STATS count BY _time → should have 2+ buckets
	scan := NewRowScanIterator(makeEDGARRows(), 1024)
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
