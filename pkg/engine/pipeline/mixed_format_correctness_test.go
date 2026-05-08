package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/memgov"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/storage/segment"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

func TestIntegration_SegmentStream_MixedBSIAndNonBSIRangePredicate_MatchesBruteForceCount(t *testing.T) {
	bsiEvents := makeSegmentStreamBSIEvents(t, 512, func(i int) int64 {
		if i%4 == 0 {
			return 503
		}
		return 200
	})
	noBSIEvents := makeSegmentStreamBSIEvents(t, 512, func(i int) int64 {
		if i%5 == 0 {
			return 500
		}
		return 404
	})
	sources := []*SegmentSource{
		writeSegmentStreamBSISource(t, bsiEvents, "main", 256),
		writeSegmentStreamNoStatusBSISource(t, noBSIEvents, "main", 256),
	}
	hints := &SegmentStreamHints{
		RangePreds: []spl2.RangePredicate{{Field: "status", Min: "500", LoweredToBSI: false}},
	}
	scan := NewSegmentStreamIterator(
		sources,
		nil,
		hints,
		128,
		memgov.NewTestBudget("test", 0).NewAccount("test"),
	)
	scan.segPreds = nil

	expr := &spl2.CompareExpr{
		Left:  &spl2.FieldExpr{Name: "status"},
		Op:    ">=",
		Right: &spl2.LiteralExpr{Value: "500"},
	}
	prog, err := vm.CompilePredicate(expr)
	if err != nil {
		t.Fatalf("CompilePredicate: %v", err)
	}
	filter := NewFilterIteratorWithExpr(scan, prog, expr)
	agg := NewAggregateIterator(filter, []AggFunc{{Name: "count", Alias: "count"}}, nil, nil)

	rows, err := CollectAll(context.Background(), agg)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("result rows = %d, want 1", len(rows))
	}
	got := rows[0]["count"].AsInt()
	want := int64(countSegmentStreamBSIMatches(bsiEvents, 500) + countSegmentStreamBSIMatches(noBSIEvents, 500))
	if got != want {
		t.Fatalf("count = %d, want brute-force count %d", got, want)
	}
	if noBSIMatches := countSegmentStreamBSIMatches(noBSIEvents, 500); noBSIMatches == 0 {
		t.Fatal("no-BSI fixture has zero matching rows; test would not prove row-VM fallback")
	}
	stats := scan.Stats()
	if stats.RGRangeBSIChecks == 0 {
		t.Fatal("RGRangeBSIChecks = 0, want BSI side to consult range BSI")
	}
	if stats.RGRangeBSIMaskBytes == 0 {
		t.Fatal("RGRangeBSIMaskBytes = 0, want BSI side to produce row masks")
	}
}

func writeSegmentStreamNoStatusBSISource(t *testing.T, events []*event.Event, indexName string, rowGroupSize int) *SegmentSource {
	t.Helper()
	var buf bytes.Buffer
	w := segment.NewWriter(&buf)
	w.SetRowGroupSize(rowGroupSize)
	w.SetIndexConfig(segment.IndexConfig{
		ProfileOverrides: map[string]segment.IndexProfile{
			"status": segment.IndexProfileDefault,
		},
		BSIMaxBitCount: 64,
	})
	if _, err := w.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}
	reader, err := segment.OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	if reader.IndexProfile("status") == segment.IndexProfileRangeBSI {
		t.Fatal("status IndexProfile = RangeBSI, want default profile")
	}

	return &SegmentSource{
		Reader: reader,
		Index:  indexName,
		Meta: SegmentMeta{
			ID:         fmt.Sprintf("no-status-bsi-%s-%d", indexName, len(events)),
			MinTime:    events[0].Time,
			MaxTime:    events[len(events)-1].Time,
			EventCount: int64(len(events)),
			SizeBytes:  int64(buf.Len()),
		},
	}
}
