package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

func TestCompareIteratorUsesAbsoluteChange(t *testing.T) {
	current := []map[string]event.Value{
		{
			"service": event.StringValue("api"),
			"n":       event.IntValue(15),
		},
		{
			"service": event.StringValue("worker"),
			"n":       event.IntValue(5),
		},
	}
	previous := []map[string]event.Value{
		{
			"service": event.StringValue("api"),
			"n":       event.IntValue(10),
		},
		{
			"service": event.StringValue("worker"),
			"n":       event.IntValue(0),
		},
	}

	iter := NewCompareIterator(
		NewRowScanIterator(current, 2),
		time.Hour,
		func(ctx context.Context) (Iterator, error) {
			return NewRowScanIterator(previous, 2), nil
		},
		2,
	)

	rows, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}

	byService := make(map[string]map[string]event.Value, len(rows))
	for _, row := range rows {
		byService[row["service"].AsString()] = row
	}

	assertCompareValue(t, byService["api"], "previous_n", 10)
	assertCompareValue(t, byService["api"], "change_n", 5)
	assertCompareValue(t, byService["worker"], "previous_n", 0)
	assertCompareValue(t, byService["worker"], "change_n", 5)
}

func TestComparePreviousShiftsCurrentWindowEnd(t *testing.T) {
	source := &spl2.SourceClause{
		Index: "nginx",
		TimeRange: &spl2.SourceTimeRange{
			Relative: "-1h",
		},
	}

	shifted := shiftSourceClause(source, time.Hour)
	if shifted == nil || shifted.TimeRange == nil {
		t.Fatalf("missing shifted time range")
	}
	if shifted.TimeRange.Relative != "-2h" {
		t.Fatalf("relative: got %q, want -2h", shifted.TimeRange.Relative)
	}
	if shifted.TimeRange.End != "-1h" {
		t.Fatalf("end: got %q, want -1h", shifted.TimeRange.End)
	}
}

func TestBuildPipelineComparePreviousReplaysPriorWindow(t *testing.T) {
	now := time.Now()
	events := []*event.Event{
		event.NewEvent(now.Add(-30*time.Minute), "current"),
		event.NewEvent(now.Add(-90*time.Minute), "previous"),
		event.NewEvent(now.Add(-150*time.Minute), "older"),
	}
	store := &ServerIndexStore{
		Events: map[string][]*event.Event{"nginx": events},
	}

	query, err := spl2.Parse(`FROM nginx[-1h] | stats count() AS n | compare previous 1h`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	iter, err := BuildPipeline(context.Background(), query, store, 2)
	if err != nil {
		t.Fatalf("BuildPipeline: %v", err)
	}

	rows, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	assertCompareValue(t, rows[0], "n", 1)
	assertCompareValue(t, rows[0], "previous_n", 1)
	assertCompareValue(t, rows[0], "change_n", 0)
}

func assertCompareValue(t *testing.T, row map[string]event.Value, field string, want float64) {
	t.Helper()
	if row == nil {
		t.Fatalf("missing row")
	}
	got := row[field]
	if got.IsNull() {
		t.Fatalf("%s is null, want %v", field, want)
	}
	gotF, ok := vm.ValueToFloat(got)
	if !ok {
		t.Fatalf("%s is not numeric: %v", field, got)
	}
	if gotF != want {
		t.Fatalf("%s: got %v, want %v", field, gotF, want)
	}
}
