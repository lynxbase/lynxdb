package pipeline

import (
	"context"
	"fmt"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestEventStatsEnrichesRows(t *testing.T) {
	n := 100
	rows := make([]map[string]event.Value, n)
	for i := 0; i < n; i++ {
		rows[i] = map[string]event.Value{
			"group": event.StringValue(fmt.Sprintf("g%d", i%5)),
			"val":   event.FloatValue(float64(i)),
		}
	}

	child := NewRowScanIterator(rows, 64)
	aggs := []AggFunc{
		{Name: "count", Alias: "cnt"},
	}
	iter := NewEventStatsIterator(child, aggs, []string{"group"}, 128)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != n {
		t.Fatalf("expected %d rows, got %d", n, len(result))
	}

	// 5 groups, 20 rows each.
	for _, row := range result {
		cnt := row["cnt"]
		if cnt.AsInt() != 20 {
			t.Errorf("expected count=20 for group %v, got %d", row["group"], cnt.AsInt())
		}
	}
}
