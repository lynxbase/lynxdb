package pipeline

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func largeRawRow(size int) map[string]event.Value {
	return map[string]event.Value{
		"_raw": event.StringValue(strings.Repeat("x", size)),
		"key":  event.StringValue("k"),
		"n":    event.IntValue(1),
	}
}

func TestEstimateRowAndBatchBytesIncludeStringPayload(t *testing.T) {
	row := largeRawRow(4096)

	rowBytes := EstimateRowBytes(row)
	if rowBytes <= 4096 {
		t.Fatalf("EstimateRowBytes() = %d, want it to include string payload", rowBytes)
	}

	batch := BatchFromRows([]map[string]event.Value{row})
	batchBytes := EstimateBatchBytes(batch)
	if batchBytes <= 4096 {
		t.Fatalf("EstimateBatchBytes() = %d, want it to include string payload", batchBytes)
	}
}

func TestLargeRowsTripBlockingOperatorBudgets(t *testing.T) {
	ctx := context.Background()
	row := largeRawRow(4096)

	t.Run("eventstats", func(t *testing.T) {
		acct := newTestBudget(2048).NewAccount()
		iter := NewEventStatsIteratorWithBudget(
			NewRowScanIterator([]map[string]event.Value{row}, 1),
			[]AggFunc{{Name: "count", Alias: "cnt"}},
			nil,
			1,
			acct,
		)
		if _, err := CollectAll(ctx, iter); err == nil {
			t.Fatal("expected large eventstats row to exceed 2KB budget")
		}
	})
}

func TestBoundedOperatorsTrackActualLargeRowBytes(t *testing.T) {
	ctx := context.Background()
	row := largeRawRow(4096)

	t.Run("topn", func(t *testing.T) {
		acct := newTestBudget(0).NewAccount()
		iter := NewTopNIteratorWithBudget(
			NewRowScanIterator([]map[string]event.Value{row}, 1),
			[]SortField{{Name: "n"}},
			1,
			1,
			acct,
		)
		if _, err := CollectAll(ctx, iter); err != nil {
			t.Fatal(err)
		}
		if acct.MaxUsed() <= 4096 {
			t.Fatalf("topn MaxUsed() = %d, want large row payload tracked", acct.MaxUsed())
		}
	})
}

func TestEstimateRowBytesArrayAccountsForPayload(t *testing.T) {
	// A row with a large array of 1000 strings should account for substantially
	// more memory than a row with a single scalar.
	elems := make([]event.Value, 1000)
	for i := range elems {
		elems[i] = event.StringValue(fmt.Sprintf("long_string_value_%06d", i))
	}
	arrRow := map[string]event.Value{
		"arr": event.ArrayValue(elems),
	}
	scalarRow := map[string]event.Value{
		"val": event.IntValue(42),
	}

	arrBytes := EstimateRowBytes(arrRow)
	scalarBytes := EstimateRowBytes(scalarRow)

	if arrBytes <= scalarBytes*10 {
		t.Errorf("arrBytes=%d should be >> scalarBytes=%d*10 for 1000 strings", arrBytes, scalarBytes)
	}
	// The array has 1000 strings each ~25 chars = ~25KB minimum.
	if arrBytes < 25000 {
		t.Errorf("arrBytes=%d, expected >= 25000 for 1000 strings", arrBytes)
	}
}

func TestEstimateBatchBytesIncludesArrayPayload(t *testing.T) {
	elems := make([]event.Value, 100)
	for i := range elems {
		elems[i] = event.StringValue("some_value")
	}
	batch := NewBatch(2)
	batch.AddRow(map[string]event.Value{
		"arr": event.ArrayValue(elems),
	})
	batch.AddRow(map[string]event.Value{
		"arr": event.ArrayValue(elems),
	})

	got := EstimateBatchBytes(batch)
	// 100 strings * 10 chars * 2 rows = at least 2000 bytes of string payload.
	if got < 2000 {
		t.Errorf("EstimateBatchBytes = %d, expected >= 2000", got)
	}
}

func TestEstimateRowMapBytes(t *testing.T) {
	tests := []struct {
		name     string
		row      map[string]event.Value
		minBytes int64
		maxBytes int64
	}{
		{
			name:     "empty row",
			row:      map[string]event.Value{},
			minBytes: 64,
			maxBytes: 64,
		},
		{
			name: "small string fields",
			row: map[string]event.Value{
				"key":   event.StringValue("hello"),
				"level": event.StringValue("info"),
			},
			minBytes: 64 + 2*(56+3) + 5 + 4, // overhead + 2 entries + string lens
			maxBytes: 300,
		},
		{
			name: "large _raw field",
			row: map[string]event.Value{
				"_raw":  event.StringValue(strings.Repeat("x", 500_000)),
				"_time": event.IntValue(1234567890),
			},
			minBytes: 500_000, // must account for the large string
			maxBytes: 501_000,
		},
		{
			name: "int and float fields only",
			row: map[string]event.Value{
				"status":   event.IntValue(200),
				"duration": event.FloatValue(1.5),
			},
			minBytes: 64 + 2*56,
			maxBytes: 300,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := estimateRowMapBytes(tt.row)
			if got < tt.minBytes {
				t.Errorf("estimate %d < min %d", got, tt.minBytes)
			}
			if got > tt.maxBytes {
				t.Errorf("estimate %d > max %d", got, tt.maxBytes)
			}
		})
	}
}
