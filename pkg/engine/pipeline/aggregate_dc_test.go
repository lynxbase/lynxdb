package pipeline

import (
	"context"
	"fmt"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/memgov"
)

func TestAggregateDistinctCountAcrossHLLPromotion(t *testing.T) {
	for _, distinct := range []int{hllPromotionThreshold, hllPromotionThreshold + 1, 3 * hllPromotionThreshold} {
		t.Run(fmt.Sprint(distinct), func(t *testing.T) {
			rows := make([]map[string]event.Value, 0, distinct)
			for i := 0; i < distinct; i++ {
				rows = append(rows, map[string]event.Value{"x": event.StringValue(fmt.Sprintf("v%d", i))})
			}

			acct := memgov.NewTestBudget("test", 1<<30).NewAccount("agg")
			iter := NewAggregateIterator(NewRowScanIterator(rows, 512), []AggFunc{
				{Name: "dc", Field: "x", Alias: "d"},
				{Name: "estdc_error", Field: "x", Alias: "e"},
			}, nil, acct)

			ctx := context.Background()
			if err := iter.Init(ctx); err != nil {
				t.Fatal(err)
			}
			result, err := CollectAll(ctx, iter)
			if err != nil {
				t.Fatal(err)
			}
			if len(result) != 1 {
				t.Fatalf("expected 1 row, got %d", len(result))
			}

			got, ok := result[0]["d"].TryAsInt()
			if !ok {
				t.Fatalf("d is not an int: %v", result[0]["d"])
			}
			tolerance := float64(distinct) * 0.05
			if diff := float64(got) - float64(distinct); diff > tolerance || diff < -tolerance {
				t.Fatalf("distinct count %d, want %d within %.0f", got, distinct, tolerance)
			}
			if result[0]["e"].IsNull() {
				t.Fatal("estdc_error is null")
			}
		})
	}
}
