package pipeline

import (
	"context"
	"testing"

	"github.com/RoaringBitmap/roaring"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

func TestIntegration_Filter_BSIHandledRows_ShortCircuitLoweredRangePredicate(t *testing.T) {
	expr := bsiFilterStatusExpr()
	prog, err := vm.CompilePredicate(expr)
	if err != nil {
		t.Fatalf("CompilePredicate: %v", err)
	}
	input := bsiFilterBatch(200, 200, 200)
	input.BSIHandledFields = map[string]*roaring.Bitmap{
		"status": roaring.BitmapOf(0, 2),
	}
	iter := NewFilterIteratorWithExpr(&singleBatchIterator{batch: input}, prog, expr)

	got := collectSingleBatch(t, iter)
	if got == nil {
		t.Fatal("filtered batch = nil, want BSI-handled rows")
	}
	if got.Len != 2 {
		t.Fatalf("filtered batch Len = %d, want 2", got.Len)
	}
	for i, wantRowID := range []int64{0, 2} {
		gotRowID, ok := got.Value("row_id", i).TryAsInt()
		if !ok || gotRowID != wantRowID {
			t.Fatalf("row %d row_id = %v, want %d", i, got.Value("row_id", i), wantRowID)
		}
	}
}

func TestIntegration_Filter_NoBSIHandledRows_EvaluatesLoweredRangePredicatePerRow(t *testing.T) {
	expr := bsiFilterStatusExpr()
	prog, err := vm.CompilePredicate(expr)
	if err != nil {
		t.Fatalf("CompilePredicate: %v", err)
	}
	iter := NewFilterIteratorWithExpr(&singleBatchIterator{batch: bsiFilterBatch(200, 499, 500)}, prog, expr)

	got := collectSingleBatch(t, iter)
	if got == nil {
		t.Fatal("filtered batch = nil, want row with status 500")
	}
	if got.Len != 1 {
		t.Fatalf("filtered batch Len = %d, want 1", got.Len)
	}
	if rowID, ok := got.Value("row_id", 0).TryAsInt(); !ok || rowID != 2 {
		t.Fatalf("row_id = %v, want 2", got.Value("row_id", 0))
	}
}

func bsiFilterStatusExpr() *spl2.CompareExpr {
	return &spl2.CompareExpr{
		Left:         &spl2.FieldExpr{Name: "status"},
		Op:           ">=",
		Right:        &spl2.LiteralExpr{Value: "500"},
		LoweredToBSI: true,
	}
}

func bsiFilterBatch(statuses ...int64) *Batch {
	b := NewBatch(len(statuses))
	for i, status := range statuses {
		b.AddRow(map[string]event.Value{
			"row_id": event.IntValue(int64(i)),
			"status": event.IntValue(status),
		})
	}

	return b
}

type singleBatchIterator struct {
	batch *Batch
	seen  bool
}

func (i *singleBatchIterator) Init(context.Context) error { return nil }

func (i *singleBatchIterator) Next(context.Context) (*Batch, error) {
	if i.seen {
		return nil, nil
	}
	i.seen = true

	return i.batch, nil
}

func (i *singleBatchIterator) Close() error { return nil }

func (i *singleBatchIterator) Schema() []FieldInfo { return nil }

func collectSingleBatch(t *testing.T, iter Iterator) *Batch {
	t.Helper()
	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}
	defer iter.Close()
	batch, err := iter.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	next, err := iter.Next(ctx)
	if err != nil {
		t.Fatalf("Next exhausted: %v", err)
	}
	if next != nil {
		t.Fatalf("second Next = non-nil batch Len %d, want nil", next.Len)
	}

	return batch
}
