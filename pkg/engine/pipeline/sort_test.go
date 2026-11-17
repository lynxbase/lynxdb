package pipeline

import (
	"context"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// makeRowsWithField creates n rows with field "key" set to sequential integers
// and "data" set to a string of the given size.
func makeRowsWithField(n, dataSize int) []map[string]event.Value {
	data := make([]byte, dataSize)
	for i := range data {
		data[i] = 'x'
	}
	dataStr := string(data)

	rows := make([]map[string]event.Value, n)
	for i := 0; i < n; i++ {
		rows[i] = map[string]event.Value{
			"key":  event.IntValue(int64(i)),
			"data": event.StringValue(dataStr),
		}
	}

	return rows
}

func TestSortInMemory(t *testing.T) {
	rows := makeRowsWithField(100, 8)
	// Reverse order so sort actually has work to do.
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}

	child := NewRowScanIterator(rows, DefaultBatchSize)
	iter := NewSortIterator(child, []SortField{{Name: "key", Desc: false}}, DefaultBatchSize)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 100 {
		t.Fatalf("expected 100 rows, got %d", len(result))
	}
	for i := 0; i < len(result); i++ {
		got := result[i]["key"].AsInt()
		if got != int64(i) {
			t.Fatalf("row %d: expected key=%d, got %d", i, i, got)
		}
	}
}

func TestSortEmptyInput(t *testing.T) {
	child := NewRowScanIterator(nil, DefaultBatchSize)
	iter := NewSortIterator(child, []SortField{{Name: "key", Desc: false}}, DefaultBatchSize)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(result))
	}
}
