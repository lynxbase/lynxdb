package pipeline

import (
	"context"
	"fmt"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestTopN_BasicOrdering(t *testing.T) {
	// 100 rows, top 10 by "val" ascending.
	rows := make([]map[string]event.Value, 100)
	for i := 0; i < 100; i++ {
		rows[i] = map[string]event.Value{
			"val": event.IntValue(int64(i)),
		}
	}
	child := NewRowScanIterator(rows, 64)
	topn := NewTopNIterator(child, []SortField{{Name: "val", Desc: false}}, 10, 64)

	ctx := context.Background()
	result, err := CollectAll(ctx, topn)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 10 {
		t.Fatalf("expected 10 rows, got %d", len(result))
	}
	// Should be [0, 1, 2, ..., 9].
	for i, row := range result {
		v := row["val"].AsInt()
		if v != int64(i) {
			t.Errorf("row %d: expected %d, got %d", i, i, v)
		}
	}
}

func TestTopN_Desc(t *testing.T) {
	// 100 rows, top 10 by "val" descending.
	rows := make([]map[string]event.Value, 100)
	for i := 0; i < 100; i++ {
		rows[i] = map[string]event.Value{
			"val": event.IntValue(int64(i)),
		}
	}
	child := NewRowScanIterator(rows, 64)
	topn := NewTopNIterator(child, []SortField{{Name: "val", Desc: true}}, 10, 64)

	ctx := context.Background()
	result, err := CollectAll(ctx, topn)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 10 {
		t.Fatalf("expected 10 rows, got %d", len(result))
	}
	// Should be [99, 98, 97, ..., 90].
	for i, row := range result {
		expected := int64(99 - i)
		v := row["val"].AsInt()
		if v != expected {
			t.Errorf("row %d: expected %d, got %d", i, expected, v)
		}
	}
}

func TestTopN_MultiField(t *testing.T) {
	// Sort by "a" ascending, then "b" descending. Top 3.
	rows := []map[string]event.Value{
		{"a": event.IntValue(1), "b": event.IntValue(10)},
		{"a": event.IntValue(1), "b": event.IntValue(20)},
		{"a": event.IntValue(2), "b": event.IntValue(5)},
		{"a": event.IntValue(1), "b": event.IntValue(15)},
		{"a": event.IntValue(2), "b": event.IntValue(25)},
	}
	child := NewRowScanIterator(rows, 64)
	topn := NewTopNIterator(child, []SortField{
		{Name: "a", Desc: false},
		{Name: "b", Desc: true},
	}, 3, 64)

	ctx := context.Background()
	result, err := CollectAll(ctx, topn)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result))
	}
	// Expected: (1,20), (1,15), (1,10). a=1 first (asc), then b desc within.
	expected := []struct{ a, b int64 }{
		{1, 20}, {1, 15}, {1, 10},
	}
	for i, row := range result {
		a := row["a"].AsInt()
		b := row["b"].AsInt()
		if a != expected[i].a || b != expected[i].b {
			t.Errorf("row %d: expected (%d,%d), got (%d,%d)", i, expected[i].a, expected[i].b, a, b)
		}
	}
}

func TestTopN_MatchesSortPlusHead(t *testing.T) {
	// Compare TopN vs Sort+Head — results must be identical.
	rows := make([]map[string]event.Value, 200)
	for i := 0; i < 200; i++ {
		rows[i] = map[string]event.Value{
			"val": event.IntValue(int64(i * 3 % 200)), // non-trivial ordering
		}
	}

	// TopN.
	child1 := NewRowScanIterator(rows, 64)
	topn := NewTopNIterator(child1, []SortField{{Name: "val", Desc: true}}, 15, 64)
	ctx := context.Background()
	topnResult, err := CollectAll(ctx, topn)
	if err != nil {
		t.Fatal(err)
	}

	// Sort + Head.
	child2 := NewRowScanIterator(rows, 64)
	sortIter := NewSortIterator(child2, []SortField{{Name: "val", Desc: true}}, 64)
	limitIter := NewLimitIterator(sortIter, 15)
	sortResult, err := CollectAll(ctx, limitIter)
	if err != nil {
		t.Fatal(err)
	}

	if len(topnResult) != len(sortResult) {
		t.Fatalf("length mismatch: topn=%d, sort+head=%d", len(topnResult), len(sortResult))
	}
	for i := 0; i < len(topnResult); i++ {
		tv := topnResult[i]["val"].AsInt()
		sv := sortResult[i]["val"].AsInt()
		if tv != sv {
			t.Errorf("row %d: topn=%d, sort+head=%d", i, tv, sv)
		}
	}
}

func TestTopN_LimitLargerThanInput(t *testing.T) {
	rows := []map[string]event.Value{
		{"val": event.IntValue(3)},
		{"val": event.IntValue(1)},
		{"val": event.IntValue(2)},
	}
	child := NewRowScanIterator(rows, 64)
	topn := NewTopNIterator(child, []SortField{{Name: "val", Desc: false}}, 10, 64)

	ctx := context.Background()
	result, err := CollectAll(ctx, topn)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result))
	}
	// Should be sorted: 1, 2, 3.
	expected := []int64{1, 2, 3}
	for i, row := range result {
		v := row["val"].AsInt()
		if v != expected[i] {
			t.Errorf("row %d: expected %d, got %d", i, expected[i], v)
		}
	}
}

func TestTopN_EmptyInput(t *testing.T) {
	child := NewRowScanIterator(nil, 64)
	topn := NewTopNIterator(child, []SortField{{Name: "val", Desc: false}}, 10, 64)

	ctx := context.Background()
	result, err := CollectAll(ctx, topn)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(result))
	}
}

func TestTopN_MatchesSortPlusTail(t *testing.T) {
	// Verify that TopN with inverted sort directions produces the same result
	// set as sort + tail. This validates the optimizer's sort+tail→TopN rewrite.
	//
	// Equivalence: sort -val | tail 10 selects the same rows as TopN(asc val, 10).
	// The output ordering differs (sort+tail preserves desc, TopN outputs asc),
	// so we compare result sets rather than positional equality.
	rows := make([]map[string]event.Value, 200)
	for i := 0; i < 200; i++ {
		rows[i] = map[string]event.Value{
			"val": event.IntValue(int64(i * 7 % 200)), // non-trivial ordering
		}
	}

	ctx := context.Background()

	// Sort descending + tail 10 (reference implementation).
	child1 := NewRowScanIterator(rows, 64)
	sortIter := NewSortIterator(child1, []SortField{{Name: "val", Desc: true}}, 64)
	tailIter := NewTailIterator(sortIter, 10, 64)
	tailResult, err := CollectAll(ctx, tailIter)
	if err != nil {
		t.Fatal(err)
	}

	// TopN with inverted direction (asc instead of desc), same limit.
	// This is exactly what the optimizer produces for sort -val | tail 10.
	child2 := NewRowScanIterator(rows, 64)
	topn := NewTopNIterator(child2, []SortField{{Name: "val", Desc: false}}, 10, 64)
	topnResult, err := CollectAll(ctx, topn)
	if err != nil {
		t.Fatal(err)
	}

	if len(tailResult) != len(topnResult) {
		t.Fatalf("length mismatch: sort+tail=%d, topn(inverted)=%d", len(tailResult), len(topnResult))
	}

	// Collect values into sets and compare.
	tailSet := make(map[int64]bool, len(tailResult))
	for _, row := range tailResult {
		tailSet[row["val"].AsInt()] = true
	}
	topnSet := make(map[int64]bool, len(topnResult))
	for _, row := range topnResult {
		topnSet[row["val"].AsInt()] = true
	}
	for v := range tailSet {
		if !topnSet[v] {
			t.Errorf("value %d in sort+tail result but not in topn(inverted)", v)
		}
	}
	for v := range topnSet {
		if !tailSet[v] {
			t.Errorf("value %d in topn(inverted) result but not in sort+tail", v)
		}
	}
}

func TestTopN_MatchesSortPlusTail_MultiField(t *testing.T) {
	// Multi-field: sort -a b | tail 3  ≡  TopN(asc a, desc b, 3) (same result set).
	rows := []map[string]event.Value{
		{"a": event.IntValue(1), "b": event.IntValue(10)},
		{"a": event.IntValue(2), "b": event.IntValue(5)},
		{"a": event.IntValue(1), "b": event.IntValue(20)},
		{"a": event.IntValue(3), "b": event.IntValue(1)},
		{"a": event.IntValue(2), "b": event.IntValue(25)},
		{"a": event.IntValue(1), "b": event.IntValue(15)},
		{"a": event.IntValue(3), "b": event.IntValue(8)},
		{"a": event.IntValue(2), "b": event.IntValue(12)},
	}

	ctx := context.Background()

	// sort -a b | tail 3 (reference).
	child1 := NewRowScanIterator(rows, 64)
	sortIter := NewSortIterator(child1, []SortField{
		{Name: "a", Desc: true},
		{Name: "b", Desc: false},
	}, 64)
	tailIter := NewTailIterator(sortIter, 3, 64)
	tailResult, err := CollectAll(ctx, tailIter)
	if err != nil {
		t.Fatal(err)
	}

	// TopN(asc a, desc b, 3) — inverted directions.
	child2 := NewRowScanIterator(rows, 64)
	topn := NewTopNIterator(child2, []SortField{
		{Name: "a", Desc: false},
		{Name: "b", Desc: true},
	}, 3, 64)
	topnResult, err := CollectAll(ctx, topn)
	if err != nil {
		t.Fatal(err)
	}

	if len(tailResult) != len(topnResult) {
		t.Fatalf("length mismatch: sort+tail=%d, topn(inverted)=%d", len(tailResult), len(topnResult))
	}

	// Compare as sets of (a, b) pairs.
	type pair struct{ a, b int64 }
	tailSet := make(map[pair]bool, len(tailResult))
	for _, row := range tailResult {
		tailSet[pair{row["a"].AsInt(), row["b"].AsInt()}] = true
	}
	topnSet := make(map[pair]bool, len(topnResult))
	for _, row := range topnResult {
		topnSet[pair{row["a"].AsInt(), row["b"].AsInt()}] = true
	}
	for p := range tailSet {
		if !topnSet[p] {
			t.Errorf("pair (%d,%d) in sort+tail but not in topn(inverted)", p.a, p.b)
		}
	}
	for p := range topnSet {
		if !tailSet[p] {
			t.Errorf("pair (%d,%d) in topn(inverted) but not in sort+tail", p.a, p.b)
		}
	}
}

func BenchmarkTopN_vs_SortHead(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000} {
		rows := make([]map[string]event.Value, n)
		for i := 0; i < n; i++ {
			rows[i] = map[string]event.Value{
				"val": event.IntValue(int64(i * 7 % n)),
			}
		}
		k := 10

		b.Run(fmt.Sprintf("TopN_%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				child := NewRowScanIterator(rows, 1024)
				topn := NewTopNIterator(child, []SortField{{Name: "val", Desc: true}}, k, 1024)
				_, _ = CollectAll(context.Background(), topn)
			}
		})

		b.Run(fmt.Sprintf("SortHead_%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				child := NewRowScanIterator(rows, 1024)
				sortIter := NewSortIterator(child, []SortField{{Name: "val", Desc: true}}, 1024)
				limitIter := NewLimitIterator(sortIter, k)
				_, _ = CollectAll(context.Background(), limitIter)
			}
		})
	}
}
