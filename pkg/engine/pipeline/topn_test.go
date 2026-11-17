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

func TestTopN_MatchesSortPlusLimit(t *testing.T) {
	// TopN selects the same row set as a full sort followed by a limit.
	rows := make([]map[string]event.Value, 200)
	for i := 0; i < 200; i++ {
		rows[i] = map[string]event.Value{
			"val": event.IntValue(int64(i * 7 % 200)), // non-trivial ordering
		}
	}

	ctx := context.Background()

	child1 := NewRowScanIterator(rows, 64)
	sortIter := NewSortIterator(child1, []SortField{{Name: "val", Desc: false}}, 64)
	limitResult, err := CollectAll(ctx, NewLimitIterator(sortIter, 10))
	if err != nil {
		t.Fatal(err)
	}

	child2 := NewRowScanIterator(rows, 64)
	topn := NewTopNIterator(child2, []SortField{{Name: "val", Desc: false}}, 10, 64)
	topnResult, err := CollectAll(ctx, topn)
	if err != nil {
		t.Fatal(err)
	}

	if len(limitResult) != len(topnResult) {
		t.Fatalf("length mismatch: sort+limit=%d, topn=%d", len(limitResult), len(topnResult))
	}

	limitSet := make(map[int64]bool, len(limitResult))
	for _, row := range limitResult {
		limitSet[row["val"].AsInt()] = true
	}
	topnSet := make(map[int64]bool, len(topnResult))
	for _, row := range topnResult {
		topnSet[row["val"].AsInt()] = true
	}
	for v := range limitSet {
		if !topnSet[v] {
			t.Errorf("value %d in sort+limit result but not in topn", v)
		}
	}
	for v := range topnSet {
		if !limitSet[v] {
			t.Errorf("value %d in topn result but not in sort+limit", v)
		}
	}
}

func TestTopN_MatchesSortPlusLimit_MultiField(t *testing.T) {
	// sort a -b | head 3 and TopN(asc a, desc b, 3) select the same row set.
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

	fields := []SortField{
		{Name: "a", Desc: false},
		{Name: "b", Desc: true},
	}
	child1 := NewRowScanIterator(rows, 64)
	sortIter := NewSortIterator(child1, fields, 64)
	limitResult, err := CollectAll(ctx, NewLimitIterator(sortIter, 3))
	if err != nil {
		t.Fatal(err)
	}

	child2 := NewRowScanIterator(rows, 64)
	topn := NewTopNIterator(child2, fields, 3, 64)
	topnResult, err := CollectAll(ctx, topn)
	if err != nil {
		t.Fatal(err)
	}

	if len(limitResult) != len(topnResult) {
		t.Fatalf("length mismatch: sort+limit=%d, topn=%d", len(limitResult), len(topnResult))
	}

	// Compare as sets of (a, b) pairs.
	type pair struct{ a, b int64 }
	limitSet := make(map[pair]bool, len(limitResult))
	for _, row := range limitResult {
		limitSet[pair{row["a"].AsInt(), row["b"].AsInt()}] = true
	}
	topnSet := make(map[pair]bool, len(topnResult))
	for _, row := range topnResult {
		topnSet[pair{row["a"].AsInt(), row["b"].AsInt()}] = true
	}
	for p := range limitSet {
		if !topnSet[p] {
			t.Errorf("pair (%d,%d) in sort+limit but not in topn", p.a, p.b)
		}
	}
	for p := range topnSet {
		if !limitSet[p] {
			t.Errorf("pair (%d,%d) in topn but not in sort+limit", p.a, p.b)
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
