package pipeline

import (
	"context"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ir"
)

func TestAggregateGroupByObjectPath(t *testing.T) {
	commit := func(collection string) event.Value {
		return event.ObjectValue(map[string]event.Value{"collection": event.StringValue(collection)})
	}
	rows := []map[string]event.Value{
		{"commit": commit("a")},
		{"commit": commit("b")},
		{"commit": commit("a")},
		{"commit": event.StringValue("not an object")},
		{"other": event.IntValue(1)},
		{"commit.collection": event.StringValue("flat"), "commit": commit("ignored")},
	}

	iter, err := NewAggregateIterator(NewRowScanIterator(rows, 4),
		[]AggFunc{{Alias: "n", Agg: exec.RowAgg{Func: exec.Count, Rows: true}, Kind: ir.KindInt}}, []string{"commit.collection"}, []ir.Kind{ir.KindAny})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}
	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	got := make(map[string]int64)
	for _, row := range result {
		key := ""
		if v := row["commit.collection"]; !v.IsNull() {
			key = v.AsString()
		}
		n, _ := row["n"].TryAsInt()
		got[key] = n
	}
	want := map[string]int64{"a": 2, "b": 1, "flat": 1, "": 2}
	if len(got) != len(want) {
		t.Fatalf("groups = %v, want %v", got, want)
	}
	for k, n := range want {
		if got[k] != n {
			t.Fatalf("groups = %v, want %v", got, want)
		}
	}
}
