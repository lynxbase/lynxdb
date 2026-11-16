package pipeline

import (
	"context"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/memgov"
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

	acct := memgov.NewTestBudget("test", 1<<30).NewAccount("agg")
	iter := NewAggregateIterator(NewRowScanIterator(rows, 4),
		[]AggFunc{{Name: "count", Alias: "n"}}, []string{"commit.collection"}, acct)

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
