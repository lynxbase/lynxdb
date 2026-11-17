package pipeline

import (
	"context"
	"fmt"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func reverseMemgovRows(n int) []map[string]event.Value {
	rows := make([]map[string]event.Value, n)
	for i := range rows {
		rows[i] = map[string]event.Value{
			"seq":  event.IntValue(int64(i)),
			"host": event.StringValue(fmt.Sprintf("host-with-a-long-name-%04d", i)),
		}
	}

	return rows
}

func assertReversed(t *testing.T, got []map[string]event.Value, n int) {
	t.Helper()
	if len(got) != n {
		t.Fatalf("rows: got %d, want %d", len(got), n)
	}
	for i, row := range got {
		want := int64(n - 1 - i)
		if seq := row["seq"].AsInt(); seq != want {
			t.Fatalf("row %d: seq = %d, want %d", i, seq, want)
		}
	}
}

func TestReverseWithinBudget(t *testing.T) {
	const n = 500
	acct := newTestBudget(1 << 30).NewAccount()
	iter := NewReverseIteratorWithBudget(NewRowScanIterator(reverseMemgovRows(n), 32), 32, acct)

	got, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatal(err)
	}
	assertReversed(t, got, n)
}

func TestReverseBudgetExceeded(t *testing.T) {
	acct := newTestBudget(64).NewAccount()
	iter := NewReverseIteratorWithBudget(NewRowScanIterator(reverseMemgovRows(100), 16), 16, acct)

	if _, err := CollectAll(context.Background(), iter); err == nil {
		t.Fatal("expected reverse materialization to exceed tiny budget")
	}
}
