package pipeline

import (
	"context"
	"fmt"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/memgov"
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

func TestReverseBudgetExceededWithoutSpill(t *testing.T) {
	acct := memgov.NewTestBudget("reverse", 64).NewAccount("reverse")
	iter := NewReverseIteratorWithBudget(NewRowScanIterator(reverseMemgovRows(100), 16), 16, acct)

	if _, err := CollectAll(context.Background(), iter); err == nil {
		t.Fatal("expected reverse materialization to exceed tiny budget without spill")
	}
}

func TestReverseSpillsUnderBudgetPressure(t *testing.T) {
	const n = 500
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("create spill manager: %v", err)
	}
	defer mgr.CleanupAll()

	acct := memgov.NewTestBudget("reverse", 4*1024).NewAccount("reverse")
	iter := NewReverseIteratorWithSpill(NewRowScanIterator(reverseMemgovRows(n), 32), 32, acct, mgr)

	got, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatal(err)
	}
	assertReversed(t, got, n)

	if iter.spilledRows == 0 {
		t.Fatal("expected reverse spill path to write rows")
	}
}

func TestReverseSpillChunksReleasedOnEarlyClose(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("create spill manager: %v", err)
	}
	defer mgr.CleanupAll()

	acct := memgov.NewTestBudget("reverse", 4*1024).NewAccount("reverse")
	iter := NewReverseIteratorWithSpill(NewRowScanIterator(reverseMemgovRows(500), 32), 32, acct, mgr)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}
	// Read a single batch, then abandon the query.
	if _, err := iter.Next(ctx); err != nil {
		t.Fatal(err)
	}
	if err := iter.Close(); err != nil {
		t.Fatal(err)
	}
	if fileCount, _ := mgr.Stats(); fileCount != 0 {
		t.Fatalf("expected all spill chunks released on close, %d still tracked", fileCount)
	}
}
