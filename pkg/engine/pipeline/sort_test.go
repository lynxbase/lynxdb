package pipeline

import (
	"context"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/memgov"
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
	acct := memgov.NewTestBudget("test", 1<<30).NewAccount("sort")
	iter := NewSortIteratorWithBudget(child, []SortField{{Name: "key", Desc: false}}, DefaultBatchSize, acct)

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
	acct := memgov.NewTestBudget("test", 1<<20).NewAccount("sort")
	iter := NewSortIteratorWithBudget(child, []SortField{{Name: "key", Desc: false}}, DefaultBatchSize, acct)

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

func TestSortBudgetExceeded(t *testing.T) {
	rows := makeRowsWithField(100, 8)
	child := NewRowScanIterator(rows, 64)

	acct := memgov.NewTestBudget("test", 1024).NewAccount("sort")
	iter := NewSortIteratorWithBudget(child, []SortField{{Name: "key", Desc: false}}, 64, acct)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	_, err := CollectAll(ctx, iter)
	if err == nil {
		t.Fatal("expected error when budget exceeded")
	}
	if !memgov.IsBudgetExceeded(err) {
		t.Fatalf("expected BudgetExceededError, got: %v", err)
	}
}

func TestSortLargeRowClearError(t *testing.T) {
	// A single row larger than the entire budget fails with a budget error.
	const rowSize = 1 << 20 // 1MB

	rows := makeRowsWithField(1, rowSize)
	child := NewRowScanIterator(rows, 1)

	acct := memgov.NewTestBudget("test", 256*1024).NewAccount("sort")
	iter := NewSortIteratorWithBudget(child, []SortField{{Name: "key", Desc: false}}, 1, acct)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	_, err := CollectAll(ctx, iter)
	if err == nil {
		t.Fatal("expected error for row larger than budget")
	}
	if !memgov.IsBudgetExceeded(err) {
		t.Fatalf("expected BudgetExceededError, got: %v", err)
	}
}

// budgetErrorIterator wraps an iterator and fails with a budget error from a
// shared account once failAfter batches have been returned.
type budgetErrorIterator struct {
	inner       Iterator
	batchesSeen int
	failAfter   int
	failAccount memgov.MemoryAccount
	failAmount  int64
}

func (b *budgetErrorIterator) Init(ctx context.Context) error { return b.inner.Init(ctx) }
func (b *budgetErrorIterator) Close() error                   { return b.inner.Close() }
func (b *budgetErrorIterator) Schema() []FieldInfo            { return b.inner.Schema() }

func (b *budgetErrorIterator) Next(ctx context.Context) (*Batch, error) {
	if b.batchesSeen >= b.failAfter {
		if err := b.failAccount.Grow(b.failAmount); err != nil {
			return nil, err
		}
		b.failAccount.Shrink(b.failAmount)
	}

	batch, err := b.inner.Next(ctx)
	if err != nil {
		return nil, err
	}
	if batch != nil {
		b.batchesSeen++
	}

	return batch, nil
}

func TestSortChildBudgetErrorPropagates(t *testing.T) {
	rows := makeRowsWithField(200, 64)
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}

	monitor := memgov.NewTestBudget("test", 200*1024)
	sortAcct := monitor.NewAccount("sort")
	scanAcct := monitor.NewAccount("scan")

	budgetChild := &budgetErrorIterator{
		inner:       NewRowScanIterator(rows, 32),
		failAfter:   4,
		failAccount: scanAcct,
		failAmount:  200 * 1024,
	}
	iter := NewSortIteratorWithBudget(budgetChild, []SortField{{Name: "key", Desc: false}}, 32, sortAcct)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	_, err := CollectAll(ctx, iter)
	if err == nil {
		t.Fatal("expected the child's budget error")
	}
	if !memgov.IsBudgetExceeded(err) {
		t.Fatalf("expected BudgetExceededError, got: %v", err)
	}
}
