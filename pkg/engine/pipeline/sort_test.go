package pipeline

import (
	"context"
	"sync"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/memgov"
)

// testBudget is a byte limit shared by the accounts it opens; zero means no limit.
type testBudget struct {
	mu    sync.Mutex
	limit int64
	used  int64
}

func newTestBudget(limit int64) *testBudget { return &testBudget{limit: limit} }

func (b *testBudget) NewAccount() memgov.MemoryAccount { return &testAccount{budget: b} }

type testAccount struct {
	budget  *testBudget
	used    int64
	maxUsed int64
}

func (a *testAccount) Grow(n int64) error {
	if n <= 0 {
		return nil
	}
	a.budget.mu.Lock()
	defer a.budget.mu.Unlock()
	if a.budget.limit > 0 && a.budget.used+n > a.budget.limit {
		return memgov.ErrQueryBudgetExceeded
	}
	a.budget.used += n
	a.used += n
	a.maxUsed = max(a.maxUsed, a.used)
	return nil
}

func (a *testAccount) Shrink(n int64) {
	n = min(max(n, 0), a.used)
	a.budget.mu.Lock()
	a.budget.used -= n
	a.budget.mu.Unlock()
	a.used -= n
}

func (a *testAccount) Close()         { a.Shrink(a.used) }
func (a *testAccount) Used() int64    { return a.used }
func (a *testAccount) MaxUsed() int64 { return a.maxUsed }

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
	acct := newTestBudget(1 << 30).NewAccount()
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
	acct := newTestBudget(1 << 20).NewAccount()
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

	acct := newTestBudget(1024).NewAccount()
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

	acct := newTestBudget(256 * 1024).NewAccount()
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

	monitor := newTestBudget(200 * 1024)
	sortAcct := monitor.NewAccount()
	scanAcct := monitor.NewAccount()

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
