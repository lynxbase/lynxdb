package pipeline

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
)

// cuMockIterator is a test helper that produces N batches of the given size.
// Named with "cu" prefix to avoid collision with mockIterator in instrumented_test.go.
type cuMockIterator struct {
	batches   []*Batch
	pos       int
	initCount atomic.Int32 // tracks Init calls for lazy-start verification
	nextCount atomic.Int32 // tracks Next calls
	closed    atomic.Bool
	delay     time.Duration // optional per-Next delay to simulate I/O
}

func newCUMockIterator(numBatches, batchSize int) *cuMockIterator {
	m := &cuMockIterator{batches: make([]*Batch, numBatches)}
	for i := range m.batches {
		b := NewBatch(batchSize)
		for j := 0; j < batchSize; j++ {
			b.AddRow(map[string]event.Value{
				"_iter":  event.IntValue(int64(i)),
				"_batch": event.IntValue(int64(i)),
				"_row":   event.IntValue(int64(j)),
			})
		}
		m.batches[i] = b
	}

	return m
}

func newCUMockIteratorWithTag(tag string, numBatches, batchSize int) *cuMockIterator {
	m := &cuMockIterator{batches: make([]*Batch, numBatches)}
	for i := range m.batches {
		b := NewBatch(batchSize)
		for j := 0; j < batchSize; j++ {
			b.AddRow(map[string]event.Value{
				"tag":    event.StringValue(tag),
				"_batch": event.IntValue(int64(i)),
				"_row":   event.IntValue(int64(j)),
			})
		}
		m.batches[i] = b
	}

	return m
}

func (m *cuMockIterator) Init(_ context.Context) error {
	m.initCount.Add(1)

	return nil
}

func (m *cuMockIterator) Next(ctx context.Context) (*Batch, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	m.nextCount.Add(1)
	if m.delay > 0 {
		select {
		case <-time.After(m.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if m.pos >= len(m.batches) {
		return nil, nil
	}
	b := m.batches[m.pos]
	m.pos++

	return b, nil
}

func (m *cuMockIterator) Close() error {
	m.closed.Store(true)

	return nil
}

func (m *cuMockIterator) Schema() []FieldInfo { return nil }

// cuErrorIterator returns an error after producing a given number of batches.
type cuErrorIterator struct {
	batchesBefore int
	produced      int
	err           error
}

func (e *cuErrorIterator) Init(_ context.Context) error { return nil }

func (e *cuErrorIterator) Next(_ context.Context) (*Batch, error) {
	if e.produced >= e.batchesBefore {
		return nil, e.err
	}
	e.produced++
	b := NewBatch(1)
	b.AddRow(map[string]event.Value{"x": event.IntValue(1)})

	return b, nil
}

func (e *cuErrorIterator) Close() error        { return nil }
func (e *cuErrorIterator) Schema() []FieldInfo { return nil }

func defaultParallelCfg() *ParallelConfig {
	return &ParallelConfig{
		MaxBranchParallelism: 4,
		ChannelBufferSize:    2,
		Enabled:              true,
	}
}

func collectAllFromCUIter(ctx context.Context, t *testing.T, iter Iterator) int {
	t.Helper()
	if err := iter.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}
	total := 0
	for {
		b, err := iter.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if b == nil {
			break
		}
		total += b.Len
	}

	return total
}

func TestConcurrentUnion_Interleaved_AllBatches(t *testing.T) {
	// 3 children, 4 batches of 10 rows each = 120 total rows.
	children := []Iterator{
		newCUMockIterator(4, 10),
		newCUMockIterator(4, 10),
		newCUMockIterator(4, 10),
	}
	iter := NewConcurrentUnionIterator(children, OrderInterleaved, defaultParallelCfg())
	ctx := context.Background()

	total := collectAllFromCUIter(ctx, t, iter)
	if total != 120 {
		t.Errorf("got %d rows, want 120", total)
	}
	iter.Close()
}

func TestConcurrentUnion_Preserved_Ordering(t *testing.T) {
	// 2 children with distinct tags. In preserved mode, all of child[0]
	// must come before child[1].
	child0 := newCUMockIteratorWithTag("A", 3, 5) // 15 rows
	child1 := newCUMockIteratorWithTag("B", 3, 5) // 15 rows

	iter := NewConcurrentUnionIterator(
		[]Iterator{child0, child1}, OrderPreserved, defaultParallelCfg(),
	)
	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	var tags []string
	for {
		b, err := iter.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		for i := 0; i < b.Len; i++ {
			tags = append(tags, b.Value("tag", i).String())
		}
	}
	iter.Close()

	if len(tags) != 30 {
		t.Fatalf("got %d rows, want 30", len(tags))
	}

	// All A's must come before all B's.
	seenB := false
	for _, tag := range tags {
		if tag == "B" {
			seenB = true
		}
		if tag == "A" && seenB {
			t.Fatal("child[0] row appeared after child[1] in OrderPreserved mode")
		}
	}
}

func TestConcurrentUnion_Preserved_LazyStart(t *testing.T) {
	// child[1] should not have Next called until child[0] is fully drained.
	child0 := newCUMockIterator(2, 5) // 10 rows (2 batches)
	child1 := newCUMockIterator(2, 5) // 10 rows (2 batches)

	iter := NewConcurrentUnionIterator(
		[]Iterator{child0, child1}, OrderPreserved, defaultParallelCfg(),
	)
	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// Read 2 batches from child[0]. After this, child[0]'s goroutine may
	// have finished but child[1] has NOT been started yet because the
	// consumer hasn't seen child[0]'s channel close.
	for i := 0; i < 2; i++ {
		b, err := iter.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			t.Fatal("expected batch from child[0]")
		}
	}

	// child[1] should not have been called yet.
	if child1.nextCount.Load() != 0 {
		t.Errorf("child[1] Next called %d times before child[0] exhausted, want 0",
			child1.nextCount.Load())
	}

	// Now drain the rest (child[0] nil + all of child[1]).
	total := 0
	for {
		b, err := iter.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		total += b.Len
	}
	iter.Close()

	if total != 10 {
		t.Errorf("remaining rows: got %d, want 10", total)
	}
	// Verify child[1] was eventually called.
	if child1.nextCount.Load() == 0 {
		t.Error("child[1] Next was never called")
	}
}

func TestConcurrentUnion_ErrorPropagation(t *testing.T) {
	testErr := errors.New("test error")
	children := []Iterator{
		newCUMockIterator(10, 100), // many batches
		&cuErrorIterator{batchesBefore: 1, err: testErr},
	}

	iter := NewConcurrentUnionIterator(children, OrderInterleaved, defaultParallelCfg())
	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// Drain until error surfaces.
	var gotErr error
	for {
		_, err := iter.Next(ctx)
		if err != nil {
			gotErr = err

			break
		}
	}
	iter.Close()

	if gotErr == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(gotErr, testErr) {
		t.Errorf("got error %v, want %v", gotErr, testErr)
	}
}

func TestConcurrentUnion_ContextCancellation(t *testing.T) {
	// Slow children that should be canceled.
	child := newCUMockIterator(100, 10)
	child.delay = 100 * time.Millisecond

	iter := NewConcurrentUnionIterator(
		[]Iterator{child}, OrderInterleaved, defaultParallelCfg(),
	)
	ctx, cancel := context.WithCancel(context.Background())
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// Read one batch then cancel.
	_, err := iter.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()

	// Subsequent Next should return context error or nil (channel closed).
	_, _ = iter.Next(ctx)
	iter.Close()
}

func TestConcurrentUnion_SingleChild(t *testing.T) {
	// Single child should degenerate to sequential behavior.
	child := newCUMockIterator(3, 10)
	iter := NewConcurrentUnionIterator(
		[]Iterator{child}, OrderInterleaved, defaultParallelCfg(),
	)
	ctx := context.Background()

	total := collectAllFromCUIter(ctx, t, iter)
	iter.Close()

	if total != 30 {
		t.Errorf("got %d rows, want 30", total)
	}
}

func TestConcurrentUnion_EmptyChildren(t *testing.T) {
	// Some children produce 0 rows. Create fresh iterators for each mode
	// to avoid sharing mutable state (pos) across goroutines from the
	// previous mode's Close() timeout window.
	for _, mode := range []UnionOrderMode{OrderInterleaved, OrderPreserved} {
		children := []Iterator{
			newCUMockIterator(0, 0), // empty
			newCUMockIterator(2, 5), // 10 rows
			newCUMockIterator(0, 0), // empty
		}

		iter := NewConcurrentUnionIterator(children, mode, defaultParallelCfg())
		ctx := context.Background()

		total := collectAllFromCUIter(ctx, t, iter)
		iter.Close()

		if total != 10 {
			t.Errorf("mode=%d: got %d rows, want 10", mode, total)
		}
	}
}

func TestConcurrentUnion_Backpressure(t *testing.T) {
	// Slow consumer + fast producer: producer should block on channel send.
	child := newCUMockIterator(20, 10) // 200 rows
	cfg := &ParallelConfig{
		MaxBranchParallelism: 4,
		ChannelBufferSize:    1, // small buffer to test backpressure
		Enabled:              true,
	}

	iter := NewConcurrentUnionIterator(
		[]Iterator{child}, OrderInterleaved, cfg,
	)
	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// Read slowly — producer should not race ahead.
	total := 0
	for {
		b, err := iter.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		total += b.Len
		time.Sleep(time.Millisecond) // slow consumer
	}
	iter.Close()

	if total != 200 {
		t.Errorf("got %d rows, want 200", total)
	}
}

func TestConcurrentUnion_CloseTimeout(t *testing.T) {
	// A misbehaving child that returns 1 batch then blocks forever.
	// The first Next() call completes start() (including all wg.Add calls),
	// ensuring no wg.Add/wg.Wait race when Close() is called later.
	blocker := &batchThenBlockIterator{block: make(chan struct{})}
	defer close(blocker.block) // cleanup: unblock goroutine after test

	iter := NewConcurrentUnionIterator(
		[]Iterator{blocker}, OrderInterleaved, defaultParallelCfg(),
	)
	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// First Next() triggers start() and returns the batch.
	// All wg.Add calls are done after this returns.
	batch, err := iter.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if batch == nil || batch.Len != 1 {
		t.Fatal("expected 1-row batch from first Next()")
	}

	// Second Next() blocks (child goroutine is stuck in child.Next on block chan).
	nextDone := make(chan struct{})
	go func() {
		_, _ = iter.Next(ctx)
		close(nextDone)
	}()
	time.Sleep(50 * time.Millisecond) // let goroutine enter blocking read

	// Close should complete within the 5s timeout, not block forever.
	closeDone := make(chan struct{})
	go func() {
		iter.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
		// OK — Close() returned (either clean or after timeout).
	case <-time.After(10 * time.Second):
		t.Fatal("Close() did not return within 10s — goroutine leak")
	}

	// Wait for Next goroutine (unblocked by defer close(blocker.block)).
	<-nextDone
}

// batchThenBlockIterator returns one batch, then blocks forever on the
// second Next() call (ignores context). Used to test Close() timeout
// while ensuring the goroutine has been fully started (no wg.Add race).
type batchThenBlockIterator struct {
	returned bool
	block    chan struct{}
}

func (b *batchThenBlockIterator) Init(_ context.Context) error { return nil }

func (b *batchThenBlockIterator) Next(_ context.Context) (*Batch, error) {
	if !b.returned {
		b.returned = true
		batch := NewBatch(1)
		batch.AddRow(map[string]event.Value{"x": event.IntValue(1)})

		return batch, nil
	}
	<-b.block // block forever on second call

	return nil, nil
}

func (b *batchThenBlockIterator) Close() error        { return nil }
func (b *batchThenBlockIterator) Schema() []FieldInfo { return nil }

func TestConcurrentUnion_CloseTimeout_Preserved(t *testing.T) {
	// Verifies M10 fix: Close() cancels child goroutines via masterCtx
	// and returns within timeout even when a child ignores cancellation.
	//
	// Uses batchThenBlockIterator: first Next() returns a batch (which
	// triggers startPreservedChild and wg.Add), second Next() blocks.
	// This ensures wg.Add is complete before Close() calls wg.Wait().
	blocker := &batchThenBlockIterator{block: make(chan struct{})}
	defer close(blocker.block) // cleanup: unblock goroutine after test

	iter := NewConcurrentUnionIterator(
		[]Iterator{blocker}, OrderPreserved, defaultParallelCfg(),
	)
	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// First Next() returns a batch — startPreservedChild completes, wg.Add done.
	batch, err := iter.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if batch == nil || batch.Len != 1 {
		t.Fatal("expected 1-row batch from first Next()")
	}

	// Second Next() blocks in a goroutine (child.Next blocks on blocker.block).
	// No wg.Add happens here because childChs[0] is already set.
	nextDone := make(chan struct{})
	go func() {
		_, _ = iter.Next(ctx)
		close(nextDone)
	}()
	time.Sleep(50 * time.Millisecond) // let goroutine enter blocking read

	// Close should cancel masterCtx → childCtx, and return within timeout.
	closeDone := make(chan struct{})
	go func() {
		iter.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
		// OK — Close returned (either clean or after 5s timeout).
	case <-time.After(10 * time.Second):
		t.Fatal("Close() did not return within 10s — goroutine leak in OrderPreserved mode")
	}

	// Wait for the Next goroutine to finish (unblocked by defer close(blocker.block)).
	<-nextDone
}

func TestConcurrentUnion_ErrorBeforeCleanBatches(t *testing.T) {
	// When one child errors after producing some batches, the consumer
	// should receive those partial batches (best-effort) before the error.
	// This documents the partial results behavior.
	testErr := errors.New("child failed")
	children := []Iterator{
		newCUMockIterator(5, 10),                         // 50 rows, no error
		&cuErrorIterator{batchesBefore: 2, err: testErr}, // 2 batches then error
	}

	iter := NewConcurrentUnionIterator(children, OrderInterleaved, defaultParallelCfg())
	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// Collect everything until error.
	var totalRows int
	var gotErr error
	for {
		batch, err := iter.Next(ctx)
		if err != nil {
			gotErr = err

			break
		}
		if batch == nil {
			break
		}
		totalRows += batch.Len
	}
	iter.Close()

	if gotErr == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(gotErr, testErr) {
		t.Errorf("got error %v, want %v", gotErr, testErr)
	}
	// We should have received at least some rows before the error.
	// The exact count depends on goroutine scheduling, but we expect > 0.
	t.Logf("received %d rows before error (partial results)", totalRows)
}

func BenchmarkConcurrentUnion_vs_Sequential(b *testing.B) {
	const numChildren = 4
	const numBatches = 100
	const batchSize = 1024

	b.Run("Sequential", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			children := make([]Iterator, numChildren)
			for j := range children {
				children[j] = newCUMockIterator(numBatches, batchSize)
			}
			iter := NewUnionIterator(children)
			ctx := context.Background()
			_ = iter.Init(ctx)
			for {
				batch, err := iter.Next(ctx)
				if err != nil || batch == nil {
					break
				}
			}
			iter.Close()
		}
	})

	b.Run("Interleaved", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			children := make([]Iterator, numChildren)
			for j := range children {
				children[j] = newCUMockIterator(numBatches, batchSize)
			}
			iter := NewConcurrentUnionIterator(children, OrderInterleaved, defaultParallelCfg())
			ctx := context.Background()
			_ = iter.Init(ctx)
			for {
				batch, err := iter.Next(ctx)
				if err != nil || batch == nil {
					break
				}
			}
			iter.Close()
		}
	})

	b.Run("Preserved", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			children := make([]Iterator, numChildren)
			for j := range children {
				children[j] = newCUMockIterator(numBatches, batchSize)
			}
			iter := NewConcurrentUnionIterator(children, OrderPreserved, defaultParallelCfg())
			ctx := context.Background()
			_ = iter.Init(ctx)
			for {
				batch, err := iter.Next(ctx)
				if err != nil || batch == nil {
					break
				}
			}
			iter.Close()
		}
	})
}
