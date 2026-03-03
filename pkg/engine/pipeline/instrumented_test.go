package pipeline

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
	"github.com/OrlovEvgeny/Lynxdb/pkg/stats"
)

// mockIterator yields a fixed number of batches, each with batchLen rows.
type mockIterator struct {
	batches   int
	batchLen  int
	emitted   int
	initDelay time.Duration
	nextDelay time.Duration
}

func (m *mockIterator) Init(_ context.Context) error {
	if m.initDelay > 0 {
		time.Sleep(m.initDelay)
	}

	return nil
}

func (m *mockIterator) Next(_ context.Context) (*Batch, error) {
	if m.emitted >= m.batches {
		return nil, nil
	}

	if m.nextDelay > 0 {
		time.Sleep(m.nextDelay)
	}

	b := NewBatch(m.batchLen)
	for i := 0; i < m.batchLen; i++ {
		b.AddRow(map[string]event.Value{
			"_raw": event.StringValue("test"),
		})
	}
	m.emitted++

	return b, nil
}

func (m *mockIterator) Close() error        { return nil }
func (m *mockIterator) Schema() []FieldInfo { return nil }

func TestInstrumentedIterator_RowCounts(t *testing.T) {
	t.Parallel()

	mock := &mockIterator{batches: 3, batchLen: 10}
	ii := WrapInstrumented(mock, "TestOp")

	ctx := context.Background()
	if err := ii.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}

	totalRows := 0
	for {
		batch, err := ii.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if batch == nil {
			break
		}
		totalRows += batch.Len
	}

	if err := ii.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	st := ii.StageStats()
	if st.Name != "TestOp" {
		t.Errorf("Name = %q, want %q", st.Name, "TestOp")
	}
	if st.OutputRows != 30 {
		t.Errorf("OutputRows = %d, want 30", st.OutputRows)
	}
	if st.InputRows != 30 {
		t.Errorf("InputRows = %d, want 30", st.InputRows)
	}
	if totalRows != 30 {
		t.Errorf("totalRows = %d, want 30", totalRows)
	}
}

func TestInstrumentedIterator_Duration(t *testing.T) {
	t.Parallel()

	mock := &mockIterator{batches: 2, batchLen: 1, nextDelay: 5 * time.Millisecond}
	ii := WrapInstrumented(mock, "SlowOp")

	ctx := context.Background()
	_ = ii.Init(ctx)

	for {
		batch, _ := ii.Next(ctx)
		if batch == nil {
			break
		}
	}
	_ = ii.Close()

	st := ii.StageStats()
	// 2 batches * 5ms delay + 1 nil-returning call ≈ 10-15ms minimum.
	if st.Duration < 10*time.Millisecond {
		t.Errorf("Duration = %v, want >= 10ms", st.Duration)
	}
}

func TestInstrumentedIterator_EmptyInput(t *testing.T) {
	t.Parallel()

	mock := &mockIterator{batches: 0, batchLen: 0}
	ii := WrapInstrumented(mock, "Empty")

	ctx := context.Background()
	_ = ii.Init(ctx)

	batch, err := ii.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if batch != nil {
		t.Errorf("expected nil batch for empty input")
	}

	st := ii.StageStats()
	if st.OutputRows != 0 {
		t.Errorf("OutputRows = %d, want 0", st.OutputRows)
	}
}

func TestWrapInstrumented_AlreadyWrapped(t *testing.T) {
	t.Parallel()

	mock := &mockIterator{batches: 1, batchLen: 5}
	ii := WrapInstrumented(mock, "First")
	ii2 := WrapInstrumented(ii, "")

	// Should return the same pointer, not double-wrap.
	if ii != ii2 {
		t.Error("WrapInstrumented should return same instance when already wrapped")
	}
	if ii2.name != "First" {
		t.Errorf("name = %q, want %q", ii2.name, "First")
	}
}

func TestWrapInstrumented_UpdatesEmptyName(t *testing.T) {
	t.Parallel()

	mock := &mockIterator{batches: 1, batchLen: 5}
	ii := WrapInstrumented(mock, "")
	ii2 := WrapInstrumented(ii, "Updated")

	if ii2.name != "Updated" {
		t.Errorf("name = %q, want %q", ii2.name, "Updated")
	}
}

func TestInstrumentedFilterIterator_RowCounts(t *testing.T) {
	t.Parallel()

	// Simulate: child produces 100 rows (10 batches × 10 rows).
	// Filter keeps every 5th row → indices 4, 9 per batch = 2 per batch = 20 total.
	mock := &mockIterator{batches: 10, batchLen: 10}
	childII := WrapInstrumented(mock, "Scan")

	filterMock := &filteringIterator{child: childII, keepEvery: 5}
	fi := WrapInstrumentedFilter(filterMock, "Filter", childII)

	ctx := context.Background()
	_ = fi.Init(ctx)

	var totalOut int64
	for {
		batch, err := fi.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if batch == nil {
			break
		}
		totalOut += int64(batch.Len)
	}
	_ = fi.Close()

	st := fi.StageStats()
	if st.Name != "Filter" {
		t.Errorf("Name = %q, want %q", st.Name, "Filter")
	}
	// InputRows = child's output = 100 rows.
	if st.InputRows != 100 {
		t.Errorf("InputRows = %d, want 100", st.InputRows)
	}
	// OutputRows = what the filter emitted = 20 (2 per batch × 10 batches).
	if st.OutputRows != 20 {
		t.Errorf("OutputRows = %d, want 20", st.OutputRows)
	}
	if totalOut != 20 {
		t.Errorf("totalOut = %d, want 20", totalOut)
	}
}

func TestCollectStageStats_InstrumentedChain(t *testing.T) {
	t.Parallel()

	// Build a chain matching the real pipeline structure:
	//   InstrumentedIterator("Head") → LimitIterator
	//     → InstrumentedIterator("Scan") → ScanIterator
	//
	// This mirrors how buildQuery wraps operators: each operator gets
	// WrapInstrumented after creation, and uses the previous instrumented
	// iterator as its child.
	events := makeTestEvents(50)
	scanIter := NewScanIterator(events, DefaultBatchSize)
	scanII := WrapInstrumented(scanIter, "Scan")

	limitIter := NewLimitIterator(scanII, 10)
	limitII := WrapInstrumented(limitIter, "Head")

	ctx := context.Background()
	_ = limitII.Init(ctx)

	for {
		batch, _ := limitII.Next(ctx)
		if batch == nil {
			break
		}
	}
	_ = limitII.Close()

	stages := CollectStageStats(limitII)
	if len(stages) != 2 {
		t.Fatalf("stages = %d, want 2", len(stages))
	}

	// First stage should be Scan (leaf, collected first).
	if stages[0].Name != "Scan" {
		t.Errorf("stages[0].Name = %q, want Scan", stages[0].Name)
	}
	// Scan emits all 50 events in one batch (batch size 1024 > 50).
	// LimitIterator truncates downstream, but Scan's wrapper sees the full batch.
	if stages[0].OutputRows != 50 {
		t.Errorf("stages[0].OutputRows = %d, want 50", stages[0].OutputRows)
	}

	// Second stage should be Head.
	if stages[1].Name != "Head" {
		t.Errorf("stages[1].Name = %q, want Head", stages[1].Name)
	}
	if stages[1].OutputRows != 10 {
		t.Errorf("stages[1].OutputRows = %d, want 10", stages[1].OutputRows)
	}
}

// makeTestEvents creates n simple test events.
func makeTestEvents(n int) []*event.Event {
	events := make([]*event.Event, n)
	for i := range events {
		events[i] = event.NewEvent(time.Now(), "test line")
	}

	return events
}

func TestCollectStageStats_NilIterator(t *testing.T) {
	t.Parallel()

	stages := CollectStageStats(nil)
	if stages != nil {
		t.Errorf("expected nil stages for nil iterator, got %v", stages)
	}
}

func TestCollectStageStats_NonInstrumented(t *testing.T) {
	t.Parallel()

	mock := &mockIterator{batches: 1, batchLen: 5}
	stages := CollectStageStats(mock)
	if stages != nil {
		t.Errorf("expected nil stages for non-instrumented iterator, got %v", stages)
	}
}

func TestResourceReporterSort(t *testing.T) {
	t.Parallel()

	// Sort 500 rows with a tiny budget to force spilling.
	rows := makeRowsWithField(500, 16)
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}

	child := NewRowScanIterator(rows, 32)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	acct := stats.NewBudgetMonitor("test", 16*1024).NewAccount("sort")
	sortIter := NewSortIteratorWithSpill(child, []SortField{{Name: "key", Desc: false}}, 32, acct, mgr)

	// Wrap with instrumentation.
	ii := WrapInstrumented(sortIter, "Sort")

	ctx := context.Background()
	if err := ii.Init(ctx); err != nil {
		t.Fatal(err)
	}
	for {
		batch, err := ii.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if batch == nil {
			break
		}
	}

	st := ii.StageStats()
	if st.SpilledRows == 0 {
		t.Fatal("expected SpilledRows > 0 for sort with spill")
	}
	t.Logf("sort spilled %d rows", st.SpilledRows)
}

func TestResourceReporterAggregate(t *testing.T) {
	t.Parallel()

	// Create rows with diverse group keys to force aggregate spilling.
	rows := make([]map[string]event.Value, 500)
	for i := 0; i < 500; i++ {
		rows[i] = map[string]event.Value{
			"grp":   event.StringValue(fmt.Sprintf("g%d", i)),
			"value": event.IntValue(int64(i)),
		}
	}

	child := NewRowScanIterator(rows, 32)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	acct := stats.NewBudgetMonitor("test", 8*1024).NewAccount("agg")
	aggIter := NewAggregateIteratorWithSpill(
		child,
		[]AggFunc{{Name: "count", Alias: "cnt"}},
		[]string{"grp"},
		acct,
		mgr,
	)

	ii := WrapInstrumented(aggIter, "Aggregate")
	ctx := context.Background()
	if err := ii.Init(ctx); err != nil {
		t.Fatal(err)
	}
	for {
		batch, err := ii.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if batch == nil {
			break
		}
	}

	st := ii.StageStats()
	if st.SpilledRows == 0 {
		t.Fatal("expected SpilledRows > 0 for aggregate with spill")
	}
	t.Logf("aggregate spilled %d rows", st.SpilledRows)
}

func TestCollectStageStatsIncludesSpill(t *testing.T) {
	t.Parallel()

	// Build an instrumented pipeline: Scan -> Sort (with spill).
	rows := makeRowsWithField(500, 16)
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}

	child := NewRowScanIterator(rows, 32)
	scanII := WrapInstrumented(child, "Scan")

	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	acct := stats.NewBudgetMonitor("test", 16*1024).NewAccount("sort")
	sortIter := NewSortIteratorWithSpill(scanII, []SortField{{Name: "key", Desc: false}}, 32, acct, mgr)
	sortII := WrapInstrumented(sortIter, "Sort")

	ctx := context.Background()
	if err := sortII.Init(ctx); err != nil {
		t.Fatal(err)
	}
	for {
		batch, err := sortII.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if batch == nil {
			break
		}
	}

	stages := CollectStageStats(sortII)
	if len(stages) != 2 {
		t.Fatalf("expected 2 stages, got %d", len(stages))
	}

	// Sort stage should have spill stats.
	sortStage := stages[1] // Sort is the outer operator.
	if sortStage.Name != "Sort" {
		t.Fatalf("expected Sort stage, got %q", sortStage.Name)
	}
	if sortStage.SpilledRows == 0 {
		t.Fatal("expected SpilledRows > 0 in Sort stage")
	}
	t.Logf("Sort stage: SpilledRows=%d", sortStage.SpilledRows)
}

func TestCollectWarnings_NoWarnings(t *testing.T) {
	t.Parallel()

	mock := &mockIterator{batches: 3, batchLen: 10}
	ii := WrapInstrumented(mock, "Test")

	ctx := context.Background()
	_ = ii.Init(ctx)
	for {
		batch, _ := ii.Next(ctx)
		if batch == nil {
			break
		}
	}

	warnings := CollectWarnings(ii)
	if len(warnings) != 0 {
		t.Fatalf("expected no warnings, got %v", warnings)
	}
}

// filteringIterator keeps every Nth row for testing.
type filteringIterator struct {
	child     Iterator
	keepEvery int
}

func (f *filteringIterator) Init(ctx context.Context) error {
	return f.child.Init(ctx)
}

func (f *filteringIterator) Next(ctx context.Context) (*Batch, error) {
	for {
		batch, err := f.child.Next(ctx)
		if batch == nil || err != nil {
			return nil, err
		}

		result := NewBatch(batch.Len / f.keepEvery)
		for i := 0; i < batch.Len; i++ {
			if (i+1)%f.keepEvery == 0 {
				result.AddRow(batch.Row(i))
			}
		}

		if result.Len > 0 {
			return result, nil
		}
	}
}

func (f *filteringIterator) Close() error        { return f.child.Close() }
func (f *filteringIterator) Schema() []FieldInfo { return f.child.Schema() }
