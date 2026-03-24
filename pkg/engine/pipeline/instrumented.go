package pipeline

import (
	"context"
	"time"

	"github.com/lynxbase/lynxdb/pkg/stats"
)

// InstrumentedIterator wraps any Iterator to collect per-operator statistics.
// It records input/output row counts and wall-clock duration for the operator.
//
// Because the Volcano iterator model is strictly pull-based within a single
// goroutine, no synchronization is needed — all counters are plain fields.
//
// Timing note: Duration is inclusive — it includes time spent in child
// operators since Next() calls recursively into children. This matches
// the EXPLAIN ANALYZE model used by PostgreSQL and other databases.
type InstrumentedIterator struct {
	inner      Iterator
	name       string
	inputRows  int64
	outputRows int64
	duration   time.Duration
}

// WrapInstrumented wraps an Iterator with instrumentation under the given name.
// The original iterator is returned as-is if it is already instrumented.
func WrapInstrumented(iter Iterator, name string) *InstrumentedIterator {
	if ii, ok := iter.(*InstrumentedIterator); ok {
		// Already instrumented — update name if needed and return.
		if ii.name == "" {
			ii.name = name
		}

		return ii
	}

	return &InstrumentedIterator{inner: iter, name: name}
}

// Init delegates to the wrapped iterator.
func (ii *InstrumentedIterator) Init(ctx context.Context) error {
	return ii.inner.Init(ctx)
}

// Next delegates to the wrapped iterator, recording timing and row counts.
func (ii *InstrumentedIterator) Next(ctx context.Context) (*Batch, error) {
	start := time.Now()
	batch, err := ii.inner.Next(ctx)
	ii.duration += time.Since(start)

	if batch != nil {
		ii.inputRows += int64(batch.Len)
		ii.outputRows += int64(batch.Len)
	}

	return batch, err
}

// Close delegates to the wrapped iterator.
func (ii *InstrumentedIterator) Close() error {
	return ii.inner.Close()
}

// Schema delegates to the wrapped iterator.
func (ii *InstrumentedIterator) Schema() []FieldInfo {
	return ii.inner.Schema()
}

// MemoryAccounter is implemented by operators that track memory via MemoryAccount.
type MemoryAccounter interface {
	MemoryUsed() int64
}

// ResourceReporter is implemented by operators that can report spill statistics.
// Used by InstrumentedIterator to harvest per-operator spill metrics.
type ResourceReporter interface {
	ResourceStats() OperatorResourceStats
}

// OperatorResourceStats holds resource usage metrics for a single operator.
type OperatorResourceStats struct {
	PeakBytes       int64
	SpilledRows     int64
	SpillBytes      int64
	BloomAllocBytes int64 // untracked bloom filter heap allocation (dedup only)
}

// StageStats returns the collected metrics as a stats.StageStats.
func (ii *InstrumentedIterator) StageStats() stats.StageStats {
	st := stats.StageStats{
		Name:       ii.name,
		InputRows:  ii.inputRows,
		OutputRows: ii.outputRows,
		Duration:   ii.duration,
	}
	if ma, ok := ii.inner.(MemoryAccounter); ok {
		st.MemoryBytes = ma.MemoryUsed()
	}
	if rr, ok := ii.inner.(ResourceReporter); ok {
		rs := rr.ResourceStats()
		st.SpilledRows = rs.SpilledRows
		st.SpillBytes = rs.SpillBytes
	}

	return st
}

// Inner returns the wrapped iterator, useful for type assertions.
func (ii *InstrumentedIterator) Inner() Iterator {
	return ii.inner
}

// CollectOperatorBudgets extracts per-operator memory budget statistics from
// a MemoryCoordinator. Returns nil if the coordinator is nil or has no slots.
// Used by --analyze to surface memory distribution in the query profile.
func CollectOperatorBudgets(mc *MemoryCoordinator) []stats.OperatorBudgetStats {
	if mc == nil {
		return nil
	}

	slots := mc.Stats()
	if len(slots) == 0 {
		return nil
	}

	result := make([]stats.OperatorBudgetStats, len(slots))
	for i, s := range slots {
		phase := "idle"
		switch s.Phase {
		case PhaseBuilding:
			phase = "building"
		case PhaseProbing:
			phase = "probing"
		case PhaseComplete:
			phase = "complete"
		}
		result[i] = stats.OperatorBudgetStats{
			Name:      s.Label,
			SoftLimit: s.SoftLimit,
			PeakBytes: s.MaxUsed,
			Phase:     phase,
			Spilled:   s.Spilled,
		}
	}

	return result
}

// InstrumentedFilterIterator wraps filter-type iterators that reduce row count.
// Unlike InstrumentedIterator, it tracks inputRows and outputRows separately:
// inputRows = rows entering the filter (from child), outputRows = rows passing.
type InstrumentedFilterIterator struct {
	inner      Iterator              // the filter iterator
	child      *InstrumentedIterator // instrumented child (to read its outputRows)
	name       string
	outputRows int64
	duration   time.Duration
}

// Init delegates to the wrapped iterator.
func (fi *InstrumentedFilterIterator) Init(ctx context.Context) error {
	return fi.inner.Init(ctx)
}

// Next delegates to the wrapped iterator, recording timing and output row count.
func (fi *InstrumentedFilterIterator) Next(ctx context.Context) (*Batch, error) {
	start := time.Now()
	batch, err := fi.inner.Next(ctx)
	fi.duration += time.Since(start)

	if batch != nil {
		fi.outputRows += int64(batch.Len)
	}

	return batch, err
}

// Close delegates to the wrapped iterator.
func (fi *InstrumentedFilterIterator) Close() error {
	return fi.inner.Close()
}

// Schema delegates to the wrapped iterator.
func (fi *InstrumentedFilterIterator) Schema() []FieldInfo {
	return fi.inner.Schema()
}

// StageStats returns the collected metrics. InputRows is derived from the
// child iterator's output rows (what entered this filter).
func (fi *InstrumentedFilterIterator) StageStats() stats.StageStats {
	inputRows := fi.outputRows // fallback: if no child, input = output
	if fi.child != nil {
		inputRows = fi.child.outputRows
	}

	return stats.StageStats{
		Name:       fi.name,
		InputRows:  inputRows,
		OutputRows: fi.outputRows,
		Duration:   fi.duration,
	}
}

// CollectStageStats walks an iterator chain and extracts StageStats from
// all instrumented operators. Returns nil if no instrumented operators found.
func CollectStageStats(iter Iterator) []stats.StageStats {
	var stages []stats.StageStats
	collectStages(iter, &stages)

	return stages
}

// collectStages recursively extracts stage stats from the iterator chain.
func collectStages(iter Iterator, out *[]stats.StageStats) {
	if iter == nil {
		return
	}

	switch it := iter.(type) {
	case *InstrumentedIterator:
		// First collect from the inner (child) iterator.
		collectStages(it.inner, out)
		st := it.StageStats()
		// Harvest VM stats from the wrapped operator if available.
		enrichWithVMStats(&st, it.inner)
		*out = append(*out, st)
	case *InstrumentedFilterIterator:
		// First collect from the inner (the filter wraps its child).
		collectStages(it.inner, out)
		st := it.StageStats()
		enrichWithVMStats(&st, it.inner)
		*out = append(*out, st)
	case *ConcurrentUnionIterator:
		for _, child := range it.children {
			collectStages(child, out)
		}
	case *UnionIterator:
		for _, child := range it.children {
			collectStages(child, out)
		}
	default:
		// Try to find child via known iterator types.
		if child := iteratorChild(iter); child != nil {
			collectStages(child, out)
		}
	}
}

// enrichWithVMStats extracts VM profiling metrics from known operator types
// (FilterIterator, EvalIterator) and populates them on the StageStats.
func enrichWithVMStats(st *stats.StageStats, inner Iterator) {
	switch it := inner.(type) {
	case *FilterIterator:
		st.VMCalls, st.VMTimeNS = it.VMStats()
	case *EvalIterator:
		st.VMCalls, st.VMTimeNS = it.VMStats()
	}
}

// CollectWarnings walks an iterator chain and collects user-visible warnings.
func CollectWarnings(iter Iterator) []string {
	var warnings []string
	collectWarningsRecurse(iter, &warnings)

	return warnings
}

// collectWarningsRecurse recursively extracts warnings from the iterator chain.
func collectWarningsRecurse(iter Iterator, out *[]string) {
	if iter == nil {
		return
	}

	switch it := iter.(type) {
	case *InstrumentedIterator:
		collectWarningsRecurse(it.inner, out)
	case *InstrumentedFilterIterator:
		collectWarningsRecurse(it.inner, out)
	case *ConcurrentUnionIterator:
		for _, child := range it.children {
			collectWarningsRecurse(child, out)
		}
	case *UnionIterator:
		for _, child := range it.children {
			collectWarningsRecurse(child, out)
		}
	default:
		if child := iteratorChild(iter); child != nil {
			collectWarningsRecurse(child, out)
		}
	}
}

// iteratorChild returns the child iterator for known operator types.
// Returns nil if the type is unknown or has no child (leaf operators).
func iteratorChild(iter Iterator) Iterator {
	switch it := iter.(type) {
	// Filter operators.
	case *FilterIterator:
		return it.child
	case *SearchExprIterator:
		return it.child
	// Aggregation operators.
	case *AggregateIterator:
		return it.child
	case *EventStatsIterator:
		return it.child
	case *StreamStatsIterator:
		return it.child
	// Projection / transformation operators.
	case *EvalIterator:
		return it.child
	case *ProjectIterator:
		return it.child
	case *RenameIterator:
		return it.child
	case *RexIterator:
		return it.child
	case *BinIterator:
		return it.child
	case *FillnullIterator:
		return it.child
	// Ordering / limiting operators.
	case *LimitIterator:
		return it.child
	case *TailIterator:
		return it.child
	case *SortIterator:
		return it.child
	case *TopNIterator:
		return it.child
	case *DedupIterator:
		return it.child
	case *TopIterator:
		return it.child
	// Complex operators.
	case *JoinIterator:
		return it.left // probe side is the pipeline chain
	case *XYSeriesIterator:
		return it.child
	case *TransactionIterator:
		return it.child
	// New Phase 3 P2 operators.
	case *DescribeIterator:
		return it.child
	case *TeeIterator:
		return it.child
	case *RollupIterator:
		return it.child
	case *SessionizeIterator:
		return it.child
	case *CorrelateIterator:
		return it.child
	case *TopologyIterator:
		return it.child
	// Existing analysis operators.
	case *GlimpseIterator:
		return it.child
	case *PatternsIterator:
		return it.child
	case *OutliersIterator:
		return it.child
	case *TraceIterator:
		return it.child
	// Union operators (no single child — returns nil).
	case *ConcurrentUnionIterator:
		return nil
	case *UnionIterator:
		return nil
	// Leaf operators (no child).
	case *ScanIterator:
		return nil
	case *RowScanIterator:
		return nil
	case *SegmentStreamIterator:
		return nil
	case *ColumnarScanIterator:
		return nil
	default:
		return nil
	}
}
