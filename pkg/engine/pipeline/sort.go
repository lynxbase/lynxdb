package pipeline

import (
	"context"
	"fmt"
	"sort"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/stats"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

// DefaultMaxSortRows is the maximum number of rows that SortIterator will
// materialize in memory. Beyond this limit, the sort spills to disk if a
// SpillManager is configured, or the query is aborted with ErrSortLimitExceeded.
const DefaultMaxSortRows = 10_000_000

// ErrSortLimitExceeded is returned when a sort operation exceeds the maximum
// number of materializable rows.
var ErrSortLimitExceeded = fmt.Errorf("sort: row limit exceeded (max %d rows)", DefaultMaxSortRows)

// SortField describes a sort key.
type SortField struct {
	Name string
	Desc bool
}

// estimatedRowBytes is a fixed estimate of memory per materialized row in sort/join buffers.
// Conservative estimate: map header + ~8 fields * (string key + Value).
//
// Deprecated: Use estimateRowMapBytes for accurate per-row estimation in sort.
// Kept for backward compatibility — still used in join buffers.
const estimatedRowBytes int64 = 256

// estimateRowMapBytes estimates the actual heap size of a materialized row map.
// It mirrors the approach used by event.EstimateEventSize but operates on
// map[string]event.Value. This gives accurate memory tracking for sort buffers,
// avoiding the 1000x undercount that the fixed 256-byte estimate produces for
// rows with large string fields (e.g., _raw with 500KB log lines).
func estimateRowMapBytes(row map[string]event.Value) int64 {
	// Base overhead: Go map header (~8 bytes hmap struct pointer) + bucket array.
	// A typical map with N entries uses ~(N/6.5) buckets of 208 bytes each.
	const mapOverhead int64 = 64
	// Per-entry: string header (16 bytes) + Value struct (typ uint8 + str string
	// header 16 + int64 8 + float64 8 = ~56 bytes with padding) + map bucket slot.
	const entryOverhead int64 = 56

	size := mapOverhead
	for k, v := range row {
		size += entryOverhead + int64(len(k))
		if v.Type() == event.FieldTypeString {
			size += int64(len(v.String()))
		}
	}

	return size
}

// SortIterator fully materializes input, sorts, then streams output.
// When a memory budget is set and exceeded, it transparently spills sorted
// runs to disk and performs an external k-way merge sort on output.
type SortIterator struct {
	child       Iterator
	fields      []SortField
	rows        []map[string]event.Value
	sorted      bool
	offset      int
	batchSize   int
	maxSortRows int
	acct        stats.MemoryAccount // per-operator memory tracking (nil *BoundAccount = no tracking)

	// External merge sort state (populated only when spill occurs).
	spillFiles  []string      // paths of sorted spill run files
	merger      SpillMergerI  // k-way merge iterator (nil = in-memory path)
	spillMgr    *SpillManager // lifecycle manager for spill files (nil = no spill support)
	spilledRows int64         // total rows written to spill files (for ResourceReporter)
}

// NewSortIterator creates a full-materialization sort operator.
// The acct parameter is optional (nil = no memory tracking).
func NewSortIterator(child Iterator, fields []SortField, batchSize int) *SortIterator {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	return &SortIterator{
		child:       child,
		fields:      fields,
		batchSize:   batchSize,
		maxSortRows: DefaultMaxSortRows,
		acct:        stats.NopAccount(),
	}
}

// NewSortIteratorWithBudget creates a sort operator with memory budget tracking.
func NewSortIteratorWithBudget(child Iterator, fields []SortField, batchSize int, acct stats.MemoryAccount) *SortIterator {
	s := NewSortIterator(child, fields, batchSize)
	s.acct = stats.EnsureAccount(acct)

	return s
}

// NewSortIteratorWithSpill creates a sort operator with memory budget tracking
// and disk spill support. When the budget is exceeded, sorted runs are written
// to disk via the SpillManager and merged on output using a k-way merge.
func NewSortIteratorWithSpill(child Iterator, fields []SortField, batchSize int, acct stats.MemoryAccount, mgr *SpillManager) *SortIterator {
	s := NewSortIteratorWithBudget(child, fields, batchSize, acct)
	s.spillMgr = mgr

	return s
}

func (s *SortIterator) Init(ctx context.Context) error {
	return s.child.Init(ctx)
}

func (s *SortIterator) Next(ctx context.Context) (*Batch, error) {
	if !s.sorted {
		if err := s.materialize(ctx); err != nil {
			return nil, err
		}
	}

	// External merge path: read from the k-way merger.
	if s.merger != nil {
		return s.merger.NextBatch(s.batchSize)
	}

	// In-memory path: emit from sorted s.rows.
	if s.offset >= len(s.rows) {
		return nil, nil
	}
	end := s.offset + s.batchSize
	if end > len(s.rows) {
		end = len(s.rows)
	}
	batch := BatchFromRows(s.rows[s.offset:end])
	s.offset = end

	return batch, nil
}

func (s *SortIterator) Close() error {
	// Transition to complete phase — memory can be reclaimed by coordinator.
	if pn, ok := s.acct.(PhaseNotifier); ok {
		pn.SetPhase(PhaseComplete)
	}

	// Close the merger first (closes all SpillReaders).
	if s.merger != nil {
		s.merger.Close()
		s.merger = nil
	}

	// Release all spill files through the manager.
	for _, path := range s.spillFiles {
		if s.spillMgr != nil {
			s.spillMgr.Release(path)
		}
	}
	s.spillFiles = nil

	s.acct.Close()

	return s.child.Close()
}

// MemoryUsed returns the current tracked memory for this operator.
func (s *SortIterator) MemoryUsed() int64 {
	return s.acct.Used()
}

func (s *SortIterator) Schema() []FieldInfo { return s.child.Schema() }

func (s *SortIterator) materialize(ctx context.Context) error {
	// Transition to building phase — accumulating rows for sort.
	if pn, ok := s.acct.(PhaseNotifier); ok {
		pn.SetPhase(PhaseBuilding)
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		batch, err := s.child.Next(ctx)
		if err != nil {
			// Bug fix: when scan (or another child) fails because the shared
			// budget is exhausted, sort may hold a large spillable buffer.
			// Spill that buffer to free shared budget capacity, then retry.
			if stats.IsMemoryExhausted(err) && len(s.rows) > 0 && s.spillMgr != nil {
				if spillErr := s.spillCurrentRun(); spillErr != nil {
					return fmt.Errorf("sort.materialize: spill on child budget pressure: %w", spillErr)
				}

				batch, err = s.child.Next(ctx)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
		if batch == nil {
			break
		}

		// Materialize rows one at a time with per-row memory accounting.
		// This replaces the previous batch-level Grow which would fail when
		// a batch's total size exceeded the post-spill reservation (e.g.,
		// 292KB batch vs 256KB reservation), even though individual rows
		// are much smaller and can be accumulated with intermediate spills.
		for i := 0; i < batch.Len; i++ {
			row := batch.Row(i)
			rowBytes := estimateRowMapBytes(row)

			if err := s.growOrSpill(rowBytes); err != nil {
				return err
			}

			// Row count safety valve.
			if len(s.rows)+1 > s.maxSortRows {
				if s.spillMgr == nil {
					return ErrSortLimitExceeded
				}
				if len(s.rows) > 0 {
					if err := s.spillCurrentRun(); err != nil {
						return fmt.Errorf("sort.materialize: row limit spill: %w", err)
					}
				}
			}

			s.rows = append(s.rows, row)
		}
	}

	// All input consumed. Choose in-memory or external merge path.
	if len(s.spillFiles) == 0 {
		// Fast path: sort in-place, no spill occurred.
		if err := s.sortInPlaceCtx(ctx); err != nil {
			return err
		}
		// Transition to probing phase — producing sorted output.
		if pn, ok := s.acct.(PhaseNotifier); ok {
			pn.SetPhase(PhaseProbing)
		}
		s.sorted = true

		return nil
	}

	// External merge path: spill any remaining in-memory rows as the final run.
	if len(s.rows) > 0 {
		if err := s.spillCurrentRun(); err != nil {
			return fmt.Errorf("sort.materialize: final spill failed: %w", err)
		}
	}

	// Create the k-way merger over all spill files.
	merger, err := NewColumnarSpillMerger(s.spillFiles, s.fields)
	if err != nil {
		return fmt.Errorf("sort.materialize: create merger: %w", err)
	}
	s.merger = merger
	// Transition to probing phase — producing merged output from spill files.
	if pn, ok := s.acct.(PhaseNotifier); ok {
		pn.SetPhase(PhaseProbing)
	}
	s.sorted = true

	return nil
}

// ResourceStats implements ResourceReporter for per-operator spill metrics.
func (s *SortIterator) ResourceStats() OperatorResourceStats {
	return OperatorResourceStats{
		PeakBytes:   s.acct.MaxUsed(),
		SpilledRows: s.spilledRows,
	}
}

// growOrSpill attempts to reserve rowBytes in the memory budget. If the budget
// is exceeded, it spills accumulated rows to free capacity and retries. If the
// row still cannot fit after spilling (e.g., a single row exceeds the entire
// reservation), the underlying budget error is propagated (preserving the
// *stats.BudgetExceededError type for callers that inspect it).
func (s *SortIterator) growOrSpill(rowBytes int64) error {
	growErr := s.acct.Grow(rowBytes)
	if growErr == nil {
		return nil // fast path
	}

	// Try spilling accumulated rows to free capacity.
	if len(s.rows) > 0 && s.spillMgr != nil {
		spilledCount := len(s.rows)
		if spillErr := s.spillCurrentRun(); spillErr != nil {
			return fmt.Errorf("sort.materialize: spill failed: %w", spillErr)
		}

		if err := s.acct.Grow(rowBytes); err == nil {
			return nil
		}

		// Spill freed memory but still not enough for this row.
		available := s.acct.MaxUsed() - s.acct.Used()
		if available < 0 {
			available = 0
		}
		suggestedMin := rowBytes * 2 // need at least 2x row size for working room

		return fmt.Errorf("sort operator cannot make progress: row size (%d bytes) exceeds "+
			"available memory (%d bytes) after spilling %d rows to disk; "+
			"try increasing --memory to at least %d bytes: %w",
			rowBytes, available, spilledCount, suggestedMin, growErr)
	}

	// No rows to spill or no spill manager — propagate original error.
	return fmt.Errorf("sort.materialize: %w", growErr)
}

// spillCurrentRun sorts the current in-memory rows, writes them to a spill file,
// and releases the tracked memory.
func (s *SortIterator) spillCurrentRun() error {
	s.spilledRows += int64(len(s.rows))

	// Sort in-place using the same comparator as the final sort.
	s.sortInPlace()

	sw, err := NewColumnarSpillWriter(s.spillMgr, "sort")
	if err != nil {
		return fmt.Errorf("sort.spillCurrentRun: %w", err)
	}

	for _, row := range s.rows {
		if err := sw.WriteRow(row); err != nil {
			_ = sw.CloseFile()

			return fmt.Errorf("sort.spillCurrentRun: write: %w", err)
		}
	}

	if err := sw.CloseFile(); err != nil {
		return fmt.Errorf("sort.spillCurrentRun: close: %w", err)
	}

	s.spillFiles = append(s.spillFiles, sw.Path())

	// Release all tracked memory and reset the buffer.
	s.acct.Shrink(s.acct.Used())
	s.rows = s.rows[:0]

	// Notify coordinator that this operator has spilled, allowing rebalancing.
	if sn, ok := s.acct.(SpillNotifier); ok {
		sn.NotifySpilled()
	}

	return nil
}

// sortInPlace sorts s.rows using the configured sort fields.
// Used by spillCurrentRun where we always want the sort to complete.
func (s *SortIterator) sortInPlace() {
	sort.SliceStable(s.rows, func(i, j int) bool {
		for _, sf := range s.fields {
			a := s.rows[i][sf.Name]
			b := s.rows[j][sf.Name]
			cmp := vm.CompareValues(a, b)
			if cmp == 0 {
				continue
			}
			if sf.Desc {
				return cmp > 0
			}

			return cmp < 0
		}

		return false
	})
}

// sortInPlaceCtx sorts s.rows with periodic context cancellation checks.
// Every 1024 comparisons the context is polled; if canceled the sort
// exits early and ctx.Err() is returned.
func (s *SortIterator) sortInPlaceCtx(ctx context.Context) error {
	var canceled bool
	var comparisons int64
	sort.SliceStable(s.rows, func(i, j int) bool {
		if canceled {
			return false
		}
		comparisons++
		if comparisons&0x3FF == 0 { // check every 1024 comparisons
			select {
			case <-ctx.Done():
				canceled = true

				return false
			default:
			}
		}
		for _, sf := range s.fields {
			a := s.rows[i][sf.Name]
			b := s.rows[j][sf.Name]
			cmp := vm.CompareValues(a, b)
			if cmp == 0 {
				continue
			}
			if sf.Desc {
				return cmp > 0
			}

			return cmp < 0
		}

		return false
	})
	if canceled {
		return ctx.Err()
	}

	return nil
}
