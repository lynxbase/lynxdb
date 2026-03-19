package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/memgov"
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
//
// Fast path: when all data fits in memory without spilling, the sort operates
// directly on columnar batches using a permutation index. This avoids
// materializing row maps (map[string]event.Value per row) and eliminates
// per-comparison hash lookups. Fallback to row-based sort occurs when spill
// is triggered (budget exceeded or row limit hit).
type SortIterator struct {
	child       Iterator
	fields      []SortField
	rows        []map[string]event.Value
	sorted      bool
	offset      int
	batchSize   int
	maxSortRows int
	acct        memgov.MemoryAccount // per-operator memory tracking

	// External merge sort state (populated only when spill occurs).
	spillFiles  []string      // paths of sorted spill run files
	merger      SpillMergerI  // k-way merge iterator (nil = in-memory path)
	spillMgr    *SpillManager // lifecycle manager for spill files (nil = no spill support)
	spilledRows int64         // total rows written to spill files (for ResourceReporter)

	// Columnar fast path state (used when no spill occurs).
	mergedBatch   *Batch // accumulated columnar data from child batches
	sortedIndices []int  // permutation index after sort
	useColumnar   bool   // true when columnar path is active
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
		acct:        memgov.NopAccount(),
	}
}

// NewSortIteratorWithBudget creates a sort operator with memory budget tracking.
func NewSortIteratorWithBudget(child Iterator, fields []SortField, batchSize int, acct memgov.MemoryAccount) *SortIterator {
	s := NewSortIterator(child, fields, batchSize)
	s.acct = memgov.EnsureAccount(acct)

	return s
}

// NewSortIteratorWithSpill creates a sort operator with memory budget tracking
// and disk spill support. When the budget is exceeded, sorted runs are written
// to disk via the SpillManager and merged on output using a k-way merge.
func NewSortIteratorWithSpill(child Iterator, fields []SortField, batchSize int, acct memgov.MemoryAccount, mgr *SpillManager) *SortIterator {
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

	// Columnar fast path: emit slices of the sorted permutation.
	if s.useColumnar {
		if s.offset >= len(s.sortedIndices) {
			return nil, nil
		}
		end := s.offset + s.batchSize
		if end > len(s.sortedIndices) {
			end = len(s.sortedIndices)
		}
		batch := s.mergedBatch.PermuteSlice(s.sortedIndices[s.offset:end])
		s.offset = end

		return batch, nil
	}

	// Row-based in-memory path (fallback after columnar→row degradation).
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

// estimateColumnarBatchBytes estimates the heap size of a columnar Batch.
// Called once per incoming batch for incremental memory tracking.
func estimateColumnarBatchBytes(batch *Batch) int64 {
	var total int64
	for _, col := range batch.Columns {
		// Slice header (24 bytes) + per-element Value (~40 bytes each).
		total += 24 + int64(len(col))*40
		for _, v := range col {
			if v.Type() == event.FieldTypeString {
				total += int64(len(v.String()))
			}
		}
	}

	return total
}

func (s *SortIterator) materialize(ctx context.Context) error {
	// Transition to building phase — accumulating rows for sort.
	if pn, ok := s.acct.(PhaseNotifier); ok {
		pn.SetPhase(PhaseBuilding)
	}

	// Start in columnar mode. We accumulate child batches directly into a
	// merged columnar buffer. If a spill is triggered (budget exceeded or
	// row limit hit), we degrade to the legacy row-based path.
	columnarMode := true
	s.mergedBatch = NewBatch(0)
	var columnarRows int // total rows accumulated in columnar mode

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		batch, err := s.child.Next(ctx)
		if err != nil {
			// Bug fix: when scan (or another child) fails because the shared
			// budget is exhausted, sort may hold a large spillable buffer.
			// Spill that buffer to free shared budget capacity, then retry.
			if memgov.IsMemoryExhausted(err) && s.spillMgr != nil {
				if columnarMode && columnarRows > 0 {
					// Degrade to row path: convert columnar data to rows before spilling.
					s.degradeToRowPath()
					columnarMode = false
				}
				if len(s.rows) > 0 {
					if spillErr := s.spillCurrentRun(); spillErr != nil {
						return fmt.Errorf("sort.materialize: spill on child budget pressure: %w", spillErr)
					}
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

		if columnarMode {
			batchBytes := estimateColumnarBatchBytes(batch)
			growErr := s.acct.Grow(batchBytes)

			if growErr != nil {
				// Budget exceeded — degrade to row path and try spilling.
				s.degradeToRowPath()
				columnarMode = false

				// Now handle the current batch in row mode (below).
			} else {
				// Check row count safety valve.
				if columnarRows+batch.Len > s.maxSortRows {
					if s.spillMgr == nil {
						return ErrSortLimitExceeded
					}
					// Degrade to row path and spill.
					s.degradeToRowPath()
					columnarMode = false
					// Fall through to row-based accumulation for this batch.
				} else {
					s.mergedBatch.AppendBatch(batch)
					columnarRows += batch.Len
					continue
				}
			}
		}

		// Row-based accumulation path (after degradation or for spill support).
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

	// All input consumed. Choose sort strategy.
	if columnarMode && columnarRows > 0 && len(s.spillFiles) == 0 {
		// Columnar fast path: sort via permutation index.
		if err := s.sortColumnar(ctx); err != nil {
			return err
		}
		if pn, ok := s.acct.(PhaseNotifier); ok {
			pn.SetPhase(PhaseProbing)
		}
		s.sorted = true

		return nil
	}

	// Clean up unused columnar state.
	s.mergedBatch = nil

	if len(s.spillFiles) == 0 {
		// Row-based in-memory fast path: sort in-place, no spill occurred.
		if err := s.sortInPlaceCtx(ctx); err != nil {
			return err
		}
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

// degradeToRowPath converts accumulated columnar data to row maps and
// resets the columnar state. Called when a spill is needed (budget exceeded
// or row limit hit) since the spill infrastructure operates on row maps.
func (s *SortIterator) degradeToRowPath() {
	if s.mergedBatch == nil || s.mergedBatch.Len == 0 {
		s.mergedBatch = nil
		return
	}
	s.rows = make([]map[string]event.Value, 0, s.mergedBatch.Len)
	for i := 0; i < s.mergedBatch.Len; i++ {
		s.rows = append(s.rows, s.mergedBatch.Row(i))
	}
	s.mergedBatch = nil
}

// sortColumnar sorts the accumulated columnar data using a permutation index.
// Direct column access (Batch.Value) replaces per-comparison map lookups,
// reducing sort overhead from ~30-50ns/lookup to ~5ns/lookup for 1.5M+ rows.
func (s *SortIterator) sortColumnar(ctx context.Context) error {
	n := s.mergedBatch.Len
	s.sortedIndices = make([]int, n)
	for i := range s.sortedIndices {
		s.sortedIndices[i] = i
	}

	var canceled bool
	var comparisons int64
	sort.SliceStable(s.sortedIndices, func(i, j int) bool {
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
			a := s.mergedBatch.Value(sf.Name, s.sortedIndices[i])
			b := s.mergedBatch.Value(sf.Name, s.sortedIndices[j])
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

	s.useColumnar = true
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
// *memgov.BudgetExceededError type for callers that inspect it).
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
			closeErr := sw.CloseFile()

			return errors.Join(
				fmt.Errorf("sort.spillCurrentRun: write: %w", err),
				closeErr,
			)
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
