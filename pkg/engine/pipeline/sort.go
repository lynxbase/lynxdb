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
// materialize in memory. Beyond this limit the query is aborted with
// ErrSortLimitExceeded.
const DefaultMaxSortRows = 10_000_000

// ErrSortLimitExceeded is returned when a sort operation exceeds the maximum
// number of materializable rows. Return sites wrap it with the effective
// per-query limit; match with errors.Is.
var ErrSortLimitExceeded = errors.New("sort: row limit exceeded")

// SortField describes a sort key.
type SortField struct {
	Name string
	Desc bool
}

// SortIterator fully materializes input, sorts, then streams output.
//
// Fast path: the sort operates directly on columnar batches using a
// permutation index. This avoids materializing row maps
// (map[string]event.Value per row) and eliminates per-comparison hash
// lookups. When the memory budget rejects a whole batch, the sort falls back
// to accumulating row maps, accounted row by row.
type SortIterator struct {
	child       Iterator
	fields      []SortField
	rows        []map[string]event.Value
	sorted      bool
	offset      int
	batchSize   int
	maxSortRows int
	acct        memgov.MemoryAccount // per-operator memory tracking

	// Columnar fast path state.
	mergedBatch   *Batch // accumulated columnar data from child batches
	sortedIndices []int  // permutation index after sort
	useColumnar   bool   // true when columnar path is active

	// tiebreakCols is the sorted list of column names NOT in the sort fields.
	// Used as a deterministic tiebreaker when all explicit sort keys are equal.
	// Computed lazily on first use.
	tiebreakCols []string
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

func (s *SortIterator) Init(ctx context.Context) error {
	return s.child.Init(ctx)
}

func (s *SortIterator) Next(ctx context.Context) (*Batch, error) {
	if !s.sorted {
		if err := s.materialize(ctx); err != nil {
			return nil, err
		}
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

	// Row-based path (fallback after columnar→row degradation).
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
	s.acct.Close()

	return s.child.Close()
}

func (s *SortIterator) Schema() []FieldInfo { return s.child.Schema() }

func (s *SortIterator) materialize(ctx context.Context) error {
	// Start in columnar mode. We accumulate child batches directly into a
	// merged columnar buffer. If the budget rejects a batch, we degrade to
	// the row-based path.
	columnarMode := true
	s.mergedBatch = NewBatch(0)
	var columnarRows int // total rows accumulated in columnar mode

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		batch, err := s.child.Next(ctx)
		if err != nil {
			return err
		}
		if batch == nil {
			break
		}

		if columnarMode {
			if growErr := s.acct.Grow(EstimateBatchBytes(batch)); growErr != nil {
				// Budget exceeded — handle the current batch in row mode below.
				s.degradeToRowPath()
				columnarMode = false
			} else {
				if columnarRows+batch.Len > s.maxSortRows {
					return fmt.Errorf("%w (limit %d rows)", ErrSortLimitExceeded, s.maxSortRows)
				}
				s.mergedBatch.AppendBatch(batch)
				columnarRows += batch.Len
				continue
			}
		}

		for i := 0; i < batch.Len; i++ {
			row := batch.Row(i)
			if err := s.acct.Grow(estimateRowMapBytes(row)); err != nil {
				return fmt.Errorf("sort.materialize: %w", err)
			}
			if len(s.rows)+1 > s.maxSortRows {
				return fmt.Errorf("%w (limit %d rows)", ErrSortLimitExceeded, s.maxSortRows)
			}
			s.rows = append(s.rows, row)
		}
	}

	// All input consumed. Choose sort strategy.
	if columnarMode && columnarRows > 0 {
		// Columnar fast path: sort via permutation index.
		if err := s.sortColumnar(ctx); err != nil {
			return err
		}
		s.sorted = true

		return nil
	}

	// Clean up unused columnar state.
	s.mergedBatch = nil

	if err := s.sortInPlaceCtx(ctx); err != nil {
		return err
	}
	s.sorted = true

	return nil
}

// degradeToRowPath converts accumulated columnar data to row maps and
// resets the columnar state. Called when the budget rejects a whole batch.
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
		if comparisons&0x3FF == 0 {
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

		// Tiebreaker: all explicit sort keys are equal.
		return s.tiebreakColumnarLess(s.sortedIndices[i], s.sortedIndices[j])
	})
	if canceled {
		return ctx.Err()
	}

	s.useColumnar = true
	s.sorted = true

	return nil
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
		if comparisons&0x3FF == 0 {
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

		// Tiebreaker: all explicit sort keys are equal.
		return s.tiebreakRowLess(s.rows[i], s.rows[j])
	})
	if canceled {
		return ctx.Err()
	}

	return nil
}

// buildTiebreakCols computes the sorted list of column names not already
// in the explicit sort fields. Called lazily on the first tiebreaker
// comparison — the cost is O(C log C) where C is the number of columns,
// which is negligible compared to the sort itself.
func (s *SortIterator) buildTiebreakColsFromRow(row map[string]event.Value) {
	sortSet := make(map[string]bool, len(s.fields))
	for _, sf := range s.fields {
		sortSet[sf.Name] = true
	}
	s.tiebreakCols = make([]string, 0, len(row))
	for k := range row {
		if !sortSet[k] {
			s.tiebreakCols = append(s.tiebreakCols, k)
		}
	}
	sort.Strings(s.tiebreakCols)
}

func (s *SortIterator) buildTiebreakColsFromBatch() {
	sortSet := make(map[string]bool, len(s.fields))
	for _, sf := range s.fields {
		sortSet[sf.Name] = true
	}
	s.tiebreakCols = make([]string, 0, len(s.mergedBatch.Columns))
	for k := range s.mergedBatch.Columns {
		if !sortSet[k] {
			s.tiebreakCols = append(s.tiebreakCols, k)
		}
	}
	sort.Strings(s.tiebreakCols)
}

// tiebreakRowLess compares two rows by non-sort columns in alphabetical
// order. Returns true if a < b. Used as a deterministic tiebreaker when
// all explicit sort keys are equal — ensures query output is identical
// across runs regardless of Go map iteration randomness.
func (s *SortIterator) tiebreakRowLess(a, b map[string]event.Value) bool {
	if s.tiebreakCols == nil {
		s.buildTiebreakColsFromRow(a)
	}
	for _, col := range s.tiebreakCols {
		cmp := vm.CompareValues(a[col], b[col])
		if cmp != 0 {
			return cmp < 0
		}
	}

	return false
}

// tiebreakMapLess is a standalone tiebreaker for row maps used by both
// SortIterator and TopNIterator. It compares all columns NOT in sortFields,
// in alphabetical column-name order, ascending by value.
func tiebreakMapLess(a, b map[string]event.Value, sortFields []SortField) bool {
	// Build sorted list of non-sort column names from the row.
	sortSet := make(map[string]bool, len(sortFields))
	for _, sf := range sortFields {
		sortSet[sf.Name] = true
	}
	cols := make([]string, 0, len(a))
	for k := range a {
		if !sortSet[k] {
			cols = append(cols, k)
		}
	}
	sort.Strings(cols)

	for _, col := range cols {
		cmp := vm.CompareValues(a[col], b[col])
		if cmp != 0 {
			return cmp < 0
		}
	}

	return false
}

// tiebreakColumnarLess compares two rows (by index) in the columnar
// batch using non-sort columns. Same logic as tiebreakRowLess but
// operates on the columnar fast path.
func (s *SortIterator) tiebreakColumnarLess(i, j int) bool {
	if s.tiebreakCols == nil {
		s.buildTiebreakColsFromBatch()
	}
	for _, col := range s.tiebreakCols {
		a := s.mergedBatch.Value(col, i)
		b := s.mergedBatch.Value(col, j)
		cmp := vm.CompareValues(a, b)
		if cmp != 0 {
			return cmp < 0
		}
	}

	return false
}
