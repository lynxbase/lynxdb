package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/lynxbase/lynxdb/pkg/event"
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

// SortIterator fully materializes input, sorts, then streams output. It
// merges the input batches into one and sorts a permutation index over it,
// which avoids building a row map per row and a map lookup per comparison.
type SortIterator struct {
	child       Iterator
	fields      []SortField
	sorted      bool
	offset      int
	batchSize   int
	maxSortRows int

	mergedBatch   *Batch // accumulated columnar data from child batches
	sortedIndices []int  // permutation index after sort

	// tiebreakCols is the sorted list of column names NOT in the sort fields.
	// Used as a deterministic tiebreaker when all explicit sort keys are equal.
	// Computed lazily on first use.
	tiebreakCols []string
}

// NewSortIterator creates a full-materialization sort operator.
func NewSortIterator(child Iterator, fields []SortField, batchSize int) *SortIterator {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	return &SortIterator{
		child:       child,
		fields:      fields,
		batchSize:   batchSize,
		maxSortRows: DefaultMaxSortRows,
	}
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

func (s *SortIterator) Close() error {
	return s.child.Close()
}

func (s *SortIterator) materialize(ctx context.Context) error {
	s.mergedBatch = NewBatch(0)
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
		if s.mergedBatch.Len+batch.Len > s.maxSortRows {
			return fmt.Errorf("%w (limit %d rows)", ErrSortLimitExceeded, s.maxSortRows)
		}
		s.mergedBatch.AppendBatch(batch)
	}

	if err := s.sortColumnar(ctx); err != nil {
		return err
	}
	s.sorted = true

	return nil
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

	return nil
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
