package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/event"
)

// DefaultMaxSortRows is the maximum number of rows that SortIterator will
// materialize in memory. Beyond this limit the query is aborted with
// ErrSortLimitExceeded.
const DefaultMaxSortRows = 10_000_000

// ErrSortLimitExceeded is returned when a sort operation exceeds the maximum
// number of materializable rows. Return sites wrap it with the effective
// per-query limit; match with errors.Is.
var ErrSortLimitExceeded = errors.New("sort: row limit exceeded")

// SortField describes a sort key: a column in the order of exec.Compare,
// with its nulls last or, with NullsFirst, first.
type SortField struct {
	Name       string
	Desc       bool
	NullsFirst bool
}

// compareRows orders two rows by the sort fields; buf serves the values
// that the comparison converts, and is returned for the next call.
func compareRows(fields []SortField, a, b map[string]event.Value, buf []byte) (int, []byte) {
	for _, f := range fields {
		var x, y exec.Value
		x, buf = ExecValue(a[f.Name], buf[:0])
		y, buf = ExecValue(b[f.Name], buf)
		if c := exec.Compare(x, y, f.Desc, f.NullsFirst); c != 0 {
			return c, buf
		}
	}

	return 0, buf
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
	var buf []byte
	a := make(map[string]event.Value, len(s.fields))
	b := make(map[string]event.Value, len(s.fields))
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
		for _, f := range s.fields {
			a[f.Name] = s.mergedBatch.Value(f.Name, s.sortedIndices[i])
			b[f.Name] = s.mergedBatch.Value(f.Name, s.sortedIndices[j])
		}
		var c int
		c, buf = compareRows(s.fields, a, b, buf)

		return c < 0
	})
	if canceled {
		return ctx.Err()
	}

	return nil
}
