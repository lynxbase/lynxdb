package pipeline

import (
	"context"
	"errors"
	"fmt"
	"slices"

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

// sortKeys holds the values of the sort fields of rows as the executor
// compares them, converted once per row, row after row.
type sortKeys struct {
	fields []SortField
	vals   []exec.Value
	// buf holds the bytes of the arrays and objects among the values.
	buf []byte
}

// add appends the value of the next field of the row being added.
func (k *sortKeys) add(v event.Value) {
	var x exec.Value
	x, k.buf = ExecValue(v, k.buf)
	k.vals = append(k.vals, x)
}

// compare orders the rows a and b by the sort fields.
func (k *sortKeys) compare(a, b int) int {
	n := len(k.fields)
	for f, field := range k.fields {
		if c := exec.Compare(k.vals[a*n+f], k.vals[b*n+f], field.Desc, field.NullsFirst); c != 0 {
			return c
		}
	}

	return 0
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

// sortColumnar sorts the accumulated columnar data using a permutation
// index, over the sort fields of each row converted once.
func (s *SortIterator) sortColumnar(ctx context.Context) error {
	n := s.mergedBatch.Len
	keys := &sortKeys{fields: s.fields, vals: make([]exec.Value, 0, n*len(s.fields))}
	cols := make([][]event.Value, len(s.fields))
	for f, field := range s.fields {
		cols[f] = s.mergedBatch.Columns[field.Name]
	}
	s.sortedIndices = make([]int, n)
	for i := range s.sortedIndices {
		s.sortedIndices[i] = i
		for _, col := range cols {
			var v event.Value
			if i < len(col) {
				v = col[i]
			}
			keys.add(v)
		}
	}

	var canceled bool
	var comparisons int64
	slices.SortStableFunc(s.sortedIndices, func(a, b int) int {
		if canceled {
			return 0
		}
		comparisons++
		if comparisons&0x3FF == 0 {
			select {
			case <-ctx.Done():
				canceled = true

				return 0
			default:
			}
		}

		return keys.compare(a, b)
	})
	if canceled {
		return ctx.Err()
	}

	return nil
}
