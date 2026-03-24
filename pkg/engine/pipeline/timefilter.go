package pipeline

import (
	"context"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// TimeFilterIterator filters events by the _time field, keeping only events
// within the [earliest, latest] time range. Used by inline time syntax:
// FROM source[-1h], FROM source[-7d..-1d].
type TimeFilterIterator struct {
	child    Iterator
	earliest time.Time
	latest   time.Time
}

// NewTimeFilterIterator creates a time filter that keeps events where
// earliest <= _time <= latest.
func NewTimeFilterIterator(child Iterator, earliest, latest time.Time) *TimeFilterIterator {
	return &TimeFilterIterator{
		child:    child,
		earliest: earliest,
		latest:   latest,
	}
}

func (t *TimeFilterIterator) Init(ctx context.Context) error {
	return t.child.Init(ctx)
}

func (t *TimeFilterIterator) Next(ctx context.Context) (*Batch, error) {
	for {
		batch, err := t.child.Next(ctx)
		if err != nil || batch == nil {
			return batch, err
		}

		timeCol, hasTime := batch.Columns["_time"]
		if !hasTime {
			// No _time column — pass through (shouldn't happen normally).
			return batch, nil
		}

		// Build a mask of rows to keep.
		keep := make([]int, 0, batch.Len)
		for i := 0; i < batch.Len; i++ {
			v := timeCol[i]
			if v.IsNull() {
				continue
			}
			ts := time.Unix(0, v.AsInt()).In(t.earliest.Location())
			if (t.earliest.IsZero() || !ts.Before(t.earliest)) &&
				(t.latest.IsZero() || !ts.After(t.latest)) {
				keep = append(keep, i)
			}
		}

		if len(keep) == 0 {
			continue // try next batch
		}

		if len(keep) == batch.Len {
			return batch, nil // all rows kept
		}

		// Filter the batch to keep only matching rows.
		return filterBatchRows(batch, keep), nil
	}
}

// filterBatchRows returns a new batch containing only the rows at the given indices.
func filterBatchRows(src *Batch, indices []int) *Batch {
	dst := NewBatch(len(indices))
	for name, col := range src.Columns {
		newCol := make([]event.Value, len(indices))
		for j, idx := range indices {
			newCol[j] = col[idx]
		}
		dst.Columns[name] = newCol
	}
	dst.Len = len(indices)

	return dst
}

func (t *TimeFilterIterator) Close() error {
	return t.child.Close()
}

func (t *TimeFilterIterator) Schema() []FieldInfo {
	return t.child.Schema()
}
