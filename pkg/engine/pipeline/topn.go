package pipeline

import (
	"container/heap"
	"context"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// TopNIterator keeps the first rows of a sort, in O(n log k) for k rows; of
// rows that sort alike, the earlier comes first.
type TopNIterator struct {
	child     Iterator
	fields    []SortField
	limit     int
	batchSize int

	h      *topNHeap
	result []map[string]event.Value
	built  bool
	offset int
}

// NewTopNIterator creates a top-N selection operator.
func NewTopNIterator(child Iterator, fields []SortField, limit, batchSize int) *TopNIterator {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	return &TopNIterator{
		child:     child,
		fields:    fields,
		limit:     limit,
		batchSize: batchSize,
	}
}

func (t *TopNIterator) Init(ctx context.Context) error {
	return t.child.Init(ctx)
}

func (t *TopNIterator) Next(ctx context.Context) (*Batch, error) {
	if !t.built {
		if err := t.buildTopN(ctx); err != nil {
			return nil, err
		}
	}
	if t.offset >= len(t.result) {
		return nil, nil
	}
	end := min(t.offset+t.batchSize, len(t.result))
	batch := BatchFromRows(t.result[t.offset:end])
	t.offset = end

	return batch, nil
}

func (t *TopNIterator) Close() error {
	return t.child.Close()
}

func (t *TopNIterator) buildTopN(ctx context.Context) error {
	t.h = &topNHeap{fields: t.fields}
	seq := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch, err := t.child.Next(ctx)
		if err != nil {
			return err
		}
		if batch == nil {
			break
		}
		for i := 0; i < batch.Len; i++ {
			r := ranked{row: batch.Row(i), seq: seq}
			seq++
			switch {
			case t.limit <= 0:
			case t.h.Len() < t.limit:
				heap.Push(t.h, r)
			case t.h.before(r, t.h.rows[0]):
				t.h.rows[0] = r
				heap.Fix(t.h, 0)
			}
		}
	}
	t.result = make([]map[string]event.Value, t.h.Len())
	for i := len(t.result) - 1; i >= 0; i-- {
		t.result[i] = heap.Pop(t.h).(ranked).row
	}
	t.built = true

	return nil
}

// ranked is a row and its place in the input.
type ranked struct {
	row map[string]event.Value
	seq int
}

// topNHeap keeps at its root the row that sorts last.
type topNHeap struct {
	rows   []ranked
	fields []SortField
	buf    []byte
}

// before reports whether a sorts before b.
func (h *topNHeap) before(a, b ranked) bool {
	var c int
	c, h.buf = compareRows(h.fields, a.row, b.row, h.buf)
	if c != 0 {
		return c < 0
	}

	return a.seq < b.seq
}

func (h *topNHeap) Len() int           { return len(h.rows) }
func (h *topNHeap) Less(i, j int) bool { return h.before(h.rows[j], h.rows[i]) }
func (h *topNHeap) Swap(i, j int)      { h.rows[i], h.rows[j] = h.rows[j], h.rows[i] }
func (h *topNHeap) Push(x any)         { h.rows = append(h.rows, x.(ranked)) }

func (h *topNHeap) Pop() any {
	n := len(h.rows)
	item := h.rows[n-1]
	h.rows = h.rows[:n-1]

	return item
}
