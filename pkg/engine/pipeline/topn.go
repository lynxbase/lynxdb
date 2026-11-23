package pipeline

import (
	"container/heap"
	"context"
	"slices"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/vec"
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
	t.h = &topNHeap{keys: sortKeys{fields: t.fields}}
	// The keys of the row offered go after those of the rows the heap holds.
	cand := sortKeys{fields: t.fields}
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
			seq++
			if t.limit <= 0 {
				continue
			}
			cand.vals, cand.buf = cand.vals[:0], cand.buf[:0]
			for _, f := range t.fields {
				cand.add(batch.Value(f.Name, i))
			}
			switch {
			case t.h.Len() < t.limit:
				heap.Push(t.h, t.h.kept(batch.Row(i), seq, cand.vals))
			case t.h.beats(cand.vals, seq):
				t.h.replace(batch.Row(i), seq, cand.vals)
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

// ranked is a row, its place in the input and the place of its keys.
type ranked struct {
	row map[string]event.Value
	seq int
	at  int
}

// topNHeap keeps at its root the row that sorts last; keys holds the sort
// keys of the rows it holds, one slot per row that it reuses.
type topNHeap struct {
	rows []ranked
	keys sortKeys
}

// ownBytes gives the arrays and objects among vals bytes of their own.
func ownBytes(vals []exec.Value) {
	for i := range vals {
		if vals[i].Kind == vec.KindVariant {
			vals[i].Bytes = slices.Clone(vals[i].Bytes)
		}
	}
}

// kept returns the entry of a row that the heap takes, with its keys in a
// new slot.
func (h *topNHeap) kept(row map[string]event.Value, seq int, keys []exec.Value) ranked {
	at := len(h.keys.vals) / max(len(h.keys.fields), 1)
	h.keys.vals = append(h.keys.vals, keys...)
	ownBytes(h.keys.vals[len(h.keys.vals)-len(keys):])

	return ranked{row: row, seq: seq, at: at}
}

// replace puts a row in place of the root, in the root's slot of keys.
func (h *topNHeap) replace(row map[string]event.Value, seq int, keys []exec.Value) {
	root := &h.rows[0]
	n := len(h.keys.fields)
	slot := h.keys.vals[root.at*n : (root.at+1)*n]
	copy(slot, keys)
	ownBytes(slot)
	root.row, root.seq = row, seq
}

// beats reports whether the row of keys and seq sorts before the root.
func (h *topNHeap) beats(keys []exec.Value, seq int) bool {
	root := h.rows[0]
	n := len(h.keys.fields)
	for f, field := range h.keys.fields {
		if c := exec.Compare(keys[f], h.keys.vals[root.at*n+f], field.Desc, field.NullsFirst); c != 0 {
			return c < 0
		}
	}

	return seq < root.seq
}

// before reports whether a sorts before b.
func (h *topNHeap) before(a, b ranked) bool {
	if c := h.keys.compare(a.at, b.at); c != 0 {
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
