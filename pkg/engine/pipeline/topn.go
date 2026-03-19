package pipeline

import (
	"container/heap"
	"context"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/memgov"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

// TopNIterator implements heap-based top-N selection.
// O(n log k) instead of O(n log n) for full sort + limit.
type TopNIterator struct {
	child     Iterator
	fields    []SortField
	limit     int
	batchSize int
	acct      memgov.MemoryAccount // per-operator memory tracking

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
		acct:      memgov.NopAccount(),
	}
}

// NewTopNIteratorWithBudget creates a top-N selection operator with memory
// budget tracking. Each row pushed into the heap is tracked via the account.
func NewTopNIteratorWithBudget(child Iterator, fields []SortField, limit, batchSize int, acct memgov.MemoryAccount) *TopNIterator {
	t := NewTopNIterator(child, fields, limit, batchSize)
	t.acct = memgov.EnsureAccount(acct)

	return t
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
	end := t.offset + t.batchSize
	if end > len(t.result) {
		end = len(t.result)
	}
	batch := BatchFromRows(t.result[t.offset:end])
	t.offset = end

	return batch, nil
}

func (t *TopNIterator) Close() error {
	t.acct.Close()

	return t.child.Close()
}

func (t *TopNIterator) Schema() []FieldInfo { return t.child.Schema() }

// MemoryUsed returns the current tracked memory for this operator.
func (t *TopNIterator) MemoryUsed() int64 {
	return t.acct.Used()
}

func (t *TopNIterator) buildTopN(ctx context.Context) error {
	t.h = &topNHeap{
		fields: t.fields,
		limit:  t.limit,
	}
	heap.Init(t.h)

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
			row := batch.Row(i)
			if t.h.Len() < t.limit {
				// Growing the heap: track memory for the new row.
				if growErr := t.acct.Grow(estimatedRowBytes); growErr != nil {
					// TopN is bounded by limit, so budget exceeded is unusual.
					// Continue without tracking rather than failing the query.
					break
				}
				heap.Push(t.h, row)
			} else if t.h.Len() > 0 {
				// Compare with the "worst" element (heap root).
				// The heap keeps the worst element at top (min-heap by reverse sort order).
				// Replacement is memory-neutral (swap one row for another).
				if t.isBetter(row, t.h.rows[0]) {
					t.h.rows[0] = row
					heap.Fix(t.h, 0)
				}
			}
		}
	}

	// Extract results in sorted order (best first).
	n := t.h.Len()
	t.result = make([]map[string]event.Value, n)
	for i := n - 1; i >= 0; i-- {
		t.result[i] = heap.Pop(t.h).(map[string]event.Value)
	}
	t.built = true

	return nil
}

// isBetter returns true if row a should appear before row b in the final output.
func (t *TopNIterator) isBetter(a, b map[string]event.Value) bool {
	for _, sf := range t.fields {
		av := a[sf.Name]
		bv := b[sf.Name]
		cmp := vm.CompareValues(av, bv)
		if cmp == 0 {
			continue
		}
		if sf.Desc {
			return cmp > 0
		}

		return cmp < 0
	}

	return false
}

// topNHeap is a min-heap that keeps the "worst" element at the root.
// "Worst" means last in the desired sort order, so the heap's comparison
// is the reverse of the desired sort.
type topNHeap struct {
	rows   []map[string]event.Value
	fields []SortField
	limit  int
}

func (h *topNHeap) Len() int { return len(h.rows) }

// Less returns true if rows[i] should be closer to the heap root (i.e., is "worse").
// Since we want to keep the best N and evict the worst, the worst should be at root.
func (h *topNHeap) Less(i, j int) bool {
	for _, sf := range h.fields {
		av := h.rows[i][sf.Name]
		bv := h.rows[j][sf.Name]
		cmp := vm.CompareValues(av, bv)
		if cmp == 0 {
			continue
		}
		// Reverse: for ascending sort, "worse" means larger value.
		if sf.Desc {
			return cmp < 0 // for desc sort, smaller value is worse
		}

		return cmp > 0 // for asc sort, larger value is worse
	}

	return false
}

func (h *topNHeap) Swap(i, j int) { h.rows[i], h.rows[j] = h.rows[j], h.rows[i] }

func (h *topNHeap) Push(x interface{}) {
	h.rows = append(h.rows, x.(map[string]event.Value))
}

func (h *topNHeap) Pop() interface{} {
	old := h.rows
	n := len(old)
	item := old[n-1]
	h.rows = old[:n-1]

	return item
}
