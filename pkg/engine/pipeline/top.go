package pipeline

import (
	"context"
	"fmt"
	"sort"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/memgov"
)

// TopIterator implements the top/rare command.
// It aggregates count by field, sorts descending (top) or ascending (rare),
// and returns the top/bottom N values.
type TopIterator struct {
	child     Iterator
	field     string
	byField   string
	n         int
	ascending bool // true for rare, false for top
	batchSize int
	rows      []map[string]event.Value
	emitted   bool
	offset    int
	acct      memgov.MemoryAccount // per-operator memory tracking
}

// NewTopIterator creates a top/rare iterator.
func NewTopIterator(child Iterator, field, byField string, n int, ascending bool, batchSize int) *TopIterator {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	return &TopIterator{
		child:     child,
		field:     field,
		byField:   byField,
		n:         n,
		ascending: ascending,
		batchSize: batchSize,
		acct:      memgov.NopAccount(),
	}
}

// NewTopIteratorWithBudget creates a top/rare iterator with memory budget tracking.
func NewTopIteratorWithBudget(child Iterator, field, byField string, n int, ascending bool, batchSize int, acct memgov.MemoryAccount) *TopIterator {
	t := NewTopIterator(child, field, byField, n, ascending, batchSize)
	t.acct = memgov.EnsureAccount(acct)

	return t
}

func (t *TopIterator) Init(ctx context.Context) error {
	return t.child.Init(ctx)
}

func (t *TopIterator) Next(ctx context.Context) (*Batch, error) {
	if !t.emitted {
		if err := t.materialize(ctx); err != nil {
			return nil, err
		}
		t.emitted = true
	}

	if t.offset >= len(t.rows) {
		return nil, nil
	}

	end := t.offset + t.batchSize
	if end > len(t.rows) {
		end = len(t.rows)
	}

	batch := NewBatch(end - t.offset)
	for _, row := range t.rows[t.offset:end] {
		batch.AddRow(row)
	}
	t.offset = end

	return batch, nil
}

func (t *TopIterator) Close() error {
	t.acct.Close()

	return t.child.Close()
}

// MemoryUsed returns the current tracked memory for this operator.
func (t *TopIterator) MemoryUsed() int64 {
	return t.acct.Used()
}

func (t *TopIterator) Schema() []FieldInfo {
	fields := []FieldInfo{
		{Name: t.field},
		{Name: "count"},
		{Name: "percent"},
	}
	if t.byField != "" {
		fields = append(fields, FieldInfo{Name: t.byField})
	}

	return fields
}

func (t *TopIterator) materialize(ctx context.Context) error {
	type counterKey struct {
		fieldVal string
		byVal    string
	}
	counts := make(map[counterKey]int64)
	total := int64(0)

	for {
		batch, err := t.child.Next(ctx)
		if err != nil {
			return err
		}
		if batch == nil {
			break
		}

		for i := 0; i < batch.Len; i++ {
			row := batch.Row(i)
			fv := ""
			if v, ok := row[t.field]; ok {
				fv = v.String()
			}

			bv := ""
			if t.byField != "" {
				if v, ok := row[t.byField]; ok {
					bv = v.String()
				}
			}

			ck := counterKey{fv, bv}
			if counts[ck] == 0 {
				// New counter key — track memory.
				if err := t.acct.Grow(estimatedDedupExactKeyBytes); err != nil {
					return fmt.Errorf("top.materialize: %w", err)
				}
			}
			counts[ck]++
			total++
		}
	}

	type countEntry struct {
		fieldVal string
		byVal    string
		count    int64
	}
	entries := make([]countEntry, 0, len(counts))
	for k, c := range counts {
		entries = append(entries, countEntry{k.fieldVal, k.byVal, c})
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].count != entries[j].count {
			if t.ascending {
				return entries[i].count < entries[j].count
			}

			return entries[i].count > entries[j].count
		}
		// Secondary sort by field value for deterministic output.
		if entries[i].fieldVal != entries[j].fieldVal {
			return entries[i].fieldVal < entries[j].fieldVal
		}

		return entries[i].byVal < entries[j].byVal
	})

	if len(entries) > t.n {
		entries = entries[:t.n]
	}

	t.rows = make([]map[string]event.Value, len(entries))
	for i, e := range entries {
		row := map[string]event.Value{
			t.field: event.StringValue(e.fieldVal),
			"count": event.IntValue(e.count),
		}
		if total > 0 {
			pct := float64(e.count) / float64(total) * 100
			row["percent"] = event.FloatValue(pct)
		}
		if t.byField != "" {
			row[t.byField] = event.StringValue(e.byVal)
		}
		t.rows[i] = row
	}

	return nil
}
