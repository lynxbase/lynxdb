package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/memgov"
)

// TransactionIterator groups events by a field with maxspan/startswith/endswith.
type TransactionIterator struct {
	child      Iterator
	field      string
	maxSpan    time.Duration
	startsWith string
	endsWith   string
	rows       []map[string]event.Value
	emitted    bool
	offset     int
	batchSize  int
	acct       memgov.MemoryAccount // per-operator memory tracking
}

// NewTransactionIterator creates a transaction grouping operator.
func NewTransactionIterator(child Iterator, field string, maxSpan time.Duration, startsWith, endsWith string, batchSize int) *TransactionIterator {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	return &TransactionIterator{
		child:      child,
		field:      field,
		maxSpan:    maxSpan,
		startsWith: startsWith,
		endsWith:   endsWith,
		batchSize:  batchSize,
		acct:       memgov.NopAccount(),
	}
}

// NewTransactionIteratorWithBudget creates a transaction operator with memory budget tracking.
func NewTransactionIteratorWithBudget(child Iterator, field string, maxSpan time.Duration, startsWith, endsWith string, batchSize int, acct memgov.MemoryAccount) *TransactionIterator {
	t := NewTransactionIterator(child, field, maxSpan, startsWith, endsWith, batchSize)
	t.acct = memgov.EnsureAccount(acct)

	return t
}

func (t *TransactionIterator) Init(ctx context.Context) error {
	return t.child.Init(ctx)
}

func (t *TransactionIterator) Next(ctx context.Context) (*Batch, error) {
	if !t.emitted {
		if err := t.materialize(ctx); err != nil {
			return nil, err
		}
	}
	if t.offset >= len(t.rows) {
		return nil, nil
	}
	end := t.offset + t.batchSize
	if end > len(t.rows) {
		end = len(t.rows)
	}
	batch := BatchFromRows(t.rows[t.offset:end])
	t.offset = end

	return batch, nil
}

func (t *TransactionIterator) Close() error {
	t.acct.Close()

	return t.child.Close()
}

// MemoryUsed returns the current tracked memory for this operator.
func (t *TransactionIterator) MemoryUsed() int64 {
	return t.acct.Used()
}

func (t *TransactionIterator) Schema() []FieldInfo { return nil }

func (t *TransactionIterator) materialize(ctx context.Context) error {
	// Collect all events grouped by field value.
	type txGroup struct {
		events []map[string]event.Value
	}
	groups := make(map[string]*txGroup)
	groupOrder := make([]string, 0)

	for {
		batch, err := t.child.Next(ctx)
		if err != nil {
			return err
		}
		if batch == nil {
			break
		}
		// Track memory for accumulated rows.
		if err := t.acct.Grow(int64(batch.Len) * estimatedRowBytes); err != nil {
			return fmt.Errorf("transaction.materialize: %w", err)
		}
		for i := 0; i < batch.Len; i++ {
			row := batch.Row(i)
			key := ""
			if v, ok := row[t.field]; ok {
				key = v.String()
			}
			g, ok := groups[key]
			if !ok {
				g = &txGroup{}
				groups[key] = g
				groupOrder = append(groupOrder, key)
			}
			g.events = append(g.events, row)
		}
	}

	for _, key := range groupOrder {
		g := groups[key]
		events := g.events
		if len(events) == 0 {
			continue
		}

		// Apply maxspan filter.
		if t.maxSpan > 0 && len(events) >= 2 {
			firstTime := getTime(events[0])
			lastTime := getTime(events[len(events)-1])
			if lastTime.Sub(firstTime) > t.maxSpan {
				continue
			}
		}

		txRow := make(map[string]event.Value)
		txRow[t.field] = event.StringValue(key)
		txRow["eventcount"] = event.IntValue(int64(len(events)))

		// Duration.
		if len(events) >= 2 {
			firstTime := getTime(events[0])
			lastTime := getTime(events[len(events)-1])
			dur := lastTime.Sub(firstTime).Seconds()
			txRow["duration"] = event.FloatValue(dur)
		} else {
			txRow["duration"] = event.FloatValue(0)
		}

		// Merge _raw.
		var raws []string
		for _, e := range events {
			if r, ok := e["_raw"]; ok && !r.IsNull() {
				raws = append(raws, r.String())
			}
		}
		txRow["_raw"] = event.StringValue(strings.Join(raws, "\n"))

		// Copy first event's _time.
		if ts, ok := events[0]["_time"]; ok {
			txRow["_time"] = ts
		}

		// Copy all other fields from first event.
		for k, v := range events[0] {
			if _, exists := txRow[k]; !exists {
				txRow[k] = v
			}
		}

		t.rows = append(t.rows, txRow)
	}
	t.emitted = true

	return nil
}

func getTime(row map[string]event.Value) time.Time {
	if v, ok := row["_time"]; ok {
		if t, tok := v.TryAsTimestamp(); tok {
			return t
		}
	}

	return time.Time{}
}
