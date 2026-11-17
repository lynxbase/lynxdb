package pipeline

import (
	"context"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

// EventStatsIterator implements two-pass aggregation: accumulate globals, then enrich rows.
type EventStatsIterator struct {
	child     Iterator
	aggs      []AggFunc
	groupBy   []string
	rows      []map[string]event.Value
	vmInst    vm.VM
	emitted   bool
	batchSize int
	offset    int
}

// NewEventStatsIterator creates a two-pass eventstats operator.
func NewEventStatsIterator(child Iterator, aggs []AggFunc, groupBy []string, batchSize int) *EventStatsIterator {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	return &EventStatsIterator{
		child:     child,
		aggs:      aggs,
		groupBy:   groupBy,
		batchSize: batchSize,
	}
}

// Init delegates to the child iterator.
func (e *EventStatsIterator) Init(ctx context.Context) error {
	return e.child.Init(ctx)
}

// Next returns the next batch of enriched rows. On first call, materializes
// all input (pass 1: accumulate aggregates, buffer rows), then emits
// enriched rows in batches (pass 2).
func (e *EventStatsIterator) Next(ctx context.Context) (*Batch, error) {
	if !e.emitted {
		if err := e.materialize(ctx); err != nil {
			return nil, err
		}
	}

	if e.offset >= len(e.rows) {
		return nil, nil
	}
	end := e.offset + e.batchSize
	if end > len(e.rows) {
		end = len(e.rows)
	}
	batch := BatchFromRows(e.rows[e.offset:end])
	e.offset = end

	return batch, nil
}

// Close closes the child.
func (e *EventStatsIterator) Close() error {
	return e.child.Close()
}

func (e *EventStatsIterator) materialize(ctx context.Context) error {
	// Pass 1: collect all rows and compute aggregates.
	groups := make(map[string]*aggGroup)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch, err := e.child.Next(ctx)
		if err != nil {
			return err
		}
		if batch == nil {
			break
		}

		for i := 0; i < batch.Len; i++ {
			row := batch.Row(i)
			e.rows = append(e.rows, row)

			key := e.groupKey(row)
			group, ok := groups[key]
			if !ok {
				group = &aggGroup{
					key:    e.extractGroupKey(row),
					states: make([]aggState, len(e.aggs)),
				}
				for j := range group.states {
					group.states[j].min = event.NullValue()
					group.states[j].max = event.NullValue()
					group.states[j].first = event.NullValue()
					group.states[j].last = event.NullValue()
					group.states[j].values = make(map[string]bool)
				}
				groups[key] = group
			}
			for j, agg := range e.aggs {
				updateEventStatsState(&group.states[j], agg.Name, e.extractValue(agg, row))
			}
		}
	}

	// Pass 2: enrich rows with aggregated values.
	for _, row := range e.rows {
		key := e.groupKey(row)
		group := groups[key]
		for j, agg := range e.aggs {
			row[agg.Alias] = finalizeEventStatsAgg(&group.states[j], agg)
		}
	}

	e.emitted = true

	return nil
}

// groupKey builds a composite key from the BY-clause fields of a row.
//
// Uses null byte (\x00) as separator instead of '|' to avoid collisions when
// field values contain the separator character. Each field is prefixed with a
// presence marker: \x01 for present values, \x00 for null/missing. This
// ensures that ("a", null) and (null, "a") produce distinct keys, and that
// values like "x|y" in a single field don't collide with "x" and "y" in two
// separate fields.
func (e *EventStatsIterator) groupKey(row map[string]event.Value) string {
	if len(e.groupBy) == 0 {
		return ""
	}
	var sb strings.Builder
	for i, g := range e.groupBy {
		if i > 0 {
			sb.WriteByte(0) // null byte separator
		}
		v, ok := row[g]
		if !ok || v.IsNull() {
			sb.WriteByte(0) // null/missing marker
		} else {
			sb.WriteByte(1) // present marker
			sb.WriteString(v.String())
		}
	}

	return sb.String()
}

func (e *EventStatsIterator) extractGroupKey(row map[string]event.Value) map[string]event.Value {
	key := make(map[string]event.Value, len(e.groupBy))
	for _, g := range e.groupBy {
		if v, ok := row[g]; ok {
			key[g] = v
		} else {
			key[g] = event.NullValue()
		}
	}

	return key
}

func (e *EventStatsIterator) extractValue(agg AggFunc, row map[string]event.Value) event.Value {
	if agg.Field == "" {
		return event.IntValue(1)
	}
	if v, ok := row[agg.Field]; ok {
		return v
	}

	return event.NullValue()
}

func updateEventStatsState(s *aggState, fn string, val event.Value) {
	switch strings.ToLower(fn) {
	case aggCount:
		if !val.IsNull() {
			s.count++
		}
	case aggSum:
		if f, ok := vm.ValueToFloat(val); ok {
			s.sum += f
		}
	case aggAvg:
		if f, ok := vm.ValueToFloat(val); ok {
			s.sum += f
			s.count++
		}
	case aggMin:
		if !val.IsNull() && (s.min.IsNull() || vm.CompareValues(val, s.min) < 0) {
			s.min = val
		}
	case aggMax:
		if !val.IsNull() && (s.max.IsNull() || vm.CompareValues(val, s.max) > 0) {
			s.max = val
		}
	case aggDC:
		if !val.IsNull() {
			s.values[val.String()] = true
		}
	case aggValues:
		if !val.IsNull() {
			str := val.String()
			if !s.values[str] {
				s.values[str] = true
				s.all = append(s.all, str)
			}
		}
	case aggList:
		if !val.IsNull() {
			s.all = append(s.all, val.String())
		}
	case aggPerc:
		if f, ok := vm.ValueToFloat(val); ok {
			if s.tdigest == nil {
				s.tdigest = NewTDigest(defaultTDigestCompression)
			}
			s.tdigest.Add(f)
			if len(s.all) < maxValuesPerGroup {
				s.all = append(s.all, f)
			}
		}
	}
}

func finalizeEventStatsAgg(s *aggState, agg AggFunc) event.Value {
	switch strings.ToLower(agg.Name) {
	case aggCount:
		return event.IntValue(s.count)
	case aggSum:
		return event.FloatValue(s.sum)
	case aggAvg:
		if s.count == 0 {
			return event.NullValue()
		}

		return event.FloatValue(s.sum / float64(s.count))
	case aggMin:
		return s.min
	case aggMax:
		return s.max
	case aggDC:
		return event.IntValue(int64(len(s.values)))
	case aggValues, aggList:
		return event.StringValue(joinAllStrings(s.all, "|||"))
	case aggPerc:
		return finalizePercentile(s, agg.Quantile)
	}

	return event.NullValue()
}
