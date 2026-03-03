package pipeline

import (
	"context"
	"math"
	"strings"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
	"github.com/OrlovEvgeny/Lynxdb/pkg/vm"
)

// StreamStatsIterator implements rolling window aggregation.
type StreamStatsIterator struct {
	child    Iterator
	aggs     []AggFunc
	groupBy  []string
	window   int
	current  bool
	ringBufs map[string]*ringBuffer
}

type ringBuffer struct {
	buf      []map[string]event.Value
	pos      int
	count    int
	capacity int // 0 means unlimited (use append-only mode)
}

func newRingBuffer(size int) *ringBuffer {
	if size >= math.MaxInt32/2 {
		// Unlimited window: use append-only dynamic slice
		return &ringBuffer{capacity: 0}
	}

	return &ringBuffer{buf: make([]map[string]event.Value, size), capacity: size}
}

func (r *ringBuffer) add(row map[string]event.Value) {
	if r.capacity == 0 {
		// Unlimited: just append
		r.buf = append(r.buf, row)
		r.count = len(r.buf)

		return
	}
	r.buf[r.pos] = row
	r.pos = (r.pos + 1) % len(r.buf)
	if r.count < len(r.buf) {
		r.count++
	}
}

func (r *ringBuffer) items() []map[string]event.Value {
	if r.capacity == 0 {
		// Unlimited: return the whole slice
		return r.buf
	}
	result := make([]map[string]event.Value, 0, r.count)
	start := r.pos - r.count
	if start < 0 {
		start += len(r.buf)
	}
	for i := 0; i < r.count; i++ {
		idx := (start + i) % len(r.buf)
		result = append(result, r.buf[idx])
	}

	return result
}

// NewStreamStatsIterator creates a streaming rolling window aggregation.
func NewStreamStatsIterator(child Iterator, aggs []AggFunc, groupBy []string, window int, current bool) *StreamStatsIterator {
	if window <= 0 {
		window = math.MaxInt32
	}

	return &StreamStatsIterator{
		child:    child,
		aggs:     aggs,
		groupBy:  groupBy,
		window:   window,
		current:  current,
		ringBufs: make(map[string]*ringBuffer),
	}
}

func (s *StreamStatsIterator) Init(ctx context.Context) error {
	return s.child.Init(ctx)
}

func (s *StreamStatsIterator) Next(ctx context.Context) (*Batch, error) {
	batch, err := s.child.Next(ctx)
	if batch == nil || err != nil {
		return nil, err
	}

	for i := 0; i < batch.Len; i++ {
		row := batch.Row(i)
		key := s.groupKey(row)
		rb, ok := s.ringBufs[key]
		if !ok {
			rb = newRingBuffer(s.window)
			s.ringBufs[key] = rb
		}

		if s.current {
			rb.add(row)
		}

		// Compute aggregates over window
		items := rb.items()
		for _, agg := range s.aggs {
			val := s.computeAgg(agg, items)
			row[agg.Alias] = val
			if _, exists := batch.Columns[agg.Alias]; !exists {
				batch.Columns[agg.Alias] = make([]event.Value, batch.Len)
			}
			batch.Columns[agg.Alias][i] = val
		}

		if !s.current {
			rb.add(row)
		}
	}

	return batch, nil
}

func (s *StreamStatsIterator) Close() error { return s.child.Close() }

func (s *StreamStatsIterator) Schema() []FieldInfo { return s.child.Schema() }

func (s *StreamStatsIterator) groupKey(row map[string]event.Value) string {
	if len(s.groupBy) == 0 {
		return ""
	}
	var sb strings.Builder
	for i, g := range s.groupBy {
		if i > 0 {
			sb.WriteByte('|')
		}
		if v, ok := row[g]; ok {
			sb.WriteString(v.String())
		}
	}

	return sb.String()
}

func (s *StreamStatsIterator) computeAgg(agg AggFunc, items []map[string]event.Value) event.Value {
	switch strings.ToLower(agg.Name) {
	case aggCount:
		count := int64(0)
		for _, item := range items {
			if v, ok := item[agg.Field]; ok && !v.IsNull() {
				count++
			} else if agg.Field == "" {
				count++
			}
		}

		return event.IntValue(count)
	case aggSum:
		sum := 0.0
		for _, item := range items {
			if v, ok := item[agg.Field]; ok {
				if f, fok := vm.ValueToFloat(v); fok {
					sum += f
				}
			}
		}

		return event.FloatValue(sum)
	case aggAvg:
		sum, count := 0.0, 0
		for _, item := range items {
			if v, ok := item[agg.Field]; ok {
				if f, fok := vm.ValueToFloat(v); fok {
					sum += f
					count++
				}
			}
		}
		if count == 0 {
			return event.NullValue()
		}

		return event.FloatValue(sum / float64(count))
	case aggMin:
		var minVal event.Value
		for _, item := range items {
			if v, ok := item[agg.Field]; ok && !v.IsNull() {
				if minVal.IsNull() || vm.CompareValues(v, minVal) < 0 {
					minVal = v
				}
			}
		}

		return minVal
	case aggMax:
		var maxVal event.Value
		for _, item := range items {
			if v, ok := item[agg.Field]; ok && !v.IsNull() {
				if maxVal.IsNull() || vm.CompareValues(v, maxVal) > 0 {
					maxVal = v
				}
			}
		}

		return maxVal
	case "dc":
		seen := make(map[string]bool)
		for _, item := range items {
			if v, ok := item[agg.Field]; ok && !v.IsNull() {
				seen[v.String()] = true
			}
		}

		return event.IntValue(int64(len(seen)))
	case aggValues:
		var vals []string
		seen := make(map[string]bool)
		for _, item := range items {
			if v, ok := item[agg.Field]; ok && !v.IsNull() {
				s := v.String()
				if !seen[s] {
					seen[s] = true
					vals = append(vals, s)
				}
			}
		}

		return event.StringValue(strings.Join(vals, "|||"))
	}

	return event.NullValue()
}
