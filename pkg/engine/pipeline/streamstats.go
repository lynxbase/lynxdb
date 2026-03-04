package pipeline

import (
	"context"
	"math"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/stats"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

// estimatedRingBufferOverhead is the estimated per-group memory overhead for a
// ring buffer entry in the streamstats group map: ringBuffer struct (~64B) +
// map entry overhead (~48B).
const estimatedRingBufferOverhead int64 = 112

// StreamStatsIterator implements rolling window aggregation.
type StreamStatsIterator struct {
	child    Iterator
	aggs     []AggFunc
	groupBy  []string
	window   int
	current  bool
	ringBufs map[string]*ringBuffer
	acct     stats.MemoryAccount // per-operator memory tracking
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
		acct:     stats.NopAccount(),
	}
}

// NewStreamStatsIteratorWithBudget creates a streaming rolling window aggregation
// with memory budget tracking. The account tracks ring buffer allocations for
// observability — streamstats has no spill support, so tracking is best-effort.
func NewStreamStatsIteratorWithBudget(child Iterator, aggs []AggFunc, groupBy []string,
	window int, current bool, acct stats.MemoryAccount) *StreamStatsIterator {
	s := NewStreamStatsIterator(child, aggs, groupBy, window, current)
	s.acct = stats.EnsureAccount(acct)

	return s
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
			// Track ring buffer struct + map entry + key string overhead.
			_ = s.acct.Grow(estimatedRingBufferOverhead + int64(len(key)))
			// Pre-allocated slots for bounded windows.
			if rb.capacity > 0 {
				_ = s.acct.Grow(int64(rb.capacity) * estimatedRowBytes)
			}
		}

		if s.current {
			// For unlimited window, each append grows memory; for bounded
			// window not yet full, track the new slot. At capacity, the add
			// replaces an existing slot — memory-neutral.
			if rb.capacity == 0 || rb.count < rb.capacity {
				_ = s.acct.Grow(estimatedRowBytes)
			}
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
			if rb.capacity == 0 || rb.count < rb.capacity {
				_ = s.acct.Grow(estimatedRowBytes)
			}
			rb.add(row)
		}
	}

	return batch, nil
}

func (s *StreamStatsIterator) Close() error {
	s.acct.Close()
	s.ringBufs = nil

	return s.child.Close()
}

func (s *StreamStatsIterator) Schema() []FieldInfo { return s.child.Schema() }

// groupKey builds a composite key from the BY-clause fields of a row.
//
// Uses null byte (\x00) as separator instead of '|' to avoid collisions when
// field values contain the separator character. Each field is prefixed with a
// presence marker: \x01 for present values, \x00 for null/missing. This
// ensures that ("a", null) and (null, "a") produce distinct keys, and that
// values like "x|y" in a single field don't collide with "x" and "y" in two
// separate fields.
func (s *StreamStatsIterator) groupKey(row map[string]event.Value) string {
	if len(s.groupBy) == 0 {
		return ""
	}
	var sb strings.Builder
	for i, g := range s.groupBy {
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
