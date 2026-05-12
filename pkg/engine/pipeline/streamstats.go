package pipeline

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/memgov"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

// estimatedRingBufferOverhead is the estimated per-group memory overhead for a
// ring buffer entry in the streamstats group map: ringBuffer struct (~64B) +
// map entry overhead (~48B).
const estimatedRingBufferOverhead int64 = 112

// estimatedRingBufferSlotBytes tracks the pre-allocated []map slot in bounded
// ring buffers. Row payload bytes are accounted when rows are inserted.
const estimatedRingBufferSlotBytes int64 = 8

// StreamStatsIterator implements rolling window aggregation with O(N) incremental updates.
type StreamStatsIterator struct {
	child     Iterator
	aggs      []AggFunc
	groupBy   []string
	window    int
	current   bool
	ringBufs  map[string]*ringBuffer
	acct      memgov.MemoryAccount          // per-operator memory tracking
	running   map[string][]*runningAggState // per-group, per-aggregate running state
	storeRows bool                          // true when window rows are needed for eviction/values()
}

// runningAggState maintains incremental aggregate state for O(1) per-row updates.
type runningAggState struct {
	sum    float64
	count  int64
	minVal event.Value
	maxVal event.Value
	freq   map[string]int64 // for dc: value → frequency
}

type ringBuffer struct {
	buf      []map[string]event.Value
	bytes    []int64
	pos      int
	count    int
	capacity int // 0 means unlimited (use append-only mode)
}

func newRingBuffer(size int) *ringBuffer {
	if size >= math.MaxInt32/2 {
		// Unlimited window: use append-only dynamic slice
		return &ringBuffer{capacity: 0}
	}

	return &ringBuffer{
		buf:      make([]map[string]event.Value, size),
		bytes:    make([]int64, size),
		capacity: size,
	}
}

func (r *ringBuffer) add(row map[string]event.Value, rowBytes int64) {
	if r.capacity == 0 {
		// Unlimited: just append
		r.buf = append(r.buf, row)
		r.bytes = append(r.bytes, rowBytes)
		r.count = len(r.buf)

		return
	}
	r.buf[r.pos] = row
	r.bytes[r.pos] = rowBytes
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
		child:     child,
		aggs:      aggs,
		groupBy:   groupBy,
		window:    window,
		current:   current,
		ringBufs:  make(map[string]*ringBuffer),
		acct:      memgov.NopAccount(),
		running:   make(map[string][]*runningAggState),
		storeRows: streamStatsNeedsRows(aggs, window),
	}
}

// NewStreamStatsIteratorWithBudget creates a streaming rolling window aggregation
// with memory budget tracking. The account tracks ring buffer allocations for
// observability — streamstats has no spill support, so tracking is best-effort.
func NewStreamStatsIteratorWithBudget(child Iterator, aggs []AggFunc, groupBy []string,
	window int, current bool, acct memgov.MemoryAccount) *StreamStatsIterator {
	s := NewStreamStatsIterator(child, aggs, groupBy, window, current)
	s.acct = memgov.EnsureAccount(acct)

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
			if err := s.acct.Grow(estimatedRingBufferOverhead + int64(len(key))); err != nil {
				return nil, fmt.Errorf("streamstats: memory budget exceeded (ring buffer alloc): %w", err)
			}
			// Pre-allocated slots for bounded windows.
			if rb.capacity > 0 {
				if err := s.acct.Grow(int64(rb.capacity) * estimatedRingBufferSlotBytes); err != nil {
					return nil, fmt.Errorf("streamstats: memory budget exceeded (window pre-alloc): %w", err)
				}
			}
			// Initialize running aggregate state for this group.
			states := make([]*runningAggState, len(s.aggs))
			for j, agg := range s.aggs {
				states[j] = newRunningAggState(agg.Name)
			}
			s.running[key] = states
		}
		states := s.running[key]

		rowBytes := int64(0)
		if s.storeRows {
			rowBytes = EstimateRowBytes(row)
		}

		// Determine which row is being evicted (if window is full).
		var evictedRow map[string]event.Value
		var evictedBytes int64
		if s.current {
			if s.storeRows && rb.capacity > 0 && rb.count >= rb.capacity {
				// Window is full: the oldest entry will be overwritten.
				evictedRow = rb.oldest()
				evictedBytes = rb.nextEvictedBytes()
			}
			if s.storeRows && rowBytes > evictedBytes {
				if err := s.acct.Grow(rowBytes - evictedBytes); err != nil {
					return nil, fmt.Errorf("streamstats: memory budget exceeded (current window grow): %w", err)
				}
			}
			if s.storeRows {
				rb.add(row, rowBytes)
			}
			if s.storeRows && evictedBytes > rowBytes {
				s.acct.Shrink(evictedBytes - rowBytes)
			}
		}

		// Incremental aggregate update: add new row, evict old if applicable.
		for j, agg := range s.aggs {
			if evictedRow != nil {
				removeValueFromRunning(states[j], agg, evictedRow)
			}
			addValueToRunning(states[j], agg, row)
			var val event.Value
			if strings.EqualFold(agg.Name, aggValues) || strings.EqualFold(agg.Name, aggList) {
				// Values/list aggregates require full scan for order-sensitive output.
				val = s.computeAgg(agg, rb.items())
			} else {
				val = readRunningAgg(states[j], agg, rb)
			}
			row[agg.Alias] = val
			if _, exists := batch.Columns[agg.Alias]; !exists {
				batch.Columns[agg.Alias] = make([]event.Value, batch.Len)
			}
			batch.Columns[agg.Alias][i] = val
		}

		if !s.current {
			// Trailing window: add after computing.
			var willEvict map[string]event.Value
			var willEvictBytes int64
			if s.storeRows && rb.capacity > 0 && rb.count >= rb.capacity {
				willEvict = rb.oldest()
				willEvictBytes = rb.nextEvictedBytes()
			}
			if s.storeRows && rowBytes > willEvictBytes {
				if err := s.acct.Grow(rowBytes - willEvictBytes); err != nil {
					return nil, fmt.Errorf("streamstats: memory budget exceeded (trailing window grow): %w", err)
				}
			}
			if s.storeRows {
				rb.add(row, rowBytes)
			}
			if s.storeRows && willEvictBytes > rowBytes {
				s.acct.Shrink(willEvictBytes - rowBytes)
			}
			for j, agg := range s.aggs {
				if willEvict != nil {
					removeValueFromRunning(states[j], agg, willEvict)
				}
				addValueToRunning(states[j], agg, row)
			}
		}
	}

	return batch, nil
}

func streamStatsNeedsRows(aggs []AggFunc, window int) bool {
	if window < math.MaxInt32/2 {
		return true
	}
	for _, agg := range aggs {
		if strings.EqualFold(agg.Name, aggValues) || strings.EqualFold(agg.Name, aggList) {
			return true
		}
	}

	return false
}

func (s *StreamStatsIterator) Close() error {
	s.acct.Close()
	s.ringBufs = nil
	s.running = nil

	return s.child.Close()
}

func (s *StreamStatsIterator) Schema() []FieldInfo { return s.child.Schema() }

// oldest returns the oldest entry in the ring buffer without removing it.
// Returns nil if the buffer is empty.
func (r *ringBuffer) oldest() map[string]event.Value {
	if r.count == 0 {
		return nil
	}
	if r.capacity == 0 {
		return r.buf[0]
	}
	start := r.pos - r.count
	if start < 0 {
		start += len(r.buf)
	}

	return r.buf[start]
}

func (r *ringBuffer) nextEvictedBytes() int64 {
	if r.count == 0 {
		return 0
	}
	if r.capacity == 0 {
		return r.bytes[0]
	}
	if r.count < r.capacity {
		return 0
	}

	return r.bytes[r.pos]
}

// newRunningAggState creates an initialized running state for the given aggregate.
func newRunningAggState(aggName string) *runningAggState {
	st := &runningAggState{}
	if strings.EqualFold(aggName, aggDC) || strings.EqualFold(aggName, aggEstDCE) {
		st.freq = make(map[string]int64)
	}

	return st
}

// addValueToRunning incorporates a new row's field value into the running aggregate.
func addValueToRunning(st *runningAggState, agg AggFunc, row map[string]event.Value) {
	switch strings.ToLower(agg.Name) {
	case aggCount:
		if agg.Field == "" {
			st.count++
		} else if v, ok := row[agg.Field]; ok && !v.IsNull() {
			st.count++
		}
	case aggSum:
		if v, ok := row[agg.Field]; ok {
			if f, fok := vm.ValueToFloat(v); fok {
				st.sum += f
				st.count++
			}
		}
	case aggSumSq:
		if v, ok := row[agg.Field]; ok {
			if f, fok := vm.ValueToFloat(v); fok {
				st.sum += f * f
				st.count++
			}
		}
	case aggAvg:
		if v, ok := row[agg.Field]; ok {
			if f, fok := vm.ValueToFloat(v); fok {
				st.sum += f
				st.count++
			}
		}
	case aggMin:
		if v, ok := row[agg.Field]; ok && !v.IsNull() {
			if st.minVal.IsNull() || vm.CompareValues(v, st.minVal) < 0 {
				st.minVal = v
			}
			st.count++
		}
	case aggMax:
		if v, ok := row[agg.Field]; ok && !v.IsNull() {
			if st.maxVal.IsNull() || vm.CompareValues(v, st.maxVal) > 0 {
				st.maxVal = v
			}
			st.count++
		}
	case aggDC, aggEstDCE:
		if v, ok := row[agg.Field]; ok && !v.IsNull() {
			st.freq[v.String()]++
			st.count++
		}
	case aggValues, aggList:
		// Values/list aggregates still require full scan for correctness.
		// Fall through to readRunningAgg which does the scan.
		st.count++
	}
}

// removeValueFromRunning removes a row's field value from the running aggregate.
func removeValueFromRunning(st *runningAggState, agg AggFunc, row map[string]event.Value) {
	switch strings.ToLower(agg.Name) {
	case aggCount:
		if agg.Field == "" {
			st.count--
		} else if v, ok := row[agg.Field]; ok && !v.IsNull() {
			st.count--
		}
	case aggSum:
		if v, ok := row[agg.Field]; ok {
			if f, fok := vm.ValueToFloat(v); fok {
				st.sum -= f
				st.count--
			}
		}
	case aggSumSq:
		if v, ok := row[agg.Field]; ok {
			if f, fok := vm.ValueToFloat(v); fok {
				st.sum -= f * f
				st.count--
			}
		}
	case aggAvg:
		if v, ok := row[agg.Field]; ok {
			if f, fok := vm.ValueToFloat(v); fok {
				st.sum -= f
				st.count--
			}
		}
	case aggMin:
		if v, ok := row[agg.Field]; ok && !v.IsNull() {
			st.count--
			// If we removed the current min, invalidate so next readRunningAgg recomputes.
			if !st.minVal.IsNull() && vm.CompareValues(v, st.minVal) == 0 {
				st.minVal = event.Value{} // invalidate
			}
		}
	case aggMax:
		if v, ok := row[agg.Field]; ok && !v.IsNull() {
			st.count--
			if !st.maxVal.IsNull() && vm.CompareValues(v, st.maxVal) == 0 {
				st.maxVal = event.Value{} // invalidate
			}
		}
	case aggDC, aggEstDCE:
		if v, ok := row[agg.Field]; ok && !v.IsNull() {
			key := v.String()
			st.freq[key]--
			if st.freq[key] <= 0 {
				delete(st.freq, key)
			}
			st.count--
		}
	case aggValues, aggList:
		st.count--
	}
}

// readRunningAgg returns the current aggregate value from the running state.
// For min/max with invalidated state (after eviction of the extremum), rescans
// the ring buffer to find the new extremum and updates the running state.
func readRunningAgg(st *runningAggState, agg AggFunc, rb *ringBuffer) event.Value {
	switch strings.ToLower(agg.Name) {
	case aggCount:
		return event.IntValue(st.count)
	case aggSum:
		return event.FloatValue(st.sum)
	case aggSumSq:
		return event.FloatValue(st.sum)
	case aggAvg:
		if st.count == 0 {
			return event.NullValue()
		}

		return event.FloatValue(st.sum / float64(st.count))
	case aggMin:
		if st.minVal.IsNull() && st.count > 0 {
			// Min was evicted — rescan window to find new minimum.
			for _, item := range rb.items() {
				if v, ok := item[agg.Field]; ok && !v.IsNull() {
					if st.minVal.IsNull() || vm.CompareValues(v, st.minVal) < 0 {
						st.minVal = v
					}
				}
			}
		}

		return st.minVal
	case aggMax:
		if st.maxVal.IsNull() && st.count > 0 {
			// Max was evicted — rescan window to find new maximum.
			for _, item := range rb.items() {
				if v, ok := item[agg.Field]; ok && !v.IsNull() {
					if st.maxVal.IsNull() || vm.CompareValues(v, st.maxVal) > 0 {
						st.maxVal = v
					}
				}
			}
		}

		return st.maxVal
	case aggDC:
		return event.IntValue(int64(len(st.freq)))
	case aggEstDCE:
		return event.FloatValue(0)
	}

	return event.NullValue()
}

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
	case aggSumSq:
		sum := 0.0
		for _, item := range items {
			if v, ok := item[agg.Field]; ok {
				if f, fok := vm.ValueToFloat(v); fok {
					sum += f * f
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
	case aggDC, aggEstDCE:
		seen := make(map[string]bool)
		for _, item := range items {
			if v, ok := item[agg.Field]; ok && !v.IsNull() {
				seen[v.String()] = true
			}
		}
		if strings.EqualFold(agg.Name, aggEstDCE) {
			return event.FloatValue(0)
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
	case aggList:
		var vals []string
		for _, item := range items {
			if v, ok := item[agg.Field]; ok && !v.IsNull() {
				vals = append(vals, v.String())
			}
		}

		return event.StringValue(strings.Join(vals, "|||"))
	}

	return event.NullValue()
}
