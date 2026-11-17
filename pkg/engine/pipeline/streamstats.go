package pipeline

import (
	"context"
	"math"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

// StreamStatsIterator implements rolling window aggregation with O(N) incremental updates.
type StreamStatsIterator struct {
	child      Iterator
	aggs       []AggFunc
	groupBy    []string
	window     int
	timeWindow time.Duration
	current    bool
	ringBufs   map[string]*ringBuffer
	running    map[string][]*runningAggState // per-group, per-aggregate running state
	storeRows  bool                          // true when window rows are needed for eviction/values()
	output     []map[string]event.Value      // materialized output for lookahead functions such as lead()
	offset     int
}

// runningAggState maintains incremental aggregate state for O(1) per-row updates.
type runningAggState struct {
	sum       float64
	count     int64
	rowNumber int64
	minVal    event.Value
	maxVal    event.Value
	freq      map[string]int64 // for dc: value → frequency
	weightSum float64
	sumSq     float64
	sumY2     float64
	sumXY     float64
	objectSum map[string]float64
	objectN   map[string]int64
	sumCube   float64
	sumFourth float64
	ema       float64
	emaSet    bool
	rankRows  int64
	rank      int64
	denseRank int64
	rankKey   []event.Value
	rankSet   bool
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

	return &ringBuffer{
		buf:      make([]map[string]event.Value, size),
		capacity: size,
	}
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

// NewStreamStatsIteratorWithDuration creates a streaming rolling window
// aggregation with either a row-count window or a _time-based duration window.
func NewStreamStatsIteratorWithDuration(
	child Iterator,
	aggs []AggFunc,
	groupBy []string,
	window int,
	timeWindow time.Duration,
	current bool,
) *StreamStatsIterator {
	if window <= 0 {
		window = math.MaxInt32
	}
	if timeWindow > 0 {
		window = math.MaxInt32
	}

	return &StreamStatsIterator{
		child:      child,
		aggs:       aggs,
		groupBy:    groupBy,
		window:     window,
		timeWindow: timeWindow,
		current:    current,
		ringBufs:   make(map[string]*ringBuffer),
		running:    make(map[string][]*runningAggState),
		storeRows:  timeWindow > 0 || streamStatsNeedsRows(aggs, window),
	}
}

func (s *StreamStatsIterator) Init(ctx context.Context) error {
	if streamStatsHasLead(s.aggs) {
		rows, err := s.collectChild(ctx)
		if err != nil {
			return err
		}
		s.output = s.computeMaterialized(rows)

		return nil
	}

	return s.child.Init(ctx)
}

func (s *StreamStatsIterator) Next(ctx context.Context) (*Batch, error) {
	if s.output != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if s.offset >= len(s.output) {
			return nil, nil
		}
		end := s.offset + DefaultBatchSize
		if end > len(s.output) {
			end = len(s.output)
		}
		batch := BatchFromRows(s.output[s.offset:end])
		s.offset = end

		return batch, nil
	}

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
			// Initialize running aggregate state for this group.
			states := make([]*runningAggState, len(s.aggs))
			for j, agg := range s.aggs {
				states[j] = newRunningAggState(agg.Name)
			}
			s.running[key] = states
		}
		states := s.running[key]

		if s.current {
			if s.timeWindow > 0 {
				s.evictExpiredRows(row, rb, states)
			}
			// Determine which row is being evicted (if window is full).
			var evictedRow map[string]event.Value
			if s.timeWindow == 0 && s.storeRows && rb.capacity > 0 && rb.count >= rb.capacity {
				// Window is full: the oldest entry will be overwritten.
				evictedRow = rb.oldest()
			}
			if s.storeRows {
				rb.add(row)
			}
			// Current window: add before computing.
			for j, agg := range s.aggs {
				if evictedRow != nil {
					removeValueFromRunning(states[j], agg, evictedRow)
				}
				addValueToRunning(states[j], agg, row)
				s.writeAggValue(batch, row, i, states[j], agg, rb)
			}
		} else {
			if s.timeWindow > 0 {
				s.evictExpiredRows(row, rb, states)
			}
			// Trailing window: compute first, then add the current row.
			for j, agg := range s.aggs {
				s.writeAggValue(batch, row, i, states[j], agg, rb)
			}

			var willEvict map[string]event.Value
			if s.timeWindow == 0 && s.storeRows && rb.capacity > 0 && rb.count >= rb.capacity {
				willEvict = rb.oldest()
			}
			if s.storeRows {
				rb.add(row)
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

func (s *StreamStatsIterator) evictExpiredRows(
	current map[string]event.Value,
	rb *ringBuffer,
	states []*runningAggState,
) {
	ts, ok := streamStatsRowTime(current)
	if !ok {
		return
	}
	for _, row := range rb.evictBefore(ts.Add(-s.timeWindow)) {
		for j, agg := range s.aggs {
			removeValueFromRunning(states[j], agg, row)
		}
	}
}

func streamStatsHasLead(aggs []AggFunc) bool {
	for _, agg := range aggs {
		if strings.EqualFold(agg.Name, aggLead) {
			return true
		}
	}

	return false
}

func (s *StreamStatsIterator) collectChild(ctx context.Context) ([]map[string]event.Value, error) {
	if err := s.child.Init(ctx); err != nil {
		return nil, err
	}
	var rows []map[string]event.Value
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch, err := s.child.Next(ctx)
		if err != nil {
			return nil, err
		}
		if batch == nil {
			break
		}
		for i := 0; i < batch.Len; i++ {
			rows = append(rows, batch.Row(i))
		}
	}

	return rows, nil
}

func (s *StreamStatsIterator) computeMaterialized(rows []map[string]event.Value) []map[string]event.Value {
	groupIndexes := make(map[string][]int)
	groupOrder := make([]string, 0)
	for i, row := range rows {
		key := s.groupKey(row)
		if _, ok := groupIndexes[key]; !ok {
			groupOrder = append(groupOrder, key)
		}
		groupIndexes[key] = append(groupIndexes[key], i)
	}
	for _, key := range groupOrder {
		indexes := groupIndexes[key]
		for groupPos, rowIndex := range indexes {
			row := rows[rowIndex]
			windowRows := s.materializedWindowRows(rows, indexes, groupPos)
			for _, agg := range s.aggs {
				row[agg.Alias] = s.materializedAggValue(rows, indexes, groupPos, windowRows, agg)
			}
		}
	}

	return rows
}

func (s *StreamStatsIterator) materializedWindowRows(
	rows []map[string]event.Value,
	indexes []int,
	groupPos int,
) []map[string]event.Value {
	end := groupPos
	if s.current {
		end++
	}
	if end < 0 {
		end = 0
	}
	if s.timeWindow > 0 {
		currentTime, ok := streamStatsRowTime(rows[indexes[groupPos]])
		if ok {
			cutoff := currentTime.Add(-s.timeWindow)
			result := make([]map[string]event.Value, 0, end)
			for _, rowIndex := range indexes[:end] {
				row := rows[rowIndex]
				rowTime, hasTime := streamStatsRowTime(row)
				if !hasTime || !rowTime.Before(cutoff) {
					result = append(result, row)
				}
			}

			return result
		}
	}
	start := 0
	if s.window < math.MaxInt32/2 && end > s.window {
		start = end - s.window
	}
	result := make([]map[string]event.Value, 0, end-start)
	for _, rowIndex := range indexes[start:end] {
		result = append(result, rows[rowIndex])
	}

	return result
}

func (s *StreamStatsIterator) materializedAggValue(
	rows []map[string]event.Value,
	indexes []int,
	groupPos int,
	windowRows []map[string]event.Value,
	agg AggFunc,
) event.Value {
	switch strings.ToLower(agg.Name) {
	case aggLead:
		n := agg.Window
		if n <= 0 {
			n = 1
		}
		target := groupPos + n
		if target >= len(indexes) {
			return event.NullValue()
		}
		v, ok := rows[indexes[target]][agg.Field]
		if !ok {
			return event.NullValue()
		}
		return v
	case aggLag:
		n := agg.Window
		if n <= 0 {
			n = 1
		}
		target := groupPos - n
		if target < 0 {
			return event.NullValue()
		}
		v, ok := rows[indexes[target]][agg.Field]
		if !ok {
			return event.NullValue()
		}
		return v
	case aggRowNum:
		return event.IntValue(int64(groupPos + 1))
	case aggRank:
		return materializedRank(rows, indexes, groupPos, agg, false)
	case aggDense:
		return materializedRank(rows, indexes, groupPos, agg, true)
	case aggRunSum:
		return materializedSum(rows, indexes[:groupPos+1], agg.Field)
	case aggMovAvg:
		return materializedMovingAvg(rows, indexes, groupPos, agg)
	case aggDelta:
		return materializedDelta(rows, indexes, groupPos, agg.Field)
	case aggEMA:
		return materializedEMA(rows, indexes, groupPos, agg, s.current)
	default:
		return s.computeAgg(agg, windowRows)
	}
}

func materializedSum(rows []map[string]event.Value, indexes []int, field string) event.Value {
	sum := 0.0
	for _, rowIndex := range indexes {
		if v, ok := rows[rowIndex][field]; ok {
			if f, fok := vm.ValueToFloat(v); fok {
				sum += f
			}
		}
	}

	return event.FloatValue(sum)
}

func materializedRank(
	rows []map[string]event.Value,
	indexes []int,
	groupPos int,
	agg AggFunc,
	dense bool,
) event.Value {
	var previous []event.Value
	var rank int64
	var denseRank int64
	for i, rowIndex := range indexes[:groupPos+1] {
		key := rankKey(rows[rowIndex], agg.RankFields)
		if i == 0 || !sameRankKey(previous, key) {
			rank = int64(i + 1)
			denseRank++
			previous = key
		}
	}
	if dense {
		return event.IntValue(denseRank)
	}
	return event.IntValue(rank)
}

func materializedMovingAvg(
	rows []map[string]event.Value,
	indexes []int,
	groupPos int,
	agg AggFunc,
) event.Value {
	n := agg.Window
	if n <= 0 {
		return event.NullValue()
	}
	start := groupPos - n + 1
	if start < 0 {
		start = 0
	}
	sum := 0.0
	count := 0
	for _, rowIndex := range indexes[start : groupPos+1] {
		if v, ok := rows[rowIndex][agg.Field]; ok {
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
}

func materializedDelta(rows []map[string]event.Value, indexes []int, groupPos int, field string) event.Value {
	if groupPos == 0 {
		return event.NullValue()
	}
	current, ok := rows[indexes[groupPos]][field]
	if !ok {
		return event.NullValue()
	}
	currentNum, ok := vm.ValueToFloat(current)
	if !ok {
		return event.NullValue()
	}
	previous, ok := rows[indexes[groupPos-1]][field]
	if !ok {
		return event.NullValue()
	}
	previousNum, ok := vm.ValueToFloat(previous)
	if !ok {
		return event.NullValue()
	}

	return event.FloatValue(currentNum - previousNum)
}

func materializedEMA(
	rows []map[string]event.Value,
	indexes []int,
	groupPos int,
	agg AggFunc,
	current bool,
) event.Value {
	if agg.Window <= 0 {
		return event.NullValue()
	}
	end := groupPos
	if current {
		end++
	}
	ema, ok := computeEMA(rows, indexes[:end], agg.Field, agg.Window)
	if !ok {
		return event.NullValue()
	}

	return event.FloatValue(ema)
}

func computeEMA(rows []map[string]event.Value, indexes []int, field string, n int) (float64, bool) {
	alpha := 2.0 / float64(n+1)
	ema := 0.0
	seen := false
	for _, rowIndex := range indexes {
		v, ok := rows[rowIndex][field]
		if !ok {
			continue
		}
		f, ok := vm.ValueToFloat(v)
		if !ok {
			continue
		}
		if !seen {
			ema = f
			seen = true
			continue
		}
		ema = alpha*f + (1-alpha)*ema
	}

	return ema, seen
}

func (s *StreamStatsIterator) writeAggValue(
	batch *Batch,
	row map[string]event.Value,
	rowIndex int,
	st *runningAggState,
	agg AggFunc,
	rb *ringBuffer,
) {
	var val event.Value
	switch strings.ToLower(agg.Name) {
	case aggValues, aggList, aggMAD, aggTopKW, aggPerc, aggPerc25, aggPerc50, aggPerc75, aggPerc90, aggPerc95, aggPerc99, aggPercW:
		// These aggregates require full window scans for ordered or distributional output.
		val = s.computeAgg(agg, rb.items())
	case aggRowNum:
		rowNumber := st.rowNumber
		if !s.current {
			rowNumber++
		}
		val = event.IntValue(rowNumber)
	case aggRank:
		val = readRank(row, st, agg, false)
	case aggDense:
		val = readRank(row, st, agg, true)
	case aggLag:
		val = s.readLag(agg, rb)
	case aggMovAvg:
		val = s.readMovingAvg(agg, rb)
	case aggDelta:
		val = s.readDelta(row, agg, rb)
	case aggEMA:
		val = readRunningEMA(st)
	default:
		val = readRunningAgg(st, agg, rb)
	}
	row[agg.Alias] = val
	if _, exists := batch.Columns[agg.Alias]; !exists {
		batch.Columns[agg.Alias] = make([]event.Value, batch.Len)
	}
	batch.Columns[agg.Alias][rowIndex] = val
}

func readRank(row map[string]event.Value, st *runningAggState, agg AggFunc, dense bool) event.Value {
	st.rankRows++
	key := rankKey(row, agg.RankFields)
	if !st.rankSet || !sameRankKey(st.rankKey, key) {
		st.rank = st.rankRows
		st.denseRank++
		st.rankKey = key
		st.rankSet = true
	}
	if dense {
		return event.IntValue(st.denseRank)
	}
	return event.IntValue(st.rank)
}

func rankKey(row map[string]event.Value, fields []string) []event.Value {
	key := make([]event.Value, len(fields))
	for i, field := range fields {
		key[i] = row[field]
	}
	return key
}

func sameRankKey(a, b []event.Value) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if vm.CompareValues(a[i], b[i]) != 0 {
			return false
		}
	}
	return true
}

func (s *StreamStatsIterator) readLag(agg AggFunc, rb *ringBuffer) event.Value {
	n := agg.Window
	if n <= 0 {
		n = 1
	}
	items := rb.items()
	idx := len(items) - n
	if s.current {
		idx--
	}
	if idx < 0 || idx >= len(items) {
		return event.NullValue()
	}
	v, ok := items[idx][agg.Field]
	if !ok {
		return event.NullValue()
	}

	return v
}

func (s *StreamStatsIterator) readMovingAvg(agg AggFunc, rb *ringBuffer) event.Value {
	n := agg.Window
	if n <= 0 {
		return event.NullValue()
	}
	items := rb.items()
	end := len(items)
	if end == 0 {
		return event.NullValue()
	}
	start := end - n
	if start < 0 {
		start = 0
	}
	sum := 0.0
	count := 0
	for _, item := range items[start:end] {
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
}

func (s *StreamStatsIterator) readDelta(row map[string]event.Value, agg AggFunc, rb *ringBuffer) event.Value {
	current, ok := row[agg.Field]
	if !ok {
		return event.NullValue()
	}
	currentNum, ok := vm.ValueToFloat(current)
	if !ok {
		return event.NullValue()
	}
	items := rb.items()
	idx := len(items) - 1
	if s.current {
		idx--
	}
	if idx < 0 || idx >= len(items) {
		return event.NullValue()
	}
	previous, ok := items[idx][agg.Field]
	if !ok {
		return event.NullValue()
	}
	previousNum, ok := vm.ValueToFloat(previous)
	if !ok {
		return event.NullValue()
	}

	return event.FloatValue(currentNum - previousNum)
}

func streamStatsNeedsRows(aggs []AggFunc, window int) bool {
	if window < math.MaxInt32/2 {
		return true
	}
	for _, agg := range aggs {
		switch strings.ToLower(agg.Name) {
		case aggValues, aggList, aggLag, aggLead, aggMovAvg, aggDelta, aggDeltaSum, aggHist,
			aggMAD, aggTopKW, aggPerc, aggPerc25, aggPerc50, aggPerc75, aggPerc90, aggPerc95, aggPerc99,
			aggPercW:
			return true
		}
	}

	return false
}

func (s *StreamStatsIterator) Close() error {
	s.ringBufs = nil
	s.running = nil
	s.output = nil

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

func (r *ringBuffer) evictBefore(cutoff time.Time) []map[string]event.Value {
	if r.count == 0 {
		return nil
	}
	items := r.items()
	keptRows := items[:0]
	var evicted []map[string]event.Value
	for _, row := range items {
		ts, ok := streamStatsRowTime(row)
		if ok && ts.Before(cutoff) {
			evicted = append(evicted, row)
			continue
		}
		keptRows = append(keptRows, row)
	}
	if len(evicted) == 0 {
		return nil
	}
	r.buf = keptRows
	r.count = len(keptRows)
	r.pos = 0
	r.capacity = 0

	return evicted
}

func streamStatsRowTime(row map[string]event.Value) (time.Time, bool) {
	v, ok := row["_time"]
	if !ok {
		return time.Time{}, false
	}
	return v.TryAsTimestamp()
}

// newRunningAggState creates an initialized running state for the given aggregate.
func newRunningAggState(aggName string) *runningAggState {
	st := &runningAggState{}
	if strings.EqualFold(aggName, aggDC) || strings.EqualFold(aggName, aggEstDCE) || strings.EqualFold(aggName, aggMode) {
		st.freq = make(map[string]int64)
	}

	return st
}

// addValueToRunning incorporates a new row's field value into the running aggregate.
func addValueToRunning(st *runningAggState, agg AggFunc, row map[string]event.Value) {
	switch strings.ToLower(agg.Name) {
	case aggRowNum:
		st.rowNumber++
	case aggRunSum:
		if v, ok := row[agg.Field]; ok {
			if f, fok := vm.ValueToFloat(v); fok {
				st.sum += f
				st.count++
			}
		}
	case aggCount:
		if agg.Field == "" {
			st.count++
		} else if v, ok := row[agg.Field]; ok && !v.IsNull() {
			st.count++
		}
	case aggSum, aggPerSec, aggPerMin, aggPerHr, aggPerDay:
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
	case aggCorr, aggCovar, aggLinFit:
		addPairValueToRunning(st, agg, row)
	case aggSumObj:
		addObjectValueToRunning(st, agg, row, 1)
	case aggSkew, aggKurt:
		addMomentValueToRunning(st, agg, row, 1)
	case aggEMA:
		addEMAValueToRunning(st, agg, row)
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
	case aggDC, aggEstDCE, aggMode:
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
	case aggRowNum:
		// row_number is a partition ordinal, not a sliding aggregate.
	case aggEMA:
		// EMA is recursive and cannot be updated by subtracting evicted rows.
	case aggRunSum:
		if v, ok := row[agg.Field]; ok {
			if f, fok := vm.ValueToFloat(v); fok {
				st.sum -= f
				st.count--
			}
		}
	case aggCount:
		if agg.Field == "" {
			st.count--
		} else if v, ok := row[agg.Field]; ok && !v.IsNull() {
			st.count--
		}
	case aggSum, aggPerSec, aggPerMin, aggPerHr, aggPerDay:
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
	case aggCorr, aggCovar, aggLinFit:
		removePairValueFromRunning(st, agg, row)
	case aggSumObj:
		addObjectValueToRunning(st, agg, row, -1)
	case aggSkew, aggKurt:
		addMomentValueToRunning(st, agg, row, -1)
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
	case aggDC, aggEstDCE, aggMode:
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
	case aggRunSum:
		return event.FloatValue(st.sum)
	case aggCount:
		return event.IntValue(st.count)
	case aggSum, aggPerSec, aggPerMin, aggPerHr, aggPerDay:
		return event.FloatValue(st.sum)
	case aggSumSq:
		return event.FloatValue(st.sum)
	case aggAvg:
		if st.count == 0 {
			return event.NullValue()
		}

		return event.FloatValue(st.sum / float64(st.count))
	case aggCorr:
		return finalizeRunningCorr(st)
	case aggCovar:
		return finalizeRunningCovar(st)
	case aggLinFit:
		return finalizeRunningLinearFit(st)
	case aggSumObj:
		return objectSumValue(st.objectSum)
	case aggSkew:
		return finalizeSkewness(momentAggState(st))
	case aggKurt:
		return finalizeKurtosis(momentAggState(st))
	case aggDeltaSum:
		return deltaSumRows(rb.items(), agg.Field)
	case aggHist:
		return histogramRows(rb.items(), agg.Field, agg.Limit)
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
	case aggMode:
		return modeFromCounts(st.freq)
	case aggEMA:
		return readRunningEMA(st)
	}

	return event.NullValue()
}

func addEMAValueToRunning(st *runningAggState, agg AggFunc, row map[string]event.Value) {
	if agg.Window <= 0 {
		return
	}
	v, ok := row[agg.Field]
	if !ok {
		return
	}
	f, ok := vm.ValueToFloat(v)
	if !ok {
		return
	}
	if !st.emaSet {
		st.ema = f
		st.emaSet = true
		return
	}
	alpha := 2.0 / float64(agg.Window+1)
	st.ema = alpha*f + (1-alpha)*st.ema
}

func readRunningEMA(st *runningAggState) event.Value {
	if !st.emaSet {
		return event.NullValue()
	}

	return event.FloatValue(st.ema)
}

func addPairValueToRunning(st *runningAggState, agg AggFunc, row map[string]event.Value) {
	x, y, ok := pairValuesFromRow(agg, row)
	if !ok {
		return
	}
	st.count++
	st.sum += x
	st.weightSum += y
	st.sumSq += x * x
	st.sumY2 += y * y
	st.sumXY += x * y
}

func removePairValueFromRunning(st *runningAggState, agg AggFunc, row map[string]event.Value) {
	x, y, ok := pairValuesFromRow(agg, row)
	if !ok {
		return
	}
	st.count--
	st.sum -= x
	st.weightSum -= y
	st.sumSq -= x * x
	st.sumY2 -= y * y
	st.sumXY -= x * y
}

func pairValuesFromRow(agg AggFunc, row map[string]event.Value) (float64, float64, bool) {
	xVal, ok := row[agg.Field]
	if !ok {
		return 0, 0, false
	}
	yVal, ok := row[agg.WeightField]
	if !ok {
		return 0, 0, false
	}
	x, ok := vm.ValueToFloat(xVal)
	if !ok {
		return 0, 0, false
	}
	y, ok := vm.ValueToFloat(yVal)
	if !ok {
		return 0, 0, false
	}
	return x, y, true
}

func addObjectValueToRunning(st *runningAggState, agg AggFunc, row map[string]event.Value, delta int64) {
	val, ok := row[agg.Field]
	if !ok {
		return
	}
	obj, ok := val.TryAsObject()
	if !ok {
		return
	}
	if st.objectSum == nil {
		st.objectSum = make(map[string]float64)
	}
	if st.objectN == nil {
		st.objectN = make(map[string]int64)
	}
	for key, fieldVal := range obj {
		f, ok := vm.ValueToFloat(fieldVal)
		if !ok {
			continue
		}
		st.objectSum[key] += float64(delta) * f
		st.objectN[key] += delta
		if st.objectN[key] <= 0 {
			delete(st.objectSum, key)
			delete(st.objectN, key)
		}
	}
}

func addMomentValueToRunning(st *runningAggState, agg AggFunc, row map[string]event.Value, delta int64) {
	val, ok := row[agg.Field]
	if !ok {
		return
	}
	x, ok := vm.ValueToFloat(val)
	if !ok {
		return
	}
	x2 := x * x
	sign := float64(delta)
	st.count += delta
	st.sum += sign * x
	st.sumSq += sign * x2
	st.sumCube += sign * x2 * x
	st.sumFourth += sign * x2 * x2
}

func momentAggState(st *runningAggState) *aggState {
	return &aggState{
		count:     st.count,
		sum:       st.sum,
		sumSq:     st.sumSq,
		sumCube:   st.sumCube,
		sumFourth: st.sumFourth,
	}
}

func finalizeRunningCovar(st *runningAggState) event.Value {
	if st.count < 2 {
		return event.NullValue()
	}
	n := float64(st.count)
	return event.FloatValue((st.sumXY - st.sum*st.weightSum/n) / (n - 1))
}

func finalizeRunningCorr(st *runningAggState) event.Value {
	if st.count < 2 {
		return event.NullValue()
	}
	n := float64(st.count)
	num := n*st.sumXY - st.sum*st.weightSum
	xDen := n*st.sumSq - st.sum*st.sum
	yDen := n*st.sumY2 - st.weightSum*st.weightSum
	if xDen <= 0 || yDen <= 0 {
		return event.NullValue()
	}
	return event.FloatValue(num / math.Sqrt(xDen*yDen))
}

func finalizeRunningLinearFit(st *runningAggState) event.Value {
	if st.count < 2 {
		return event.NullValue()
	}
	n := float64(st.count)
	xDen := n*st.sumSq - st.sum*st.sum
	if xDen <= 0 {
		return event.NullValue()
	}
	slope := (n*st.sumXY - st.sum*st.weightSum) / xDen
	intercept := (st.weightSum - slope*st.sum) / n
	fields := map[string]event.Value{
		"slope":     event.FloatValue(slope),
		"intercept": event.FloatValue(intercept),
		"r2":        event.NullValue(),
	}
	yDen := n*st.sumY2 - st.weightSum*st.weightSum
	if yDen > 0 {
		corr := (n*st.sumXY - st.sum*st.weightSum) / math.Sqrt(xDen*yDen)
		fields["r2"] = event.FloatValue(corr * corr)
	}
	return event.ObjectValue(fields)
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
	case aggSum, aggPerSec, aggPerMin, aggPerHr, aggPerDay:
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
	case aggPerc, aggPerc25, aggPerc50, aggPerc75, aggPerc90, aggPerc95, aggPerc99:
		var values []interface{}
		for _, item := range items {
			if v, ok := item[agg.Field]; ok {
				if f, fok := vm.ValueToFloat(v); fok {
					values = append(values, f)
				}
			}
		}
		switch strings.ToLower(agg.Name) {
		case aggPerc:
			return percentile(values, agg.Quantile*100)
		case aggPerc25:
			return percentile(values, 25)
		case aggPerc50:
			return percentile(values, 50)
		case aggPerc75:
			return percentile(values, 75)
		case aggPerc90:
			return percentile(values, 90)
		case aggPerc95:
			return percentile(values, 95)
		case aggPerc99:
			return percentile(values, 99)
		}
	case aggPercW:
		st := aggState{}
		for _, item := range items {
			xVal, xOK := item[agg.Field]
			wVal, wOK := item[agg.WeightField]
			if xOK && wOK {
				updateWeightedPercentileState(&st, xVal, wVal)
			}
		}

		return finalizePercentile(&st, agg.Quantile)
	case aggTopKW:
		st := aggState{}
		for _, item := range items {
			val, valOK := item[agg.Field]
			weight, weightOK := item[agg.WeightField]
			if valOK && weightOK {
				updateWeightedTopKState(&st, val, weight)
			}
		}

		return finalizeWeightedTopK(&st, agg.Limit)
	case aggMAD:
		st := aggState{}
		for _, item := range items {
			if v, ok := item[agg.Field]; ok {
				updateMADState(&st, v)
			}
		}

		return finalizeMAD(&st)
	case aggDeltaSum:
		return deltaSumRows(items, agg.Field)
	case aggHist:
		return histogramRows(items, agg.Field, agg.Limit)
	case aggCorr, aggCovar, aggLinFit:
		st := runningAggState{}
		for _, item := range items {
			addPairValueToRunning(&st, agg, item)
		}
		if strings.EqualFold(agg.Name, aggCorr) {
			return finalizeRunningCorr(&st)
		}
		if strings.EqualFold(agg.Name, aggLinFit) {
			return finalizeRunningLinearFit(&st)
		}
		return finalizeRunningCovar(&st)
	case aggSumObj:
		st := runningAggState{}
		for _, item := range items {
			addObjectValueToRunning(&st, agg, item, 1)
		}
		return objectSumValue(st.objectSum)
	case aggSkew, aggKurt:
		st := runningAggState{}
		for _, item := range items {
			addMomentValueToRunning(&st, agg, item, 1)
		}
		if strings.EqualFold(agg.Name, aggSkew) {
			return finalizeSkewness(momentAggState(&st))
		}
		return finalizeKurtosis(momentAggState(&st))
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
	case aggMode:
		counts := make(map[string]int64)
		for _, item := range items {
			if v, ok := item[agg.Field]; ok && !v.IsNull() {
				counts[v.String()]++
			}
		}

		return modeFromCounts(counts)
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

		return event.StringValue(joinLimitedStringSlice(vals, agg.Limit))
	case aggList:
		var vals []string
		for _, item := range items {
			if v, ok := item[agg.Field]; ok && !v.IsNull() {
				vals = append(vals, v.String())
			}
		}

		return event.StringValue(joinLimitedStringSlice(vals, agg.Limit))
	}

	return event.NullValue()
}

func deltaSumRows(items []map[string]event.Value, field string) event.Value {
	var st aggState
	for _, item := range items {
		if v, ok := item[field]; ok {
			updateDeltaSumState(&st, v)
		}
	}
	if st.count == 0 {
		return event.NullValue()
	}

	return event.FloatValue(st.sum)
}

func histogramRows(items []map[string]event.Value, field string, bins int) event.Value {
	var st aggState
	for _, item := range items {
		if v, ok := item[field]; ok {
			updateHistogramState(&st, v)
		}
	}

	return finalizeHistogram(&st, bins)
}
