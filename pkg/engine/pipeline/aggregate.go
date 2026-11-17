package pipeline

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

// AggFunc describes an aggregation function.
type AggFunc struct {
	Name          string      // "count", "sum", "avg", "min", "max", "dc", "values", "first", "last"
	Field         string      // field to aggregate (empty for count)
	Alias         string      // output field name
	Program       *vm.Program // optional compiled expression for nested eval
	OrderField    string      // optional order field for arg_min/arg_max
	OrderProgram  *vm.Program // optional compiled order expression for arg_min/arg_max
	WeightField   string      // optional weight field for weighted aggregates
	WeightProgram *vm.Program // optional compiled weight expression for weighted aggregates
	Limit         int         // optional per-function limit for top_k
	Quantile      float64     // optional percentile quantile in [0,1] for perc(x, p)
	Scale         float64     // optional multiplier applied at finalize time
	Window        int         // optional per-function row window/offset for streamstats functions
	RankFields    []string    // sort-key fields used by rank/dense_rank streamstats functions
	// CondProgram is an optional compiled predicate for conditional aggregation.
	// When non-nil, it is evaluated per row BEFORE extracting the value. If the
	// predicate returns false or null, the row is skipped for this aggregate
	// (treated as null), as in count(eval(condition)).
	CondProgram *vm.Program
}

// maxValuesPerGroup caps the number of distinct values tracked per aggregation
// group for values() and stdev. Prevents unbounded memory for high-cardinality groups.
const maxValuesPerGroup = 100_000

// hllPromotionThreshold is the cardinality at which exact tracking is
// promoted to HyperLogLog approximation for distinct count.
const hllPromotionThreshold = 10_000

// defaultTDigestCompression is the compression parameter for t-digest
// percentile estimation. Higher values = more accurate but more memory.
const defaultTDigestCompression = 100.0

// Aggregation function name constants.
const (
	aggCount    = "count"
	aggSum      = "sum"
	aggSumSq    = "sumsq"
	aggAvg      = "avg"
	aggMin      = "min"
	aggMax      = "max"
	aggRange    = "range"
	aggValues   = "values"
	aggList     = "list"
	aggMode     = "mode"
	aggPerSec   = "per_second"
	aggPerMin   = "per_minute"
	aggPerHr    = "per_hour"
	aggPerDay   = "per_day"
	aggEarT     = "earliest_time"
	aggLatT     = "latest_time"
	aggRate     = "rate"
	aggDC       = "dc"
	aggEstDCE   = "estdc_error"
	aggStdev    = "stdev"
	aggStdevP   = "stdevp"
	aggVar      = "var"
	aggVarP     = "varp"
	aggPerc     = "perc"
	aggPerc25   = "perc25"
	aggPerc50   = "perc50"
	aggPerc75   = "perc75"
	aggPerc90   = "perc90"
	aggPerc95   = "perc95"
	aggPerc99   = "perc99"
	aggPercW    = "perc_weighted"
	aggLag      = "lag"
	aggLead     = "lead"
	aggRowNum   = "row_number"
	aggRank     = "rank"
	aggDense    = "dense_rank"
	aggRunSum   = "running_sum"
	aggMovAvg   = "moving_avg"
	aggDelta    = "delta"
	aggEMA      = "ema"
	aggArgMax   = "arg_max"
	aggArgMin   = "arg_min"
	aggAnyVal   = "any_value"
	aggTopK     = "top_k"
	aggTopKW    = "top_k_weighted"
	aggValCnt   = "value_counts"
	aggAvgW     = "avg_weighted"
	aggEntropy  = "entropy"
	aggMaxN     = "max_n"
	aggMinN     = "min_n"
	aggCorr     = "corr"
	aggCovar    = "covar"
	aggLinFit   = "linear_fit"
	aggSumObj   = "sum_object"
	aggSkew     = "skewness"
	aggKurt     = "kurtosis"
	aggMAD      = "mad"
	aggDeltaSum = "delta_sum"
	aggHist     = "histogram"
)

// AggregateIterator implements streaming hash aggregation (STATS command).
type AggregateIterator struct {
	child       Iterator
	aggs        []AggFunc
	groupBy     []string
	groupPaths  []string               // dotted group-by names resolved through object members
	groups      map[uint64][]*aggGroup // FNV hash → collision chain
	emitted     bool
	vmInst      vm.VM
	needsValues []bool      // per-agg: true if dc/values/perc*/stdev need values map/all slice
	groupCount  int         // total groups across all chains
	hasher      hash.Hash64 // persistent FNV-1a hasher, reset per call
}

type aggGroup struct {
	key    map[string]event.Value
	states []aggState
}

type aggState struct {
	count     int64
	sum       float64
	min       event.Value
	max       event.Value
	values    map[string]bool
	all       []interface{}
	mode      map[string]int64
	first     event.Value
	last      event.Value
	hasFirst  bool
	firstTS   time.Time
	lastTS    time.Time
	hll       *HyperLogLog // for approximate dc when cardinality exceeds threshold
	tdigest   *TDigest     // for approximate percentiles
	sumSq     float64      // sum of squared values
	topK      map[string]topKItem
	weightSum float64
	sumY2     float64
	sumXY     float64
	objectSum map[string]float64
	objectN   map[string]int64
	sumCube   float64
	sumFourth float64
}

type topKItem struct {
	Value  event.Value
	Count  int64
	Weight float64
}

// NewAggregateIterator creates a streaming hash aggregation operator.
func NewAggregateIterator(child Iterator, aggs []AggFunc, groupBy []string) *AggregateIterator {
	needsValues := make([]bool, len(aggs))
	for i, a := range aggs {
		switch strings.ToLower(a.Name) {
		case aggDC, aggEstDCE, aggValues, aggList, aggPerc, aggPerc25, aggPerc50, aggPerc75, aggPerc90, aggPerc95, aggPerc99, aggStdev, aggStdevP, aggVar, aggVarP, aggMAD:
			needsValues[i] = true
		case aggHist:
			needsValues[i] = true
		}
	}

	var groupPaths []string
	for _, g := range groupBy {
		if strings.Contains(g, ".") {
			groupPaths = append(groupPaths, g)
		}
	}

	return &AggregateIterator{
		child:       child,
		aggs:        aggs,
		groupBy:     groupBy,
		groupPaths:  groupPaths,
		groups:      make(map[uint64][]*aggGroup),
		needsValues: needsValues,
		hasher:      fnv.New64a(),
	}
}

func (a *AggregateIterator) Init(ctx context.Context) error {
	return a.child.Init(ctx)
}

func (a *AggregateIterator) Next(ctx context.Context) (*Batch, error) {
	if a.emitted {
		return nil, nil
	}

	// Consume all input.
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		batch, err := a.child.Next(ctx)
		if err != nil {
			return nil, err
		}
		if batch == nil {
			break
		}
		a.processBatch(batch)
	}

	a.emitted = true

	return a.emitAllGroups(), nil
}

func (a *AggregateIterator) Close() error {
	return a.child.Close()
}

func (a *AggregateIterator) processBatch(batch *Batch) {
	row := make(map[string]event.Value, len(batch.Columns))
	for i := 0; i < batch.Len; i++ {
		batch.RowInto(i, row)
		for _, g := range a.groupPaths {
			if _, ok := row[g]; !ok {
				if v := vm.LoadPath(row, g); !v.IsNull() {
					row[g] = v
				}
			}
		}

		h := a.groupKeyHash(row)
		group := a.findOrCreateGroup(h, row)

		for j, agg := range a.aggs {
			val := a.extractValue(agg, row)
			orderVal := a.extractOrderValue(agg, row)
			weightVal := a.extractWeightValue(agg, row)
			a.updateState(&group.states[j], agg.Name, val, orderVal, weightVal, row)
		}
	}
}

// groupKeyHash computes FNV-1a hash of typed group-by values.
// No string conversion or builder allocation.
func (a *AggregateIterator) groupKeyHash(row map[string]event.Value) uint64 {
	if len(a.groupBy) == 0 {
		return 0
	}
	a.hasher.Reset()
	h := a.hasher
	var buf [8]byte
	for _, g := range a.groupBy {
		v, ok := row[g]
		if !ok || v.IsNull() {
			h.Write([]byte{0}) // null tag

			continue
		}
		switch v.Type() {
		case event.FieldTypeString:
			h.Write([]byte{1})
			if s, ok := v.TryAsString(); ok {
				h.Write([]byte(s))
			}
		case event.FieldTypeInt:
			h.Write([]byte{2})
			if n, ok := v.TryAsInt(); ok {
				binary.LittleEndian.PutUint64(buf[:], uint64(n))
				h.Write(buf[:])
			}
		case event.FieldTypeFloat:
			h.Write([]byte{3})
			if f, ok := v.TryAsFloat(); ok {
				binary.LittleEndian.PutUint64(buf[:], math.Float64bits(f))
				h.Write(buf[:])
			}
		case event.FieldTypeBool:
			h.Write([]byte{4})
			if b, ok := v.TryAsBool(); ok && b {
				h.Write([]byte{1})
			} else {
				h.Write([]byte{0})
			}
		case event.FieldTypeTimestamp:
			h.Write([]byte{5})
			if t, ok := v.TryAsTimestamp(); ok {
				binary.LittleEndian.PutUint64(buf[:], uint64(t.UnixNano()))
				h.Write(buf[:])
			}
		case event.FieldTypeDuration:
			h.Write([]byte{6})
			if d, ok := v.TryAsDuration(); ok {
				binary.LittleEndian.PutUint64(buf[:], uint64(d))
				h.Write(buf[:])
			}
		case event.FieldTypeArray, event.FieldTypeObject:
			h.Write([]byte{7})
			h.Write([]byte(v.String()))
		default:
			h.Write([]byte{0})
		}
	}

	return h.Sum64()
}

// findOrCreateGroup looks up or creates an agg group by hash with collision handling.
func (a *AggregateIterator) findOrCreateGroup(h uint64, row map[string]event.Value) *aggGroup {
	chain := a.groups[h]
	for _, g := range chain {
		if a.groupKeysEqual(g.key, row) {
			return g
		}
	}

	// New group.
	group := &aggGroup{
		key:    a.extractGroupKey(row),
		states: make([]aggState, len(a.aggs)),
	}
	for j := range group.states {
		group.states[j].min = event.NullValue()
		group.states[j].max = event.NullValue()
		group.states[j].first = event.NullValue()
		group.states[j].last = event.NullValue()
		// Lazy: only allocate values map for aggs that need it.
		if a.needsValues[j] {
			group.states[j].values = make(map[string]bool)
		}
	}
	a.groups[h] = append(chain, group)
	a.groupCount++

	return group
}

// groupKeysEqual checks if a group's stored key matches the current row's group-by fields.
// Missing fields and null values are treated as equivalent (both represent "no value").
func (a *AggregateIterator) groupKeysEqual(key, row map[string]event.Value) bool {
	for _, g := range a.groupBy {
		// Direct map access: missing key returns zero Value which equals NullValue().
		if key[g] != row[g] {
			return false
		}
	}

	return true
}

func (a *AggregateIterator) extractGroupKey(row map[string]event.Value) map[string]event.Value {
	key := make(map[string]event.Value, len(a.groupBy))
	for _, g := range a.groupBy {
		if v, ok := row[g]; ok {
			key[g] = v
		} else {
			key[g] = event.NullValue()
		}
	}

	return key
}

func (a *AggregateIterator) extractValue(agg AggFunc, row map[string]event.Value) event.Value {
	// Conditional aggregation: a false or NULL predicate skips the row.
	if agg.CondProgram != nil {
		condResult, condErr := a.vmInst.Execute(agg.CondProgram, row)
		if condErr != nil {
			return event.NullValue()
		}
		if condResult.IsNull() {
			return event.NullValue()
		}
		if condResult.Type() == event.FieldTypeBool && !condResult.AsBool() {
			return event.NullValue()
		}
	}

	if agg.Program != nil {
		result, err := a.vmInst.Execute(agg.Program, row)
		if err != nil {
			return event.NullValue()
		}
		// Conditional aggregation: eval(boolean_condition) returns true/false.
		// Splunk convention: false means "no match" → null → count() skips it.
		if result.Type() == event.FieldTypeBool && !result.AsBool() {
			return event.NullValue()
		}
		return result
	}
	if agg.Field == "" {
		return event.IntValue(1) // count(*)
	}
	if v, ok := row[agg.Field]; ok {
		return v
	}

	return event.NullValue()
}

func (a *AggregateIterator) extractOrderValue(agg AggFunc, row map[string]event.Value) event.Value {
	if agg.OrderProgram != nil {
		result, err := a.vmInst.Execute(agg.OrderProgram, row)
		if err != nil {
			return event.NullValue()
		}
		return result
	}
	if agg.OrderField != "" {
		if v, ok := row[agg.OrderField]; ok {
			return v
		}
		return event.NullValue()
	}
	return event.NullValue()
}

func (a *AggregateIterator) extractWeightValue(agg AggFunc, row map[string]event.Value) event.Value {
	if agg.WeightProgram != nil {
		result, err := a.vmInst.Execute(agg.WeightProgram, row)
		if err != nil {
			return event.NullValue()
		}
		return result
	}
	if agg.WeightField != "" {
		if v, ok := row[agg.WeightField]; ok {
			return v
		}
		return event.NullValue()
	}
	return event.NullValue()
}

func (a *AggregateIterator) updateState(s *aggState, fn string, val, orderVal, weightVal event.Value, row map[string]event.Value) {
	switch strings.ToLower(fn) {
	case aggCount:
		if !val.IsNull() {
			s.count++
		}
	case aggSum, aggPerSec, aggPerMin, aggPerHr, aggPerDay:
		if f, ok := vm.ValueToFloat(val); ok {
			s.sum += f
			s.count++
		}
	case aggSumSq:
		if f, ok := vm.ValueToFloat(val); ok {
			s.sum += f * f
			s.count++
		}
	case aggAvg:
		if f, ok := vm.ValueToFloat(val); ok {
			s.sum += f
			s.count++
		}
	case aggAvgW:
		updateWeightedAvgState(s, val, weightVal)
	case aggCorr, aggCovar, aggLinFit:
		updatePairStatsState(s, val, weightVal)
	case aggSumObj:
		updateObjectSumState(s, val, 1)
	case aggSkew, aggKurt:
		updateMomentState(s, val)
	case aggMAD:
		updateMADState(s, val)
	case aggDeltaSum:
		updateDeltaSumState(s, val)
	case aggHist:
		updateHistogramState(s, val)
	case aggMin:
		if !val.IsNull() {
			if s.min.IsNull() || vm.CompareValues(val, s.min) < 0 {
				s.min = val
			}
		}
	case aggMax:
		if !val.IsNull() {
			if s.max.IsNull() || vm.CompareValues(val, s.max) > 0 {
				s.max = val
			}
		}
	case aggRange:
		if f, ok := vm.ValueToFloat(val); ok {
			v := event.FloatValue(f)
			if s.count == 0 || vm.CompareValues(v, s.min) < 0 {
				s.min = v
			}
			if s.count == 0 || vm.CompareValues(v, s.max) > 0 {
				s.max = v
			}
			s.count++
		}
	case aggDC, aggEstDCE:
		if !val.IsNull() {
			str := val.String()
			if s.hll != nil {
				s.hll.Add(str)

				break
			}
			s.values[str] = true
			// Switch to HLL when cardinality exceeds threshold.
			if len(s.values) > hllPromotionThreshold {
				s.hll = NewHyperLogLog()
				for k := range s.values {
					s.hll.Add(k)
				}
				s.values = nil // free exact set
			}
		}
	case aggValues:
		if !val.IsNull() && len(s.all) < maxValuesPerGroup {
			str := val.String()
			if !s.values[str] {
				s.values[str] = true
				s.all = append(s.all, str)
			}
		}
	case aggList:
		if !val.IsNull() && len(s.all) < maxValuesPerGroup {
			s.all = append(s.all, val.String())
		}
	case aggMode:
		if !val.IsNull() {
			if s.mode == nil {
				s.mode = make(map[string]int64)
			}
			s.mode[val.String()]++
		}
	case aggTopK, aggValCnt, aggEntropy, aggMaxN, aggMinN:
		updateTopKState(s, val, 1)
	case aggTopKW:
		updateWeightedTopKState(s, val, weightVal)
	case "first":
		if !val.IsNull() && !s.hasFirst {
			s.first = val
			s.hasFirst = true
		}
	case aggAnyVal:
		if !val.IsNull() && !s.hasFirst {
			s.first = val
			s.hasFirst = true
		}
	case "last":
		if !val.IsNull() {
			s.last = val
		}
	case aggArgMax:
		updateArgState(s, val, orderVal, true)
	case aggArgMin:
		updateArgState(s, val, orderVal, false)
	case aggPerc, aggPerc25, aggPerc50, aggPerc75, aggPerc90, aggPerc95, aggPerc99:
		if f, ok := vm.ValueToFloat(val); ok {
			if s.tdigest == nil {
				s.tdigest = NewTDigest(defaultTDigestCompression)
			}
			s.tdigest.Add(f)
			// Keep exact values only for small datasets; t-digest handles large ones.
			if len(s.all) < maxValuesPerGroup {
				s.all = append(s.all, f)
			}
		}
	case aggPercW:
		updateWeightedPercentileState(s, val, weightVal)
	case aggStdev, aggStdevP, aggVar, aggVarP:
		if f, ok := vm.ValueToFloat(val); ok {
			if len(s.all) < maxValuesPerGroup {
				s.all = append(s.all, f)
			}
			s.sum += f
			s.count++
		}
	case "earliest", aggEarT:
		a.updateChronoState(s, val, row, true)
	case "latest", aggLatT:
		a.updateChronoState(s, val, row, false)
	case aggRate:
		a.updateChronoState(s, val, row, true)
		a.updateChronoState(s, val, row, false)
	}
}

func updateDeltaSumState(s *aggState, val event.Value) {
	f, ok := vm.ValueToFloat(val)
	if !ok {
		return
	}
	current := event.FloatValue(f)
	if !s.hasFirst {
		s.first = current
		s.last = current
		s.hasFirst = true
		s.count = 1

		return
	}
	last, ok := vm.ValueToFloat(s.last)
	if ok && f > last {
		s.sum += f - last
	}
	s.last = current
	s.count++
}

func updateHistogramState(s *aggState, val event.Value) {
	f, ok := vm.ValueToFloat(val)
	if !ok {
		return
	}
	if len(s.all) < maxValuesPerGroup {
		s.all = append(s.all, f)
	}
	s.count++
}

func updateArgState(s *aggState, val, orderVal event.Value, maxOrder bool) {
	if val.IsNull() || orderVal.IsNull() {
		return
	}
	if !s.hasFirst {
		s.first = val
		if maxOrder {
			s.max = orderVal
		} else {
			s.min = orderVal
		}
		s.hasFirst = true
		return
	}
	cmp := vm.CompareValues(orderVal, argOrderValue(s, maxOrder))
	if (maxOrder && cmp > 0) || (!maxOrder && cmp < 0) {
		s.first = val
		if maxOrder {
			s.max = orderVal
		} else {
			s.min = orderVal
		}
	}
}

func argOrderValue(s *aggState, maxOrder bool) event.Value {
	if maxOrder {
		return s.max
	}
	return s.min
}

func updateTopKState(s *aggState, val event.Value, count int64) {
	if val.IsNull() || count <= 0 {
		return
	}
	key := topKKey(val)
	if s.topK == nil {
		s.topK = make(map[string]topKItem)
	}
	item := s.topK[key]
	if item.Count == 0 {
		item.Value = val
	}
	item.Count += count
	s.topK[key] = item
}

func updateWeightedTopKState(s *aggState, val, weightVal event.Value) {
	if val.IsNull() {
		return
	}
	weight, ok := vm.ValueToFloat(weightVal)
	if !ok || weight <= 0 || math.IsNaN(weight) || math.IsInf(weight, 0) {
		return
	}
	key := topKKey(val)
	if s.topK == nil {
		s.topK = make(map[string]topKItem)
	}
	item := s.topK[key]
	if item.Weight == 0 {
		item.Value = val
	}
	item.Weight += weight
	s.topK[key] = item
}

func topKKey(val event.Value) string {
	return val.Type().String() + "\x00" + val.String()
}

func finalizeTopK(s *aggState, limit int) event.Value {
	if len(s.topK) == 0 {
		return event.ArrayValue(nil)
	}
	if limit <= 0 {
		limit = len(s.topK)
	}
	items := make([]topKItem, 0, len(s.topK))
	for _, item := range s.topK {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Count != items[j].Count {
			return items[i].Count > items[j].Count
		}
		return items[i].Value.String() < items[j].Value.String()
	})
	if limit > len(items) {
		limit = len(items)
	}
	result := make([]event.Value, 0, limit)
	for _, item := range items[:limit] {
		result = append(result, event.ObjectValue(map[string]event.Value{
			"value": item.Value,
			"count": event.IntValue(item.Count),
		}))
	}
	return event.ArrayValue(result)
}

func finalizeWeightedTopK(s *aggState, limit int) event.Value {
	if len(s.topK) == 0 {
		return event.ArrayValue(nil)
	}
	if limit <= 0 {
		limit = len(s.topK)
	}
	items := make([]topKItem, 0, len(s.topK))
	for _, item := range s.topK {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Weight != items[j].Weight {
			return items[i].Weight > items[j].Weight
		}
		return items[i].Value.String() < items[j].Value.String()
	})
	if limit > len(items) {
		limit = len(items)
	}
	result := make([]event.Value, 0, limit)
	for _, item := range items[:limit] {
		result = append(result, event.ObjectValue(map[string]event.Value{
			"value":  item.Value,
			"weight": event.FloatValue(item.Weight),
		}))
	}
	return event.ArrayValue(result)
}

func finalizeExtremaN(s *aggState, limit int, maxOrder bool) event.Value {
	if len(s.topK) == 0 || limit <= 0 {
		return event.ArrayValue(nil)
	}
	items := make([]topKItem, 0, len(s.topK))
	for _, item := range s.topK {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		cmp := vm.CompareValues(items[i].Value, items[j].Value)
		if cmp == 0 {
			return items[i].Value.String() < items[j].Value.String()
		}
		if maxOrder {
			return cmp > 0
		}
		return cmp < 0
	})

	result := make([]event.Value, 0, limit)
	for _, item := range items {
		for i := int64(0); i < item.Count; i++ {
			if len(result) == limit {
				return event.ArrayValue(result)
			}
			result = append(result, item.Value)
		}
	}
	return event.ArrayValue(result)
}

func joinLimitedAllStrings(values []interface{}, limit int) string {
	if limit > 0 && limit < len(values) {
		values = values[:limit]
	}
	return joinAllStrings(values, "|||")
}

func joinLimitedStringSlice(values []string, limit int) string {
	if limit > 0 && limit < len(values) {
		values = values[:limit]
	}
	return strings.Join(values, "|||")
}

func updateWeightedAvgState(s *aggState, val, weightVal event.Value) {
	x, ok := vm.ValueToFloat(val)
	if !ok {
		return
	}
	weight, ok := vm.ValueToFloat(weightVal)
	if !ok {
		return
	}
	s.sum += x * weight
	s.weightSum += weight
}

func updateWeightedPercentileState(s *aggState, val, weightVal event.Value) {
	x, ok := vm.ValueToFloat(val)
	if !ok {
		return
	}
	weight, ok := vm.ValueToFloat(weightVal)
	if !ok {
		return
	}
	if weight <= 0 || math.IsNaN(x) || math.IsNaN(weight) ||
		math.IsInf(x, 0) || math.IsInf(weight, 0) {
		return
	}
	if s.tdigest == nil {
		s.tdigest = NewTDigest(defaultTDigestCompression)
	}
	s.tdigest.AddWeighted(x, weight)
}

func updatePairStatsState(s *aggState, val, otherVal event.Value) {
	x, ok := vm.ValueToFloat(val)
	if !ok {
		return
	}
	y, ok := vm.ValueToFloat(otherVal)
	if !ok {
		return
	}
	s.count++
	s.sum += x
	s.weightSum += y
	s.sumSq += x * x
	s.sumY2 += y * y
	s.sumXY += x * y
}

func updateObjectSumState(s *aggState, val event.Value, delta int64) {
	obj, ok := val.TryAsObject()
	if !ok {
		return
	}
	if s.objectSum == nil {
		s.objectSum = make(map[string]float64)
	}
	if s.objectN == nil {
		s.objectN = make(map[string]int64)
	}
	for key, fieldVal := range obj {
		f, ok := vm.ValueToFloat(fieldVal)
		if !ok {
			continue
		}
		s.objectSum[key] += float64(delta) * f
		s.objectN[key] += delta
		if s.objectN[key] <= 0 {
			delete(s.objectSum, key)
			delete(s.objectN, key)
		}
	}
}

func updateMomentState(s *aggState, val event.Value) {
	x, ok := vm.ValueToFloat(val)
	if !ok {
		return
	}
	x2 := x * x
	s.count++
	s.sum += x
	s.sumSq += x2
	s.sumCube += x2 * x
	s.sumFourth += x2 * x2
}

func updateMADState(s *aggState, val event.Value) {
	x, ok := vm.ValueToFloat(val)
	if !ok {
		return
	}
	s.count++
	if len(s.all) < maxValuesPerGroup {
		s.all = append(s.all, x)
	}
}

func objectSumValue(sums map[string]float64) event.Value {
	fields := make(map[string]event.Value, len(sums))
	for key, sum := range sums {
		fields[key] = event.FloatValue(sum)
	}
	return event.ObjectValue(fields)
}

func finalizeCovar(s *aggState) event.Value {
	if s.count < 2 {
		return event.NullValue()
	}
	n := float64(s.count)
	cov := (s.sumXY - s.sum*s.weightSum/n) / (n - 1)
	return event.FloatValue(cov)
}

func finalizeCorr(s *aggState) event.Value {
	if s.count < 2 {
		return event.NullValue()
	}
	n := float64(s.count)
	num := n*s.sumXY - s.sum*s.weightSum
	xDen := n*s.sumSq - s.sum*s.sum
	yDen := n*s.sumY2 - s.weightSum*s.weightSum
	if xDen <= 0 || yDen <= 0 {
		return event.NullValue()
	}
	return event.FloatValue(num / math.Sqrt(xDen*yDen))
}

func finalizeLinearFit(s *aggState) event.Value {
	if s.count < 2 {
		return event.NullValue()
	}
	n := float64(s.count)
	xDen := n*s.sumSq - s.sum*s.sum
	if xDen <= 0 {
		return event.NullValue()
	}
	slope := (n*s.sumXY - s.sum*s.weightSum) / xDen
	intercept := (s.weightSum - slope*s.sum) / n

	fields := map[string]event.Value{
		"slope":     event.FloatValue(slope),
		"intercept": event.FloatValue(intercept),
		"r2":        event.NullValue(),
	}
	yDen := n*s.sumY2 - s.weightSum*s.weightSum
	if yDen > 0 {
		corr := (n*s.sumXY - s.sum*s.weightSum) / math.Sqrt(xDen*yDen)
		fields["r2"] = event.FloatValue(corr * corr)
	}
	return event.ObjectValue(fields)
}

func finalizeHistogram(s *aggState, bins int) event.Value {
	if bins <= 0 || s.count == 0 || int64(len(s.all)) < s.count {
		return event.NullValue()
	}
	values := float64SliceFromAll(s.all)
	if len(values) == 0 {
		return event.NullValue()
	}
	minVal := values[0]
	maxVal := values[0]
	for _, value := range values[1:] {
		if value < minVal {
			minVal = value
		}
		if value > maxVal {
			maxVal = value
		}
	}
	if minVal == maxVal {
		return event.ArrayValue([]event.Value{
			event.ObjectValue(map[string]event.Value{
				"lo":    event.FloatValue(minVal),
				"hi":    event.FloatValue(maxVal),
				"count": event.IntValue(int64(len(values))),
			}),
		})
	}
	width := (maxVal - minVal) / float64(bins)
	counts := make([]int64, bins)
	for _, value := range values {
		idx := int((value - minVal) / width)
		if idx >= bins {
			idx = bins - 1
		}
		if idx < 0 {
			idx = 0
		}
		counts[idx]++
	}
	result := make([]event.Value, 0, bins)
	for i, count := range counts {
		lo := minVal + float64(i)*width
		hi := lo + width
		if i == bins-1 {
			hi = maxVal
		}
		result = append(result, event.ObjectValue(map[string]event.Value{
			"lo":    event.FloatValue(lo),
			"hi":    event.FloatValue(hi),
			"count": event.IntValue(count),
		}))
	}

	return event.ArrayValue(result)
}

func finalizePercentile(s *aggState, quantile float64) event.Value {
	if s.tdigest != nil && (len(s.all) == 0 || len(s.all) > hllPromotionThreshold) {
		return event.FloatValue(s.tdigest.Quantile(quantile))
	}

	return percentile(s.all, quantile*100)
}

func centralMoments(s *aggState) (float64, float64, float64, bool) {
	if s.count == 0 {
		return 0, 0, 0, false
	}
	n := float64(s.count)
	mean := s.sum / n
	m2 := s.sumSq - n*mean*mean
	m3 := s.sumCube - 3*mean*s.sumSq + 3*mean*mean*s.sum - n*mean*mean*mean
	m4 := s.sumFourth - 4*mean*s.sumCube + 6*mean*mean*s.sumSq -
		4*mean*mean*mean*s.sum + n*mean*mean*mean*mean
	if m2 <= 0 {
		return 0, 0, 0, false
	}
	return m2 / n, m3 / n, m4 / n, true
}

func finalizeSkewness(s *aggState) event.Value {
	if s.count < 3 {
		return event.NullValue()
	}
	m2, m3, _, ok := centralMoments(s)
	if !ok {
		return event.NullValue()
	}
	n := float64(s.count)
	g1 := m3 / math.Pow(m2, 1.5)
	return event.FloatValue(math.Sqrt(n*(n-1)) / (n - 2) * g1)
}

func finalizeKurtosis(s *aggState) event.Value {
	if s.count < 4 {
		return event.NullValue()
	}
	m2, _, m4, ok := centralMoments(s)
	if !ok {
		return event.NullValue()
	}
	n := float64(s.count)
	g2 := m4/(m2*m2) - 3
	return event.FloatValue((n - 1) / ((n - 2) * (n - 3)) * ((n+1)*g2 + 6))
}

func finalizeMAD(s *aggState) event.Value {
	if s.count == 0 || int64(len(s.all)) != s.count {
		return event.NullValue()
	}
	values := float64SliceFromAll(s.all)
	sort.Float64s(values)
	median := medianSortedFloat64(values)

	deviations := make([]float64, len(values))
	for i, value := range values {
		deviations[i] = math.Abs(value - median)
	}
	sort.Float64s(deviations)

	return event.FloatValue(medianSortedFloat64(deviations))
}

func finalizeEntropy(s *aggState) event.Value {
	var total int64
	for _, item := range s.topK {
		total += item.Count
	}
	if total == 0 {
		return event.NullValue()
	}
	var entropy float64
	for _, item := range s.topK {
		p := float64(item.Count) / float64(total)
		entropy -= p * math.Log2(p)
	}
	return event.FloatValue(entropy)
}

func (a *AggregateIterator) updateChronoState(s *aggState, val event.Value, row map[string]event.Value, earliest bool) {
	if val.IsNull() {
		return
	}
	ts, hasTS := rowTimestamp(row)
	if earliest {
		if !s.hasFirst || (hasTS && (s.firstTS.IsZero() || ts.Before(s.firstTS))) {
			s.first = val
			s.firstTS = ts
			s.hasFirst = true
		}
		return
	}
	if !s.hasFirst || (hasTS && (s.lastTS.IsZero() || ts.After(s.lastTS))) || (!hasTS && s.last.IsNull()) {
		s.last = val
		s.lastTS = ts
		s.hasFirst = true
	}
}

func rowTimestamp(row map[string]event.Value) (time.Time, bool) {
	v, ok := row["_time"]
	if !ok || v.IsNull() {
		return time.Time{}, false
	}
	switch v.Type() {
	case event.FieldTypeTimestamp:
		return v.TryAsTimestamp()
	case event.FieldTypeInt:
		n, ok := v.TryAsInt()
		if !ok {
			return time.Time{}, false
		}
		return time.Unix(0, n), true
	case event.FieldTypeString:
		s, ok := v.TryAsString()
		if !ok {
			return time.Time{}, false
		}
		return tryParseTimestamp(s)
	default:
		return time.Time{}, false
	}
}

// emitAllGroups finalizes all in-memory groups into a result batch.
func (a *AggregateIterator) emitAllGroups() *Batch {
	// Count total groups across all chains.
	totalGroups := 0
	for _, chain := range a.groups {
		totalGroups += len(chain)
	}

	result := NewBatch(totalGroups)
	if totalGroups == 0 && len(a.groupBy) == 0 {
		// No input, no group-by: emit one row with zero values.
		row := make(map[string]event.Value, len(a.aggs))
		for _, agg := range a.aggs {
			row[agg.Alias] = a.finalizeAgg(&aggState{values: make(map[string]bool)}, agg)
		}
		result.AddRow(row)

		return result
	}
	for _, chain := range a.groups {
		for _, group := range chain {
			row := make(map[string]event.Value, len(a.groupBy)+len(a.aggs))
			for k, v := range group.key {
				row[k] = v
			}
			for j, agg := range a.aggs {
				row[agg.Alias] = a.finalizeAgg(&group.states[j], agg)
			}
			result.AddRow(row)
		}
	}

	return result
}

func (a *AggregateIterator) finalizeState(s *aggState, fn string) event.Value {
	switch strings.ToLower(fn) {
	case aggCount:
		return event.IntValue(s.count)
	case aggSum, aggPerSec, aggPerMin, aggPerHr, aggPerDay:
		return event.FloatValue(s.sum)
	case aggSumSq:
		return event.FloatValue(s.sum)
	case aggAvg:
		if s.count == 0 {
			return event.NullValue()
		}

		return event.FloatValue(s.sum / float64(s.count))
	case aggAvgW:
		if s.weightSum == 0 {
			return event.NullValue()
		}
		return event.FloatValue(s.sum / s.weightSum)
	case aggCorr:
		return finalizeCorr(s)
	case aggCovar:
		return finalizeCovar(s)
	case aggLinFit:
		return finalizeLinearFit(s)
	case aggSumObj:
		return objectSumValue(s.objectSum)
	case aggSkew:
		return finalizeSkewness(s)
	case aggKurt:
		return finalizeKurtosis(s)
	case aggMAD:
		return finalizeMAD(s)
	case aggDeltaSum:
		if s.count == 0 {
			return event.NullValue()
		}
		return event.FloatValue(s.sum)
	case aggHist:
		return finalizeHistogram(s, 0)
	case aggMin:
		return s.min
	case aggMax:
		return s.max
	case aggRange:
		if s.count == 0 {
			return event.NullValue()
		}
		min, minOK := vm.ValueToFloat(s.min)
		max, maxOK := vm.ValueToFloat(s.max)
		if !minOK || !maxOK {
			return event.NullValue()
		}

		return event.FloatValue(max - min)
	case aggDC:
		if s.hll != nil {
			return event.IntValue(s.hll.Count())
		}

		return event.IntValue(int64(len(s.values)))
	case aggEstDCE:
		if s.hll != nil {
			return event.FloatValue(s.hll.StandardError())
		}

		return event.FloatValue(0)
	case aggValues:
		return event.StringValue(joinAllStrings(s.all, "|||"))
	case aggList:
		return event.StringValue(joinAllStrings(s.all, "|||"))
	case aggMode:
		return modeFromCounts(s.mode)
	case "first", "earliest", aggAnyVal, aggArgMax, aggArgMin:
		return s.first
	case "last", "latest":
		return s.last
	case aggEarT:
		if s.firstTS.IsZero() {
			return event.NullValue()
		}
		return event.FloatValue(float64(s.firstTS.UnixNano()) / float64(time.Second))
	case aggLatT:
		if s.lastTS.IsZero() {
			return event.NullValue()
		}
		return event.FloatValue(float64(s.lastTS.UnixNano()) / float64(time.Second))
	case aggRate:
		if s.firstTS.IsZero() || s.lastTS.IsZero() || !s.lastTS.After(s.firstTS) {
			return event.NullValue()
		}
		first, firstOK := vm.ValueToFloat(s.first)
		last, lastOK := vm.ValueToFloat(s.last)
		if !firstOK || !lastOK {
			return event.NullValue()
		}
		return event.FloatValue((last - first) / s.lastTS.Sub(s.firstTS).Seconds())
	case aggPerc25:
		return finalizePercentile(s, 0.25)
	case aggPerc50:
		return finalizePercentile(s, 0.50)
	case aggPerc75:
		return finalizePercentile(s, 0.75)
	case aggPerc90:
		return finalizePercentile(s, 0.90)
	case aggPerc95:
		return finalizePercentile(s, 0.95)
	case aggPerc99:
		return finalizePercentile(s, 0.99)
	case aggStdev:
		return finalizeVarianceState(s, false, true)
	case aggStdevP:
		return finalizeVarianceState(s, true, true)
	case aggVar:
		return finalizeVarianceState(s, false, false)
	case aggVarP:
		return finalizeVarianceState(s, true, false)
	}

	return event.NullValue()
}

func (a *AggregateIterator) finalizeAgg(s *aggState, agg AggFunc) event.Value {
	switch strings.ToLower(agg.Name) {
	case aggTopK:
		return finalizeTopK(s, agg.Limit)
	case aggTopKW:
		return finalizeWeightedTopK(s, agg.Limit)
	case aggValCnt:
		return finalizeTopK(s, agg.Limit)
	case aggEntropy:
		return finalizeEntropy(s)
	case aggMaxN:
		return finalizeExtremaN(s, agg.Limit, true)
	case aggMinN:
		return finalizeExtremaN(s, agg.Limit, false)
	case aggValues, aggList:
		return event.StringValue(joinLimitedAllStrings(s.all, agg.Limit))
	case aggCorr:
		return finalizeCorr(s)
	case aggCovar:
		return finalizeCovar(s)
	case aggLinFit:
		return finalizeLinearFit(s)
	case aggPerc:
		return finalizePercentile(s, agg.Quantile)
	case aggPercW:
		return finalizePercentile(s, agg.Quantile)
	case aggMAD:
		return finalizeMAD(s)
	case aggHist:
		return finalizeHistogram(s, agg.Limit)
	}
	val := a.finalizeState(s, agg.Name)
	if val.IsNull() || agg.Scale == 0 {
		return val
	}
	switch strings.ToLower(agg.Name) {
	case aggPerSec, aggPerMin, aggPerHr, aggPerDay:
		if f, ok := vm.ValueToFloat(val); ok {
			return event.FloatValue(f * agg.Scale)
		}
	}

	return val
}

func finalizeVarianceState(s *aggState, population, root bool) event.Value {
	if s.count == 0 || (!population && s.count < 2) {
		return event.NullValue()
	}
	sumSq := s.sumSq
	if len(s.all) > 0 {
		mean := s.sum / float64(s.count)
		sumSq = 0
		for _, v := range s.all {
			f := v.(float64)
			diff := f - mean
			sumSq += diff * diff
		}
	}
	denom := float64(s.count)
	if !population {
		denom = float64(s.count - 1)
	}
	variance := sumSq / denom
	if root {
		return event.FloatValue(math.Sqrt(variance))
	}

	return event.FloatValue(variance)
}

// percentile returns the value that has floor(pct/100*n) of the n values
// before it, the greatest for a pct of 100.
func percentile(all []interface{}, pct float64) event.Value {
	if len(all) == 0 {
		return event.NullValue()
	}
	floats := float64SliceFromAll(all)
	sort.Float64s(floats)

	return event.FloatValue(floats[min(int(pct/100*float64(len(floats))), len(floats)-1)])
}

func float64SliceFromAll(all []interface{}) []float64 {
	floats := make([]float64, len(all))
	for i, v := range all {
		floats[i] = v.(float64)
	}

	return floats
}

func medianSortedFloat64(values []float64) float64 {
	mid := len(values) / 2
	if len(values)%2 == 1 {
		return values[mid]
	}

	return (values[mid-1] + values[mid]) / 2
}

// joinAllStrings joins interface{} values (expected strings) with a separator.
// Values are emitted as strings; non-string entries fall back to fmt.Sprint to
// preserve historical behavior while avoiding the format-parsing cost for the
// hot string-only path used by values()/list() aggregations.
func joinAllStrings(all []interface{}, sep string) string {
	strs := make([]string, len(all))
	for i, v := range all {
		if s, ok := v.(string); ok {
			strs[i] = s
			continue
		}
		strs[i] = fmt.Sprint(v)
	}

	return strings.Join(strs, sep)
}

func modeFromCounts(counts map[string]int64) event.Value {
	var best string
	var bestCount int64
	hasBest := false
	for value, count := range counts {
		if !hasBest || count > bestCount || (count == bestCount && value < best) {
			best = value
			bestCount = count
			hasBest = true
		}
	}
	if !hasBest || bestCount == 0 {
		return event.NullValue()
	}

	return event.StringValue(best)
}
