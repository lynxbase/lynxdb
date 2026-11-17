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
	// Name is one of count, sum, avg, min, max, var, varp, stdev, stdevp,
	// dc, perc, earliest, latest, first, last, list and values.
	Name     string
	Field    string      // field to aggregate (empty for count)
	Alias    string      // output field name
	Program  *vm.Program // optional compiled expression for nested eval
	Quantile float64     // percentile quantile in [0,1] for perc(x, p)
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
	aggCount  = "count"
	aggSum    = "sum"
	aggAvg    = "avg"
	aggMin    = "min"
	aggMax    = "max"
	aggValues = "values"
	aggList   = "list"
	aggDC     = "dc"
	aggStdev  = "stdev"
	aggStdevP = "stdevp"
	aggVar    = "var"
	aggVarP   = "varp"
	aggPerc   = "perc"
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
	count    int64
	sum      float64
	min      event.Value
	max      event.Value
	values   map[string]bool
	all      []interface{}
	first    event.Value
	last     event.Value
	hasFirst bool
	firstTS  time.Time
	lastTS   time.Time
	hll      *HyperLogLog // for approximate dc when cardinality exceeds threshold
	tdigest  *TDigest     // for approximate percentiles
}

// NewAggregateIterator creates a streaming hash aggregation operator.
func NewAggregateIterator(child Iterator, aggs []AggFunc, groupBy []string) *AggregateIterator {
	needsValues := make([]bool, len(aggs))
	for i, a := range aggs {
		switch strings.ToLower(a.Name) {
		case aggDC, aggValues, aggList, aggPerc, aggStdev, aggStdevP, aggVar, aggVarP:
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
			a.updateState(&group.states[j], agg.Name, aggValue(&a.vmInst, agg, row), row)
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

// aggValue returns the value an aggregate takes from a row: its argument
// evaluated by machine, or NULL where its condition does not hold.
func aggValue(machine *vm.VM, agg AggFunc, row map[string]event.Value) event.Value {
	// Conditional aggregation: a false or NULL predicate skips the row.
	if agg.CondProgram != nil {
		condResult, condErr := machine.Execute(agg.CondProgram, row)
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
		result, err := machine.Execute(agg.Program, row)
		if err != nil {
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

func (a *AggregateIterator) updateState(s *aggState, fn string, val event.Value, row map[string]event.Value) {
	switch strings.ToLower(fn) {
	case aggCount:
		if !val.IsNull() {
			s.count++
		}
	case aggSum, aggAvg:
		if f, ok := vm.ValueToFloat(val); ok {
			s.sum += f
			s.count++
		}
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
	case aggDC:
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
	case "first":
		if !val.IsNull() && !s.hasFirst {
			s.first = val
			s.hasFirst = true
		}
	case "last":
		if !val.IsNull() {
			s.last = val
		}
	case aggPerc:
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
	case aggStdev, aggStdevP, aggVar, aggVarP:
		if f, ok := vm.ValueToFloat(val); ok {
			if len(s.all) < maxValuesPerGroup {
				s.all = append(s.all, f)
			}
			s.sum += f
			s.count++
		}
	case "earliest":
		chronoState(s, val, row, true)
	case "latest":
		chronoState(s, val, row, false)
	}
}

func finalizePercentile(s *aggState, quantile float64) event.Value {
	if s.tdigest != nil && (len(s.all) == 0 || len(s.all) > hllPromotionThreshold) {
		return event.FloatValue(s.tdigest.Quantile(quantile))
	}

	return percentile(s.all, quantile*100)
}

// chronoState keeps in s the value of the row with the earliest time, or
// with the latest.
func chronoState(s *aggState, val event.Value, row map[string]event.Value, earliest bool) {
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
		if s.hll != nil {
			return event.IntValue(s.hll.Count())
		}

		return event.IntValue(int64(len(s.values)))
	case aggValues, aggList:
		return event.StringValue(joinAllStrings(s.all, "|||"))
	case "first", "earliest":
		return s.first
	case "last", "latest":
		return s.last
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
	if strings.EqualFold(agg.Name, aggPerc) {
		return finalizePercentile(s, agg.Quantile)
	}

	return a.finalizeState(s, agg.Name)
}

func finalizeVarianceState(s *aggState, population, root bool) event.Value {
	if s.count == 0 || (!population && s.count < 2) {
		return event.NullValue()
	}
	mean := s.sum / float64(s.count)
	var sumSq float64
	for _, v := range s.all {
		diff := v.(float64) - mean
		sumSq += diff * diff
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
