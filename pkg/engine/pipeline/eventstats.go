package pipeline

import (
	"context"
	"fmt"
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

// Schema delegates to the child iterator.
func (e *EventStatsIterator) Schema() []FieldInfo { return e.child.Schema() }

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
				val := e.extractValue(agg, row)
				orderVal := e.extractOrderValue(agg, row)
				weightVal := e.extractWeightValue(agg, row)
				updateAggStateWithOrder(&group.states[j], agg.Name, val, orderVal, weightVal)
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

func (e *EventStatsIterator) extractOrderValue(agg AggFunc, row map[string]event.Value) event.Value {
	if agg.OrderProgram != nil {
		result, err := e.vmInst.Execute(agg.OrderProgram, row)
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

func (e *EventStatsIterator) extractWeightValue(agg AggFunc, row map[string]event.Value) event.Value {
	if agg.WeightProgram != nil {
		result, err := e.vmInst.Execute(agg.WeightProgram, row)
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

func updateAggStateWithOrder(s *aggState, fn string, val, orderVal, weightVal event.Value) {
	switch strings.ToLower(fn) {
	case aggCount:
		if !val.IsNull() {
			s.count++
		}
	case aggSum, aggPerSec, aggPerMin, aggPerHr, aggPerDay:
		if f, ok := vm.ValueToFloat(val); ok {
			s.sum += f
		}
	case aggSumSq:
		if f, ok := vm.ValueToFloat(val); ok {
			s.sum += f * f
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
		if !val.IsNull() && (s.min.IsNull() || vm.CompareValues(val, s.min) < 0) {
			s.min = val
		}
	case aggMax:
		if !val.IsNull() && (s.max.IsNull() || vm.CompareValues(val, s.max) > 0) {
			s.max = val
		}
	case "dc", aggEstDCE:
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
	case aggAnyVal:
		if !val.IsNull() && !s.hasFirst {
			s.first = val
			s.hasFirst = true
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
			if len(s.all) < maxValuesPerGroup {
				s.all = append(s.all, f)
			}
		}
	case aggPercW:
		updateWeightedPercentileState(s, val, weightVal)
	}
}

func finalizeEventStatsAgg(s *aggState, agg AggFunc) event.Value {
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
	case aggSumObj:
		return objectSumValue(s.objectSum)
	case aggPerc:
		return finalizePercentile(s, agg.Quantile)
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
	case aggPercW:
		return finalizePercentile(s, agg.Quantile)
	case aggSkew:
		return finalizeSkewness(s)
	case aggKurt:
		return finalizeKurtosis(s)
	case aggMAD:
		return finalizeMAD(s)
	case aggHist:
		return finalizeHistogram(s, agg.Limit)
	case aggDeltaSum:
		return finalizeAggState(s, agg.Name)
	}
	return finalizeAggState(s, agg.Name)
}

func finalizeAggState(s *aggState, fn string) event.Value {
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
	case aggMin:
		return s.min
	case aggMax:
		return s.max
	case "dc":
		return event.IntValue(int64(len(s.values)))
	case aggEstDCE:
		return event.FloatValue(0)
	case aggValues, aggList:
		var strs []string
		for _, v := range s.all {
			strs = append(strs, fmt.Sprintf("%v", v))
		}

		return event.StringValue(strings.Join(strs, "|||"))
	case aggMode:
		return modeFromCounts(s.mode)
	case aggAnyVal, aggArgMax, aggArgMin:
		return s.first
	}

	return event.NullValue()
}
