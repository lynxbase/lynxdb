package pipeline

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/memgov"
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
	// (treated as null). This is the LynxFlow v2 counterpart of the SPL2
	// count(eval(condition)) pattern; the old path continues to use Program
	// for that purpose and is unaffected by this field.
	CondProgram *vm.Program
}

// maxInMemoryGroups is a safety valve that prevents degenerate cases where
// estimateGroupBytes under-estimates. Even if the budget allows it, we cap
// in-memory groups at this count and force a spill. Configurable per-operator
// would be ideal, but this constant provides a reasonable default.
const maxInMemoryGroups = 10_000_000

// maxValuesPerGroup caps the number of distinct values tracked per aggregation
// group for values() and stdev. Prevents unbounded memory for high-cardinality groups.
const maxValuesPerGroup = 100_000

// mergePartitionsLegacy is the number of hash partitions used for the legacy
// (unpartitioned) spill file merge. During merge, K passes are made over the
// spill files; on pass k only groups with hash(groupKey) % K == k are loaded.
// This bounds peak merge memory to approximately totalGroups/K instead of
// loading all groups simultaneously. Must be >= 1.
const mergePartitionsLegacy = 16

// hllPromotionThreshold is the cardinality at which exact tracking is
// promoted to HyperLogLog approximation for distinct count.
const hllPromotionThreshold = 10_000

// defaultTDigestCompression is the compression parameter for t-digest
// percentile estimation. Higher values = more accurate but more memory.
const defaultTDigestCompression = 100.0

// Memory estimation constants for operator tracking.
// These are deliberately conservative (over-estimates) to avoid under-counting.
const (
	estimatedKeyBytes      int64 = 48 // per group-by key map entry (string key + Value + map overhead)
	estimatedAggStateBytes int64 = 64 // per aggState struct (count/sum/min/max/pointers)
)

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
	needsValues []bool // per-agg: true if dc/values/perc*/stdev need values map/all slice
	groupCount  int    // total groups across all chains
	spillFiles  []string
	spillErr    error                // first spill error; checked in Next() to abort query
	hasher      hash.Hash64          // persistent FNV-1a hasher, reset per call
	acct        memgov.MemoryAccount // per-operator memory tracking
	spillMgr    *SpillManager        // lifecycle manager for spill files (nil = unmanaged)
	spilledRows int64                // total rows written to spill files (for ResourceReporter)
	partitions  *aggPartitionSet     // nil until first partitioned spill (lazy init)
	budgetLimit int64                // query memory budget limit (0 = use default partitions)
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
	sumSq     float64      // accumulated M2 (sum of squared deviations) for stdev after spill merge
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
// The acct parameter is optional (nil = no memory tracking).
func NewAggregateIterator(child Iterator, aggs []AggFunc, groupBy []string, acct memgov.MemoryAccount) *AggregateIterator {
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
		acct:        memgov.EnsureAccount(acct),
	}
}

// NewAggregateIteratorWithSpill creates an aggregation operator with spill support.
// When the memory budget is exceeded, groups are spilled to disk via the SpillManager.
func NewAggregateIteratorWithSpill(child Iterator, aggs []AggFunc, groupBy []string, acct memgov.MemoryAccount, mgr *SpillManager) *AggregateIterator {
	a := NewAggregateIterator(child, aggs, groupBy, acct)
	a.spillMgr = mgr
	if ca, ok := a.acct.(*CoordinatedAccount); ok && mgr != nil {
		ca.SetOnRevoke(func(target int64) int64 {
			if a.groupCount == 0 {
				return 0
			}
			before := a.acct.Used()
			a.spillToDisk()
			if a.spillErr != nil {
				return 0
			}
			freed := before - a.acct.Used()
			if freed < 0 {
				return 0
			}

			return freed
		})
	}

	return a
}

func (a *AggregateIterator) Init(ctx context.Context) error {
	return a.child.Init(ctx)
}

func (a *AggregateIterator) Next(ctx context.Context) (*Batch, error) {
	if a.emitted {
		return nil, nil
	}

	// Transition to building phase — accumulating groups from input.
	if pn, ok := a.acct.(PhaseNotifier); ok {
		pn.SetPhase(PhaseBuilding)
	}

	// Consume all input.
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		batch, err := a.child.Next(ctx)
		if err != nil {
			// Bug fix: when the child (e.g., scan) fails because the shared
			// budget is exhausted, aggregate may hold spillable groups.
			// Spill those groups to free shared budget capacity, then retry.
			if memgov.IsMemoryExhausted(err) && a.groupCount > 0 && a.spillMgr != nil {
				a.spillToDisk()
				if a.spillErr != nil {
					return nil, fmt.Errorf("aggregate: spill on child budget pressure: %w", a.spillErr)
				}

				batch, err = a.child.Next(ctx)
				if err != nil {
					return nil, err
				}
			} else {
				return nil, err
			}
		}
		if batch == nil {
			break
		}
		if err := a.processBatch(batch); err != nil {
			return nil, err
		}
	}

	// Check if any spill operation failed — abort with error rather than
	// returning silently wrong partial results.
	if a.spillErr != nil {
		return nil, fmt.Errorf("aggregate: spill failed during execution: %w", a.spillErr)
	}

	a.emitted = true

	// Transition to probing phase — producing finalized output.
	if pn, ok := a.acct.(PhaseNotifier); ok {
		pn.SetPhase(PhaseProbing)
	}

	result := a.buildResult()
	// buildResultPartitioned may set spillErr during merge — check after.
	if a.spillErr != nil {
		return nil, fmt.Errorf("aggregate: merge failed: %w", a.spillErr)
	}

	return result, nil
}

func (a *AggregateIterator) Close() error {
	// Transition to complete phase — memory can be reclaimed by coordinator.
	if pn, ok := a.acct.(PhaseNotifier); ok {
		pn.SetPhase(PhaseComplete)
	}

	a.acct.Close()
	// Clean up legacy spill files (backward compat for spillMgr == nil path).
	for _, path := range a.spillFiles {
		if a.spillMgr != nil {
			a.spillMgr.Release(path)
		} else {
			removeSpillFile(path)
		}
	}
	a.spillFiles = nil
	// Clean up partitioned spill files.
	if a.partitions != nil {
		a.partitions.close()
		a.partitions = nil
	}

	return a.child.Close()
}

// MemoryUsed returns the current tracked memory for this operator.
func (a *AggregateIterator) MemoryUsed() int64 {
	return a.acct.Used()
}

func (a *AggregateIterator) Schema() []FieldInfo {
	var schema []FieldInfo
	for _, g := range a.groupBy {
		schema = append(schema, FieldInfo{Name: g, Type: "any"})
	}
	for _, agg := range a.aggs {
		schema = append(schema, FieldInfo{Name: agg.Alias, Type: aggResultType(agg.Name)})
	}

	return schema
}

// aggResultType returns the output type of an aggregate function. Group keys
// and input-dependent aggregates (sum, min, max, first, last, ...) stay "any"
// until typed scan schemas land with the logical-IR physical builder.
func aggResultType(name string) string {
	switch name {
	case aggCount, aggDC, aggRowNum:
		return "int"
	case aggAvg, aggRate, aggPerSec, aggPerMin, aggPerHr, aggPerDay,
		aggStdev, aggStdevP, aggVar, aggVarP, aggEstDCE,
		aggPerc, aggPerc25, aggPerc50, aggPerc75, aggPerc90, aggPerc95, aggPerc99, aggPercW,
		aggRunSum, aggMovAvg, aggDelta, aggAvgW, aggEntropy, aggCorr, aggCovar,
		aggMAD:
		return "float"
	case aggEarT, aggLatT:
		return "timestamp"
	case aggLinFit:
		return "object"
	case aggTopK, aggTopKW, aggValCnt, aggMaxN, aggMinN:
		return "array"
	default:
		return "any"
	}
}

func (a *AggregateIterator) processBatch(batch *Batch) error {
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
		group, err := a.findOrCreateGroup(h, row)
		if err != nil {
			return err
		}

		for j, agg := range a.aggs {
			val := a.extractValue(agg, row)
			orderVal := a.extractOrderValue(agg, row)
			weightVal := a.extractWeightValue(agg, row)
			a.updateState(&group.states[j], agg.Name, val, orderVal, weightVal, row)
		}
	}

	return nil
}

// estimateGroupBytes returns the estimated memory for a new aggregation group,
// including the key map entries and aggregation state structs. For string
// group-by fields with values longer than 8 bytes, actual string length is added.
func (a *AggregateIterator) estimateGroupBytes(row map[string]event.Value) int64 {
	base := int64(len(a.groupBy))*estimatedKeyBytes + int64(len(a.aggs))*estimatedAggStateBytes
	for _, g := range a.groupBy {
		v, ok := row[g]
		if ok {
			if s, ok := v.TryAsString(); ok && len(s) > 8 {
				base += int64(len(s))
			}
		}
	}

	return base
}

// ResourceStats implements ResourceReporter for per-operator spill metrics.
func (a *AggregateIterator) ResourceStats() OperatorResourceStats {
	return OperatorResourceStats{
		PeakBytes:   a.acct.MaxUsed(),
		SpilledRows: a.spilledRows,
	}
}

// spillToDisk writes current aggregation state to spill files and resets in-memory groups.
// When a SpillManager is available, groups are distributed across hash-partitioned
// files so that the merge phase can process one partition at a time, bounding peak
// merge memory to totalGroups/K instead of loading all groups simultaneously.
func (a *AggregateIterator) spillToDisk() {
	if a.spillMgr == nil {
		return // no spill support
	}

	// First spill: initialize partition set.
	if a.partitions == nil {
		groupBytes := int64(a.groupCount) * (estimatedKeyBytes + estimatedAggStateBytes*int64(len(a.aggs)))
		a.partitions = newAggPartitionSet(groupBytes, a.budgetLimit, a.spillMgr)
	}

	// Distribute groups to partition files.
	count, err := a.partitions.spillToPartitions(a.groups, a.aggs, a.serializeGroup)
	if err != nil {
		a.recordSpillError("partitioned spill", err)

		return
	}
	a.spilledRows += count

	// Reset in-memory groups and release tracked memory.
	a.groups = make(map[uint64][]*aggGroup)
	a.groupCount = 0
	a.acct.Shrink(a.acct.Used())

	// Notify coordinator that this operator has spilled, allowing rebalancing.
	if sn, ok := a.acct.(SpillNotifier); ok {
		sn.NotifySpilled()
	}
}

// recordSpillError records the first spill error. The error is checked in
// Next() to abort the query rather than returning silently wrong results.
func (a *AggregateIterator) recordSpillError(op string, err error) {
	if a.spillErr == nil {
		a.spillErr = fmt.Errorf("aggregate spill %s: %w", op, err)
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
// Returns a budget error if creating a new group would exceed the memory limit.
// When the budget is exceeded, the operator spills to disk and retries. If it
// still cannot grow, the error is propagated to the caller.
func (a *AggregateIterator) findOrCreateGroup(h uint64, row map[string]event.Value) (*aggGroup, error) {
	chain := a.groups[h]
	for _, g := range chain {
		if a.groupKeysEqual(g.key, row) {
			return g, nil
		}
	}

	// Estimate memory for the new group using actual key sizes.
	groupBytes := a.estimateGroupBytes(row)

	// Try to grow the budget.
	if err := a.acct.Grow(groupBytes); err != nil {
		// Budget exceeded — attempt to spill current groups to free memory.
		a.spillToDisk()

		// Retry the Grow after spilling.
		if retryErr := a.acct.Grow(groupBytes); retryErr != nil {
			return nil, fmt.Errorf("aggregate.findOrCreateGroup: still out of memory after spill: %w", retryErr)
		}
	}

	// Safety valve: cap in-memory groups even if budget allows it.
	// This prevents degenerate cases where estimateGroupBytes under-estimates.
	if a.groupCount >= maxInMemoryGroups {
		a.spillToDisk()
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

	return group, nil
}

// findOrCreateGroupMerge is a budget-exempt variant of findOrCreateGroup used
// during the merge phase. It looks up an existing group by hash or creates a new
// one without enforcing memory budget tracking. See mergeSpillFiles for rationale.
func (a *AggregateIterator) findOrCreateGroupMerge(h uint64, row map[string]event.Value) *aggGroup {
	chain := a.groups[h]
	for _, g := range chain {
		if a.groupKeysEqual(g.key, row) {
			return g
		}
	}

	group := &aggGroup{
		key:    a.extractGroupKey(row),
		states: make([]aggState, len(a.aggs)),
	}
	for j := range group.states {
		group.states[j].min = event.NullValue()
		group.states[j].max = event.NullValue()
		group.states[j].first = event.NullValue()
		group.states[j].last = event.NullValue()
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
	// Conditional aggregation (LynxFlow v2 path): evaluate the predicate
	// first; if it returns false/null, skip this row for this aggregate.
	// When CondProgram is nil (the common case and all SPL2 paths), this
	// branch is never taken — zero overhead.
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

func objectCountValue(counts map[string]int64) event.Value {
	fields := make(map[string]event.Value, len(counts))
	for key, count := range counts {
		fields[key] = event.IntValue(count)
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

func (a *AggregateIterator) buildResult() *Batch {
	// Partitioned path: spill files are distributed across hash partitions.
	// Process one partition at a time to bound merge memory.
	if a.partitions != nil {
		return a.buildResultPartitioned()
	}

	// Legacy path with spill files: use partitioned merge to bound memory.
	if len(a.spillFiles) > 0 {
		return a.mergeSpillFilesPartitioned()
	}

	// No spill: emit all in-memory groups directly.
	return a.emitAllGroups()
}

// emitAllGroups finalizes all in-memory groups into a result batch.
// Used when no spill files exist (everything fits in memory).
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
				if v.IsNull() {
					row[k] = event.StringValue("")
				} else {
					row[k] = v
				}
			}
			for j, agg := range a.aggs {
				row[agg.Alias] = a.finalizeAgg(&group.states[j], agg)
			}
			result.AddRow(row)
		}
	}

	return result
}

// buildResultPartitioned processes partitioned spill files one partition
// at a time, emitting finalized groups into a result batch. Peak merge
// memory is bounded to totalGroups/numPartitions.
func (a *AggregateIterator) buildResultPartitioned() *Batch {
	// Distribute remaining in-memory groups by partition.
	inMemPartitions := make([]map[uint64][]*aggGroup, a.partitions.numPartitions)
	for i := range inMemPartitions {
		inMemPartitions[i] = make(map[uint64][]*aggGroup)
	}
	totalInMem := 0
	for h, chain := range a.groups {
		partIdx := a.partitions.partitionOf(h)
		inMemPartitions[partIdx][h] = chain
		totalInMem += len(chain)
	}

	// Estimate total groups for result batch pre-allocation.
	estimatedTotal := totalInMem + int(a.spilledRows)
	capLimit := 1_000_000
	if estimatedTotal < capLimit {
		capLimit = estimatedTotal
	}
	result := NewBatch(capLimit)

	// Process each partition.
	if err := a.partitions.mergePartitioned(a, inMemPartitions, result); err != nil {
		a.spillErr = fmt.Errorf("aggregate: partitioned merge: %w", err)
		// Return partial results collected so far.
	}

	// Handle no-groups + no-group-by case.
	if result.Len == 0 && len(a.groupBy) == 0 {
		row := make(map[string]event.Value, len(a.aggs))
		for _, agg := range a.aggs {
			row[agg.Alias] = a.finalizeAgg(&aggState{values: make(map[string]bool)}, agg)
		}
		result.AddRow(row)
	}

	return result
}

// mergeSpillFilesPartitioned performs a hash-partitioned multi-pass merge of
// legacy (unpartitioned) spill files. Instead of loading ALL unique groups
// into memory simultaneously, it makes K passes over the spill files
// (K = mergePartitionsLegacy). On pass k, only rows whose
// hash(groupKey) % K == k are loaded and merged. After each pass, finalized
// groups are emitted to the result batch and the in-memory groups map is
// cleared. This bounds peak merge memory to approximately totalGroups/K
// instead of totalGroups.
//
// The merge within each partition is budget-exempt (uses findOrCreateGroupMerge).
// Rationale: partitioning already reduces peak memory by K; enforcing the
// per-query budget would cause infinite spill-merge-spill loops.
//
// For queries with very few spill files or groups, the overhead of K passes
// is minimal because the I/O is sequential and typically OS-cached after the
// first pass.
func (a *AggregateIterator) mergeSpillFilesPartitioned() *Batch {
	K := mergePartitionsLegacy
	if K < 1 {
		K = 1
	}

	// Pre-partition in-memory groups by hash bucket so we can process them
	// alongside the spill files on the matching pass.
	inMemPartitions := make([]map[uint64][]*aggGroup, K)
	for i := range inMemPartitions {
		inMemPartitions[i] = make(map[uint64][]*aggGroup)
	}
	totalInMem := 0
	for h, chain := range a.groups {
		partIdx := int(h % uint64(K))
		inMemPartitions[partIdx][h] = chain
		totalInMem += len(chain)
	}

	// Estimate result size for pre-allocation (capped to avoid huge allocs).
	estimatedTotal := totalInMem + int(a.spilledRows)
	capLimit := 1_000_000
	if estimatedTotal < capLimit {
		capLimit = estimatedTotal
	}
	if capLimit < 64 {
		capLimit = 64
	}
	result := NewBatch(capLimit)

	// Process each partition: load only groups matching this partition,
	// merge with spill files, emit, then clear.
	for partition := 0; partition < K; partition++ {
		// Start with a clean groups map for this partition.
		a.groups = make(map[uint64][]*aggGroup)
		a.groupCount = 0

		// Read all spill files, only processing rows in this partition.
		for _, path := range a.spillFiles {
			sr, err := NewSpillReader(path)
			if err != nil {
				slog.Error("aggregate: failed to open legacy spill file for partitioned merge",
					"path", path, "error", err, "partition", partition)

				continue
			}
			for {
				row, readErr := sr.ReadRow()
				if errors.Is(readErr, io.EOF) || row == nil {
					break
				}
				if readErr != nil {
					break
				}

				h := a.groupKeyHash(row)
				if int(h%uint64(K)) != partition {
					continue // skip rows not in this partition
				}

				group := a.findOrCreateGroupMerge(h, row)
				a.mergeAggStateFromSpillRow(group, row)
			}
			sr.Close()
		}

		// Merge the in-memory tail after older spill files to preserve row-order
		// state for first(), last(), list(), and delta-style aggregates.
		if len(inMemPartitions[partition]) > 0 {
			a.mergeInMemoryPartition(inMemPartitions[partition])
		}

		// Emit finalized groups for this partition into the result batch.
		emitPartitionGroups(a, result)
	}

	// Clean up spill files after all partitions are processed.
	for _, path := range a.spillFiles {
		if a.spillMgr != nil {
			a.spillMgr.Release(path)
		} else {
			removeSpillFile(path)
		}
	}
	a.spillFiles = nil

	// Clear the groups map (last partition's groups are no longer needed).
	a.groups = make(map[uint64][]*aggGroup)
	a.groupCount = 0

	// Handle no-groups + no-group-by case.
	if result.Len == 0 && len(a.groupBy) == 0 {
		row := make(map[string]event.Value, len(a.aggs))
		for _, agg := range a.aggs {
			row[agg.Alias] = a.finalizeAgg(&aggState{values: make(map[string]bool)}, agg)
		}
		result.AddRow(row)
	}

	return result
}

// mergeAggStateFromSpillRow merges aggregation state from a legacy spill row
// into an existing aggGroup. Handles all agg types with their suffixed-key
// serialization format. This is the legacy counterpart of mergeAggStateFromRow
// (which is used by the partitioned spill path in aggregate_partition.go).
func (a *AggregateIterator) mergeAggStateFromSpillRow(group *aggGroup, row map[string]event.Value) {
	for j, agg := range a.aggs {
		fn := strings.ToLower(agg.Name)
		switch fn {
		case aggAvg:
			// Read (sum, count) tuple from suffixed keys.
			sumVal := row[agg.Alias+"__sum"]
			countVal := row[agg.Alias+"__count"]
			if sumF, ok := vm.ValueToFloat(sumVal); ok {
				group.states[j].sum += sumF
			}
			if countF, ok := vm.ValueToFloat(countVal); ok {
				group.states[j].count += int64(countF)
			}
		case aggAvgW:
			a.mergeWeightedAvgFromRow(&group.states[j], row, agg.Alias)
		case aggCorr, aggCovar, aggLinFit:
			a.mergePairStatsFromRow(&group.states[j], row, agg.Alias)
		case aggSumObj:
			a.mergeObjectSumFromRow(&group.states[j], row, agg.Alias)
		case aggSkew, aggKurt:
			a.mergeMomentsFromRow(&group.states[j], row, agg.Alias)
		case aggMAD:
			a.mergeMADFromRow(&group.states[j], row, agg.Alias)
		case aggDeltaSum:
			a.mergeDeltaSumFromRow(&group.states[j], row, agg.Alias)
		case aggHist:
			a.mergeFloatValuesFromRow(&group.states[j], row, agg.Alias, "__histvals")
		case aggSum, aggSumSq, aggPerSec, aggPerMin, aggPerHr, aggPerDay:
			// Read raw sum from suffixed key.
			sumVal := row[agg.Alias+"__sum"]
			a.mergeSpilledValue(&group.states[j], agg.Name, sumVal)
		case aggRange:
			a.mergeRangeFromRow(&group.states[j], row, agg.Alias)
		case aggDC, aggEstDCE:
			a.mergeDCFromRow(&group.states[j], row, agg.Alias)
		case aggValues:
			a.mergeValuesFromRow(&group.states[j], row, agg.Alias)
		case aggList:
			a.mergeListFromRow(&group.states[j], row, agg.Alias)
		case aggMode:
			a.mergeModeFromRow(&group.states[j], row, agg.Alias)
		case aggTopK, aggValCnt, aggEntropy, aggMaxN, aggMinN:
			a.mergeTopKFromRow(&group.states[j], row, agg.Alias)
		case aggTopKW:
			a.mergeWeightedTopKFromRow(&group.states[j], row, agg.Alias)
		case "earliest":
			a.mergeEarliestValueFromRow(&group.states[j], row, agg.Alias)
		case "latest":
			a.mergeLatestValueFromRow(&group.states[j], row, agg.Alias)
		case aggArgMax:
			a.mergeArgValueFromRow(&group.states[j], row, agg.Alias, true)
		case aggArgMin:
			a.mergeArgValueFromRow(&group.states[j], row, agg.Alias, false)
		case aggEarT:
			a.mergeEarliestTimeFromRow(&group.states[j], row, agg.Alias)
		case aggLatT:
			a.mergeLatestTimeFromRow(&group.states[j], row, agg.Alias)
		case aggRate:
			a.mergeRateFromRow(&group.states[j], row, agg.Alias)
		case aggStdev, aggStdevP, aggVar, aggVarP:
			a.mergeStdevFromRow(&group.states[j], row, agg.Alias)
		case aggPerc, aggPerc25, aggPerc50, aggPerc75, aggPerc90, aggPerc95, aggPerc99, aggPercW:
			a.mergePercFromRow(&group.states[j], row, agg.Alias)
		default:
			val, ok := row[agg.Alias]
			if !ok {
				continue
			}
			a.mergeSpilledValue(&group.states[j], agg.Name, val)
		}
	}
}

// mergeSpilledValue merges a finalized aggregate value from a spill file into
// an existing aggregate state. AVG and SUM are handled directly in mergeSpillFiles
// using their raw (sum, count) tuple format; this method handles all other agg types.
func (a *AggregateIterator) mergeSpilledValue(s *aggState, fn string, val event.Value) {
	if val.IsNull() {
		return
	}
	switch strings.ToLower(fn) {
	case aggCount:
		if n, ok := vm.ValueToFloat(val); ok {
			s.count += int64(n)
		}
	case aggSum, aggSumSq, aggPerSec, aggPerMin, aggPerHr, aggPerDay:
		if f, ok := vm.ValueToFloat(val); ok {
			s.sum += f
			s.count++ // track that we have at least one contribution
		}
	case aggMin:
		if s.min.IsNull() || vm.CompareValues(val, s.min) < 0 {
			s.min = val
		}
	case aggMax:
		if s.max.IsNull() || vm.CompareValues(val, s.max) > 0 {
			s.max = val
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
	case "first", "earliest", aggAnyVal:
		if !s.hasFirst {
			s.first = val
			s.hasFirst = true
		}
	case "last", "latest":
		s.last = val
	case aggEarT:
		if f, ok := vm.ValueToFloat(val); ok {
			ts := time.Unix(0, int64(f*float64(time.Second)))
			if s.firstTS.IsZero() || ts.Before(s.firstTS) {
				s.firstTS = ts
			}
		}
	case aggLatT:
		if f, ok := vm.ValueToFloat(val); ok {
			ts := time.Unix(0, int64(f*float64(time.Second)))
			if s.lastTS.IsZero() || ts.After(s.lastTS) {
				s.lastTS = ts
			}
		}
	}
}

func (a *AggregateIterator) mergeDeltaSumFromRow(s *aggState, row map[string]event.Value, alias string) {
	countF, ok := vm.ValueToFloat(row[alias+"__count"])
	if !ok || countF == 0 {
		return
	}
	firstVal := row[alias+"__first_value"]
	lastVal := row[alias+"__last_value"]
	firstF, firstOK := vm.ValueToFloat(firstVal)
	lastF, lastOK := vm.ValueToFloat(lastVal)
	if !firstOK || !lastOK {
		return
	}
	segmentSum, _ := vm.ValueToFloat(row[alias+"__sum"])
	segmentCount := int64(countF)
	if !s.hasFirst {
		s.sum = segmentSum
		s.first = event.FloatValue(firstF)
		s.last = event.FloatValue(lastF)
		s.count = segmentCount
		s.hasFirst = true

		return
	}
	if currentLast, ok := vm.ValueToFloat(s.last); ok && firstF > currentLast {
		s.sum += firstF - currentLast
	}
	s.sum += segmentSum
	s.last = event.FloatValue(lastF)
	s.count += segmentCount
}

func (a *AggregateIterator) mergeFloatValuesFromRow(s *aggState, row map[string]event.Value, alias, suffix string) {
	if countF, ok := vm.ValueToFloat(row[alias+"__count"]); ok {
		s.count += int64(countF)
	}
	valuesVal, ok := row[alias+suffix]
	if !ok || valuesVal.IsNull() {
		return
	}
	valuesStr, _ := valuesVal.TryAsString()
	for _, part := range strings.Split(valuesStr, "|") {
		if part == "" || len(s.all) >= maxValuesPerGroup {
			continue
		}
		if f, err := strconv.ParseFloat(part, 64); err == nil {
			s.all = append(s.all, f)
		}
	}
}

// mergeDCFromRow merges distinct-count state from a spill row's suffixed columns.
func (a *AggregateIterator) mergeDCFromRow(s *aggState, row map[string]event.Value, alias string) {
	// Try HLL binary first.
	if hllVal, ok := row[alias+"__hll"]; ok && !hllVal.IsNull() {
		hllStr, _ := hllVal.TryAsString()
		data, err := decodeBase64(hllStr)
		if err == nil {
			other := UnmarshalHyperLogLog(data)
			if other != nil {
				if s.hll == nil {
					// Promote exact set to HLL before merging.
					s.hll = NewHyperLogLog()
					for k := range s.values {
						s.hll.Add(k)
					}
					s.values = nil
				}
				s.hll.Merge(other)

				return
			}
		}
	}
	// Try exact set.
	if dcvalsVal, ok := row[alias+"__dcvals"]; ok && !dcvalsVal.IsNull() {
		dcStr, _ := dcvalsVal.TryAsString()
		parts := strings.Split(dcStr, "|")
		for _, p := range parts {
			if p == "" {
				continue
			}
			if s.hll != nil {
				s.hll.Add(p)
			} else {
				if s.values == nil {
					s.values = make(map[string]bool)
				}
				s.values[p] = true
				// Promote to HLL if exact set gets too large.
				if len(s.values) > hllPromotionThreshold {
					s.hll = NewHyperLogLog()
					for k := range s.values {
						s.hll.Add(k)
					}
					s.values = nil
				}
			}
		}
	}
}

func (a *AggregateIterator) mergeRangeFromRow(s *aggState, row map[string]event.Value, alias string) {
	minVal := row[alias+"__min"]
	maxVal := row[alias+"__max"]
	countVal := row[alias+"__count"]
	countF, ok := vm.ValueToFloat(countVal)
	if !ok || countF == 0 {
		return
	}
	if f, ok := vm.ValueToFloat(minVal); ok {
		v := event.FloatValue(f)
		if s.count == 0 || vm.CompareValues(v, s.min) < 0 {
			s.min = v
		}
	}
	if f, ok := vm.ValueToFloat(maxVal); ok {
		v := event.FloatValue(f)
		if s.count == 0 || vm.CompareValues(v, s.max) > 0 {
			s.max = v
		}
	}
	s.count += int64(countF)
}

// mergeValuesFromRow merges values() state from a spill row's suffixed columns.
func (a *AggregateIterator) mergeValuesFromRow(s *aggState, row map[string]event.Value, alias string) {
	valsVal, ok := row[alias+"__vals"]
	if !ok || valsVal.IsNull() {
		return
	}
	valsStr, _ := valsVal.TryAsString()
	parts := strings.Split(valsStr, "|||")
	for _, p := range parts {
		if p == "" {
			continue
		}
		if len(s.all) >= maxValuesPerGroup {
			break
		}
		if s.values == nil {
			s.values = make(map[string]bool)
		}
		if !s.values[p] {
			s.values[p] = true
			s.all = append(s.all, p)
		}
	}
}

func (a *AggregateIterator) mergeListFromRow(s *aggState, row map[string]event.Value, alias string) {
	valsVal, ok := row[alias+"__listvals"]
	if !ok || valsVal.IsNull() {
		return
	}
	valsStr, _ := valsVal.TryAsString()
	parts := strings.Split(valsStr, "|||")
	for _, p := range parts {
		if len(s.all) >= maxValuesPerGroup {
			break
		}
		s.all = append(s.all, p)
	}
}

func (a *AggregateIterator) mergeModeFromRow(s *aggState, row map[string]event.Value, alias string) {
	countsVal, ok := row[alias+"__modecounts"]
	if !ok || countsVal.IsNull() {
		return
	}
	countsStr, _ := countsVal.TryAsString()
	counts, err := decodeModeCounts(countsStr)
	if err != nil {
		return
	}
	if s.mode == nil {
		s.mode = make(map[string]int64, len(counts))
	}
	for value, count := range counts {
		s.mode[value] += count
	}
}

func (a *AggregateIterator) mergeWeightedAvgFromRow(s *aggState, row map[string]event.Value, alias string) {
	if sumF, ok := vm.ValueToFloat(row[alias+"__sum"]); ok {
		s.sum += sumF
	}
	if weightF, ok := vm.ValueToFloat(row[alias+"__weight_sum"]); ok {
		s.weightSum += weightF
	}
}

func (a *AggregateIterator) mergePairStatsFromRow(s *aggState, row map[string]event.Value, alias string) {
	if countF, ok := vm.ValueToFloat(row[alias+"__count"]); ok {
		s.count += int64(countF)
	}
	if sumX, ok := vm.ValueToFloat(row[alias+"__sum_x"]); ok {
		s.sum += sumX
	}
	if sumY, ok := vm.ValueToFloat(row[alias+"__sum_y"]); ok {
		s.weightSum += sumY
	}
	if sumX2, ok := vm.ValueToFloat(row[alias+"__sum_x2"]); ok {
		s.sumSq += sumX2
	}
	if sumY2, ok := vm.ValueToFloat(row[alias+"__sum_y2"]); ok {
		s.sumY2 += sumY2
	}
	if sumXY, ok := vm.ValueToFloat(row[alias+"__sum_xy"]); ok {
		s.sumXY += sumXY
	}
}

func (a *AggregateIterator) mergeObjectSumFromRow(s *aggState, row map[string]event.Value, alias string) {
	val, ok := row[alias+"__object_sum"]
	if !ok || val.IsNull() {
		return
	}
	sums, ok := val.TryAsObject()
	if !ok {
		return
	}
	counts := map[string]event.Value{}
	if countVal, ok := row[alias+"__object_count"]; ok && !countVal.IsNull() {
		counts, _ = countVal.TryAsObject()
	}
	if s.objectSum == nil {
		s.objectSum = make(map[string]float64, len(sums))
	}
	if s.objectN == nil {
		s.objectN = make(map[string]int64, len(sums))
	}
	for key, sumVal := range sums {
		sum, ok := vm.ValueToFloat(sumVal)
		if !ok {
			continue
		}
		count := int64(1)
		if countVal, ok := counts[key]; ok {
			if countF, ok := vm.ValueToFloat(countVal); ok {
				count = int64(countF)
			}
		}
		s.objectSum[key] += sum
		s.objectN[key] += count
	}
}

func (a *AggregateIterator) mergeMomentsFromRow(s *aggState, row map[string]event.Value, alias string) {
	if countF, ok := vm.ValueToFloat(row[alias+"__count"]); ok {
		s.count += int64(countF)
	}
	if sumF, ok := vm.ValueToFloat(row[alias+"__sum"]); ok {
		s.sum += sumF
	}
	if sum2F, ok := vm.ValueToFloat(row[alias+"__sum2"]); ok {
		s.sumSq += sum2F
	}
	if sum3F, ok := vm.ValueToFloat(row[alias+"__sum3"]); ok {
		s.sumCube += sum3F
	}
	if sum4F, ok := vm.ValueToFloat(row[alias+"__sum4"]); ok {
		s.sumFourth += sum4F
	}
}

func topKStateValue(s *aggState) event.Value {
	return finalizeTopK(s, 0)
}

func weightedTopKStateValue(s *aggState) event.Value {
	return finalizeWeightedTopK(s, 0)
}

func (a *AggregateIterator) mergeTopKFromRow(s *aggState, row map[string]event.Value, alias string) {
	topVal, ok := row[alias+"__topk"]
	if !ok || topVal.IsNull() {
		return
	}
	entries, ok := topVal.TryAsArray()
	if !ok {
		return
	}
	for _, entry := range entries {
		obj, ok := entry.TryAsObject()
		if !ok {
			continue
		}
		val := obj["value"]
		count, ok := obj["count"].TryAsInt()
		if !ok {
			continue
		}
		updateTopKState(s, val, count)
	}
}

func (a *AggregateIterator) mergeWeightedTopKFromRow(s *aggState, row map[string]event.Value, alias string) {
	topVal, ok := row[alias+"__topk_weighted"]
	if !ok || topVal.IsNull() {
		return
	}
	entries, ok := topVal.TryAsArray()
	if !ok {
		return
	}
	for _, entry := range entries {
		obj, ok := entry.TryAsObject()
		if !ok {
			continue
		}
		updateWeightedTopKState(s, obj["value"], obj["weight"])
	}
}

func (a *AggregateIterator) mergeEarliestValueFromRow(s *aggState, row map[string]event.Value, alias string) {
	val := row[alias+"__first_value"]
	if val.IsNull() {
		return
	}
	if ts, ok := timeFromSecondsValue(row[alias+"__first_time"]); ok {
		if !s.hasFirst || s.firstTS.IsZero() || ts.Before(s.firstTS) {
			s.first = val
			s.firstTS = ts
			s.hasFirst = true
		}
		return
	}
	if !s.hasFirst {
		s.first = val
		s.hasFirst = true
	}
}

func (a *AggregateIterator) mergeLatestValueFromRow(s *aggState, row map[string]event.Value, alias string) {
	val := row[alias+"__last_value"]
	if val.IsNull() {
		return
	}
	if ts, ok := timeFromSecondsValue(row[alias+"__last_time"]); ok {
		if !s.hasFirst || s.lastTS.IsZero() || ts.After(s.lastTS) {
			s.last = val
			s.lastTS = ts
			s.hasFirst = true
		}
		return
	}
	s.last = val
	s.hasFirst = true
}

func (a *AggregateIterator) mergeArgValueFromRow(s *aggState, row map[string]event.Value, alias string, maxOrder bool) {
	val := row[alias+"__value"]
	orderVal := row[alias+"__order"]
	updateArgState(s, val, orderVal, maxOrder)
}

func (a *AggregateIterator) mergeEarliestTimeFromRow(s *aggState, row map[string]event.Value, alias string) {
	if ts, ok := timeFromSecondsValue(row[alias+"__earliest_time"]); ok && (s.firstTS.IsZero() || ts.Before(s.firstTS)) {
		s.firstTS = ts
	}
}

func (a *AggregateIterator) mergeLatestTimeFromRow(s *aggState, row map[string]event.Value, alias string) {
	if ts, ok := timeFromSecondsValue(row[alias+"__latest_time"]); ok && (s.lastTS.IsZero() || ts.After(s.lastTS)) {
		s.lastTS = ts
	}
}

func (a *AggregateIterator) mergeRateFromRow(s *aggState, row map[string]event.Value, alias string) {
	if ts, ok := timeFromSecondsValue(row[alias+"__first_time"]); ok && (s.firstTS.IsZero() || ts.Before(s.firstTS)) {
		s.firstTS = ts
		s.first = row[alias+"__first_value"]
	}
	if ts, ok := timeFromSecondsValue(row[alias+"__last_time"]); ok && (s.lastTS.IsZero() || ts.After(s.lastTS)) {
		s.lastTS = ts
		s.last = row[alias+"__last_value"]
	}
}

func timeFromSecondsValue(v event.Value) (time.Time, bool) {
	if v.IsNull() {
		return time.Time{}, false
	}
	f, ok := vm.ValueToFloat(v)
	if !ok {
		return time.Time{}, false
	}
	sec, frac := math.Modf(f)
	return time.Unix(int64(sec), int64(frac*1e9)), true
}

// mergeStdevFromRow merges stdev state from a spill row's suffixed columns.
// Uses the parallel variance formula: combined variance from (sum, count, sumSq) tuples.
func (a *AggregateIterator) mergeStdevFromRow(s *aggState, row map[string]event.Value, alias string) {
	sumVal := row[alias+"__sum"]
	countVal := row[alias+"__count"]
	sumsqVal := row[alias+"__sumsq"]

	newSum, sumOk := vm.ValueToFloat(sumVal)
	newCountF, countOk := vm.ValueToFloat(countVal)
	newSumSq, sqOk := vm.ValueToFloat(sumsqVal)
	if !sumOk || !countOk || !sqOk {
		return
	}
	newCount := int64(newCountF)
	if newCount == 0 {
		return
	}

	if s.count == 0 {
		// First batch — initialize directly.
		s.sum = newSum
		s.count = newCount
		s.sumSq = newSumSq
		s.all = nil // mark as merged state
	} else {
		// If we still have raw values from the in-memory batch, convert to M2 first.
		if s.sumSq == 0 && len(s.all) > 0 {
			mean := s.sum / float64(s.count)
			for _, v := range s.all {
				f := v.(float64)
				diff := f - mean
				s.sumSq += diff * diff
			}
			s.all = nil // transition to merged state
		}
		// Parallel variance merge (Chan et al.):
		// M2_combined = M2_a + M2_b + delta^2 * (n_a * n_b / (n_a + n_b))
		nA := float64(s.count)
		nB := float64(newCount)
		delta := (newSum/nB - s.sum/nA)
		s.sumSq = s.sumSq + newSumSq + delta*delta*(nA*nB/(nA+nB))
		s.sum += newSum
		s.count += newCount
	}
}

// mergePercFromRow merges percentile state from a spill row's suffixed columns.
// After merge, s.all is cleared — the t-digest becomes the single source of truth.
func (a *AggregateIterator) mergePercFromRow(s *aggState, row map[string]event.Value, alias string) {
	merged := false
	// Try t-digest binary first.
	if tdVal, ok := row[alias+"__tdigest"]; ok && !tdVal.IsNull() {
		tdStr, _ := tdVal.TryAsString()
		data, err := decodeBase64(tdStr)
		if err == nil {
			other := UnmarshalTDigest(data)
			if other != nil {
				if s.tdigest == nil {
					s.tdigest = NewTDigest(defaultTDigestCompression)
				}
				s.tdigest.Merge(other)
				merged = true
			}
		}
	}
	// Try raw float values.
	if !merged {
		if pvVal, ok := row[alias+"__percvals"]; ok && !pvVal.IsNull() {
			pvStr, _ := pvVal.TryAsString()
			floats := parseFloatList(pvStr, "|")
			if s.tdigest == nil {
				s.tdigest = NewTDigest(defaultTDigestCompression)
			}
			for _, f := range floats {
				s.tdigest.Add(f)
			}
			merged = true
		}
	}
	// Clear raw values after merge — tdigest is the source of truth.
	// In-memory raw values were already added to tdigest during updateState.
	if merged {
		s.all = nil
	}
}

func (a *AggregateIterator) mergeMADFromRow(s *aggState, row map[string]event.Value, alias string) {
	if countF, ok := vm.ValueToFloat(row[alias+"__count"]); ok {
		s.count += int64(countF)
	}
	if valuesVal, ok := row[alias+"__madvals"]; ok && !valuesVal.IsNull() {
		valuesStr, _ := valuesVal.TryAsString()
		for _, f := range parseFloatList(valuesStr, "|") {
			if len(s.all) < maxValuesPerGroup {
				s.all = append(s.all, f)
			}
		}
	}
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

func percentile(all []interface{}, pct float64) event.Value {
	if len(all) == 0 {
		return event.NullValue()
	}
	floats := float64SliceFromAll(all)
	sort.Float64s(floats)
	idx := pct / 100.0 * float64(len(floats)-1)
	lower := int(idx)
	if lower >= len(floats)-1 {
		return event.FloatValue(floats[len(floats)-1])
	}
	frac := idx - float64(lower)

	return event.FloatValue(floats[lower] + frac*(floats[lower+1]-floats[lower]))
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

// encodeBase64 encodes binary data to a base64 string for safe spill storage.
func encodeBase64(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

// decodeBase64 decodes a base64 string back to binary data.
func decodeBase64(s string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(s)
}

// joinMapKeys concatenates the keys of a map[string]bool with a separator.
func joinMapKeys(m map[string]bool, sep string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	return strings.Join(keys, sep)
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

func encodeModeCounts(counts map[string]int64) string {
	data, err := json.Marshal(counts)
	if err != nil {
		return "{}"
	}

	return string(data)
}

func decodeModeCounts(s string) (map[string]int64, error) {
	counts := make(map[string]int64)
	if err := json.Unmarshal([]byte(s), &counts); err != nil {
		return nil, err
	}

	return counts, nil
}

// joinAllFloats joins interface{} values (expected float64) with a separator.
func joinAllFloats(all []interface{}, sep string) string {
	strs := make([]string, len(all))
	for i, v := range all {
		strs[i] = strconv.FormatFloat(v.(float64), 'g', -1, 64)
	}

	return strings.Join(strs, sep)
}

// parseFloatList splits a separator-delimited string into float64 values.
func parseFloatList(s, sep string) []float64 {
	parts := strings.Split(s, sep)
	result := make([]float64, 0, len(parts))
	for _, p := range parts {
		if p == "" {
			continue
		}
		f, err := strconv.ParseFloat(p, 64)
		if err == nil {
			result = append(result, f)
		}
	}

	return result
}
