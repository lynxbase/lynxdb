package pipeline

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"log/slog"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
	"github.com/OrlovEvgeny/Lynxdb/pkg/stats"
	"github.com/OrlovEvgeny/Lynxdb/pkg/vm"
)

// AggFunc describes an aggregation function.
type AggFunc struct {
	Name    string      // "count", "sum", "avg", "min", "max", "dc", "values", "first", "last"
	Field   string      // field to aggregate (empty for count)
	Alias   string      // output field name
	Program *vm.Program // optional compiled expression for nested eval
}

// maxInMemoryGroups is a safety valve that prevents degenerate cases where
// estimateGroupBytes under-estimates. Even if the budget allows it, we cap
// in-memory groups at this count and force a spill. Configurable per-operator
// would be ideal, but this constant provides a reasonable default.
const maxInMemoryGroups = 10_000_000

// maxValuesPerGroup caps the number of distinct values tracked per aggregation
// group for values() and stdev. Prevents unbounded memory for high-cardinality groups.
const maxValuesPerGroup = 100_000

// Memory estimation constants for BoundAccount tracking.
// These are deliberately conservative (over-estimates) to avoid under-counting.
const (
	estimatedKeyBytes      int64 = 48 // per group-by key map entry (string key + Value + map overhead)
	estimatedAggStateBytes int64 = 64 // per aggState struct (count/sum/min/max/pointers)
)

// Aggregation function name constants.
const (
	aggCount  = "count"
	aggSum    = "sum"
	aggAvg    = "avg"
	aggMin    = "min"
	aggMax    = "max"
	aggValues = "values"
	aggDC     = "dc"
	aggStdev  = "stdev"
	aggPerc50 = "perc50"
	aggPerc75 = "perc75"
	aggPerc90 = "perc90"
	aggPerc95 = "perc95"
	aggPerc99 = "perc99"
)

// AggregateIterator implements streaming hash aggregation (STATS command).
type AggregateIterator struct {
	child       Iterator
	aggs        []AggFunc
	groupBy     []string
	groups      map[uint64][]*aggGroup // FNV hash → collision chain
	emitted     bool
	vmInst      vm.VM
	needsValues []bool // per-agg: true if dc/values/perc*/stdev need values map/all slice
	groupCount  int    // total groups across all chains
	spillFiles  []string
	spillErr    error               // first spill error; checked in Next() to abort query
	hasher      hash.Hash64         // persistent FNV-1a hasher, reset per call
	acct        stats.MemoryAccount // per-operator memory tracking (nil *BoundAccount = no tracking)
	spillMgr    *SpillManager       // lifecycle manager for spill files (nil = unmanaged)
	spilledRows int64               // total rows written to spill files (for ResourceReporter)
	partitions  *aggPartitionSet    // nil until first partitioned spill (lazy init)
	budgetLimit int64               // query memory budget limit (0 = use default partitions)
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
	hll      *HyperLogLog // for approximate dc when cardinality exceeds threshold
	tdigest  *TDigest     // for approximate percentiles
	sumSq    float64      // accumulated M2 (sum of squared deviations) for stdev after spill merge
}

// NewAggregateIterator creates a streaming hash aggregation operator.
// The acct parameter is optional (nil = no memory tracking).
func NewAggregateIterator(child Iterator, aggs []AggFunc, groupBy []string, acct stats.MemoryAccount) *AggregateIterator {
	needsValues := make([]bool, len(aggs))
	for i, a := range aggs {
		switch strings.ToLower(a.Name) {
		case aggDC, aggValues, aggPerc50, aggPerc75, aggPerc90, aggPerc95, aggPerc99, aggStdev:
			needsValues[i] = true
		}
	}

	return &AggregateIterator{
		child:       child,
		aggs:        aggs,
		groupBy:     groupBy,
		groups:      make(map[uint64][]*aggGroup),
		needsValues: needsValues,
		hasher:      fnv.New64a(),
		acct:        stats.EnsureAccount(acct),
	}
}

// NewAggregateIteratorWithSpill creates an aggregation operator with spill support.
// When the memory budget is exceeded, groups are spilled to disk via the SpillManager.
func NewAggregateIteratorWithSpill(child Iterator, aggs []AggFunc, groupBy []string, acct stats.MemoryAccount, mgr *SpillManager) *AggregateIterator {
	a := NewAggregateIterator(child, aggs, groupBy, acct)
	a.spillMgr = mgr

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
			if stats.IsMemoryExhausted(err) && a.groupCount > 0 && a.spillMgr != nil {
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
			os.Remove(path)
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
		schema = append(schema, FieldInfo{Name: agg.Alias, Type: "any"})
	}

	return schema
}

func (a *AggregateIterator) processBatch(batch *Batch) error {
	row := make(map[string]event.Value, len(batch.Columns))
	for i := 0; i < batch.Len; i++ {
		// Populate reusable row map from columnar data.
		for k, col := range batch.Columns {
			if i < len(col) {
				row[k] = col[i]
			}
		}

		h := a.groupKeyHash(row)
		group, err := a.findOrCreateGroup(h, row)
		if err != nil {
			return err
		}

		for j, agg := range a.aggs {
			val := a.extractValue(agg, row)
			a.updateState(&group.states[j], agg.Name, val)
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
		if ok && v.Type() == event.FieldTypeString {
			s := v.AsString()
			if len(s) > 8 {
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
			h.Write([]byte(v.AsString()))
		case event.FieldTypeInt:
			h.Write([]byte{2})
			binary.LittleEndian.PutUint64(buf[:], uint64(v.AsInt()))
			h.Write(buf[:])
		case event.FieldTypeFloat:
			h.Write([]byte{3})
			binary.LittleEndian.PutUint64(buf[:], math.Float64bits(v.AsFloat()))
			h.Write(buf[:])
		case event.FieldTypeBool:
			h.Write([]byte{4})
			if v.AsBool() {
				h.Write([]byte{1})
			} else {
				h.Write([]byte{0})
			}
		case event.FieldTypeTimestamp:
			h.Write([]byte{5})
			binary.LittleEndian.PutUint64(buf[:], uint64(v.AsTimestamp().UnixNano()))
			h.Write(buf[:])
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
	if agg.Program != nil {
		result, err := a.vmInst.Execute(agg.Program, row)
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

func (a *AggregateIterator) updateState(s *aggState, fn string, val event.Value) {
	switch strings.ToLower(fn) {
	case aggCount:
		if !val.IsNull() {
			s.count++
		}
	case aggSum:
		if f, ok := vm.ValueToFloat(val); ok {
			s.sum += f
			s.count++
		}
	case aggAvg:
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
			s.values[str] = true
			// Switch to HLL when cardinality exceeds threshold.
			if len(s.values) > 10000 && s.hll == nil {
				s.hll = NewHyperLogLog()
				for k := range s.values {
					s.hll.Add(k)
				}
				s.values = nil // free exact set
			}
			if s.hll != nil {
				s.hll.Add(str)
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
	case "first":
		if !val.IsNull() && !s.hasFirst {
			s.first = val
			s.hasFirst = true
		}
	case "last":
		if !val.IsNull() {
			s.last = val
		}
	case aggPerc50, aggPerc75, aggPerc90, aggPerc95, aggPerc99:
		if f, ok := vm.ValueToFloat(val); ok {
			if s.tdigest == nil {
				s.tdigest = NewTDigest(100)
			}
			s.tdigest.Add(f)
			// Keep exact values only for small datasets; t-digest handles large ones.
			if len(s.all) < maxValuesPerGroup {
				s.all = append(s.all, f)
			}
		}
	case aggStdev:
		if f, ok := vm.ValueToFloat(val); ok {
			if len(s.all) < maxValuesPerGroup {
				s.all = append(s.all, f)
			}
			s.sum += f
			s.count++
		}
	case "earliest":
		if !val.IsNull() && !s.hasFirst {
			s.first = val
			s.hasFirst = true
		}
	case "latest":
		if !val.IsNull() {
			s.last = val
		}
	}
}

func (a *AggregateIterator) buildResult() *Batch {
	// Partitioned path: spill files are distributed across hash partitions.
	// Process one partition at a time to bound merge memory.
	if a.partitions != nil {
		return a.buildResultPartitioned()
	}

	// Legacy path: no spill occurred, or legacy unpartitioned spill files.
	if len(a.spillFiles) > 0 {
		a.mergeSpillFiles()
	}

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
			row[agg.Alias] = a.finalizeState(&aggState{values: make(map[string]bool)}, agg.Name)
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
				row[agg.Alias] = a.finalizeState(&group.states[j], agg.Name)
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
			row[agg.Alias] = a.finalizeState(&aggState{values: make(map[string]bool)}, agg.Name)
		}
		result.AddRow(row)
	}

	return result
}

// mergeSpillFiles reads back spill files and re-aggregates their rows.
// For AVG/SUM, spill files store raw (sum, count) tuples under suffixed keys
// to enable exact re-aggregation. Other agg types store finalized values.
//
// The merge phase does NOT enforce memory budget tracking. Rationale:
// during aggregation, the budget controlled spilling — the number of unique
// groups was bounded by the budget. During merge, we must reconstruct all
// unique groups to produce correct results. If total unique groups exceed
// memory, the query needs a larger budget (or fewer groups). Enforcing the
// budget here would cause an infinite spill→merge→spill loop or silently
// lose groups.
//
// LIMITATION: If a query has N unique groups that individually fit in
// memory batches but not all at once (e.g., 10M groups at 160 bytes each =
// ~1.6GB), the merge will load all N groups into memory simultaneously.
// A partitioned external aggregation (hash-partition into K buckets, merge
// one at a time) would bound merge memory to N/K, but is not yet implemented.
// If merge memory exceeds 2x the original budget, a warning is logged.
func (a *AggregateIterator) mergeSpillFiles() {
	// Track pre-merge group count to detect unbounded growth.
	preGroups := a.groupCount
	peakBudget := a.acct.MaxUsed()

	for _, path := range a.spillFiles {
		sr, err := NewSpillReader(path)
		if err != nil {
			continue
		}
		for {
			row, err := sr.ReadRow()
			if errors.Is(err, io.EOF) || row == nil {
				break
			}
			if err != nil {
				break
			}
			h := a.groupKeyHash(row)
			group := a.findOrCreateGroupMerge(h, row)
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
				case aggSum:
					// Read raw sum from suffixed key.
					sumVal := row[agg.Alias+"__sum"]
					a.mergeSpilledValue(&group.states[j], agg.Name, sumVal)
				case aggDC:
					a.mergeDCFromRow(&group.states[j], row, agg.Alias)
				case aggValues:
					a.mergeValuesFromRow(&group.states[j], row, agg.Alias)
				case aggStdev:
					a.mergeStdevFromRow(&group.states[j], row, agg.Alias)
				case aggPerc50, aggPerc75, aggPerc90, aggPerc95, aggPerc99:
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
		sr.Close()

		// Release through manager if available, else direct remove.
		if a.spillMgr != nil {
			a.spillMgr.Release(path)
		} else {
			os.Remove(path)
		}
	}
	a.spillFiles = nil

	// Warn if merge phase loaded significantly more groups than the spill budget
	// allowed. This indicates the query has very high cardinality across spill
	// batches and would benefit from partitioned external aggregation.
	mergeGroupBytes := int64(a.groupCount-preGroups) * (estimatedKeyBytes + estimatedAggStateBytes)
	if peakBudget > 0 && mergeGroupBytes > 2*peakBudget {
		slog.Warn("aggregate merge loaded groups exceeding 2x the original budget; "+
			"consider adding filters to reduce cardinality or increasing max_query_memory_bytes",
			"merge_groups", a.groupCount-preGroups,
			"merge_estimated_bytes", mergeGroupBytes,
			"peak_budget_bytes", peakBudget,
		)
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
	case aggSum:
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
	case "first", "earliest":
		if !s.hasFirst {
			s.first = val
			s.hasFirst = true
		}
	case "last", "latest":
		s.last = val
	}
}

// mergeDCFromRow merges distinct-count state from a spill row's suffixed columns.
func (a *AggregateIterator) mergeDCFromRow(s *aggState, row map[string]event.Value, alias string) {
	// Try HLL binary first.
	if hllVal, ok := row[alias+"__hll"]; ok && !hllVal.IsNull() {
		data, err := decodeBase64(hllVal.AsString())
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
		parts := strings.Split(dcvalsVal.AsString(), "|")
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
				if len(s.values) > 10000 {
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

// mergeValuesFromRow merges values() state from a spill row's suffixed columns.
func (a *AggregateIterator) mergeValuesFromRow(s *aggState, row map[string]event.Value, alias string) {
	valsVal, ok := row[alias+"__vals"]
	if !ok || valsVal.IsNull() {
		return
	}
	parts := strings.Split(valsVal.AsString(), "|||")
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
		data, err := decodeBase64(tdVal.AsString())
		if err == nil {
			other := UnmarshalTDigest(data)
			if other != nil {
				if s.tdigest == nil {
					s.tdigest = NewTDigest(100)
				}
				s.tdigest.Merge(other)
				merged = true
			}
		}
	}
	// Try raw float values.
	if !merged {
		if pvVal, ok := row[alias+"__percvals"]; ok && !pvVal.IsNull() {
			floats := parseFloatList(pvVal.AsString(), "|")
			if s.tdigest == nil {
				s.tdigest = NewTDigest(100)
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
	case aggValues:
		var strs []string
		for _, v := range s.all {
			strs = append(strs, fmt.Sprintf("%v", v))
		}

		return event.StringValue(strings.Join(strs, "|||"))
	case "first", "earliest":
		return s.first
	case "last", "latest":
		return s.last
	case aggPerc50:
		if s.tdigest != nil && (len(s.all) == 0 || len(s.all) > 10000) {
			return event.FloatValue(s.tdigest.Quantile(0.50))
		}

		return percentile(s.all, 50)
	case aggPerc75:
		if s.tdigest != nil && (len(s.all) == 0 || len(s.all) > 10000) {
			return event.FloatValue(s.tdigest.Quantile(0.75))
		}

		return percentile(s.all, 75)
	case aggPerc90:
		if s.tdigest != nil && (len(s.all) == 0 || len(s.all) > 10000) {
			return event.FloatValue(s.tdigest.Quantile(0.90))
		}

		return percentile(s.all, 90)
	case aggPerc95:
		if s.tdigest != nil && (len(s.all) == 0 || len(s.all) > 10000) {
			return event.FloatValue(s.tdigest.Quantile(0.95))
		}

		return percentile(s.all, 95)
	case aggPerc99:
		if s.tdigest != nil && (len(s.all) == 0 || len(s.all) > 10000) {
			return event.FloatValue(s.tdigest.Quantile(0.99))
		}

		return percentile(s.all, 99)
	case aggStdev:
		if s.count < 2 {
			return event.NullValue()
		}
		if len(s.all) == 0 {
			// Merged state (after spill merge): M2 was accumulated via parallel variance formula.
			return event.FloatValue(math.Sqrt(s.sumSq / float64(s.count-1)))
		}
		// Raw values: compute M2 from scratch.
		mean := s.sum / float64(s.count)
		var sumSqLocal float64
		for _, v := range s.all {
			f := v.(float64)
			diff := f - mean
			sumSqLocal += diff * diff
		}

		return event.FloatValue(math.Sqrt(sumSqLocal / float64(s.count-1)))
	}

	return event.NullValue()
}

func percentile(all []interface{}, pct float64) event.Value {
	if len(all) == 0 {
		return event.NullValue()
	}
	floats := make([]float64, len(all))
	for i, v := range all {
		floats[i] = v.(float64)
	}
	sort.Float64s(floats)
	idx := pct / 100.0 * float64(len(floats)-1)
	lower := int(idx)
	if lower >= len(floats)-1 {
		return event.FloatValue(floats[len(floats)-1])
	}
	frac := idx - float64(lower)

	return event.FloatValue(floats[lower] + frac*(floats[lower+1]-floats[lower]))
}

// Spill serialization helpers

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
func joinAllStrings(all []interface{}, sep string) string {
	strs := make([]string, len(all))
	for i, v := range all {
		strs[i] = fmt.Sprintf("%v", v)
	}

	return strings.Join(strs, sep)
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
