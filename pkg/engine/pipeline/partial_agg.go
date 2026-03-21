package pipeline

import (
	"container/heap"
	"encoding/binary"
	"hash"
	"hash/fnv"
	"math"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

// PartialAggSpec describes what to aggregate for pushdown.
type PartialAggSpec struct {
	GroupBy []string         `json:"group_by"`
	Funcs   []PartialAggFunc `json:"funcs"`
}

// PartialAggFunc describes a single aggregation function for partial aggregation.
type PartialAggFunc struct {
	Name   string `json:"name"`             // "count", "sum", "avg", "min", "max"
	Field  string `json:"field"`            // field to aggregate (empty for count)
	Alias  string `json:"alias"`            // output name
	Hidden bool   `json:"hidden,omitempty"` // if true, omitted from finalized output (e.g., auto-injected count for avg)
}

// PartialAggGroup holds partial aggregation results for one group.
type PartialAggGroup struct {
	Key    map[string]event.Value
	States []PartialAggState
}

// dcHLLThreshold is the cardinality at which dc() promotes from exact set
// tracking to HyperLogLog approximation. Below this threshold, dc() uses
// an exact map[string]bool. Above it, the set is converted to HLL (~0.8%
// error) to bound memory usage for high-cardinality fields in distributed
// partial aggregation.
//
// Configurable via config.ClusterConfig.DCHLLThreshold; call
// SetDCHLLThreshold at startup to override the default.
var dcHLLThreshold = 10_000

// SetDCHLLThreshold overrides the default dc() HLL promotion threshold.
// Must be called before any queries execute (typically at server startup).
func SetDCHLLThreshold(n int) {
	if n > 0 {
		dcHLLThreshold = n
	}
}

// PartialAggState holds running state for one group's one function.
// Fields are exported for serialization/deserialization by the MV subsystem.
// Per-function state independence: each struct is indexed by position in
// PartialAggSpec.Funcs. For `stats count, avg(X) by Y`: States[0] holds
// standalone count (Count=N), States[1] holds avg state (Sum=S, Count=M).
// Each struct has its own independent Count and Sum fields. No shared state.
type PartialAggState struct {
	Count       int64
	Sum         float64
	Min         event.Value
	Max         event.Value
	DistinctSet map[string]bool
	// DistinctHLL is used when the distinct set exceeds dcHLLThreshold.
	// When non-nil, DistinctSet is nil and cardinality is approximate.
	DistinctHLL *HyperLogLog
	// Digest holds t-digest state for approximate percentile computation
	// in partial aggregation. Merged across segments via TDigest.Merge().
	Digest *TDigest
	// StdevMean and StdevM2 hold Welford's online algorithm state for
	// numerically stable standard deviation in partial aggregation.
	// Merged across segments via Chan et al. parallel variance formula.
	StdevMean float64
	StdevM2   float64
}

// IsPushableAgg returns true if the aggregation function can be decomposed
// into partial aggregation + merge.
func IsPushableAgg(name string) bool {
	switch strings.ToLower(name) {
	case aggCount, aggSum, aggAvg, aggMin, aggMax, "dc",
		aggPerc50, aggPerc75, aggPerc90, aggPerc95, aggPerc99,
		aggStdev:
		return true
	default:
		return false
	}
}

// ComputePartialAgg computes partial aggregates from a slice of events.
func ComputePartialAgg(events []*event.Event, spec *PartialAggSpec) []*PartialAggGroup {
	groups := make(map[uint64][]*PartialAggGroup)

	for _, ev := range events {
		h := partialGroupKeyHash(ev, spec.GroupBy)
		group := findOrCreatePartialGroup(groups, h, ev, spec)

		for j, fn := range spec.Funcs {
			val := extractEventValue(ev, fn.Field)
			updatePartialState(&group.States[j], fn.Name, val)
		}
	}

	// Flatten collision chains into a single slice.
	var result []*PartialAggGroup
	for _, chain := range groups {
		result = append(result, chain...)
	}

	return result
}

// ComputePartialAggFromBatches runs partial aggregation on pipeline output batches.
// Used by the transform+partial-agg path where events have been transformed
// (rex, eval, etc.) before aggregation. Reads group-by and agg field values
// from batch columns instead of event.Event fields.
func ComputePartialAggFromBatches(batches []*Batch, spec *PartialAggSpec) []*PartialAggGroup {
	groups := make(map[uint64][]*PartialAggGroup)
	h := fnv.New64a()
	var buf [8]byte

	for _, batch := range batches {
		for i := 0; i < batch.Len; i++ {
			// Compute group key hash from batch columns.
			h.Reset()
			for _, g := range spec.GroupBy {
				v := batch.Value(g, i)
				hashValue(h, v, buf[:])
			}
			hval := h.Sum64()
			if len(spec.GroupBy) == 0 {
				hval = 0
			}

			// Find or create group.
			group := findOrCreateBatchGroup(groups, hval, batch, i, spec)

			// Update partial state for each aggregation function.
			for j, fn := range spec.Funcs {
				var val event.Value
				if fn.Field == "" {
					val = event.IntValue(1) // count(*)
				} else {
					val = batch.Value(fn.Field, i)
				}
				updatePartialState(&group.States[j], fn.Name, val)
			}
		}
	}

	var result []*PartialAggGroup
	for _, chain := range groups {
		result = append(result, chain...)
	}

	return result
}

// findOrCreateBatchGroup looks up or creates a partial agg group for a batch row.
func findOrCreateBatchGroup(groups map[uint64][]*PartialAggGroup, h uint64, batch *Batch, row int, spec *PartialAggSpec) *PartialAggGroup {
	chain := groups[h]
	for _, g := range chain {
		if batchKeysEqual(g.Key, batch, row, spec.GroupBy) {
			return g
		}
	}
	group := &PartialAggGroup{
		Key:    extractBatchGroupKey(batch, row, spec.GroupBy),
		States: make([]PartialAggState, len(spec.Funcs)),
	}
	for j := range group.States {
		group.States[j].Min = event.NullValue()
		group.States[j].Max = event.NullValue()
	}
	groups[h] = append(chain, group)

	return group
}

// extractBatchGroupKey extracts group-by field values from a batch row.
func extractBatchGroupKey(batch *Batch, row int, groupBy []string) map[string]event.Value {
	key := make(map[string]event.Value, len(groupBy))
	for _, g := range groupBy {
		key[g] = batch.Value(g, row)
	}

	return key
}

// batchKeysEqual checks if a group's stored key matches a batch row's fields.
func batchKeysEqual(key map[string]event.Value, batch *Batch, row int, groupBy []string) bool {
	for _, g := range groupBy {
		if key[g] != batch.Value(g, row) {
			return false
		}
	}

	return true
}

// MergePartialAggs merges multiple slices of partial groups into final result rows.
func MergePartialAggs(partials [][]*PartialAggGroup, spec *PartialAggSpec) []map[string]event.Value {
	merged := make(map[uint64][]*PartialAggGroup)

	for _, partial := range partials {
		for _, pg := range partial {
			h := partialGroupKeyHashFromMap(pg.Key, spec.GroupBy)
			found := false
			chain := merged[h]
			for _, existing := range chain {
				if partialKeysEqual(existing.Key, pg.Key, spec.GroupBy) {
					// Merge states.
					for j, fn := range spec.Funcs {
						mergePartialState(&existing.States[j], &pg.States[j], fn.Name)
					}
					found = true

					break
				}
			}
			if !found {
				// Clone so we don't mutate the original.
				clone := &PartialAggGroup{
					Key:    pg.Key,
					States: make([]PartialAggState, len(pg.States)),
				}
				copy(clone.States, pg.States)
				// Deep-copy DistinctSet maps, HLLs, and TDigests to avoid
				// shared-state mutation when subsequent groups merge into this clone.
				for j := range clone.States {
					if pg.States[j].DistinctSet != nil {
						clone.States[j].DistinctSet = make(map[string]bool, len(pg.States[j].DistinctSet))
						for k := range pg.States[j].DistinctSet {
							clone.States[j].DistinctSet[k] = true
						}
					}
					if pg.States[j].DistinctHLL != nil {
						data := pg.States[j].DistinctHLL.MarshalBinary()
						clone.States[j].DistinctHLL = UnmarshalHyperLogLog(data)
					}
					if pg.States[j].Digest != nil {
						data := pg.States[j].Digest.MarshalBinary()
						clone.States[j].Digest = UnmarshalTDigest(data)
					}
				}
				merged[h] = append(chain, clone)
			}
		}
	}

	// Finalize into result rows.
	var rows []map[string]event.Value
	for _, chain := range merged {
		for _, group := range chain {
			row := make(map[string]event.Value, len(spec.GroupBy)+len(spec.Funcs))
			for k, v := range group.Key {
				if v.IsNull() {
					row[k] = event.StringValue("")
				} else {
					row[k] = v
				}
			}
			for j, fn := range spec.Funcs {
				if fn.Hidden {
					continue
				}
				row[fn.Alias] = finalizePartialState(&group.States[j], fn.Name)
			}
			rows = append(rows, row)
		}
	}

	// Edge case: no groups, no group-by → emit one row with zero values.
	if len(rows) == 0 && len(spec.GroupBy) == 0 {
		row := make(map[string]event.Value, len(spec.Funcs))
		for _, fn := range spec.Funcs {
			if fn.Hidden {
				continue
			}
			row[fn.Alias] = finalizePartialState(&PartialAggState{
				Min: event.NullValue(),
				Max: event.NullValue(),
			}, fn.Name)
		}
		rows = append(rows, row)
	}

	return rows
}

// MergePartialGroupsNoFinalize merges multiple partial groups by key,
// combining intermediate state without finalizing. Used by MV compaction
// where we need to reduce segment count but preserve merge-ability.
// Returns merged groups with intermediate state (not finalized values).
func MergePartialGroupsNoFinalize(groups []*PartialAggGroup, spec *PartialAggSpec) []*PartialAggGroup {
	merged := make(map[uint64][]*PartialAggGroup)

	for _, pg := range groups {
		h := partialGroupKeyHashFromMap(pg.Key, spec.GroupBy)
		found := false
		chain := merged[h]
		for _, existing := range chain {
			if partialKeysEqual(existing.Key, pg.Key, spec.GroupBy) {
				for j, fn := range spec.Funcs {
					mergePartialState(&existing.States[j], &pg.States[j], fn.Name)
				}
				found = true

				break
			}
		}
		if !found {
			clone := &PartialAggGroup{
				Key:    pg.Key,
				States: make([]PartialAggState, len(pg.States)),
			}
			copy(clone.States, pg.States)
			// Deep-copy DistinctSet maps, HLLs, and TDigests to avoid mutation of original.
			for j := range clone.States {
				if pg.States[j].DistinctSet != nil {
					clone.States[j].DistinctSet = make(map[string]bool, len(pg.States[j].DistinctSet))
					for k := range pg.States[j].DistinctSet {
						clone.States[j].DistinctSet[k] = true
					}
				}
				if pg.States[j].DistinctHLL != nil {
					data := pg.States[j].DistinctHLL.MarshalBinary()
					clone.States[j].DistinctHLL = UnmarshalHyperLogLog(data)
				}
				if pg.States[j].Digest != nil {
					data := pg.States[j].Digest.MarshalBinary()
					clone.States[j].Digest = UnmarshalTDigest(data)
				}
			}
			merged[h] = append(chain, clone)
		}
	}

	var result []*PartialAggGroup
	for _, chain := range merged {
		result = append(result, chain...)
	}

	return result
}

// MergePartialAggsTopK merges partial aggregations and returns only the top K results.
// Uses a min-heap of size K during merge to avoid materializing all groups.
func MergePartialAggsTopK(partials [][]*PartialAggGroup, spec *PartialAggSpec, k int, sortFields []SortField) []map[string]event.Value {
	// First merge all partials normally.
	allRows := MergePartialAggs(partials, spec)

	if k <= 0 || k >= len(allRows) {
		return allRows
	}

	// Use a min-heap to keep only top K.
	h := &partialTopKHeap{
		fields: sortFields,
		limit:  k,
	}
	heap.Init(h)

	for _, row := range allRows {
		if h.Len() < k {
			heap.Push(h, row)
		} else if h.Len() > 0 && h.isBetter(row, h.rows[0]) {
			h.rows[0] = row
			heap.Fix(h, 0)
		}
	}

	// Extract in sorted order.
	n := h.Len()
	result := make([]map[string]event.Value, n)
	for i := n - 1; i >= 0; i-- {
		result[i] = heap.Pop(h).(map[string]event.Value)
	}

	return result
}

// partialTopKHeap is a min-heap for top-K selection on merged aggregation results.
type partialTopKHeap struct {
	rows   []map[string]event.Value
	fields []SortField
	limit  int
}

func (h *partialTopKHeap) Len() int { return len(h.rows) }

func (h *partialTopKHeap) Less(i, j int) bool {
	for _, sf := range h.fields {
		av := h.rows[i][sf.Name]
		bv := h.rows[j][sf.Name]
		cmp := vm.CompareValues(av, bv)
		if cmp == 0 {
			continue
		}
		if sf.Desc {
			return cmp < 0
		}

		return cmp > 0
	}

	return false
}

func (h *partialTopKHeap) Swap(i, j int) { h.rows[i], h.rows[j] = h.rows[j], h.rows[i] }

func (h *partialTopKHeap) Push(x interface{}) {
	h.rows = append(h.rows, x.(map[string]event.Value))
}

func (h *partialTopKHeap) Pop() interface{} {
	old := h.rows
	n := len(old)
	item := old[n-1]
	h.rows = old[:n-1]

	return item
}

func (h *partialTopKHeap) isBetter(a, b map[string]event.Value) bool {
	for _, sf := range h.fields {
		av := a[sf.Name]
		bv := b[sf.Name]
		cmp := vm.CompareValues(av, bv)
		if cmp == 0 {
			continue
		}
		if sf.Desc {
			return cmp > 0
		}

		return cmp < 0
	}

	return false
}

// partialGroupKeyHash computes FNV-1a hash of typed group-by values from an event.
func partialGroupKeyHash(ev *event.Event, groupBy []string) uint64 {
	if len(groupBy) == 0 {
		return 0
	}
	h := fnv.New64a()
	var buf [8]byte
	for _, g := range groupBy {
		v := extractEventValue(ev, g)
		hashValue(h, v, buf[:])
	}

	return h.Sum64()
}

// partialGroupKeyHashFromMap computes FNV-1a hash from a key map.
func partialGroupKeyHashFromMap(key map[string]event.Value, groupBy []string) uint64 {
	if len(groupBy) == 0 {
		return 0
	}
	h := fnv.New64a()
	var buf [8]byte
	for _, g := range groupBy {
		v := key[g]
		hashValue(h, v, buf[:])
	}

	return h.Sum64()
}

// hashValue writes a typed value into a hash.
func hashValue(h hash.Hash64, v event.Value, buf []byte) {
	if v.IsNull() {
		h.Write([]byte{0})

		return
	}
	switch v.Type() {
	case event.FieldTypeString:
		h.Write([]byte{1})
		s, _ := v.TryAsString()
		h.Write([]byte(s))
	case event.FieldTypeInt:
		h.Write([]byte{2})
		n, _ := v.TryAsInt()
		binary.LittleEndian.PutUint64(buf, uint64(n))
		h.Write(buf)
	case event.FieldTypeFloat:
		h.Write([]byte{3})
		f, _ := v.TryAsFloat()
		binary.LittleEndian.PutUint64(buf, math.Float64bits(f))
		h.Write(buf)
	case event.FieldTypeBool:
		h.Write([]byte{4})
		b, _ := v.TryAsBool()
		if b {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}
	case event.FieldTypeTimestamp:
		h.Write([]byte{5})
		t, _ := v.TryAsTimestamp()
		binary.LittleEndian.PutUint64(buf, uint64(t.UnixNano()))
		h.Write(buf)
	default:
		h.Write([]byte{0})
	}
}

// findOrCreatePartialGroup looks up or creates a partial agg group by hash.
func findOrCreatePartialGroup(groups map[uint64][]*PartialAggGroup, h uint64, ev *event.Event, spec *PartialAggSpec) *PartialAggGroup {
	chain := groups[h]
	for _, g := range chain {
		if partialKeysEqualEvent(g.Key, ev, spec.GroupBy) {
			return g
		}
	}
	group := &PartialAggGroup{
		Key:    extractEventGroupKey(ev, spec.GroupBy),
		States: make([]PartialAggState, len(spec.Funcs)),
	}
	for j := range group.States {
		group.States[j].Min = event.NullValue()
		group.States[j].Max = event.NullValue()
	}
	groups[h] = append(chain, group)

	return group
}

// extractEventGroupKey extracts group-by field values from an event.
func extractEventGroupKey(ev *event.Event, groupBy []string) map[string]event.Value {
	key := make(map[string]event.Value, len(groupBy))
	for _, g := range groupBy {
		key[g] = extractEventValue(ev, g)
	}

	return key
}

// extractEventValue extracts a field value from an event, handling built-in fields.
func extractEventValue(ev *event.Event, field string) event.Value {
	if field == "" {
		return event.IntValue(1) // count(*)
	}
	switch field {
	case "_raw":
		if ev.Raw != "" {
			return event.StringValue(ev.Raw)
		}
	case "host":
		if ev.Host != "" {
			return event.StringValue(ev.Host)
		}
	case "_source", "source":
		if ev.Source != "" {
			return event.StringValue(ev.Source)
		}
	case "_sourcetype", "sourcetype":
		if ev.SourceType != "" {
			return event.StringValue(ev.SourceType)
		}
	case "index":
		if ev.Index != "" {
			return event.StringValue(ev.Index)
		}
	case "_time":
		if !ev.Time.IsZero() {
			return event.TimestampValue(ev.Time)
		}
	}
	if ev.Fields != nil {
		if v, ok := ev.Fields[field]; ok {
			return v
		}
	}

	return event.NullValue()
}

// partialKeysEqualEvent checks if a group's stored key matches an event's fields.
func partialKeysEqualEvent(key map[string]event.Value, ev *event.Event, groupBy []string) bool {
	for _, g := range groupBy {
		if key[g] != extractEventValue(ev, g) {
			return false
		}
	}

	return true
}

// partialKeysEqual checks if two key maps are equal for the given group-by fields.
func partialKeysEqual(a, b map[string]event.Value, groupBy []string) bool {
	for _, g := range groupBy {
		if a[g] != b[g] {
			return false
		}
	}

	return true
}

// updatePartialState updates a partial aggregation state with a new value.
func updatePartialState(s *PartialAggState, fn string, val event.Value) {
	switch strings.ToLower(fn) {
	case aggCount:
		if !val.IsNull() {
			s.Count++
		}
	case aggSum:
		if f, ok := vm.ValueToFloat(val); ok {
			s.Sum += f
			s.Count++
		}
	case aggAvg:
		if f, ok := vm.ValueToFloat(val); ok {
			s.Sum += f
			s.Count++
		}
	case aggMin:
		if !val.IsNull() {
			if s.Min.IsNull() || vm.CompareValues(val, s.Min) < 0 {
				s.Min = val
			}
		}
	case aggMax:
		if !val.IsNull() {
			if s.Max.IsNull() || vm.CompareValues(val, s.Max) > 0 {
				s.Max = val
			}
		}
	case "dc":
		if !val.IsNull() {
			str := val.String()
			if s.DistinctHLL != nil {
				// Already promoted to HLL.
				s.DistinctHLL.Add(str)
			} else {
				if s.DistinctSet == nil {
					s.DistinctSet = make(map[string]bool)
				}
				s.DistinctSet[str] = true
				// Promote to HLL when exact set exceeds threshold.
				if len(s.DistinctSet) >= dcHLLThreshold {
					s.DistinctHLL = NewHyperLogLog()
					for k := range s.DistinctSet {
						s.DistinctHLL.Add(k)
					}
					s.DistinctSet = nil
				}
			}
		}
	case aggPerc50, aggPerc75, aggPerc90, aggPerc95, aggPerc99:
		if f, ok := vm.ValueToFloat(val); ok {
			if s.Digest == nil {
				s.Digest = NewTDigest(100)
			}
			s.Digest.Add(f)
		}
	case aggStdev:
		if f, ok := vm.ValueToFloat(val); ok {
			// Welford's online algorithm for numerically stable variance.
			s.Count++
			delta := f - s.StdevMean
			s.StdevMean += delta / float64(s.Count)
			delta2 := f - s.StdevMean
			s.StdevM2 += delta * delta2
		}
	}
}

// mergePartialState merges a source partial state into a destination.
func mergePartialState(dst, src *PartialAggState, fn string) {
	switch strings.ToLower(fn) {
	case aggCount:
		dst.Count += src.Count
	case aggSum:
		dst.Sum += src.Sum
		dst.Count += src.Count
	case aggAvg:
		dst.Sum += src.Sum
		dst.Count += src.Count
	case aggMin:
		if !src.Min.IsNull() {
			if dst.Min.IsNull() || vm.CompareValues(src.Min, dst.Min) < 0 {
				dst.Min = src.Min
			}
		}
	case aggMax:
		if !src.Max.IsNull() {
			if dst.Max.IsNull() || vm.CompareValues(src.Max, dst.Max) > 0 {
				dst.Max = src.Max
			}
		}
	case "dc":
		// If either side uses HLL, promote both to HLL and merge.
		if src.DistinctHLL != nil || dst.DistinctHLL != nil {
			// Ensure dst has an HLL.
			if dst.DistinctHLL == nil {
				dst.DistinctHLL = NewHyperLogLog()
				for k := range dst.DistinctSet {
					dst.DistinctHLL.Add(k)
				}
				dst.DistinctSet = nil
			}
			// Merge src into dst's HLL.
			if src.DistinctHLL != nil {
				dst.DistinctHLL.Merge(src.DistinctHLL)
			} else {
				for k := range src.DistinctSet {
					dst.DistinctHLL.Add(k)
				}
			}
		} else if src.DistinctSet != nil {
			// Both sides use exact sets — merge and check promotion.
			if dst.DistinctSet == nil {
				dst.DistinctSet = make(map[string]bool)
			}
			for k := range src.DistinctSet {
				dst.DistinctSet[k] = true
			}
			// Promote to HLL if merged set exceeds threshold.
			if len(dst.DistinctSet) >= dcHLLThreshold {
				dst.DistinctHLL = NewHyperLogLog()
				for k := range dst.DistinctSet {
					dst.DistinctHLL.Add(k)
				}
				dst.DistinctSet = nil
			}
		}
		// Accumulate count as upper-bound fallback for the backfill path
		// where the exact distinct set is lost but counts are preserved.
		dst.Count += src.Count
	case aggPerc50, aggPerc75, aggPerc90, aggPerc95, aggPerc99:
		if src.Digest != nil {
			if dst.Digest == nil {
				dst.Digest = NewTDigest(100)
			}
			dst.Digest.Merge(src.Digest)
		}
	case aggStdev:
		// Chan et al. parallel algorithm for combining Welford states.
		if src.Count == 0 {
			return
		}
		if dst.Count == 0 {
			dst.Count = src.Count
			dst.StdevMean = src.StdevMean
			dst.StdevM2 = src.StdevM2

			return
		}
		totalCount := dst.Count + src.Count
		delta := src.StdevMean - dst.StdevMean
		dst.StdevM2 += src.StdevM2 + delta*delta*float64(dst.Count)*float64(src.Count)/float64(totalCount)
		dst.StdevMean = (dst.StdevMean*float64(dst.Count) + src.StdevMean*float64(src.Count)) / float64(totalCount)
		dst.Count = totalCount
	}
}

// finalizePartialState produces the final value from a partial state.
func finalizePartialState(s *PartialAggState, fn string) event.Value {
	switch strings.ToLower(fn) {
	case aggCount:
		return event.IntValue(s.Count)
	case aggSum:
		return event.FloatValue(s.Sum)
	case aggAvg:
		if s.Count == 0 {
			return event.NullValue()
		}

		return event.FloatValue(s.Sum / float64(s.Count))
	case aggMin:
		return s.Min
	case aggMax:
		return s.Max
	case "dc":
		if s.DistinctHLL != nil {
			return event.IntValue(s.DistinctHLL.Count())
		}
		if len(s.DistinctSet) > 0 {
			return event.IntValue(int64(len(s.DistinctSet)))
		}
		// Fallback for the backfill path where the exact set is lost but
		// the count was preserved via serialization (dc_count field).
		if s.Count > 0 {
			return event.IntValue(s.Count)
		}

		return event.IntValue(0)
	case aggPerc50:
		return finalizeTDigest(s, 0.50)
	case aggPerc75:
		return finalizeTDigest(s, 0.75)
	case aggPerc90:
		return finalizeTDigest(s, 0.90)
	case aggPerc95:
		return finalizeTDigest(s, 0.95)
	case aggPerc99:
		return finalizeTDigest(s, 0.99)
	case aggStdev:
		if s.Count < 2 {
			return event.NullValue()
		}

		return event.FloatValue(math.Sqrt(s.StdevM2 / float64(s.Count-1)))
	}

	return event.NullValue()
}

// finalizeTDigest extracts a quantile from the partial state's t-digest.
func finalizeTDigest(s *PartialAggState, q float64) event.Value {
	if s.Digest == nil || s.Digest.Count() == 0 {
		return event.NullValue()
	}

	return event.FloatValue(s.Digest.Quantile(q))
}
