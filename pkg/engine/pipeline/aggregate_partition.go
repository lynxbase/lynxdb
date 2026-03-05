package pipeline

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/bits"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

// DefaultAggPartitions is the default number of hash partitions for
// aggregate spill. Must be a power of two for bit-shift partitioning.
const defaultAggPartitions = 64

// MaxAggPartitions caps the partition count to avoid excessive file handles.
const maxAggPartitions = 1024

// MinAggPartitions is the floor to ensure meaningful partitioning.
const minAggPartitions = 8

// aggPartitionSet manages hash-partitioned spill files for the aggregate
// operator. Groups are distributed by hash(groupKey) into K partition
// files. During merge, only one partition is loaded at a time, bounding
// peak memory to totalGroups/K.
//
// Lifecycle:
//  1. Created on first spill (lazy — nil until needed).
//  2. spillToPartitions() distributes groups across K writers.
//  3. mergePartitioned() processes one partition at a time.
//  4. close() releases all remaining files.
type aggPartitionSet struct {
	numPartitions int        // power of two, [8..1024]
	partPaths     [][]string // partPaths[partIdx] = list of spill file paths
	spillMgr      *SpillManager
	spillCount    int    // number of times spillToPartitions was called
	hashShift     uint   // bits to right-shift the hash before masking
	hashMask      uint64 // bitmask = numPartitions - 1 (power of two)
}

// newAggPartitionSet creates a partition set sized to fit approximately
// budget/4 worth of groups per partition. The partition count is clamped
// to [minAggPartitions, maxAggPartitions] and rounded to a power of two.
func newAggPartitionSet(groupBytes, budgetLimit int64, mgr *SpillManager) *aggPartitionSet {
	numPart := defaultAggPartitions

	if budgetLimit > 0 {
		// Target: each partition fits in 25% of budget.
		quarter := budgetLimit / 4
		if quarter > 0 && groupBytes > quarter {
			raw := int(groupBytes / quarter)
			if raw < minAggPartitions {
				raw = minAggPartitions
			}
			if raw > maxAggPartitions {
				raw = maxAggPartitions
			}
			// Round up to next power of two.
			numPart = 1 << bits.Len(uint(raw-1))
		}
	}

	if numPart < minAggPartitions {
		numPart = minAggPartitions
	}
	if numPart > maxAggPartitions {
		numPart = maxAggPartitions
	}

	// Use high bits of FNV-64a hash via right-shift.
	// For numPartitions=64, shift = 64 - 6 = 58.
	shift := uint(64 - bits.TrailingZeros(uint(numPart)))

	return &aggPartitionSet{
		numPartitions: numPart,
		partPaths:     make([][]string, numPart),
		spillMgr:      mgr,
		hashShift:     shift,
		hashMask:      uint64(numPart - 1),
	}
}

// partitionOf derives a partition index from a pre-computed FNV-64a hash.
// Uses the high bits of the hash (better distribution than low bits for
// FNV) via right-shift, masked to [0, numPartitions).
func (ps *aggPartitionSet) partitionOf(hash uint64) int {
	return int((hash >> ps.hashShift) & ps.hashMask)
}

// spillToPartitions distributes all groups from the iterator's group map
// into partition-specific spill files. Each partition gets its own file
// per spill invocation. Writers are created lazily (empty partitions get
// no file). Returns the number of groups written, or an error.
func (ps *aggPartitionSet) spillToPartitions(
	groups map[uint64][]*aggGroup,
	aggs []AggFunc,
	serializeFn func(*aggGroup, []AggFunc) map[string]event.Value,
) (int64, error) {
	writers := make([]*ColumnarSpillWriter, ps.numPartitions)

	var count int64
	var writeErr error

	for hash, chain := range groups {
		partIdx := ps.partitionOf(hash)
		for _, group := range chain {
			// Lazy writer creation.
			if writers[partIdx] == nil {
				w, err := NewColumnarSpillWriter(ps.spillMgr, fmt.Sprintf("agg-p%03d", partIdx))
				if err != nil {
					writeErr = err

					break
				}
				writers[partIdx] = w
			}

			row := serializeFn(group, aggs)
			if err := writers[partIdx].WriteRow(row); err != nil {
				writeErr = err

				break
			}
			count++
		}
		if writeErr != nil {
			break
		}
	}

	// Close all open writers and record paths.
	for i, w := range writers {
		if w == nil {
			continue
		}
		closeErr := w.CloseFile()
		if closeErr != nil && writeErr == nil {
			writeErr = closeErr
		}
		ps.partPaths[i] = append(ps.partPaths[i], w.Path())
	}

	if writeErr != nil {
		return count, fmt.Errorf("aggPartitionSet.spillToPartitions: %w", writeErr)
	}

	ps.spillCount++

	return count, nil
}

// mergePartitioned processes all partitions one at a time, calling the
// aggregate's merge logic for each partition to build the final result.
// This bounds peak merge memory to approximately totalGroups/numPartitions.
//
// The merge within each partition is budget-exempt (uses findOrCreateGroupMerge).
// Rationale: partitioning already reduces peak merge memory by numPartitions
// (typically 64x). Enforcing the per-query budget within each partition would
// require repartitioning logic that introduces double-counting complexity.
// Instead, we rely on the partition count to bound memory and log a warning
// if a single partition is unusually large.
func (ps *aggPartitionSet) mergePartitioned(
	a *AggregateIterator,
	inMemPartitions []map[uint64][]*aggGroup,
	result *Batch,
) error {
	for p := 0; p < ps.numPartitions; p++ {
		hasInMem := len(inMemPartitions[p]) > 0
		hasSpill := len(ps.partPaths[p]) > 0

		// Skip empty partitions.
		if !hasInMem && !hasSpill {
			continue
		}

		// Fast path: in-memory groups only (no spill files for this partition).
		// Emit directly without serialization round-trip.
		if hasInMem && !hasSpill {
			a.groups = inMemPartitions[p]
			emitPartitionGroups(a, result)

			continue
		}

		// Slow path: merge spill files (and optionally in-memory groups) for
		// this partition. Start with a clean groups map.
		a.groups = make(map[uint64][]*aggGroup)
		a.groupCount = 0

		// Restore in-memory groups for this partition first.
		if hasInMem {
			for h, chain := range inMemPartitions[p] {
				a.groups[h] = chain
				a.groupCount += len(chain)
			}
		}

		// Read all spill files for this partition and merge into a.groups.
		// Budget-exempt: partitioning already bounds peak memory to ~N/K.
		ps.mergePartitionFiles(a, p)

		// Warn if this partition is unusually large (>2x expected average).
		// This indicates hash skew that may cause higher-than-expected memory.
		avgGroupsPerPart := int(a.spilledRows) / ps.numPartitions
		if avgGroupsPerPart > 0 && a.groupCount > 2*avgGroupsPerPart {
			slog.Warn("aggregate: partition has disproportionate group count (hash skew)",
				"partition", p, "groups", a.groupCount,
				"avg_per_partition", avgGroupsPerPart)
		}

		// Emit finalized groups for this partition.
		emitPartitionGroups(a, result)

		// Release partition spill files (progressive cleanup).
		for _, path := range ps.partPaths[p] {
			ps.spillMgr.Release(path)
		}
		ps.partPaths[p] = nil
	}

	return nil
}

// mergePartitionFiles reads all spill files for the given partition and
// merges their rows into a.groups using budget-exempt group creation.
// The budget-exempt approach is safe because partitioning already bounds
// each partition's group count to approximately totalGroups/numPartitions.
func (ps *aggPartitionSet) mergePartitionFiles(a *AggregateIterator, partIdx int) {
	for _, path := range ps.partPaths[partIdx] {
		sr, err := NewColumnarSpillReader(path)
		if err != nil {
			slog.Error("aggregate: failed to open partition spill file",
				"path", path, "error", err, "partition", partIdx)

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
			group := a.findOrCreateGroupMerge(h, row)
			a.mergeAggStateFromRow(group, row)
		}
		sr.Close()
	}
}

// close releases all remaining partition spill files. Safe to call
// multiple times. Called from AggregateIterator.Close().
func (ps *aggPartitionSet) close() {
	if ps == nil {
		return
	}

	for i, paths := range ps.partPaths {
		for _, path := range paths {
			ps.spillMgr.Release(path)
		}
		ps.partPaths[i] = nil
	}
}

// emitPartitionGroups finalizes all groups currently in a.groups and
// appends the result rows to the batch.
func emitPartitionGroups(a *AggregateIterator, result *Batch) {
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
}

// serializeGroup converts an aggGroup's intermediate state into a
// map[string]event.Value suitable for spill serialization. The map
// contains group-by keys plus aggregation-specific suffixed keys
// (e.g., alias+"__sum", alias+"__count" for AVG).
func (a *AggregateIterator) serializeGroup(group *aggGroup, aggs []AggFunc) map[string]event.Value {
	row := make(map[string]event.Value, len(a.groupBy)+len(aggs)*3)
	for k, v := range group.key {
		row[k] = v
	}

	for j, agg := range aggs {
		s := &group.states[j]
		fn := strings.ToLower(agg.Name)

		switch fn {
		case aggAvg:
			row[agg.Alias+"__sum"] = event.FloatValue(s.sum)
			row[agg.Alias+"__count"] = event.IntValue(s.count)
		case aggSum:
			row[agg.Alias+"__sum"] = event.FloatValue(s.sum)
		case aggDC:
			if s.hll != nil {
				row[agg.Alias+"__hll"] = event.StringValue(
					encodeBase64(s.hll.MarshalBinary()))
			} else if s.values != nil {
				row[agg.Alias+"__dcvals"] = event.StringValue(
					joinMapKeys(s.values, "|"))
			}
		case aggValues:
			if len(s.all) > 0 {
				row[agg.Alias+"__vals"] = event.StringValue(
					joinAllStrings(s.all, "|||"))
			}
		case aggStdev:
			row[agg.Alias+"__sum"] = event.FloatValue(s.sum)
			row[agg.Alias+"__count"] = event.IntValue(s.count)
			m2 := s.sumSq
			if m2 == 0 && len(s.all) > 0 && s.count > 0 {
				mean := s.sum / float64(s.count)
				for _, v := range s.all {
					f := v.(float64)
					diff := f - mean
					m2 += diff * diff
				}
			}
			row[agg.Alias+"__sumsq"] = event.FloatValue(m2)
		case aggPerc50, aggPerc75, aggPerc90, aggPerc95, aggPerc99:
			if s.tdigest != nil {
				row[agg.Alias+"__tdigest"] = event.StringValue(
					encodeBase64(s.tdigest.MarshalBinary()))
			} else if len(s.all) > 0 {
				row[agg.Alias+"__percvals"] = event.StringValue(
					joinAllFloats(s.all, "|"))
			}
		default:
			row[agg.Alias] = a.finalizeState(s, agg.Name)
		}
	}

	return row
}

// mergeAggStateFromRow merges the aggregation state from a spill row
// into an existing aggGroup. Handles all agg types: AVG (sum/count
// tuples), SUM, DC (HLL/exact), VALUES, STDEV (parallel variance),
// PERC (t-digest), COUNT, MIN, MAX, FIRST, LAST.
func (a *AggregateIterator) mergeAggStateFromRow(group *aggGroup, row map[string]event.Value) {
	for j, agg := range a.aggs {
		fn := strings.ToLower(agg.Name)

		switch fn {
		case aggAvg:
			sumVal := row[agg.Alias+"__sum"]
			countVal := row[agg.Alias+"__count"]
			if sumF, ok := vm.ValueToFloat(sumVal); ok {
				group.states[j].sum += sumF
			}
			if countF, ok := vm.ValueToFloat(countVal); ok {
				group.states[j].count += int64(countF)
			}
		case aggSum:
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
