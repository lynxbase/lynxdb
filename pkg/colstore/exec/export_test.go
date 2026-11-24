package exec

import (
	"github.com/lynxbase/lynxdb/pkg/mem"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// Pushdown exposes the scan options derived from a filter.
var Pushdown = pushdown

// SetOrderedChunk changes how many rows of a block the aggregation in key
// order loads at once, and returns a function that restores it.
func SetOrderedChunk(rows int) (restore func()) {
	old := orderedChunk
	orderedChunk = rows

	return func() { orderedChunk = old }
}

// SetLimits changes when a worker's table spills and when workers' tables
// are merged in partitions, and returns a function that restores both.
func SetLimits(tableGroups, serial int) (restore func()) {
	oldTable, oldSerial := maxTableGroups, serialGroups
	maxTableGroups, serialGroups = tableGroups, serial

	return func() { maxTableGroups, serialGroups = oldTable, oldSerial }
}

// SetStageBudget changes the bytes of groups that the workers of a query
// hold together before they merge them while they scan, and returns a
// function that restores it.
func SetStageBudget(bytes int) (restore func()) {
	old := stageBudget
	stageBudget = bytes

	return func() { stageBudget = old }
}

// DistinctHash returns the hash that ApproxDistinct gives a value.
func DistinctHash(v Value) uint64 {
	switch v.Kind {
	case vec.KindInt:
		return hashInt(v.Int)
	case vec.KindFloat:
		return hashFloat(v.Float)
	case vec.KindBool:
		return hashBool(v.Bool)
	}

	return hashBytes(v.Kind, v.Bytes)
}

// DistinctEstimate returns what ApproxDistinct gives for a set of hashes.
func DistinctEstimate(hashes map[uint64]struct{}) int64 {
	a := mem.NewArena()
	defer a.Free()
	var h stateHeap
	h.init(a)
	var ref stateRef
	for hash := range hashes {
		ref = hllAdd(&h, ref, hash)
	}

	return hllCount(h.get(ref))
}

// SketchExactValues is the number of distinct numbers other than zero up to
// which a Percentile is exact.
const SketchExactValues = ddExactMax

// SketchValue returns what a Percentile of more distinct numbers reports for
// a quantile whose exact value is x.
func SketchValue(x float64) float64 {
	switch {
	case x > 0:
		return ddValue(ddKey(x))
	case x < 0:
		return -ddValue(ddKey(-x))
	}

	return 0
}
