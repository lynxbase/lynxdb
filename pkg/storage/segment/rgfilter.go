package segment

import "strconv"

// RGFilterOp identifies the type of a row-group filter node.
type RGFilterOp uint8

const (
	// RGFilterAnd combines children with AND: first RGSkip child short-circuits.
	RGFilterAnd RGFilterOp = iota
	// RGFilterOr combines children with OR: first RGMaybe child short-circuits.
	RGFilterOr
	// RGFilterNot negates a child. Always returns RGMaybe (bloom/zone map can't prove absence).
	RGFilterNot
	// RGFilterTerm checks _raw bloom for a set of pre-tokenized terms (AND semantics).
	RGFilterTerm
	// RGFilterFieldEq checks field = value via const column, then bloom.
	RGFilterFieldEq
	// RGFilterFieldNeq checks field != value via const column only.
	RGFilterFieldNeq
	// RGFilterFieldRange checks field <op> value via zone map.
	RGFilterFieldRange
	// RGFilterFieldIn checks field IN (values) via const column, then bloom.
	RGFilterFieldIn
)

// RGVerdict is the result of evaluating a row-group filter node.
// Zero value is RGMaybe (scan it), which is the safe default.
type RGVerdict uint8

const (
	// RGMaybe means the row group may contain matching events — must scan it.
	RGMaybe RGVerdict = 0
	// RGSkip means the row group definitely does not contain matching events.
	RGSkip RGVerdict = 1
)

// RGFilterNode is a node in the row-group filter tree. Value semantics (no
// pointers between nodes) for cache locality during evaluation. Children are
// stored as a slice of nodes, not pointers.
type RGFilterNode struct {
	Op       RGFilterOp
	Children []RGFilterNode // value slice for cache locality (And, Or, Not)
	Field    string         // physical column name (_source, not source)
	Value    string         // for Eq/Neq/Term
	Values   []string       // for FieldIn
	Terms    []string       // pre-tokenized bloom terms (computed at build time)
	RangeOp  string         // ">", ">=", "<", "<=" (for FieldRange)
	RangeVal string         // comparison value string (for FieldRange)
}

// RGFilterStats accumulates skip statistics across all row groups evaluated
// by an RGFilterEvaluator. Counters are additive — callers may sum across
// multiple segments.
type RGFilterStats struct {
	ConstSkips    int // row groups skipped by const column mismatch
	PresenceSkips int // row groups skipped by column absence
	ZoneMapSkips  int // row groups skipped by zone map range exclusion
	BloomSkips    int // row groups skipped by per-column bloom filter
	TotalChecked  int // total row groups evaluated
	TotalSkipped  int // total row groups skipped (sum of above)
	BloomsChecked int // total bloom filter consultations (VictoriaLogs-style)
}

// RGFilterEvaluator evaluates an RGFilterNode tree against a specific segment
// reader. One evaluator is created per segment (the reader changes per segment,
// but the filter tree is query-scoped and reused).
//
// NOT thread-safe. Designed for single-goroutine Volcano pipeline.
type RGFilterEvaluator struct {
	root   *RGFilterNode
	reader *Reader
}

// NewRGFilterEvaluator creates an evaluator for the given filter tree and segment reader.
// Returns nil if root is nil (callers should check for nil before calling EvaluateRowGroup).
func NewRGFilterEvaluator(root *RGFilterNode, reader *Reader) *RGFilterEvaluator {
	if root == nil || reader == nil {
		return nil
	}

	return &RGFilterEvaluator{root: root, reader: reader}
}

// EvaluateRowGroup evaluates the filter tree against row group at rgIdx.
// Returns RGSkip if the row group can be definitively excluded, RGMaybe otherwise.
// Updates stats counters (caller-owned) with the reason for the skip.
func (e *RGFilterEvaluator) EvaluateRowGroup(rgIdx int, stats *RGFilterStats) RGVerdict {
	if stats != nil {
		stats.TotalChecked++
	}
	verdict := e.eval(e.root, rgIdx, stats)
	if verdict == RGSkip && stats != nil {
		stats.TotalSkipped++
	}

	return verdict
}

// eval recursively evaluates a filter node against a row group.
func (e *RGFilterEvaluator) eval(node *RGFilterNode, rgIdx int, stats *RGFilterStats) RGVerdict {
	switch node.Op {
	case RGFilterAnd:
		return e.evalAnd(node, rgIdx, stats)
	case RGFilterOr:
		return e.evalOr(node, rgIdx, stats)
	case RGFilterNot:
		// NOT: bloom/zone maps cannot prove the absence of a value, so we
		// conservatively return RGMaybe. The actual NOT filtering happens
		// at the row level during scan.
		return RGMaybe
	case RGFilterTerm:
		return e.evalTerm(node, rgIdx, stats)
	case RGFilterFieldEq:
		return e.evalFieldEq(node, rgIdx, stats)
	case RGFilterFieldNeq:
		return e.evalFieldNeq(node, rgIdx, stats)
	case RGFilterFieldRange:
		return e.evalFieldRange(node, rgIdx, stats)
	case RGFilterFieldIn:
		return e.evalFieldIn(node, rgIdx, stats)
	default:
		return RGMaybe
	}
}

// evalAnd: first child that returns RGSkip → short-circuit RGSkip.
func (e *RGFilterEvaluator) evalAnd(node *RGFilterNode, rgIdx int, stats *RGFilterStats) RGVerdict {
	for i := range node.Children {
		if e.eval(&node.Children[i], rgIdx, stats) == RGSkip {
			return RGSkip
		}
	}

	return RGMaybe
}

// evalOr: first child that returns RGMaybe → short-circuit RGMaybe.
// All children must be RGSkip for the OR to skip.
func (e *RGFilterEvaluator) evalOr(node *RGFilterNode, rgIdx int, stats *RGFilterStats) RGVerdict {
	for i := range node.Children {
		if e.eval(&node.Children[i], rgIdx, stats) == RGMaybe {
			return RGMaybe
		}
	}

	return RGSkip
}

// evalTerm checks _raw bloom filter for pre-tokenized search terms.
func (e *RGFilterEvaluator) evalTerm(node *RGFilterNode, rgIdx int, stats *RGFilterStats) RGVerdict {
	if len(node.Terms) == 0 {
		return RGMaybe
	}
	if stats != nil {
		stats.BloomsChecked++
	}
	mayContain, err := e.reader.CheckColumnBloomAllTerms(rgIdx, "_raw", node.Terms)
	if err != nil {
		return RGMaybe // conservative on error
	}
	if !mayContain {
		if stats != nil {
			stats.BloomSkips++
		}

		return RGSkip
	}

	return RGMaybe
}

// evalFieldEq checks field = value using layered checks (cheapest first):
// const column match → column presence → zone map exclusion → per-column bloom.
func (e *RGFilterEvaluator) evalFieldEq(node *RGFilterNode, rgIdx int, stats *RGFilterStats) RGVerdict {
	// Layer 1: Const column — O(1), definitive.
	if constVal, ok := e.reader.GetConstValue(rgIdx, node.Field); ok {
		if constVal != node.Value {
			if stats != nil {
				stats.ConstSkips++
			}

			return RGSkip
		}

		return RGMaybe // const matches — must scan rows (other predicates may filter)
	}

	// Layer 2: Column presence — O(1) bitmap check.
	if !e.reader.HasColumnInRowGroup(rgIdx, node.Field) {
		if stats != nil {
			stats.PresenceSkips++
		}

		return RGSkip
	}

	// Layer 3: Zone map — string comparison for range exclusion.
	if cc := e.reader.ColumnChunkInRowGroup(rgIdx, node.Field); cc != nil {
		// For equality: value must be within [min, max].
		if node.Value < cc.MinValue || node.Value > cc.MaxValue {
			if stats != nil {
				stats.ZoneMapSkips++
			}

			return RGSkip
		}
	}

	// Layer 4: Per-column bloom filter.
	if len(node.Terms) > 0 {
		if stats != nil {
			stats.BloomsChecked++
		}
		mayContain, err := e.reader.CheckColumnBloomAllTerms(rgIdx, node.Field, node.Terms)
		if err == nil && !mayContain {
			if stats != nil {
				stats.BloomSkips++
			}

			return RGSkip
		}
	}

	return RGMaybe
}

// evalFieldNeq checks field != value. Only const column can definitively skip:
// if the const value equals the excluded value, every row in this RG has that value,
// so all rows would be filtered out → skip.
func (e *RGFilterEvaluator) evalFieldNeq(node *RGFilterNode, rgIdx int, stats *RGFilterStats) RGVerdict {
	// Only const column is useful for != : if the entire RG has that one value,
	// and we're excluding it, the RG produces zero rows.
	if constVal, ok := e.reader.GetConstValue(rgIdx, node.Field); ok {
		if constVal == node.Value {
			if stats != nil {
				stats.ConstSkips++
			}

			return RGSkip
		}
	}

	// Zone map: if min == max == excluded value, skip.
	if cc := e.reader.ColumnChunkInRowGroup(rgIdx, node.Field); cc != nil {
		if cc.MinValue == node.Value && cc.MaxValue == node.Value {
			if stats != nil {
				stats.ZoneMapSkips++
			}

			return RGSkip
		}
	}

	return RGMaybe
}

// evalFieldRange checks field <op> value using zone maps.
// Zone map comparison uses string ordering (matching how min/max are stored).
// For numeric fields, the values are stored as their string representation,
// so we attempt numeric comparison when both values parse as numbers.
func (e *RGFilterEvaluator) evalFieldRange(node *RGFilterNode, rgIdx int, stats *RGFilterStats) RGVerdict {
	// Layer 1: Const column.
	if constVal, ok := e.reader.GetConstValue(rgIdx, node.Field); ok {
		if !evalRangeCheck(constVal, node.RangeOp, node.RangeVal) {
			if stats != nil {
				stats.ConstSkips++
			}

			return RGSkip
		}

		return RGMaybe
	}

	// Layer 2: Column presence.
	if !e.reader.HasColumnInRowGroup(rgIdx, node.Field) {
		if stats != nil {
			stats.PresenceSkips++
		}

		return RGSkip
	}

	// Layer 3: Zone map range disjointness.
	if cc := e.reader.ColumnChunkInRowGroup(rgIdx, node.Field); cc != nil {
		if zoneMapExcludesRange(cc.MinValue, cc.MaxValue, node.RangeOp, node.RangeVal) {
			if stats != nil {
				stats.ZoneMapSkips++
			}

			return RGSkip
		}
	}

	return RGMaybe
}

// EvalFieldIn checks field IN (values) using const column and bloom.
func (e *RGFilterEvaluator) evalFieldIn(node *RGFilterNode, rgIdx int, stats *RGFilterStats) RGVerdict {
	// Layer 1: Const column — if const value is not in the set, skip.
	if constVal, ok := e.reader.GetConstValue(rgIdx, node.Field); ok {
		found := false
		for _, v := range node.Values {
			if constVal == v {
				found = true

				break
			}
		}
		if !found {
			if stats != nil {
				stats.ConstSkips++
			}

			return RGSkip
		}

		return RGMaybe
	}

	// Layer 2: Column presence.
	if !e.reader.HasColumnInRowGroup(rgIdx, node.Field) {
		if stats != nil {
			stats.PresenceSkips++
		}

		return RGSkip
	}

	// Layer 3: Zone map — if all values are outside [min, max], skip.
	if cc := e.reader.ColumnChunkInRowGroup(rgIdx, node.Field); cc != nil {
		anyInRange := false
		for _, v := range node.Values {
			if v >= cc.MinValue && v <= cc.MaxValue {
				anyInRange = true

				break
			}
		}
		if !anyInRange {
			if stats != nil {
				stats.ZoneMapSkips++
			}

			return RGSkip
		}
	}

	return RGMaybe
}

// zoneMapExcludesRange returns true if the zone map [min, max] is entirely
// disjoint from the predicate (op, val). Uses numeric comparison when both
// zone map bounds and the predicate value parse as numbers.
func zoneMapExcludesRange(minVal, maxVal, op, val string) bool {
	// Try numeric comparison first (handles int and float zone map values).
	if numMin, errMin := strconv.ParseFloat(minVal, 64); errMin == nil {
		if numMax, errMax := strconv.ParseFloat(maxVal, 64); errMax == nil {
			if numVal, errVal := strconv.ParseFloat(val, 64); errVal == nil {
				return numericZoneMapExcludes(numMin, numMax, op, numVal)
			}
		}
	}
	// Fall back to string comparison (lexicographic).
	return stringZoneMapExcludes(minVal, maxVal, op, val)
}

// numericZoneMapExcludes checks disjointness with numeric comparison.
func numericZoneMapExcludes(minVal, maxVal float64, op string, val float64) bool {
	switch op {
	case ">":
		// Predicate: field > val. All values in [min, max] must be > val to maybe match.
		// If max <= val, no value in the RG can satisfy field > val.
		return maxVal <= val
	case ">=":
		// If max < val, no value satisfies field >= val.
		return maxVal < val
	case "<":
		// If min >= val, no value satisfies field < val.
		return minVal >= val
	case "<=":
		// If min > val, no value satisfies field <= val.
		return minVal > val
	}

	return false
}

// stringZoneMapExcludes checks disjointness with lexicographic comparison.
func stringZoneMapExcludes(minVal, maxVal, op, val string) bool {
	switch op {
	case ">":
		return maxVal <= val
	case ">=":
		return maxVal < val
	case "<":
		return minVal >= val
	case "<=":
		return minVal > val
	}

	return false
}

// evalRangeCheck evaluates a single value against a range predicate.
// Used for const column range checks.
func evalRangeCheck(value, op, target string) bool {
	// Try numeric comparison.
	if numVal, err1 := strconv.ParseFloat(value, 64); err1 == nil {
		if numTarget, err2 := strconv.ParseFloat(target, 64); err2 == nil {
			switch op {
			case ">":
				return numVal > numTarget
			case ">=":
				return numVal >= numTarget
			case "<":
				return numVal < numTarget
			case "<=":
				return numVal <= numTarget
			}
		}
	}

	// Fall back to string comparison.
	switch op {
	case ">":
		return value > target
	case ">=":
		return value >= target
	case "<":
		return value < target
	case "<=":
		return value <= target
	}

	return false
}
