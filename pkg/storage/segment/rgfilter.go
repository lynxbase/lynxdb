package segment

import (
	"log/slog"
	"math"
	"strconv"
	"time"

	"github.com/RoaringBitmap/roaring"
	bsi "github.com/RoaringBitmap/roaring/BitSliceIndexing"

	"github.com/lynxbase/lynxdb/pkg/storage/segment/index"
)

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
	ConstSkips        int   // row groups skipped by const column mismatch
	PresenceSkips     int   // row groups skipped by column absence
	ZoneMapSkips      int   // row groups skipped by zone map range exclusion
	BloomSkips        int   // row groups skipped by per-column bloom filter
	RangeBSIChecks    int   // total range BSI filter attempts
	RangeBSISkips     int   // row groups skipped because range BSI proved no matches
	RangeBSIMaskBytes int64 // approximate serialized bytes of attached BSI row masks
	TotalChecked      int   // total row groups evaluated
	TotalSkipped      int   // total row groups skipped (sum of above)
	BloomsChecked     int   // total bloom filter consultations (VictoriaLogs-style)
}

// RGFilterEvaluator evaluates an RGFilterNode tree against a specific segment
// reader. One evaluator is created per segment (the reader changes per segment,
// but the filter tree is query-scoped and reused).
//
// NOT thread-safe. Designed for single-goroutine Volcano pipeline.
type RGFilterEvaluator struct {
	root     *RGFilterNode
	reader   *Reader
	rowMasks map[int]*roaring.Bitmap // RG-local row masks accumulated per row group
}

// NewRGFilterEvaluator creates an evaluator for the given filter tree and segment reader.
// Returns nil if root is nil (callers should check for nil before calling EvaluateRowGroup).
func NewRGFilterEvaluator(root *RGFilterNode, reader *Reader) *RGFilterEvaluator {
	if root == nil || reader == nil {
		return nil
	}

	return &RGFilterEvaluator{root: root, reader: reader}
}

// RowMaskFor returns the accumulated BSI row mask for a row group, or nil when
// no mask was produced. The returned bitmap is owned by the evaluator; callers
// must clone it before mutating.
func (e *RGFilterEvaluator) RowMaskFor(rgIdx int) *roaring.Bitmap {
	if e == nil || e.rowMasks == nil {
		return nil
	}

	return e.rowMasks[rgIdx]
}

// ResetRowMasks clears BSI row masks accumulated for the current segment.
func (e *RGFilterEvaluator) ResetRowMasks() {
	if e == nil {
		return
	}
	clear(e.rowMasks)
}

// EvaluateRowGroup evaluates the filter tree against row group at rgIdx.
// Returns RGSkip if the row group can be definitively excluded, RGMaybe otherwise.
// Updates stats counters (caller-owned) with the reason for the skip.
func (e *RGFilterEvaluator) EvaluateRowGroup(rgIdx int, stats *RGFilterStats) RGVerdict {
	if stats != nil {
		stats.TotalChecked++
	}
	verdict := e.eval(e.root, rgIdx, stats, true)
	if verdict == RGSkip && stats != nil {
		stats.TotalSkipped++
	}

	return verdict
}

// eval recursively evaluates a filter node against a row group.
func (e *RGFilterEvaluator) eval(node *RGFilterNode, rgIdx int, stats *RGFilterStats, allowRowMasks bool) RGVerdict {
	switch node.Op {
	case RGFilterAnd:
		return e.evalAnd(node, rgIdx, stats, allowRowMasks)
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
		return e.evalFieldRange(node, rgIdx, stats, allowRowMasks)
	case RGFilterFieldIn:
		return e.evalFieldIn(node, rgIdx, stats)
	default:
		return RGMaybe
	}
}

// evalAnd: first child that returns RGSkip → short-circuit RGSkip.
func (e *RGFilterEvaluator) evalAnd(node *RGFilterNode, rgIdx int, stats *RGFilterStats, allowRowMasks bool) RGVerdict {
	for i := range node.Children {
		if e.eval(&node.Children[i], rgIdx, stats, allowRowMasks) == RGSkip {
			return RGSkip
		}
	}

	return RGMaybe
}

// evalOr: first child that returns RGMaybe → short-circuit RGMaybe.
// All children must be RGSkip for the OR to skip.
func (e *RGFilterEvaluator) evalOr(node *RGFilterNode, rgIdx int, stats *RGFilterStats) RGVerdict {
	for i := range node.Children {
		if e.eval(&node.Children[i], rgIdx, stats, false) == RGMaybe {
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
	// Const column — O(1), definitive.
	if constVal, ok := e.reader.GetConstValue(rgIdx, node.Field); ok {
		if constVal != node.Value {
			if stats != nil {
				stats.ConstSkips++
			}

			return RGSkip
		}

		return RGMaybe // const matches — must scan rows (other predicates may filter)
	}

	// Column presence — O(1) bitmap check.
	if !e.reader.HasColumnInRowGroup(rgIdx, node.Field) {
		if stats != nil {
			stats.PresenceSkips++
		}

		return RGSkip
	}

	// Zone map — string comparison for range exclusion.
	if cc := e.reader.ColumnChunkInRowGroup(rgIdx, node.Field); cc != nil {
		// For equality: value must be within [min, max].
		if node.Value < cc.MinValue || node.Value > cc.MaxValue {
			if stats != nil {
				stats.ZoneMapSkips++
			}

			return RGSkip
		}
	}

	// Per-column bloom filter.
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
func (e *RGFilterEvaluator) evalFieldRange(node *RGFilterNode, rgIdx int, stats *RGFilterStats, allowRowMasks bool) RGVerdict {
	// Const column check.
	if constVal, ok := e.reader.GetConstValue(rgIdx, node.Field); ok {
		if !evalRangeCheck(constVal, node.RangeOp, node.RangeVal) {
			if stats != nil {
				stats.ConstSkips++
			}

			return RGSkip
		}

		return RGMaybe
	}

	// Column presence.
	if !e.reader.HasColumnInRowGroup(rgIdx, node.Field) {
		if stats != nil {
			stats.PresenceSkips++
		}

		return RGSkip
	}

	// Zone map range disjointness.
	if cc := e.reader.ColumnChunkInRowGroup(rgIdx, node.Field); cc != nil {
		if zoneMapExcludesRange(cc.MinValue, cc.MaxValue, node.RangeOp, node.RangeVal) {
			if stats != nil {
				stats.ZoneMapSkips++
			}

			return RGSkip
		}
	}

	if stats != nil {
		stats.RangeBSIChecks++
	}
	mask, ok, err := e.tryBSIFilter(rgIdx, node)
	if err != nil {
		slog.Default().Warn("range BSI consultation failed", "err", err, "rg", rgIdx, "field", node.Field)

		return RGMaybe
	}
	if ok {
		if mask == nil || mask.IsEmpty() {
			if stats != nil {
				stats.RangeBSISkips++
			}

			return RGSkip
		}
		if allowRowMasks {
			e.attachRowMask(rgIdx, mask, stats)
		}
	}

	return RGMaybe
}

// tryBSIFilter loads the range BSI for the field and evaluates the range
// predicate. It returns ok=false when the column has no usable range BSI.
func (e *RGFilterEvaluator) tryBSIFilter(rgIdx int, node *RGFilterNode) (*roaring.Bitmap, bool, error) {
	if e.reader == nil || !e.reader.HasRangeBSI() {
		return nil, false, nil
	}

	meta, hasMeta, err := e.reader.LoadRangeMeta(rgIdx, node.Field)
	if err != nil {
		return nil, false, err
	}
	if !hasMeta {
		return nil, false, nil
	}

	idx, err := e.reader.LoadRangeBSI(rgIdx, node.Field)
	if err != nil {
		return nil, false, err
	}
	if idx == nil {
		return nil, false, nil
	}

	op, valueOrStart, end, direct, ok, err := lowerBSIPredicate(node, meta, idx)
	if err != nil || !ok {
		return nil, false, err
	}
	if direct != nil {
		return direct, true, nil
	}

	return idx.CompareValue(0, op, valueOrStart, end, nil), true, nil
}

func (e *RGFilterEvaluator) attachRowMask(rgIdx int, mask *roaring.Bitmap, stats *RGFilterStats) {
	if mask == nil {
		return
	}
	if e.rowMasks == nil {
		e.rowMasks = make(map[int]*roaring.Bitmap)
	}
	if stats != nil {
		stats.RangeBSIMaskBytes += int64(mask.GetSerializedSizeInBytes())
	}
	cur, ok := e.rowMasks[rgIdx]
	if !ok {
		e.rowMasks[rgIdx] = mask.Clone()

		return
	}

	cur.And(mask)
}

func lowerBSIPredicate(
	node *RGFilterNode,
	meta rangeMeta,
	idx *bsi.BSI,
) (bsi.Operation, int64, int64, *roaring.Bitmap, bool, error) {
	raw, ok, err := parseRangeBSIValue(meta.ValueKind, node.RangeVal, node.RangeOp)
	if err != nil {
		return 0, 0, 0, nil, false, err
	}
	if !ok {
		return 0, 0, 0, nil, false, nil
	}

	empty := func() *roaring.Bitmap { return roaring.New() }
	all := func() *roaring.Bitmap { return idx.GetExistenceBitmap().Clone() }

	switch node.RangeOp {
	case ">":
		if raw >= meta.MaxValue {
			return 0, 0, 0, empty(), true, nil
		}
		if raw < meta.MinValue {
			return 0, 0, 0, all(), true, nil
		}

		return bsi.GT, rangeBSIOffset(raw, meta.MinValue), 0, nil, true, nil
	case ">=":
		if raw > meta.MaxValue {
			return 0, 0, 0, empty(), true, nil
		}
		if raw <= meta.MinValue {
			return 0, 0, 0, all(), true, nil
		}

		return bsi.GE, rangeBSIOffset(raw, meta.MinValue), 0, nil, true, nil
	case "<":
		if raw <= meta.MinValue {
			return 0, 0, 0, empty(), true, nil
		}
		if raw > meta.MaxValue {
			return 0, 0, 0, all(), true, nil
		}

		return bsi.LT, rangeBSIOffset(raw, meta.MinValue), 0, nil, true, nil
	case "<=":
		if raw < meta.MinValue {
			return 0, 0, 0, empty(), true, nil
		}
		if raw >= meta.MaxValue {
			return 0, 0, 0, all(), true, nil
		}

		return bsi.LE, rangeBSIOffset(raw, meta.MinValue), 0, nil, true, nil
	default:
		return 0, 0, 0, nil, false, nil
	}
}

func parseRangeBSIValue(valueKind uint8, value, op string) (int64, bool, error) {
	switch valueKind {
	case index.RangeBSIValueInt:
		return parseIntRangeValue(value, op)
	case index.RangeBSIValueFloat64Bits:
		f, err := strconv.ParseFloat(value, 64)
		if err != nil || math.IsNaN(f) {
			return 0, false, nil
		}
		if math.IsInf(f, 0) {
			return 0, false, nil
		}

		return index.FloatToOrderedInt64(f), true, nil
	case index.RangeBSIValueTimestampNS:
		n, err := strconv.ParseInt(value, 10, 64)
		if err == nil {
			return n, true, nil
		}
		ts, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return 0, false, nil
		}

		return ts.UnixNano(), true, nil
	case index.RangeBSIValueBool:
		v, err := strconv.ParseBool(value)
		if err != nil {
			return 0, false, nil
		}

		return index.BoolToInt64(v), true, nil
	default:
		return 0, false, nil
	}
}

func parseIntRangeValue(value, op string) (int64, bool, error) {
	if n, err := strconv.ParseInt(value, 10, 64); err == nil {
		return n, true, nil
	}

	f, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(f) {
		return 0, false, nil
	}
	if math.IsInf(f, 0) {
		return 0, false, nil
	}

	switch op {
	case ">":
		f = math.Floor(f)
	case ">=":
		f = math.Ceil(f)
	case "<":
		f = math.Ceil(f)
	case "<=":
		f = math.Floor(f)
	}
	const (
		minInt64Float = -9223372036854775808.0
		maxInt64Float = 9223372036854775808.0
	)
	if f < minInt64Float || f >= maxInt64Float {
		return 0, false, nil
	}

	return int64(f), true, nil
}

func rangeBSIOffset(raw, minValue int64) int64 {
	return int64(uint64(raw) - uint64(minValue))
}

// EvalFieldIn checks field IN (values) using const column, presence, zone map, and bloom.
func (e *RGFilterEvaluator) evalFieldIn(node *RGFilterNode, rgIdx int, stats *RGFilterStats) RGVerdict {
	// Const column — if the const value is not in the set, skip.
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

	// Column presence.
	if !e.reader.HasColumnInRowGroup(rgIdx, node.Field) {
		if stats != nil {
			stats.PresenceSkips++
		}

		return RGSkip
	}

	// Zone map — skip if all values are outside [min, max].
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

	// Per-column bloom filter — if none of the tokenized terms appear
	// in the bloom, the row group cannot contain any of the IN values.
	if len(node.Terms) > 0 {
		if stats != nil {
			stats.BloomsChecked++
		}
		anyMayExist := false
		for _, term := range node.Terms {
			mayContain, err := e.reader.CheckColumnBloom(rgIdx, node.Field, term)
			if err != nil {
				anyMayExist = true // conservative on error

				break
			}
			if mayContain {
				anyMayExist = true

				break
			}
		}
		if !anyMayExist {
			if stats != nil {
				stats.BloomSkips++
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
