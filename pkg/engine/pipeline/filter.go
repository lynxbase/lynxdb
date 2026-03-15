package pipeline

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

// FilterIterator evaluates a predicate per row and drops non-matching rows.
type FilterIterator struct {
	child      Iterator
	predicate  *vm.Program // compiled WHERE/search predicate
	searchTerm string      // text search term (empty = no text search)
	vmInst     vm.VM
	// Vectorized fast-path: simple CompareExpr(FieldExpr, op, LiteralExpr) patterns.
	vecField string // field name for vectorized path
	vecOp    string // comparison operator
	vecValue string // literal value
	vecReady bool   // true if vectorized fast path is available
	vecUsed  bool   // true after vectorized fast path was actually used at least once
	// Compound vectorized plan: handles AND/OR trees, IN, NULL, LIKE, BETWEEN.
	vecPlan vecNode // recursive bitmap evaluator (nil = not available)
	// VM profiling (trace level only).
	profileVM bool
	vmCalls   int64
	vmTimeNS  int64
}

// NewFilterIterator creates a filter with a VM-compiled predicate.
func NewFilterIterator(child Iterator, predicate *vm.Program) *FilterIterator {
	return &FilterIterator{child: child, predicate: predicate}
}

// NewFilterIteratorWithExpr creates a filter that can attempt vectorized execution
// for simple CompareExpr(FieldExpr, op, LiteralExpr) patterns.
func NewFilterIteratorWithExpr(child Iterator, predicate *vm.Program, expr spl2.Expr) *FilterIterator {
	fi := &FilterIterator{child: child, predicate: predicate}

	// Try compound vectorized plan first (handles AND/OR, IN, NULL, LIKE, BETWEEN).
	if plan := analyzeVecExpr(expr); plan != nil {
		fi.vecPlan = plan
		fi.vecReady = true

		return fi
	}

	// Fall back to simple vectorizable pattern detection.
	// Skip vectorized path when the literal contains glob wildcards (* or ?)
	// because the vectorized path does exact string comparison which would be
	// incorrect — the compiler rewrites these to OpStrMatch (regex) instead.
	if cmp, ok := expr.(*spl2.CompareExpr); ok {
		if field, ok := cmp.Left.(*spl2.FieldExpr); ok {
			if lit, ok := cmp.Right.(*spl2.LiteralExpr); ok {
				if !strings.ContainsAny(lit.Value, "*?") {
					fi.vecField = field.Name
					fi.vecOp = cmp.Op
					fi.vecValue = lit.Value
					fi.vecReady = true
				}
			}
		}
	}

	return fi
}

// NewSearchFilterIterator creates a filter that does text search on _raw.
// The search term is lowered once at construction time to avoid per-row allocation.
func NewSearchFilterIterator(child Iterator, term string) *FilterIterator {
	return &FilterIterator{child: child, searchTerm: strings.ToLower(term)}
}

func (f *FilterIterator) Init(ctx context.Context) error {
	return f.child.Init(ctx)
}

func (f *FilterIterator) Next(ctx context.Context) (*Batch, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch, err := f.child.Next(ctx)
		if batch == nil || err != nil {
			return nil, err
		}

		// Try vectorized fast path for simple field op literal patterns.
		if f.vecReady {
			if result, ok := f.tryVectorizedFilter(batch); ok {
				f.vecUsed = true
				if result == nil {
					continue // all filtered out
				}

				return result, nil
			}
		}

		// Evaluate predicates with reusable row map, build match bitmap.
		matches := make([]bool, batch.Len)
		matchCount := 0
		row := make(map[string]event.Value, len(batch.Columns))
		for i := 0; i < batch.Len; i++ {
			for k, col := range batch.Columns {
				if i < len(col) {
					row[k] = col[i]
				}
			}
			if f.matchesRow(row) {
				matches[i] = true
				matchCount++
			}
		}

		if matchCount == 0 {
			continue // pull next batch
		}
		if matchCount == batch.Len {
			return batch, nil // all matched, return as-is
		}

		// Compact columns using bitmap.
		result := &Batch{
			Columns: make(map[string][]event.Value, len(batch.Columns)),
			Len:     matchCount,
		}
		for k, col := range batch.Columns {
			out := make([]event.Value, 0, matchCount)
			for i, v := range col {
				if i < batch.Len && matches[i] {
					out = append(out, v)
				}
			}
			result.Columns[k] = out
		}

		return result, nil
	}
}

// tryVectorizedFilter attempts to use typed column arrays for fast filtering.
// Returns (result, true) if vectorized path was used, (nil, false) otherwise.
func (f *FilterIterator) tryVectorizedFilter(batch *Batch) (*Batch, bool) {
	// Compound vectorized plan: recursive bitmap evaluation.
	if f.vecPlan != nil {
		matches, ok := f.vecPlan.evalBitmap(batch)
		if !ok {
			return nil, false
		}
		matchCount := CountTrue(matches)
		if matchCount == 0 {
			return nil, true // all filtered
		}
		if matchCount == batch.Len {
			return batch, true
		}

		return compactBatch(batch, matches, matchCount), true
	}

	// Simple (legacy) vectorized path: single field op literal.
	col, ok := batch.Columns[f.vecField]
	if !ok || len(col) == 0 {
		return nil, false
	}

	// Detect column type from first non-null value.
	var colType event.FieldType
	for _, v := range col {
		if !v.IsNull() {
			colType = v.Type()

			break
		}
	}

	var matches []bool
	switch colType {
	case event.FieldTypeInt:
		threshold, err := strconv.ParseInt(f.vecValue, 10, 64)
		if err != nil {
			return nil, false
		}
		intCol := make([]int64, len(col))
		for i, v := range col {
			if !v.IsNull() {
				intCol[i] = v.AsInt()
			}
		}
		switch f.vecOp {
		case ">":
			matches = FilterInt64GT(intCol, threshold)
		case ">=":
			matches = FilterInt64GTE(intCol, threshold)
		case "<":
			matches = FilterInt64LT(intCol, threshold)
		case "<=":
			matches = FilterInt64LTE(intCol, threshold)
		case "=", "==":
			matches = FilterInt64EQ(intCol, threshold)
		case "!=":
			matches = FilterInt64NE(intCol, threshold)
		default:
			return nil, false
		}
	case event.FieldTypeFloat:
		threshold, err := strconv.ParseFloat(f.vecValue, 64)
		if err != nil {
			return nil, false
		}
		fCol := make([]float64, len(col))
		for i, v := range col {
			if !v.IsNull() {
				fCol[i] = v.AsFloat()
			}
		}
		switch f.vecOp {
		case ">":
			matches = FilterFloat64GT(fCol, threshold)
		case ">=":
			matches = FilterFloat64GTE(fCol, threshold)
		case "<":
			matches = FilterFloat64LT(fCol, threshold)
		case "<=":
			matches = FilterFloat64LTE(fCol, threshold)
		case "=", "==":
			matches = FilterFloat64EQ(fCol, threshold)
		case "!=":
			matches = FilterFloat64NE(fCol, threshold)
		default:
			return nil, false
		}
	case event.FieldTypeString:
		sCol := make([]string, len(col))
		for i, v := range col {
			if !v.IsNull() {
				sCol[i] = v.AsString()
			}
		}
		switch f.vecOp {
		case "=", "==":
			matches = FilterStringEQ(sCol, f.vecValue)
		case "!=":
			matches = FilterStringNE(sCol, f.vecValue)
		case ">":
			matches = FilterStringGT(sCol, f.vecValue)
		case ">=":
			matches = FilterStringGTE(sCol, f.vecValue)
		case "<":
			matches = FilterStringLT(sCol, f.vecValue)
		case "<=":
			matches = FilterStringLTE(sCol, f.vecValue)
		default:
			return nil, false
		}
	default:
		return nil, false
	}

	matchCount := CountTrue(matches)
	if matchCount == 0 {
		return nil, true // all filtered
	}
	if matchCount == batch.Len {
		return batch, true
	}

	// Compact using bitmap.
	result := &Batch{
		Columns: make(map[string][]event.Value, len(batch.Columns)),
		Len:     matchCount,
	}
	for k, c := range batch.Columns {
		out := make([]event.Value, 0, matchCount)
		for i, v := range c {
			if i < batch.Len && matches[i] {
				out = append(out, v)
			}
		}
		result.Columns[k] = out
	}

	return result, true
}

func (f *FilterIterator) Close() error {
	return f.child.Close()
}

func (f *FilterIterator) Schema() []FieldInfo {
	return f.child.Schema()
}

// WasVectorized returns true if the vectorized filter fast path was used
// at least once during query execution.
func (f *FilterIterator) WasVectorized() bool {
	return f.vecUsed
}

func (f *FilterIterator) matchesRow(row map[string]event.Value) bool {
	// Text search — searchTerm is already lowered at construction time.
	if f.searchTerm != "" {
		raw, ok := row["_raw"]
		if !ok || raw.IsNull() {
			return false
		}
		if !containsFoldASCII(raw.String(), f.searchTerm) {
			return false
		}
	}
	if f.predicate != nil {
		if f.profileVM {
			start := time.Now()
			result, err := f.vmInst.Execute(f.predicate, row)
			f.vmTimeNS += time.Since(start).Nanoseconds()
			f.vmCalls++
			if err != nil {
				return false
			}

			return vm.IsTruthy(result)
		}
		result, err := f.vmInst.Execute(f.predicate, row)
		if err != nil {
			return false
		}

		return vm.IsTruthy(result)
	}

	return true
}

// SetProfileVM enables per-call VM timing collection (trace level).
func (f *FilterIterator) SetProfileVM(enable bool) {
	f.profileVM = enable
}

// VMStats returns the accumulated VM execution metrics.
func (f *FilterIterator) VMStats() (calls, timeNS int64) {
	return f.vmCalls, f.vmTimeNS
}

// SearchExprIterator evaluates full search expressions (with AND/OR/NOT,
// field comparisons, wildcards, IN, CASE, TERM) against event rows.
//
// Optimization: if the search expression contains keyword terms (searches on _raw),
// literal substrings are extracted and used as a strings.Contains pre-filter on the
// _raw column BEFORE reconstructing rows. This avoids the expensive per-row map
// construction for rows that can't possibly match.
//
// Fast path: for simple keyword searches (single SearchKeywordExpr with no
// wildcards, no field comparisons, no boolean combinators), the pre-filter
// result IS the final result — full evaluation is skipped entirely.
// Eliminates the ~35 ms double-evaluation overhead for simple searches.
//
// Vectorized fast path: for simple field=value comparisons (single
// SearchCompareExpr with OpEq, no wildcards), the column is iterated directly
// with strings.EqualFold instead of constructing per-row maps. This eliminates
// O(rows * columns) map writes and achieves 10-20x speedup on large batches.
type SearchExprIterator struct {
	child           Iterator
	evaluator       *spl2.SearchEvaluator
	preFilterLits   []string // literal substrings that must appear in _raw (AND semantics)
	isSimpleKeyword bool     // true when expr is a single keyword without wildcards
	// Vectorized fast path for field=value search comparisons.
	vecField         string // resolved field name (e.g., "_source" for "source")
	vecValue         string // literal value to compare
	vecOp            spl2.CompareOp
	vecCaseSensitive bool // true if CASE() directive was used
	vecReady         bool // true if vectorized fast path is available
}

// NewSearchExprIterator creates a new search expression filter.
func NewSearchExprIterator(child Iterator, eval *spl2.SearchEvaluator) *SearchExprIterator {
	return &SearchExprIterator{child: child, evaluator: eval}
}

// NewSearchExprIteratorWithExpr creates a search expression filter with
// pre-filter literals extracted from the expression for fast column-level rejection.
func NewSearchExprIteratorWithExpr(child Iterator, eval *spl2.SearchEvaluator, expr spl2.SearchExpr) *SearchExprIterator {
	lits := collectPreFilterLiterals(expr)

	// Detect simple keyword: single SearchKeywordExpr with no wildcards.
	// For this case, the pre-filter (containsFoldASCII on _raw) produces
	// identical results to the full evaluator — full eval can be skipped.
	isSimple := false
	if kw, ok := expr.(*spl2.SearchKeywordExpr); ok && !kw.HasWildcard && !kw.IsTermMatch && !kw.CaseSensitive {
		isSimple = true
	}

	si := &SearchExprIterator{
		child:           child,
		evaluator:       eval,
		preFilterLits:   lits,
		isSimpleKeyword: isSimple,
	}

	// Vectorized fast path: detect simple field op value patterns
	// (SearchCompareExpr without wildcards). These can be evaluated
	// directly on the column array without constructing per-row maps.
	if cmp, ok := expr.(*spl2.SearchCompareExpr); ok && !cmp.HasWildcard {
		// Resolve "source" alias to physical column name "_source",
		// matching the SearchEvaluator.evalCompare behavior.
		field := cmp.Field
		if field == "source" {
			field = "_source"
		}
		si.vecField = field
		si.vecValue = cmp.Value
		si.vecOp = cmp.Op
		si.vecCaseSensitive = cmp.CaseSensitive
		si.vecReady = true
	}

	return si
}

// collectPreFilterLiterals walks a SearchExpr and extracts literal substrings
// that can be used for fast strings.Contains pre-filtering on _raw.
// Only collects from AND branches and keyword terms (not OR or NOT).
func collectPreFilterLiterals(expr spl2.SearchExpr) []string {
	var lits []string
	collectLiterals(expr, &lits)

	return lits
}

func collectLiterals(expr spl2.SearchExpr, lits *[]string) {
	switch ex := expr.(type) {
	case *spl2.SearchAndExpr:
		// AND: both sides must match, so we can collect from both
		collectLiterals(ex.Left, lits)
		collectLiterals(ex.Right, lits)
	case *spl2.SearchOrExpr:
		// OR: can't pre-filter (either side might match)
	case *spl2.SearchNotExpr:
		// NOT: can't pre-filter (absence doesn't help contains)
	case *spl2.SearchKeywordExpr:
		if ex.Value == "*" {
			return // match-all, no useful literal
		}
		extracted := spl2.ExtractLiterals(ex.Value)
		if !ex.CaseSensitive {
			for i, l := range extracted {
				extracted[i] = strings.ToLower(l)
			}
		}
		*lits = append(*lits, extracted...)
	}
}

func (s *SearchExprIterator) Init(ctx context.Context) error {
	return s.child.Init(ctx)
}

func (s *SearchExprIterator) Next(ctx context.Context) (*Batch, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch, err := s.child.Next(ctx)
		if batch == nil || err != nil {
			return nil, err
		}

		// Vectorized fast path for field=value comparisons.
		// Iterates the field column directly with EqualFold/compare,
		// avoiding O(rows * columns) per-row map construction.
		if s.vecReady {
			if result, ok := s.tryVectorizedSearch(batch); ok {
				if result == nil {
					continue // all filtered out
				}
				return result, nil
			}
			// Vectorized path declined (e.g., column missing, needs JSON
			// extraction fallback). Fall through to per-row evaluation.
		}

		// Column-level pre-filter using _raw column.
		// If we have pre-filter literals, check _raw with strings.Contains
		// BEFORE reconstructing rows. This is much cheaper than full evaluation.
		candidates := make([]bool, batch.Len)
		hasPreFilter := len(s.preFilterLits) > 0
		candidateCount := 0

		if hasPreFilter {
			rawCol := batch.Columns["_raw"]
			for i := 0; i < batch.Len; i++ {
				if i >= len(rawCol) {
					continue
				}
				raw := rawCol[i].String()
				// Zero-alloc case-insensitive check: preFilterLits are already lowered
				// at construction time in collectLiterals().
				match := true
				for _, lit := range s.preFilterLits {
					if !containsFoldASCII(raw, lit) {
						match = false

						break
					}
				}
				if match {
					candidates[i] = true
					candidateCount++
				}
			}

			// Entire batch rejected by pre-filter.
			if candidateCount == 0 {
				continue
			}
		}

		// Fast path: for simple keywords (no wildcards, no field comparisons,
		// no boolean combinators), the pre-filter result IS the final result.
		// Skip full evaluation — both paths use the same containsFoldASCII
		// check on _raw, so results are identical.
		if s.isSimpleKeyword && hasPreFilter {
			if candidateCount == batch.Len {
				return batch, nil
			}

			return compactBatch(batch, candidates, candidateCount), nil
		}

		// Full evaluation on candidates only.
		matches := make([]bool, batch.Len)
		matchCount := 0
		row := make(map[string]event.Value, len(batch.Columns))
		for i := 0; i < batch.Len; i++ {
			// Skip rows that failed pre-filter
			if hasPreFilter && !candidates[i] {
				continue
			}
			for k, col := range batch.Columns {
				if i < len(col) {
					row[k] = col[i]
				}
			}
			if s.evaluator.Evaluate(row) {
				matches[i] = true
				matchCount++
			}
		}

		if matchCount == 0 {
			continue
		}
		if matchCount == batch.Len {
			return batch, nil
		}

		return compactBatch(batch, matches, matchCount), nil
	}
}

// tryVectorizedSearch attempts columnar evaluation of a SearchCompareExpr.
// Returns (result, true) if the vectorized path was used.
// Returns (nil, true) if all rows were filtered out.
// Returns (nil, false) if the vectorized path cannot handle this batch
// (e.g., column missing and JSON extraction fallback is needed).
func (s *SearchExprIterator) tryVectorizedSearch(batch *Batch) (*Batch, bool) {
	// Special case: field=* means "field exists" — check for non-null values.
	if s.vecOp == spl2.OpEq && s.vecValue == "*" {
		col, ok := batch.Columns[s.vecField]
		if !ok {
			return nil, true // column doesn't exist → no rows match
		}
		matches := make([]bool, batch.Len)
		matchCount := 0
		for i := 0; i < batch.Len && i < len(col); i++ {
			if !col[i].IsNull() {
				matches[i] = true
				matchCount++
			}
		}
		if matchCount == 0 {
			return nil, true
		}
		if matchCount == batch.Len {
			return batch, true
		}
		return compactBatch(batch, matches, matchCount), true
	}

	col, ok := batch.Columns[s.vecField]
	if !ok || len(col) == 0 {
		// Column missing — may need JSON extraction from _raw. Fall back to
		// per-row evaluation which has the JSON extraction fallback path.
		if _, rawOk := batch.Columns["_raw"]; rawOk {
			return nil, false // delegate to full evaluation
		}
		// No _raw either — no rows can match.
		return nil, true
	}

	caseInsensitive := !s.vecCaseSensitive

	switch s.vecOp {
	case spl2.OpEq:
		matches := make([]bool, batch.Len)
		matchCount := 0
		for i := 0; i < batch.Len && i < len(col); i++ {
			if col[i].IsNull() {
				continue
			}
			if stringEqualSearch(col[i].String(), s.vecValue, caseInsensitive) {
				matches[i] = true
				matchCount++
			}
		}
		if matchCount == 0 {
			return nil, true
		}
		if matchCount == batch.Len {
			return batch, true
		}
		return compactBatch(batch, matches, matchCount), true

	case spl2.OpNotEq:
		matches := make([]bool, batch.Len)
		matchCount := 0
		for i := 0; i < batch.Len && i < len(col); i++ {
			if col[i].IsNull() {
				continue // NULL != anything is false in search semantics
			}
			if !stringEqualSearch(col[i].String(), s.vecValue, caseInsensitive) {
				matches[i] = true
				matchCount++
			}
		}
		if matchCount == 0 {
			return nil, true
		}
		if matchCount == batch.Len {
			return batch, true
		}
		return compactBatch(batch, matches, matchCount), true

	case spl2.OpLt, spl2.OpLte, spl2.OpGt, spl2.OpGte:
		// Numeric/lexicographic comparisons: delegate to the same logic
		// as SearchEvaluator.evalCompare for correctness.
		matches := make([]bool, batch.Len)
		matchCount := 0
		for i := 0; i < batch.Len && i < len(col); i++ {
			if col[i].IsNull() {
				continue
			}
			if numericOrLexCompareSearch(col[i].String(), s.vecValue, s.vecOp) {
				matches[i] = true
				matchCount++
			}
		}
		if matchCount == 0 {
			return nil, true
		}
		if matchCount == batch.Len {
			return batch, true
		}
		return compactBatch(batch, matches, matchCount), true

	default:
		return nil, false // unsupported op → fall back
	}
}

// stringEqualSearch compares two strings with optional case-insensitivity.
// Used by the vectorized search path. Mirrors spl2.stringEqual behavior.
func stringEqualSearch(a, b string, caseInsensitive bool) bool {
	if caseInsensitive {
		return strings.EqualFold(a, b)
	}
	return a == b
}

// numericOrLexCompareSearch performs numeric comparison if both values parse
// as numbers, otherwise falls back to lexicographic comparison.
// Mirrors spl2.numericOrLexCompare behavior for search expressions.
func numericOrLexCompareSearch(fieldVal, cmpVal string, op spl2.CompareOp) bool {
	// Try numeric comparison first.
	if fv, err := strconv.ParseFloat(fieldVal, 64); err == nil {
		if cv, err2 := strconv.ParseFloat(cmpVal, 64); err2 == nil {
			switch op {
			case spl2.OpLt:
				return fv < cv
			case spl2.OpLte:
				return fv <= cv
			case spl2.OpGt:
				return fv > cv
			case spl2.OpGte:
				return fv >= cv
			}
		}
	}
	// Lexicographic fallback.
	switch op {
	case spl2.OpLt:
		return fieldVal < cmpVal
	case spl2.OpLte:
		return fieldVal <= cmpVal
	case spl2.OpGt:
		return fieldVal > cmpVal
	case spl2.OpGte:
		return fieldVal >= cmpVal
	}
	return false
}

// compactBatch creates a new batch containing only the rows where mask[i] is true.
func compactBatch(batch *Batch, mask []bool, matchCount int) *Batch {
	result := &Batch{
		Columns: make(map[string][]event.Value, len(batch.Columns)),
		Len:     matchCount,
	}
	for k, col := range batch.Columns {
		out := make([]event.Value, 0, matchCount)
		for i, v := range col {
			if i < batch.Len && mask[i] {
				out = append(out, v)
			}
		}
		result.Columns[k] = out
	}

	return result
}

func (s *SearchExprIterator) Close() error        { return s.child.Close() }
func (s *SearchExprIterator) Schema() []FieldInfo { return s.child.Schema() }

// containsFoldASCII performs a zero-allocation, case-insensitive substring search.
// The lowerSubstr parameter must already be lowercased by the caller (done once
// at construction time). This avoids the strings.ToLower(raw) allocation on every row.
// See also pkg/storage/filter_reader.go:89 toLowerASCII for the same pattern.
func containsFoldASCII(s, lowerSubstr string) bool {
	sLen, subLen := len(s), len(lowerSubstr)
	if subLen == 0 {
		return true
	}
	if subLen > sLen {
		return false
	}
	first := lowerSubstr[0]
	for i := 0; i <= sLen-subLen; i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 32
		}
		if c != first {
			continue
		}
		match := true
		for j := 1; j < subLen; j++ {
			c = s[i+j]
			if c >= 'A' && c <= 'Z' {
				c += 32
			}
			if c != lowerSubstr[j] {
				match = false

				break
			}
		}
		if match {
			return true
		}
	}

	return false
}
