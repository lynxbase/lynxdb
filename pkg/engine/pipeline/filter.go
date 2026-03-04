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
	// Detect simple vectorizable pattern.
	if cmp, ok := expr.(*spl2.CompareExpr); ok {
		if field, ok := cmp.Left.(*spl2.FieldExpr); ok {
			if lit, ok := cmp.Right.(*spl2.LiteralExpr); ok {
				fi.vecField = field.Name
				fi.vecOp = cmp.Op
				fi.vecValue = lit.Value
				fi.vecReady = true
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

		// Pass 1: evaluate predicates with reusable row map, build match bitmap.
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

		// Pass 2: compact columns using bitmap.
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
	// VM predicate
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
// we extract literal substrings and use strings.Contains as a pre-filter on the
// _raw column BEFORE reconstructing rows. This avoids the expensive per-row map
// construction for rows that can't possibly match.
//
// Fast path: for simple keyword searches (single SearchKeywordExpr with no
// wildcards, no field comparisons, no boolean combinators), the pre-filter
// result IS the final result — full evaluation is skipped entirely.
// Eliminates the ~35 ms double-evaluation overhead for simple searches.
type SearchExprIterator struct {
	child           Iterator
	evaluator       *spl2.SearchEvaluator
	preFilterLits   []string // literal substrings that must appear in _raw (AND semantics)
	isSimpleKeyword bool     // true when expr is a single keyword without wildcards
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

	return &SearchExprIterator{
		child:           child,
		evaluator:       eval,
		preFilterLits:   lits,
		isSimpleKeyword: isSimple,
	}
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
