package spl2

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/storage/segment/index"
)

// SearchEvaluator evaluates a parsed SearchExpr against event rows.
// It is used by both the pipeline engine and the old executor.
type SearchEvaluator struct {
	expr       SearchExpr
	globCache  map[string]*regexp.Regexp
	planCache  map[string]matchPlan
	lowerCache map[string]string // cached lowered strings to avoid per-row allocation
}

// NewSearchEvaluator creates a new evaluator for the given search expression.
func NewSearchEvaluator(expr SearchExpr) *SearchEvaluator {
	return &SearchEvaluator{
		expr:       expr,
		globCache:  make(map[string]*regexp.Regexp),
		planCache:  make(map[string]matchPlan),
		lowerCache: make(map[string]string),
	}
}

// lowerKeyword returns a cached lowercase version of s.
// This avoids allocating a new lowercase copy on every row evaluation.
func (e *SearchEvaluator) lowerKeyword(s string) string {
	if low, ok := e.lowerCache[s]; ok {
		return low
	}
	low := strings.ToLower(s)
	e.lowerCache[s] = low

	return low
}

// Evaluate returns true if the row matches the search expression.
// The row uses the pipeline format: map[string]event.Value.
func (e *SearchEvaluator) Evaluate(row map[string]event.Value) bool {
	return e.eval(e.expr, row)
}

func (e *SearchEvaluator) eval(expr SearchExpr, row map[string]event.Value) bool {
	switch ex := expr.(type) {
	case *SearchAndExpr:
		return e.eval(ex.Left, row) && e.eval(ex.Right, row)
	case *SearchOrExpr:
		return e.eval(ex.Left, row) || e.eval(ex.Right, row)
	case *SearchNotExpr:
		return !e.eval(ex.Operand, row)
	case *SearchKeywordExpr:
		return e.evalKeyword(ex, row)
	case *SearchCompareExpr:
		return e.evalCompare(ex, row)
	case *SearchInExpr:
		return e.evalIn(ex, row)
	}

	return false
}

func (e *SearchEvaluator) evalKeyword(kw *SearchKeywordExpr, row map[string]event.Value) bool {
	rawVal, ok := row["_raw"]
	if !ok || rawVal.IsNull() {
		return false
	}
	raw := rawVal.AsString()

	if kw.IsTermMatch {
		return MatchTerm(raw, kw.Value)
	}

	if kw.HasWildcard {
		return e.matchKeywordPattern(raw, kw.Value, !kw.CaseSensitive)
	}

	// Simple substring search.
	if kw.CaseSensitive {
		return strings.Contains(raw, kw.Value)
	}

	return containsFoldASCII(raw, e.lowerKeyword(kw.Value))
}

// matchKeywordPattern uses the analyzed match plan for fast-path keyword matching.
// Keyword search patterns are substring matches on _raw.
func (e *SearchEvaluator) matchKeywordPattern(text, pattern string, caseInsensitive bool) bool {
	key := "kw:" + pattern
	if caseInsensitive {
		key = "kwi:" + pattern
	}

	plan, ok := e.planCache[key]
	if !ok {
		plan = analyzeContainsPattern(pattern, caseInsensitive)
		e.planCache[key] = plan
	}

	switch plan.strategy {
	case matchAll:
		return true
	case matchContains:
		if caseInsensitive {
			return containsFoldASCII(text, plan.literalCI)
		}

		return strings.Contains(text, plan.literal)
	case matchPrefix:
		if caseInsensitive {
			return hasPrefixFoldASCII(text, plan.literalCI)
		}

		return strings.HasPrefix(text, plan.literal)
	case matchSuffix:
		if caseInsensitive {
			return hasSuffixFoldASCII(text, plan.literalCI)
		}

		return strings.HasSuffix(text, plan.literal)
	case matchExact:
		if caseInsensitive {
			return containsFoldASCII(text, plan.literalCI)
		}

		return strings.Contains(text, plan.literal)
	case matchRegex:
		// Pre-filter: check all extracted literals before running regex.
		if caseInsensitive {
			for _, lit := range plan.preFilter {
				if !containsFoldASCII(text, lit) {
					return false
				}
			}
		} else {
			for _, lit := range plan.preFilter {
				if !strings.Contains(text, lit) {
					return false
				}
			}
		}
		// Fall through to regex for full match.
		return e.matchGlobContains(text, pattern, caseInsensitive)
	}

	return false
}

func (e *SearchEvaluator) evalCompare(cmp *SearchCompareExpr, row map[string]event.Value) bool {
	// Resolve "source" alias: pipeline rows store the value under "_source"
	// (the physical column name), but search expressions use "source".
	field := cmp.Field
	if field == "source" {
		field = "_source"
	}

	fieldVal, exists := row[field]
	fieldExists := exists && !fieldVal.IsNull()

	// Special case: field=* means "field exists"
	if cmp.Op == OpEq && cmp.Value == "*" {
		return fieldExists
	}

	// For any comparison, field must exist
	if !fieldExists {
		return false
	}

	valStr := fieldVal.String()

	switch cmp.Op {
	case OpEq:
		if cmp.HasWildcard {
			return e.matchGlob(valStr, cmp.Value, !cmp.CaseSensitive)
		}

		return stringEqual(valStr, cmp.Value, !cmp.CaseSensitive)
	case OpNotEq:
		if cmp.HasWildcard {
			return !e.matchGlob(valStr, cmp.Value, !cmp.CaseSensitive)
		}

		return !stringEqual(valStr, cmp.Value, !cmp.CaseSensitive)
	case OpLt, OpLte, OpGt, OpGte:
		return numericOrLexCompare(valStr, cmp.Value, cmp.Op)
	case OpLike:
		return matchLikePattern(valStr, cmp.Value)
	}

	return false
}

func (e *SearchEvaluator) evalIn(in *SearchInExpr, row map[string]event.Value) bool {
	// Resolve "source" alias: pipeline rows use "_source" as the physical column name.
	field := in.Field
	if field == "source" {
		field = "_source"
	}

	fieldVal, exists := row[field]
	if !exists || fieldVal.IsNull() {
		return false
	}

	valStr := fieldVal.String()
	for _, v := range in.Values {
		if v.HasWildcard {
			if e.matchGlob(valStr, v.Value, true) {
				return true
			}
		} else {
			if stringEqual(valStr, v.Value, true) {
				return true
			}
		}
	}

	return false
}

// stringEqual compares two strings, optionally case-insensitive.
func stringEqual(a, b string, caseInsensitive bool) bool {
	if caseInsensitive {
		return strings.EqualFold(a, b)
	}

	return a == b
}

// GlobToRegex converts a Splunk glob pattern to a compiled Go regex.
// The * character matches zero or more of any character.
// All other regex metacharacters are escaped.
func GlobToRegex(pattern string, caseInsensitive bool) *regexp.Regexp {
	var buf strings.Builder
	if caseInsensitive {
		buf.WriteString("(?i)")
	}
	buf.WriteString("^")

	for i := 0; i < len(pattern); i++ {
		ch := pattern[i]
		switch ch {
		case '*':
			buf.WriteString(".*")
		case '.', '(', ')', '[', ']', '{', '}', '+', '^', '$', '|', '\\', '?':
			buf.WriteByte('\\')
			buf.WriteByte(ch)
		default:
			buf.WriteByte(ch)
		}
	}

	buf.WriteString("$")

	return regexp.MustCompile(buf.String())
}

// matchGlob checks if text matches a glob pattern with caching.
func (e *SearchEvaluator) matchGlob(text, pattern string, caseInsensitive bool) bool {
	// Fast path: *literal* → contains check.
	if literal, ok := extractStarLiteralStar(pattern); ok {
		if caseInsensitive {
			return containsFoldASCII(text, e.lowerKeyword(literal))
		}

		return strings.Contains(text, literal)
	}

	// Fast path: literal* → prefix check.
	if literal, ok := extractLiteralStar(pattern); ok {
		if caseInsensitive {
			return hasPrefixFoldASCII(text, e.lowerKeyword(literal))
		}

		return strings.HasPrefix(text, literal)
	}

	// Fast path: *literal → suffix check.
	if literal, ok := extractStarLiteral(pattern); ok {
		if caseInsensitive {
			return hasSuffixFoldASCII(text, e.lowerKeyword(literal))
		}

		return strings.HasSuffix(text, literal)
	}

	// General case: compile to regex
	key := "s:" + pattern
	if caseInsensitive {
		key = "i:" + pattern
	}

	compiled, ok := e.globCache[key]
	if !ok {
		compiled = GlobToRegex(pattern, caseInsensitive)
		e.globCache[key] = compiled
	}

	return compiled.MatchString(text)
}

// matchGlobContains checks if text contains a substring matching the glob pattern.
// Unlike matchGlob, this does NOT anchor with ^ and $ — it's a substring/contains check.
func (e *SearchEvaluator) matchGlobContains(text, pattern string, caseInsensitive bool) bool {
	key := "cs:" + pattern
	if caseInsensitive {
		key = "ci:" + pattern
	}

	compiled, ok := e.globCache[key]
	if !ok {
		compiled = globToContainsRegex(pattern, caseInsensitive)
		e.globCache[key] = compiled
	}

	return compiled.MatchString(text)
}

// globToContainsRegex converts a glob pattern to a regex without anchoring (substring match).
func globToContainsRegex(pattern string, caseInsensitive bool) *regexp.Regexp {
	var buf strings.Builder
	if caseInsensitive {
		buf.WriteString("(?i)")
	}
	for i := 0; i < len(pattern); i++ {
		ch := pattern[i]
		switch ch {
		case '*':
			buf.WriteString(".*")
		case '.', '(', ')', '[', ']', '{', '}', '+', '^', '$', '|', '\\', '?':
			buf.WriteByte('\\')
			buf.WriteByte(ch)
		default:
			buf.WriteByte(ch)
		}
	}

	return regexp.MustCompile(buf.String())
}

// extractStarLiteralStar returns the literal from *literal* patterns.
func extractStarLiteralStar(pattern string) (string, bool) {
	if len(pattern) < 3 || pattern[0] != '*' || pattern[len(pattern)-1] != '*' {
		return "", false
	}
	inner := pattern[1 : len(pattern)-1]
	if strings.Contains(inner, "*") {
		return "", false
	}

	return inner, true
}

// extractLiteralStar returns the literal from literal* patterns.
func extractLiteralStar(pattern string) (string, bool) {
	if len(pattern) < 2 || pattern[len(pattern)-1] != '*' {
		return "", false
	}
	if pattern[0] == '*' {
		return "", false
	}
	inner := pattern[:len(pattern)-1]
	if strings.Contains(inner, "*") {
		return "", false
	}

	return inner, true
}

// extractStarLiteral returns the literal from *literal patterns.
func extractStarLiteral(pattern string) (string, bool) {
	if len(pattern) < 2 || pattern[0] != '*' {
		return "", false
	}
	if pattern[len(pattern)-1] == '*' {
		return "", false
	}
	inner := pattern[1:]
	if strings.Contains(inner, "*") {
		return "", false
	}

	return inner, true
}

// matchLikePattern implements SQL LIKE pattern matching (case-insensitive).
// '%' matches zero or more characters, '_' matches exactly one character.
func matchLikePattern(text, pattern string) bool {
	text = strings.ToLower(text)
	pattern = strings.ToLower(pattern)

	// Fast path: pure '%' matches everything.
	if pattern == "%" {
		return true
	}
	// Fast path: %literal% → strings.Contains
	if len(pattern) >= 3 && pattern[0] == '%' && pattern[len(pattern)-1] == '%' {
		inner := pattern[1 : len(pattern)-1]
		if !strings.ContainsAny(inner, "%_") {
			return strings.Contains(text, inner)
		}
	}
	// Fast path: literal% → strings.HasPrefix
	if len(pattern) >= 2 && pattern[len(pattern)-1] == '%' && !strings.ContainsAny(pattern[:len(pattern)-1], "%_") {
		return strings.HasPrefix(text, pattern[:len(pattern)-1])
	}
	// Fast path: %literal → strings.HasSuffix
	if len(pattern) >= 2 && pattern[0] == '%' && !strings.ContainsAny(pattern[1:], "%_") {
		return strings.HasSuffix(text, pattern[1:])
	}

	// General case: iterative two-pointer match with greedy '%' backtracking.
	ti, pi := 0, 0
	starIdx, matchIdx := -1, 0

	for ti < len(text) {
		if pi < len(pattern) && (pattern[pi] == '_' || pattern[pi] == text[ti]) {
			ti++
			pi++
		} else if pi < len(pattern) && pattern[pi] == '%' {
			starIdx = pi
			matchIdx = ti
			pi++
		} else if starIdx >= 0 {
			pi = starIdx + 1
			matchIdx++
			ti = matchIdx
		} else {
			return false
		}
	}

	for pi < len(pattern) && pattern[pi] == '%' {
		pi++
	}

	return pi == len(pattern)
}

// numericOrLexCompare performs numeric comparison if both values parse as numbers,
// otherwise lexicographic comparison (case-insensitive).
func numericOrLexCompare(fieldVal, searchVal string, op CompareOp) bool {
	fNum, fErr := strconv.ParseFloat(fieldVal, 64)
	sNum, sErr := strconv.ParseFloat(searchVal, 64)

	if fErr == nil && sErr == nil {
		switch op {
		case OpLt:
			return fNum < sNum
		case OpLte:
			return fNum <= sNum
		case OpGt:
			return fNum > sNum
		case OpGte:
			return fNum >= sNum
		}
	}

	fLower := strings.ToLower(fieldVal)
	sLower := strings.ToLower(searchVal)
	switch op {
	case OpLt:
		return fLower < sLower
	case OpLte:
		return fLower <= sLower
	case OpGt:
		return fLower > sLower
	case OpGte:
		return fLower >= sLower
	}

	return false
}

// majorBreakers are characters that separate terms in Splunk's search.
// TERM() matches check that the search value appears bounded by these characters.
var majorBreakers = map[byte]bool{
	' ': true, '\t': true, '\n': true, '\r': true,
	',': true, ';': true,
	'[': true, ']': true, '{': true, '}': true,
	'(': true, ')': true, '<': true, '>': true,
	'|': true, '!': true, '?': true,
	'"': true, '\'': true,
}

// MatchTerm checks if term appears in text bounded by major breakers
// (or start/end of string). Case-insensitive by default.
func MatchTerm(text, term string) bool {
	termLower := strings.ToLower(term)
	textLower := strings.ToLower(text)

	idx := 0
	for {
		pos := strings.Index(textLower[idx:], termLower)
		if pos < 0 {
			return false
		}

		absPos := idx + pos
		endPos := absPos + len(term)

		leftOk := absPos == 0 || majorBreakers[text[absPos-1]]
		rightOk := endPos >= len(text) || majorBreakers[text[endPos]]

		if leftOk && rightOk {
			return true
		}

		idx = absPos + 1
		if idx >= len(text) {
			return false
		}
	}
}

// ExtractSearchTermTree builds a boolean tree of search terms from a SearchExpr
// for inverted index bitmap evaluation. Unlike ExtractSearchTermsFromExpr (which
// only handles AND), this supports OR branches by producing a tree of AND/OR nodes
// with leaf nodes holding tokenized terms.
//
// Returns nil if the expression cannot produce useful terms (e.g., pure NOT,
// match-all wildcard, or an OR branch with no extractable terms).
func ExtractSearchTermTree(expr SearchExpr) *SearchTermTree {
	if expr == nil {
		return nil
	}

	return buildTermTree(expr)
}

func buildTermTree(expr SearchExpr) *SearchTermTree {
	switch ex := expr.(type) {
	case *SearchAndExpr:
		left := buildTermTree(ex.Left)
		right := buildTermTree(ex.Right)
		// AND(everything, right) = right. Skip nil children.
		if left == nil {
			return right
		}
		if right == nil {
			return left
		}

		return &SearchTermTree{Op: SearchTermAnd, Children: []*SearchTermTree{left, right}}

	case *SearchOrExpr:
		left := buildTermTree(ex.Left)
		right := buildTermTree(ex.Right)
		// OR(everything, X) = everything. If either branch can't produce
		// terms, the OR is unbounded — no bitmap optimization possible.
		if left == nil || right == nil {
			return nil
		}

		return &SearchTermTree{Op: SearchTermOr, Children: []*SearchTermTree{left, right}}

	case *SearchKeywordExpr:
		var tokens []string
		if ex.HasWildcard {
			if lit := extractLongestLiteral(ex.Value); lit != "" {
				tokens = index.Tokenize(strings.ToLower(lit))
			}
		} else if !ex.IsTermMatch {
			tokens = index.Tokenize(ex.Value)
		}
		if len(tokens) == 0 {
			return nil
		}

		return &SearchTermTree{Op: SearchTermLeaf, Terms: tokens}

	case *SearchNotExpr:
		return nil // NOT can't produce bitmap (bloom proves presence only)

	case *SearchCompareExpr:
		return nil // field comparisons use InvertedIndexPredicates

	default:
		return nil
	}
}

// ExtractSearchTermsFromExpr walks a SearchExpr and extracts literal search terms
// that can be used for bloom filter optimization. Terms are tokenized using the
// same tokenizer as the segment writer for consistent bloom filter lookups.
func ExtractSearchTermsFromExpr(expr SearchExpr) []string {
	var terms []string
	extractSearchTerms(expr, &terms)

	return terms
}

func extractSearchTerms(expr SearchExpr, terms *[]string) {
	switch ex := expr.(type) {
	case *SearchAndExpr:
		extractSearchTerms(ex.Left, terms)
		extractSearchTerms(ex.Right, terms)
	case *SearchOrExpr:
		// For OR, we can't use individual terms for bloom filter pruning
		// since the event might match either branch.
	case *SearchNotExpr:
		// NOT terms can't be used for bloom filter (bloom only proves presence)
	case *SearchKeywordExpr:
		if ex.HasWildcard {
			// Extract longest literal substring between wildcards and tokenize it
			// through the same tokenizer used by the segment writer. This ensures
			// multi-token literals (e.g., "invalid user") are split into individual
			// tokens that match the bloom filter and inverted index entries.
			if lit := extractLongestLiteral(ex.Value); lit != "" {
				tokens := index.Tokenize(strings.ToLower(lit))
				*terms = append(*terms, tokens...)
			}
		} else if !ex.IsTermMatch {
			// Tokenize the value using the same tokenizer as the segment writer
			// for consistent bloom filter and inverted index lookups.
			tokens := index.Tokenize(ex.Value)
			*terms = append(*terms, tokens...)
		}
	case *SearchCompareExpr:
		// Field comparisons can't use bloom filter (bloom is on _raw only)
	}
}

// extractLongestLiteral extracts the longest non-wildcard substring from a glob pattern.
func extractLongestLiteral(pattern string) string {
	parts := strings.Split(pattern, "*")
	longest := ""
	for _, p := range parts {
		if len(p) > len(longest) {
			longest = p
		}
	}

	return longest
}

// containsFoldASCII performs a zero-allocation, case-insensitive substring search.
// The lowerSubstr parameter must already be lowercased by the caller.
// This avoids the strings.ToLower(text) allocation that would otherwise happen on every row.
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

// hasPrefixFoldASCII checks if s starts with lowerPrefix (case-insensitive, zero-alloc).
// The lowerPrefix parameter must already be lowercased by the caller.
func hasPrefixFoldASCII(s, lowerPrefix string) bool {
	if len(lowerPrefix) > len(s) {
		return false
	}
	for i := 0; i < len(lowerPrefix); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 32
		}
		if c != lowerPrefix[i] {
			return false
		}
	}

	return true
}

// hasSuffixFoldASCII checks if s ends with lowerSuffix (case-insensitive, zero-alloc).
// The lowerSuffix parameter must already be lowercased by the caller.
func hasSuffixFoldASCII(s, lowerSuffix string) bool {
	if len(lowerSuffix) > len(s) {
		return false
	}
	offset := len(s) - len(lowerSuffix)
	for i := 0; i < len(lowerSuffix); i++ {
		c := s[offset+i]
		if c >= 'A' && c <= 'Z' {
			c += 32
		}
		if c != lowerSuffix[i] {
			return false
		}
	}

	return true
}
