package optimizer

import (
	"strconv"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/spl2"
)

// ViewInfo describes a materialized view for query rewrite purposes.
type ViewInfo struct {
	Name         string
	Filter       string    // e.g. "source=nginx" — display/legacy string
	FilterExpr   spl2.Expr // parsed AST of the filter (populated by parseViewFilter)
	GroupBy      []string  // GROUP BY fields
	Aggregations []string  // aggregation names (e.g. "count", "sum", "avg")
	Status       string    // "active", "backfill", etc.
	Rows         int64     // approximate row count in the MV
}

// MVAccelAnnotation holds MV acceleration metadata attached to a rewritten query.
// The server uses this to populate MV-related stats in the query response.
type MVAccelAnnotation struct {
	ViewName string // name of the MV used
	Status   string // "active", "backfilling", "partial"
	MVRows   int64  // approximate row count in the MV (for speedup estimate)
}

// ViewCatalog provides view metadata for the optimizer.
type ViewCatalog interface {
	ListViews() []ViewInfo
}

// mvRewriteRule attempts to rewrite queries to read from a materialized view
// when the MV's filter is a subset of the query filter, the query GROUP BY is
// a subset of the MV GROUP BY, and all aggregations are mergeable.
type mvRewriteRule struct {
	catalog ViewCatalog
}

// NewMVRewriteRule creates an MV rewrite optimizer rule.
func NewMVRewriteRule(catalog ViewCatalog) Rule {
	if catalog == nil {
		return &mvRewriteRule{}
	}

	return &mvRewriteRule{catalog: catalog}
}

func (r *mvRewriteRule) Name() string { return "MVQueryRewrite" }
func (r *mvRewriteRule) Description() string {
	return "Rewrites query to read from a matching materialized view"
}

func (r *mvRewriteRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	if r.catalog == nil {
		return q, false
	}

	// Only rewrite if the query has a stats command.
	var statsCmd *spl2.StatsCommand
	var statsIdx int
	for i, cmd := range q.Commands {
		if s, ok := cmd.(*spl2.StatsCommand); ok {
			statsCmd = s
			statsIdx = i

			break
		}
	}
	if statsCmd == nil {
		return q, false
	}

	// Extract query filter: try AST-based first, fall back to string-based.
	queryFilterExpr := extractQueryFilterExpr(q)
	queryFilter := extractSearchFilter(q)

	// Need at least one form of filter to match.
	if queryFilterExpr == nil && queryFilter == "" {
		return q, false
	}

	// Extract aggregation names from the query.
	queryAggs := make(map[string]bool)
	for _, agg := range statsCmd.Aggregations {
		queryAggs[aggName(agg)] = true
	}

	// Try to find a matching MV.
	views := r.catalog.ListViews()
	for _, mv := range views {
		if mv.Status != "active" && mv.Status != "backfill" {
			continue
		}

		// Check filter subset using AST-based matching when available.
		if !matchMVFilter(mv, queryFilterExpr, queryFilter) {
			continue
		}

		// Check GROUP BY: query GROUP BY must be a subset of MV GROUP BY.
		if !stringSliceSubset(statsCmd.GroupBy, mv.GroupBy) {
			continue
		}

		// Check aggregations: all query aggs must be present in MV.
		if !aggsAreMergeable(queryAggs, mv.Aggregations) {
			continue
		}

		// Rewrite: replace source + stats with FROM mv + rollup stats.
		newQ := &spl2.Query{
			Commands: make([]spl2.Command, 0, len(q.Commands)),
		}

		// Add FROM command.
		newQ.Commands = append(newQ.Commands, &spl2.FromCommand{
			ViewName: mv.Name,
		})

		// Keep stats and subsequent commands.
		newQ.Commands = append(newQ.Commands, q.Commands[statsIdx:]...)

		// Determine annotation status.
		status := mv.Status
		if status == "backfill" {
			status = "backfilling"
		}

		// Annotate the rewritten query so the server can report MV acceleration.
		newQ.Annotate("mvAccelerated", &MVAccelAnnotation{
			ViewName: mv.Name,
			Status:   status,
			MVRows:   mv.Rows,
		})

		return newQ, true
	}

	return q, false
}

// matchMVFilter checks whether the MV filter is implied by the query filter.
// Uses AST-based matching (exprImplies) when both expressions are available,
// falls back to string-based matching otherwise.
func matchMVFilter(mv ViewInfo, queryExpr spl2.Expr, queryFilter string) bool {
	// If MV has no filter, it matches everything.
	if mv.Filter == "" && mv.FilterExpr == nil {
		return true
	}

	// Try AST-based matching first.
	mvExpr := mv.FilterExpr
	if mvExpr == nil {
		mvExpr = parseViewFilter(mv.Filter)
	}

	if mvExpr != nil && queryExpr != nil {
		return exprImplies(queryExpr, mvExpr)
	}

	// Fall back to string-based matching.
	return filterIsSubset(mv.Filter, queryFilter)
}

// parseViewFilter parses a MV filter string into an AST expression.
// Returns nil if parsing fails (graceful degradation to string matching).
// The filter is wrapped in "| where <filter>" to make it parseable.
func parseViewFilter(filter string) spl2.Expr {
	if filter == "" {
		return nil
	}

	// Try parsing as a WHERE expression.
	q, err := spl2.Parse("| where " + filter)
	if err != nil {
		return nil
	}

	for _, cmd := range q.Commands {
		if w, ok := cmd.(*spl2.WhereCommand); ok {
			return w.Expr
		}
	}

	return nil
}

// extractQueryFilterExpr walks query commands and collects WHERE/Search predicates
// as an AND-conjunction AST. Returns nil if no filter predicates are found.
func extractQueryFilterExpr(q *spl2.Query) spl2.Expr {
	var exprs []spl2.Expr

	for _, cmd := range q.Commands {
		switch c := cmd.(type) {
		case *spl2.WhereCommand:
			exprs = append(exprs, c.Expr)
		case *spl2.SearchCommand:
			// Extract equality predicates from search commands.
			if c.Expression != nil {
				if expr := searchExprToExpr(c.Expression); expr != nil {
					exprs = append(exprs, expr)
				}
			}
		}
	}

	if len(exprs) == 0 {
		return nil
	}

	// Build AND conjunction.
	result := exprs[0]
	for _, e := range exprs[1:] {
		result = &spl2.BinaryExpr{Left: result, Op: "and", Right: e}
	}

	return result
}

// searchExprToExpr converts a SearchExpr to an Expr for AST comparison.
// Only handles simple patterns (field comparisons and AND conjunctions).
func searchExprToExpr(sexpr spl2.SearchExpr) spl2.Expr {
	switch e := sexpr.(type) {
	case *spl2.SearchCompareExpr:
		if e.HasWildcard {
			return nil
		}
		return &spl2.CompareExpr{
			Left:  &spl2.FieldExpr{Name: e.Field},
			Op:    e.Op.String(),
			Right: &spl2.LiteralExpr{Value: e.Value},
		}
	case *spl2.SearchAndExpr:
		left := searchExprToExpr(e.Left)
		right := searchExprToExpr(e.Right)
		if left == nil && right == nil {
			return nil
		}
		if left == nil {
			return right
		}
		if right == nil {
			return left
		}
		return &spl2.BinaryExpr{Left: left, Op: "and", Right: right}
	default:
		return nil
	}
}

// exprImplies checks whether queryExpr logically implies mvExpr.
// In other words: if queryExpr is true, is mvExpr guaranteed to be true?
// This is used to determine if a query's filter is at least as restrictive
// as a materialized view's filter.
//
// Algorithm: flatten both into AND-conjuncts, then for each MV conjunct,
// check if any query conjunct implies it.
func exprImplies(queryExpr, mvExpr spl2.Expr) bool {
	mvConjuncts := flattenAND(mvExpr)
	queryConjuncts := flattenAND(queryExpr)

	for _, mc := range mvConjuncts {
		implied := false
		for _, qc := range queryConjuncts {
			if conjunctImplies(qc, mc) {
				implied = true

				break
			}
		}
		if !implied {
			return false
		}
	}

	return true
}

// conjunctImplies checks whether a single query conjunct implies a single MV conjunct.
// Handles:
//   - Structural equality (exprEqual)
//   - Range tightening (field >= B implies field >= A when B >= A)
//   - IN-list containment (field IN (1,2) implies field IN (1,2,3))
func conjunctImplies(query, mv spl2.Expr) bool {
	// Structural equality.
	if exprEqual(query, mv) {
		return true
	}

	// Range tightening: query has a tighter range than MV.
	if rangeTightens(query, mv) {
		return true
	}

	// IN-list containment: query IN-list is a subset of MV IN-list.
	if inListContains(query, mv) {
		return true
	}

	return false
}

// rangeTightens checks if query tightens the range compared to mv.
// Examples:
//   - field >= 500 implies field >= 400 (tighter lower bound)
//   - field <= 100 implies field <= 200 (tighter upper bound)
//   - field = 500 implies field >= 400 (equality is tightest)
func rangeTightens(query, mv spl2.Expr) bool {
	qCmp, qOk := query.(*spl2.CompareExpr)
	mCmp, mOk := mv.(*spl2.CompareExpr)
	if !qOk || !mOk {
		return false
	}

	// Must be on the same field.
	qField, qFOk := qCmp.Left.(*spl2.FieldExpr)
	mField, mFOk := mCmp.Left.(*spl2.FieldExpr)
	if !qFOk || !mFOk || qField.Name != mField.Name {
		return false
	}

	// Both must have literal right sides.
	qLit, qLOk := qCmp.Right.(*spl2.LiteralExpr)
	mLit, mLOk := mCmp.Right.(*spl2.LiteralExpr)
	if !qLOk || !mLOk {
		return false
	}

	// Try numeric comparison first.
	qNum, qErr := strconv.ParseFloat(qLit.Value, 64)
	mNum, mErr := strconv.ParseFloat(mLit.Value, 64)

	if qErr == nil && mErr == nil {
		return numericRangeTightens(qCmp.Op, qNum, mCmp.Op, mNum)
	}

	// String comparison (lexicographic).
	return stringRangeTightens(qCmp.Op, qLit.Value, mCmp.Op, mLit.Value)
}

// numericRangeTightens checks numeric range tightening.
func numericRangeTightens(qOp string, qVal float64, mOp string, mVal float64) bool {
	// Query equality implies any range on same field containing that value.
	if qOp == "=" || qOp == "==" {
		switch mOp {
		case ">=":
			return qVal >= mVal
		case ">":
			return qVal > mVal
		case "<=":
			return qVal <= mVal
		case "<":
			return qVal < mVal
		case "=", "==":
			return qVal == mVal
		}
	}

	// Lower bound tightening: query >= B implies mv >= A when B >= A.
	if isLowerBound(qOp) && isLowerBound(mOp) {
		// query has >= B, mv has >= A: B >= A means query is tighter.
		if qOp == ">=" && mOp == ">=" {
			return qVal >= mVal
		}
		if qOp == ">" && mOp == ">=" {
			return qVal >= mVal // > B implies >= A when B >= A
		}
		if qOp == ">=" && mOp == ">" {
			return qVal > mVal // >= B implies > A when B > A
		}
		if qOp == ">" && mOp == ">" {
			return qVal >= mVal
		}
	}

	// Upper bound tightening.
	if isUpperBound(qOp) && isUpperBound(mOp) {
		if qOp == "<=" && mOp == "<=" {
			return qVal <= mVal
		}
		if qOp == "<" && mOp == "<=" {
			return qVal <= mVal
		}
		if qOp == "<=" && mOp == "<" {
			return qVal < mVal
		}
		if qOp == "<" && mOp == "<" {
			return qVal <= mVal
		}
	}

	return false
}

// stringRangeTightens checks string range tightening (lexicographic).
func stringRangeTightens(qOp, qVal, mOp, mVal string) bool {
	if qOp == "=" || qOp == "==" {
		switch mOp {
		case ">=":
			return qVal >= mVal
		case ">":
			return qVal > mVal
		case "<=":
			return qVal <= mVal
		case "<":
			return qVal < mVal
		case "=", "==":
			return qVal == mVal
		}
	}

	if isLowerBound(qOp) && isLowerBound(mOp) {
		if qOp == ">=" && mOp == ">=" {
			return qVal >= mVal
		}
		if qOp == ">" && mOp == ">=" {
			return qVal >= mVal
		}
	}

	if isUpperBound(qOp) && isUpperBound(mOp) {
		if qOp == "<=" && mOp == "<=" {
			return qVal <= mVal
		}
		if qOp == "<" && mOp == "<=" {
			return qVal <= mVal
		}
	}

	return false
}

func isLowerBound(op string) bool { return op == ">=" || op == ">" }
func isUpperBound(op string) bool { return op == "<=" || op == "<" }

// inListContains checks if query's IN-list is a subset of mv's IN-list.
// field IN (1,2) implies field IN (1,2,3) because the query is more restrictive.
func inListContains(query, mv spl2.Expr) bool {
	qIn, qOk := query.(*spl2.InExpr)
	mIn, mOk := mv.(*spl2.InExpr)
	if !qOk || !mOk {
		return false
	}
	// Both must be on the same field and both positive (IN, not NOT IN).
	if qIn.Negated || mIn.Negated {
		return false
	}
	if !exprEqual(qIn.Field, mIn.Field) {
		return false
	}

	// Build set from MV values.
	mvSet := make(map[string]bool, len(mIn.Values))
	for _, v := range mIn.Values {
		mvSet[v.String()] = true
	}

	// Check all query values are in MV set.
	for _, v := range qIn.Values {
		if !mvSet[v.String()] {
			return false
		}
	}

	return true
}

// extractSearchFilter extracts a simple filter string from search commands.
// Kept for backward compatibility with string-based matching.
func extractSearchFilter(q *spl2.Query) string {
	if q.Source != nil && q.Source.Index != "" {
		return q.Source.Index
	}
	for _, cmd := range q.Commands {
		if s, ok := cmd.(*spl2.SearchCommand); ok {
			if s.Index != "" {
				return s.Index
			}
			if s.Term != "" {
				return s.Term
			}
		}
		if w, ok := cmd.(*spl2.WhereCommand); ok {
			return w.Expr.String()
		}
	}

	return ""
}

// aggName returns the function name of an aggregation expression.
func aggName(agg spl2.AggExpr) string {
	return agg.Func
}

// filterIsSubset checks if the MV filter is a subset of the query filter.
// For simple equality predicates, this means the MV filter predicates are
// all present in the query filter.
// Kept for backward compatibility when AST-based matching is not available.
func filterIsSubset(mvFilter, queryFilter string) bool {
	if mvFilter == "" {
		return true // Empty MV filter matches everything.
	}
	if mvFilter == queryFilter {
		return true // Exact match.
	}
	// Decompose both filters into AND-conjuncts and check subset.
	mvParts := splitConjuncts(mvFilter)
	queryParts := splitConjuncts(queryFilter)
	querySet := make(map[string]bool, len(queryParts))
	for _, p := range queryParts {
		querySet[strings.TrimSpace(p)] = true
	}
	for _, p := range mvParts {
		if !querySet[strings.TrimSpace(p)] {
			return false
		}
	}

	return true
}

// splitConjuncts splits a filter string on " AND " (case-insensitive).
func splitConjuncts(filter string) []string {
	parts := splitOnAND(filter)
	if len(parts) == 0 {
		return []string{filter}
	}

	return parts
}

func splitOnAND(s string) []string {
	lower := strings.ToLower(s)
	var parts []string
	start := 0
	for {
		idx := strings.Index(lower[start:], " and ")
		if idx < 0 {
			parts = append(parts, strings.TrimSpace(s[start:]))

			break
		}
		parts = append(parts, strings.TrimSpace(s[start:start+idx]))
		start = start + idx + 5 // len(" and ")
	}

	return parts
}

// stringSliceSubset checks if all elements of sub are in super.
func stringSliceSubset(sub, super []string) bool {
	superSet := make(map[string]bool, len(super))
	for _, s := range super {
		superSet[s] = true
	}
	for _, s := range sub {
		if !superSet[s] {
			return false
		}
	}

	return true
}

// aggsAreMergeable checks if all query aggregations are available in the MV.
func aggsAreMergeable(queryAggs map[string]bool, mvAggs []string) bool {
	mvSet := make(map[string]bool, len(mvAggs))
	for _, a := range mvAggs {
		mvSet[a] = true
	}
	for agg := range queryAggs {
		if !mvSet[agg] {
			return false
		}
	}

	return true
}
