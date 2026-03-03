package optimizer

import (
	"strings"

	"github.com/OrlovEvgeny/Lynxdb/pkg/spl2"
)

// ViewInfo describes a materialized view for query rewrite purposes.
type ViewInfo struct {
	Name         string
	Filter       string   // e.g. "source=nginx"
	GroupBy      []string // GROUP BY fields
	Aggregations []string // aggregation names (e.g. "count", "sum", "avg")
	Status       string   // "active", "backfill", etc.
	Rows         int64    // approximate row count in the MV
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

	// Extract search predicates from the query.
	queryFilter := extractSearchFilter(q)
	if queryFilter == "" {
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
		if mv.Status != "active" {
			continue
		}

		// Check filter subset: MV filter must be a subset of query filter.
		if !filterIsSubset(mv.Filter, queryFilter) {
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

		// Annotate the rewritten query so the server can report MV acceleration.
		newQ.Annotate("mvAccelerated", &MVAccelAnnotation{
			ViewName: mv.Name,
			Status:   mv.Status,
			MVRows:   mv.Rows,
		})

		return newQ, true
	}

	return q, false
}

// extractSearchFilter extracts a simple filter string from search commands.
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
