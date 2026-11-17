package physical

import (
	"time"

	"github.com/lynxbase/lynxdb/pkg/ir"
)

// QueryHints are the storage-level facts of a whole plan, as Hints describes
// them for one scan, with the sources the plan reads described as a scope.
type QueryHints struct {
	SearchTerms     []string
	TokenGlobs      []string
	FieldPredicates []FieldEqual
	RequiredCols    []string
	TimeBounds      *TimeBounds
	// SourceScopeType is "all", "single", "list" or "glob".
	SourceScopeType    string
	SourceScopeSources []string
	SourceScopePattern string
}

// TimeBounds is the range of event times a plan keeps; a zero bound is open.
type TimeBounds struct {
	Earliest time.Time
	Latest   time.Time
}

// Scans returns the scans of a plan, including those of subqueries.
func Scans(plan *ir.Plan) []*ir.Scan {
	var out []*ir.Scan
	if plan == nil {
		return nil
	}
	ir.WalkPlan(plan.Root, func(n ir.Node) bool {
		if s, ok := n.(*ir.Scan); ok {
			out = append(out, s)
		}
		return true
	})
	return out
}

// PlanHints derives the storage hints of a whole plan. A plan with one scan
// gets all of its hints; with several scans only the union of their sources
// and time ranges is kept, since each data-skipping hint belongs to one scan.
// A scan without sources reads defaultSource.
func PlanHints(plan *ir.Plan, defaultSource string, now time.Time) *QueryHints {
	scans := Scans(plan)
	switch len(scans) {
	case 0:
		return &QueryHints{}
	case 1:
		return ScanHints(scans[0], now).queryHints()
	}
	var union Hints
	bounded := true
	for i, s := range scans {
		h := ScanHints(s, now)
		union.Sources = append(union.Sources, h.Sources...)
		if len(h.Sources) == 0 {
			union.Sources = append(union.Sources, ir.Source{Name: defaultSource})
		}
		if h.MinTime.IsZero() || h.MaxTime.IsZero() {
			bounded = false
			continue
		}
		if i == 0 || h.MinTime.Before(union.MinTime) {
			union.MinTime = h.MinTime
		}
		if i == 0 || h.MaxTime.After(union.MaxTime) {
			union.MaxTime = h.MaxTime
		}
	}
	if !bounded {
		union.MinTime, union.MaxTime = time.Time{}, time.Time{}
	}
	return union.queryHints()
}

func (h Hints) queryHints() *QueryHints {
	q := &QueryHints{
		SearchTerms:     append([]string(nil), h.Terms...),
		TokenGlobs:      append([]string(nil), h.TokenGlobs...),
		FieldPredicates: append([]FieldEqual(nil), h.FieldEquals...),
	}
	if h.Columns != nil {
		q.RequiredCols = append([]string(nil), h.Columns...)
	}
	if !h.MinTime.IsZero() || !h.MaxTime.IsZero() {
		q.TimeBounds = &TimeBounds{Earliest: h.MinTime, Latest: h.MaxTime}
	}
	setSourceScope(h.Sources, q)
	return q
}

func setSourceScope(sources []ir.Source, q *QueryHints) {
	var names, globs []string
	for _, s := range sources {
		switch {
		case s.Name == "*":
			q.SourceScopeType = "all"
			return
		case s.Pattern:
			globs = append(globs, s.Name)
		default:
			names = append(names, s.Name)
		}
	}
	switch {
	case len(globs) > 0 && len(names) > 0:
		q.SourceScopeType = "list"
		q.SourceScopeSources = names
	case len(globs) > 0:
		q.SourceScopeType = "glob"
		q.SourceScopePattern = globs[0]
	case len(names) == 1:
		q.SourceScopeType = "single"
		q.SourceScopeSources = names
	case len(names) > 1:
		q.SourceScopeType = "list"
		q.SourceScopeSources = names
	}
}
