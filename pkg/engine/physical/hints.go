package physical

import (
	"time"

	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/model"
)

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
func PlanHints(plan *ir.Plan, defaultSource string, now time.Time) *model.QueryHints {
	scans := Scans(plan)
	switch len(scans) {
	case 0:
		return &model.QueryHints{}
	case 1:
		return ScanHints(scans[0], now).QueryHints()
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
	return union.QueryHints()
}

// QueryHints converts scan hints to the storage engine's hint structure.
func (h Hints) QueryHints() *model.QueryHints {
	q := &model.QueryHints{
		SearchTerms: append([]string(nil), h.Terms...),
		TokenGlobs:  append([]string(nil), h.TokenGlobs...),
		Limit:       int(h.Limit),
	}
	if h.Columns != nil {
		q.RequiredCols = append([]string(nil), h.Columns...)
	}
	if !h.MinTime.IsZero() || !h.MaxTime.IsZero() {
		q.TimeBounds = &model.TimeBounds{Earliest: h.MinTime, Latest: h.MaxTime}
	}
	for _, f := range h.FieldEquals {
		q.FieldPredicates = append(q.FieldPredicates, model.FieldPredicate{Field: f.Field, Op: "=", Value: f.Value})
	}
	if len(h.AnyTerms) > 0 {
		tree := &model.SearchTermTree{Op: "AND"}
		for _, t := range h.Terms {
			tree.Children = append(tree.Children, &model.SearchTermTree{Op: "TERM", Term: t})
		}
		for _, group := range h.AnyTerms {
			or := &model.SearchTermTree{Op: "OR"}
			for _, t := range group {
				or.Children = append(or.Children, &model.SearchTermTree{Op: "TERM", Term: t})
			}
			tree.Children = append(tree.Children, or)
		}
		q.SearchTermTree = tree
	}
	setSourceScope(h.Sources, q)
	return q
}

func setSourceScope(sources []ir.Source, q *model.QueryHints) {
	var names, globs []string
	for _, s := range sources {
		switch {
		case s.Name == "*":
			q.SourceScopeType = model.SourceScopeAll
			return
		case s.Pattern:
			globs = append(globs, s.Name)
		default:
			names = append(names, s.Name)
		}
	}
	switch {
	case len(globs) == 1 && len(names) == 0:
		q.SourceScopeType = model.SourceScopeGlob
		q.SourceScopePattern = globs[0]
		q.SourceGlob = globs[0]
	case len(globs) > 0:
		q.SourceIncludeGlobs = globs
		if len(names) > 0 {
			q.SourceScopeType = model.SourceScopeList
			q.SourceScopeSources = names
			q.SourceIndices = names
		} else {
			q.SourceScopeType = model.SourceScopeGlob
			q.SourceScopePattern = globs[0]
		}
	case len(names) == 1:
		q.IndexName = names[0]
		q.SourceScopeType = model.SourceScopeSingle
		q.SourceScopeSources = names
	case len(names) > 1:
		q.SourceScopeType = model.SourceScopeList
		q.SourceScopeSources = names
		q.SourceIndices = names
	}
}
