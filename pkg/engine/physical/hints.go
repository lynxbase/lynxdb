package physical

import (
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/model"
)

// QueryHints converts scan hints to the storage engine's hint structure.
func (h Hints) QueryHints() *model.QueryHints {
	q := &model.QueryHints{
		SearchTerms: append([]string(nil), h.Terms...),
		TokenGlobs:  append([]string(nil), h.TokenGlobs...),
		Limit:       int(h.Limit),
		ReverseScan: h.Descending,
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
