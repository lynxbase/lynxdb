package optimizer

import (
	"path"
	"strings"

	"github.com/OrlovEvgeny/Lynxdb/pkg/spl2"
)

// Scope type constants used within the optimizer package to avoid goconst warnings.
const (
	scopeAll    = "all"
	scopeSingle = "single"
	scopeList   = "list"
	scopeGlob   = "glob"
)

// isSourceField returns true if the field name refers to the _source dimension.
// "index" and "source" are virtual aliases for "_source" in LynxDB.
func isSourceField(name string) bool {
	switch name {
	case "_source", "source", "index":
		return true
	default:
		return false
	}
}

// sourceORtoINRule rewrites source=A OR source=B (2+ OR'd equalities on
// _source/source/index) into a single InExpr. This is a specialization of
// the general inListRewriteRule that uses a threshold of 2 instead of 3
// for source fields, since multi-source queries commonly use just 2 sources.
type sourceORtoINRule struct{}

func (r *sourceORtoINRule) Name() string { return "SourceORtoIN" }
func (r *sourceORtoINRule) Description() string {
	return "Folds source=A OR source=B into _source IN (A,B) for source scope pushdown"
}

func (r *sourceORtoINRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	changed := false
	for i, cmd := range q.Commands {
		w, ok := cmd.(*spl2.WhereCommand)
		if !ok {
			continue
		}
		newExpr, rewritten := rewriteSourceORtoIN(w.Expr)
		if rewritten {
			q.Commands[i] = &spl2.WhereCommand{Expr: newExpr}
			changed = true
		}
	}

	return q, changed
}

// rewriteSourceORtoIN detects source=A OR source=B patterns (2+ leaves) on
// source fields and rewrites to InExpr. Falls through to the general
// inListRewriteRule for non-source fields.
func rewriteSourceORtoIN(expr spl2.Expr) (spl2.Expr, bool) {
	orLeaves := flattenOR(expr)
	if len(orLeaves) < 2 {
		return expr, false
	}

	// Check if all OR leaves are field=literal on the same source field.
	var fieldName string
	var values []spl2.Expr
	for _, leaf := range orLeaves {
		cmp, ok := leaf.(*spl2.CompareExpr)
		if !ok {
			return expr, false
		}
		if cmp.Op != "=" && cmp.Op != "==" {
			return expr, false
		}
		field, ok := cmp.Left.(*spl2.FieldExpr)
		if !ok {
			return expr, false
		}
		if !isSourceField(field.Name) {
			return expr, false
		}
		lit, ok := cmp.Right.(*spl2.LiteralExpr)
		if !ok {
			return expr, false
		}
		if fieldName == "" {
			fieldName = field.Name
		} else if field.Name != fieldName {
			// Different source-aliased fields in the same OR — normalize to _source.
			// source=A OR index=B → _source IN (A, B)
			fieldName = "_source"
		}
		values = append(values, lit)
	}

	// Normalize field name to _source for consistency.
	normalizedField := "_source"
	if fieldName != "" {
		normalizedField = fieldName
	}

	return &spl2.InExpr{
		Field:  &spl2.FieldExpr{Name: normalizedField},
		Values: values,
	}, true
}

// sourceScopeAnnotationRule annotates the query with resolved source scope
// information from the FROM clause and search predicates. This enables
// segment-level source filtering at scan time.
//
// The rule reads the SourceClause and search expressions, resolves glob
// patterns against a source registry (if available), and annotates the
// query with a "sourceScope" annotation containing the scope type and
// resolved source names.
type sourceScopeAnnotationRule struct{}

func (r *sourceScopeAnnotationRule) Name() string { return "SourceScopeAnnotation" }
func (r *sourceScopeAnnotationRule) Description() string {
	return "Annotates query with resolved source scope for segment-level filtering"
}

func (r *sourceScopeAnnotationRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	if q.Annotations != nil {
		if _, done := q.Annotations["sourceScope"]; done {
			return q, false
		}
	}

	scope := resolveSourceScope(q)
	if scope == nil {
		return q, false
	}

	// Store as a plain map so the hints merger (in pkg/spl2) can read it
	// without importing the optimizer package.
	anno := map[string]interface{}{
		"type": scope.Type,
	}
	if len(scope.Sources) > 0 {
		anno["sources"] = scope.Sources
	}
	if scope.Pattern != "" {
		anno["pattern"] = scope.Pattern
	}
	q.Annotate("sourceScope", anno)

	return q, true
}

// SourceScope describes which sources a query will scan.
type SourceScope struct {
	Type    string   // "all", "single", "list", "glob"
	Sources []string // resolved source names (for "single" and "list")
	Pattern string   // original glob pattern (for "glob")

	// sourceSet is a lazily built O(1) lookup set for large source lists (>16).
	sourceSet map[string]struct{}
}

// resolveSourceScope examines the query's FROM clause and search predicates
// to determine the effective source scope.
func resolveSourceScope(q *spl2.Query) *SourceScope {
	// FROM clause.
	if q.Source != nil && !q.Source.IsVariable {
		if q.Source.IsAllSources() {
			return &SourceScope{Type: scopeAll}
		}
		if q.Source.IsGlob {
			return &SourceScope{
				Type:    scopeGlob,
				Pattern: q.Source.Index,
			}
		}
		if names := q.Source.SourceNames(); len(names) > 0 {
			if len(names) == 1 {
				return &SourceScope{
					Type:    scopeSingle,
					Sources: names,
				}
			}

			return &SourceScope{
				Type:    scopeList,
				Sources: names,
			}
		}
	}

	// Search command predicates for source/index field filters.
	for _, cmd := range q.Commands {
		sc, ok := cmd.(*spl2.SearchCommand)
		if !ok || sc.Expression == nil {
			continue
		}
		if scope := extractSourceScopeFromSearch(sc.Expression); scope != nil {
			return scope
		}
	}

	// WHERE command predicates for source/index field filters.
	for _, cmd := range q.Commands {
		w, ok := cmd.(*spl2.WhereCommand)
		if !ok {
			continue
		}
		if scope := extractSourceScopeFromWhere(w.Expr); scope != nil {
			return scope
		}
	}

	return nil
}

// extractSourceScopeFromSearch extracts source scope from search expressions.
func extractSourceScopeFromSearch(expr spl2.SearchExpr) *SourceScope {
	switch e := expr.(type) {
	case *spl2.SearchCompareExpr:
		if !isSourceField(e.Field) || e.Op != spl2.OpEq {
			return nil
		}
		if e.HasWildcard {
			if e.Value == "*" {
				return &SourceScope{Type: scopeAll}
			}

			return &SourceScope{
				Type:    scopeGlob,
				Pattern: e.Value,
			}
		}

		return &SourceScope{
			Type:    scopeSingle,
			Sources: []string{e.Value},
		}

	case *spl2.SearchInExpr:
		if !isSourceField(e.Field) {
			return nil
		}
		sources := make([]string, 0, len(e.Values))
		for _, v := range e.Values {
			if v.HasWildcard {
				return nil // can't resolve wildcards in IN list statically
			}
			sources = append(sources, v.Value)
		}

		return &SourceScope{
			Type:    scopeList,
			Sources: sources,
		}

	case *spl2.SearchAndExpr:
		// Check both sides — source filters in AND are valid scope constraints.
		if scope := extractSourceScopeFromSearch(e.Left); scope != nil {
			return scope
		}

		return extractSourceScopeFromSearch(e.Right)

	case *spl2.SearchOrExpr:
		// search index=a OR index=b → combine into list scope.
		left := extractSourceScopeFromSearch(e.Left)
		right := extractSourceScopeFromSearch(e.Right)
		if left != nil && right != nil {
			if combined := mergeSourceScopes(left, right); combined != nil {
				return combined
			}
		}
	}

	return nil
}

// extractSourceScopeFromWhere extracts source scope from WHERE expressions.
func extractSourceScopeFromWhere(expr spl2.Expr) *SourceScope {
	switch e := expr.(type) {
	case *spl2.CompareExpr:
		field, ok := e.Left.(*spl2.FieldExpr)
		if !ok || !isSourceField(field.Name) {
			return nil
		}
		if e.Op != "=" && e.Op != "==" {
			return nil
		}
		lit, ok := e.Right.(*spl2.LiteralExpr)
		if ok {
			return &SourceScope{
				Type:    scopeSingle,
				Sources: []string{lit.Value},
			}
		}
		glob, ok := e.Right.(*spl2.GlobExpr)
		if ok {
			if glob.Pattern == "*" {
				return &SourceScope{Type: scopeAll}
			}

			return &SourceScope{
				Type:    scopeGlob,
				Pattern: glob.Pattern,
			}
		}

	case *spl2.InExpr:
		field, ok := e.Field.(*spl2.FieldExpr)
		if !ok || !isSourceField(field.Name) {
			return nil
		}
		sources := make([]string, 0, len(e.Values))
		for _, v := range e.Values {
			lit, ok := v.(*spl2.LiteralExpr)
			if !ok {
				return nil
			}
			sources = append(sources, lit.Value)
		}

		return &SourceScope{
			Type:    scopeList,
			Sources: sources,
		}

	case *spl2.BinaryExpr:
		if strings.EqualFold(e.Op, "and") {
			if scope := extractSourceScopeFromWhere(e.Left); scope != nil {
				return scope
			}

			return extractSourceScopeFromWhere(e.Right)
		}
	}

	return nil
}

// mergeSourceScopes combines two source scopes from OR branches into a single
// list scope. Returns nil if the scopes cannot be merged (e.g., glob patterns
// which can't be statically combined into a list).
func mergeSourceScopes(left, right *SourceScope) *SourceScope {
	// Only merge single/list scopes — globs and "all" are not combinable.
	leftSources := scopeToSources(left)
	rightSources := scopeToSources(right)
	if leftSources == nil || rightSources == nil {
		return nil
	}

	// Deduplicate combined sources.
	seen := make(map[string]struct{}, len(leftSources)+len(rightSources))
	combined := make([]string, 0, len(leftSources)+len(rightSources))
	for _, s := range leftSources {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			combined = append(combined, s)
		}
	}
	for _, s := range rightSources {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			combined = append(combined, s)
		}
	}

	if len(combined) == 1 {
		return &SourceScope{Type: scopeSingle, Sources: combined}
	}

	return &SourceScope{Type: scopeList, Sources: combined}
}

// scopeToSources returns the explicit source names from a scope, or nil
// if the scope type doesn't have a static source list (glob, all).
func scopeToSources(s *SourceScope) []string {
	switch s.Type {
	case scopeSingle, scopeList:
		if len(s.Sources) > 0 {
			return s.Sources
		}

		return nil
	default:
		return nil
	}
}

// MatchesSource returns true if the given source name matches this scope.
func (s *SourceScope) MatchesSource(name string) bool {
	switch s.Type {
	case scopeAll:
		return true
	case scopeSingle:
		return len(s.Sources) > 0 && s.Sources[0] == name
	case scopeList:
		// Use O(1) set lookup for large source lists (>16 entries).
		if len(s.Sources) > 16 {
			if s.sourceSet == nil {
				s.sourceSet = make(map[string]struct{}, len(s.Sources))
				for _, src := range s.Sources {
					s.sourceSet[src] = struct{}{}
				}
			}
			_, ok := s.sourceSet[name]

			return ok
		}

		for _, src := range s.Sources {
			if src == name {
				return true
			}
		}

		return false
	case scopeGlob:
		matched, _ := path.Match(s.Pattern, name)

		return matched
	default:
		return true
	}
}
