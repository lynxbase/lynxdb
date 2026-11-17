package rest

import (
	"net/http"
	"time"

	"github.com/lynxbase/lynxdb/pkg/auth"
	"github.com/lynxbase/lynxdb/pkg/database"
	"github.com/lynxbase/lynxdb/pkg/engine/physical"
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/ppl"
	"github.com/lynxbase/lynxdb/pkg/query"
)

// handleQueryExplain compiles a query without running it and describes its
// logical and optimized plans and the hints its scans carry; with
// analyze=true it also runs the query and reports its time and rows.
func (s *Server) handleQueryExplain(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeQuery) {
		return
	}
	q := r.URL.Query().Get("q")
	if q == "" {
		q = r.URL.Query().Get("query")
	}
	if q == "" {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, "q parameter is required")

		return
	}
	if !s.checkQueryLength(w, q) {
		return
	}
	from, to := r.URL.Query().Get("from"), r.URL.Query().Get("to")

	start := time.Now()
	c, err := query.Compile(q)
	compileTook := time.Since(start)
	if err != nil {
		pe, ok := ppl.AsError(err)
		if !ok {
			respondError(w, ErrCodePlanningError, http.StatusBadRequest, err.Error())

			return
		}
		respondData(w, http.StatusOK, map[string]interface{}{
			"is_valid": false,
			"errors":   []interface{}{explainError(pe)},
		})

		return
	}
	opts, err := queryTimeRange(from, to)
	if err != nil {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, err.Error())

		return
	}

	resp := map[string]interface{}{
		"is_valid": true,
		"errors":   []interface{}{},
		"parsed":   s.explainParsed(c, opts, compileTook),
	}
	if r.URL.Query().Get("analyze") == "true" {
		runStart := time.Now()
		if err := s.db.Sync(); err != nil {
			respondQueryFailure(w, err)
			return
		}
		res, err := s.db.Query(r.Context(), c, opts)
		if err != nil {
			respondQueryFailure(w, err)
			return
		}
		resp["execution"] = map[string]interface{}{
			"took_ms": durationMS(time.Since(runStart)),
			"rows":    res.Len(),
		}
	}
	respondData(w, http.StatusOK, resp)
}

func explainError(pe *ppl.Error) map[string]interface{} {
	obj := map[string]interface{}{
		"kind":    string(pe.Kind),
		"message": pe.Msg,
	}
	if pe.Pos.Line > 0 {
		obj["position"] = map[string]interface{}{"line": pe.Pos.Line, "column": pe.Pos.Col}
	}
	if len(pe.Expected) > 0 {
		obj["expected"] = pe.Expected
	}
	if pe.Hint != "" {
		obj["suggestion"] = pe.Hint
	}

	return obj
}

func (s *Server) explainParsed(c *query.Compiled, opts database.QueryOptions, compileTook time.Duration) map[string]interface{} {
	hints := physical.PlanHints(c.Plan, database.DefaultSource, opts.Now)
	hasTimeBounds := hints.TimeBounds != nil || !opts.From.IsZero() || !opts.To.IsZero()
	hasSearchHints := len(hints.SearchTerms) > 0 || len(hints.TokenGlobs) > 0 || len(hints.FieldPredicates) > 0
	cost := "low"
	switch {
	case !hasTimeBounds && !hasSearchHints:
		cost = "high"
	case !hasTimeBounds || !hasSearchHints:
		cost = "medium"
	}
	rules := make([]interface{}, 0, len(c.Applied))
	for _, a := range c.Applied {
		rules = append(rules, map[string]interface{}{"name": a.Rule, "count": a.Count})
	}
	parsed := map[string]interface{}{
		"result_type":     string(DetectResultType(c.Plan)),
		"estimated_cost":  cost,
		"uses_full_scan":  !hasTimeBounds && !hasSearchHints,
		"fields_read":     hints.RequiredCols,
		"search_terms":    hints.SearchTerms,
		"has_time_bounds": hasTimeBounds,
		"logical_plan":    ir.Render(c.Logical),
		"optimized_plan":  ir.Render(c.Plan),
		"compile_ms":      durationMS(compileTook),
		"optimizer_rules": rules,
	}
	if hints.SourceScopeType != "" {
		scope := map[string]interface{}{
			"type":                    hints.SourceScopeType,
			"total_sources_available": len(s.db.Names()),
		}
		if len(hints.SourceScopeSources) > 0 {
			scope["sources"] = hints.SourceScopeSources
		}
		if hints.SourceScopePattern != "" {
			scope["pattern"] = hints.SourceScopePattern
		}
		parsed["source_scope"] = scope
	}

	return parsed
}
