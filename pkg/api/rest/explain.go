package rest

import (
	"net/http"

	"github.com/lynxbase/lynxdb/pkg/usecases"
)

func (s *Server) handleQueryExplain(w http.ResponseWriter, r *http.Request) {
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

	result, err := s.queryService.Explain(r.Context(), usecases.ExplainRequest{Query: q, From: from, To: to})
	if err != nil {
		handlePlanError(w, err)

		return
	}
	resp := buildExplainResponse(result)
	if !result.IsValid || r.URL.Query().Get("analyze") != "true" {
		respondData(w, http.StatusOK, resp)

		return
	}

	submitResult, err := s.queryService.Submit(r.Context(), usecases.SubmitRequest{
		Query:   q,
		From:    from,
		To:      to,
		Mode:    usecases.QueryModeSync,
		Profile: "full",
	})
	if err != nil {
		handlePlanError(w, err)

		return
	}
	if submitResult.Done {
		resp["execution"] = searchStatsToMeta(&submitResult.Stats)
	}
	respondData(w, http.StatusOK, resp)
}

func buildExplainResponse(result *usecases.ExplainResult) map[string]interface{} {
	errs := make([]interface{}, 0, len(result.Errors))
	for _, e := range result.Errors {
		obj := map[string]interface{}{
			"kind":    e.Kind,
			"message": e.Message,
		}
		if e.Line > 0 {
			obj["position"] = map[string]interface{}{"line": e.Line, "column": e.Column}
		}
		if len(e.Expected) > 0 {
			obj["expected"] = e.Expected
		}
		if e.Suggestion != "" {
			obj["suggestion"] = e.Suggestion
		}
		errs = append(errs, obj)
	}
	resp := map[string]interface{}{
		"is_valid": result.IsValid,
		"errors":   errs,
	}
	p := result.Parsed
	if p == nil {
		return resp
	}
	rules := make([]interface{}, 0, len(p.RuleDetails))
	for _, rd := range p.RuleDetails {
		rules = append(rules, map[string]interface{}{"name": rd.Name, "count": rd.Count})
	}
	parsed := map[string]interface{}{
		"result_type":     p.ResultType,
		"estimated_cost":  p.EstimatedCost,
		"uses_full_scan":  p.UsesFullScan,
		"fields_read":     p.FieldsRead,
		"search_terms":    p.SearchTerms,
		"has_time_bounds": p.HasTimeBounds,
		"logical_plan":    p.LogicalPlan,
		"optimized_plan":  p.OptimizedPlan,
		"compile_ms":      p.CompileMS,
		"optimizer_rules": rules,
	}
	if p.SourceScope != nil {
		parsed["source_scope"] = p.SourceScope
	}
	resp["parsed"] = parsed
	return resp
}
