package rest

import (
	"net/http"

	"github.com/OrlovEvgeny/Lynxdb/pkg/usecases"
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

	// EXPLAIN ANALYZE: execute the query with profiling and return plan + stats.
	if r.URL.Query().Get("analyze") == "true" {
		s.handleExplainAnalyze(w, r, q)

		return
	}

	result, err := s.queryService.Explain(r.Context(), usecases.ExplainRequest{
		Query: q,
		From:  r.URL.Query().Get("from"),
		To:    r.URL.Query().Get("to"),
	})
	if err != nil {
		handlePlanError(w, err)

		return
	}

	respondExplainResult(w, result)
}

// handleExplainAnalyze runs both EXPLAIN and actual execution with profiling,
// returning the logical plan alongside actual execution statistics.
func (s *Server) handleExplainAnalyze(w http.ResponseWriter, r *http.Request, q string) {
	from := r.URL.Query().Get("from")
	to := r.URL.Query().Get("to")

	// Get the logical plan.
	explainResult, err := s.queryService.Explain(r.Context(), usecases.ExplainRequest{
		Query: q, From: from, To: to,
	})
	if err != nil {
		handlePlanError(w, err)

		return
	}
	if !explainResult.IsValid {
		respondExplainResult(w, explainResult)

		return
	}

	// Execute with full profiling.
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

	// Build the combined response: plan + actual execution stats.
	resp := buildExplainResponse(explainResult)
	if submitResult.Done {
		ms := searchStatsToMeta(&submitResult.Stats)
		resp["execution"] = ms
	}

	respondData(w, http.StatusOK, resp)
}

// respondExplainResult writes the standard explain response.
func respondExplainResult(w http.ResponseWriter, result *usecases.ExplainResult) {
	if !result.IsValid {
		errs := make([]map[string]interface{}, len(result.Errors))
		for i, e := range result.Errors {
			errs[i] = map[string]interface{}{
				"message":    e.Message,
				"suggestion": e.Suggestion,
			}
		}
		respondData(w, http.StatusOK, map[string]interface{}{
			"is_valid": false,
			"errors":   errs,
		})

		return
	}

	respondData(w, http.StatusOK, buildExplainResponse(result))
}

// buildExplainResponse constructs the explain JSON response from an ExplainResult.
func buildExplainResponse(result *usecases.ExplainResult) map[string]interface{} {
	stages := make([]map[string]interface{}, len(result.Parsed.Pipeline))
	for i, s := range result.Parsed.Pipeline {
		stages[i] = map[string]interface{}{
			"command": s.Command,
		}
	}

	parsed := map[string]interface{}{
		"pipeline":        stages,
		"result_type":     result.Parsed.ResultType,
		"estimated_cost":  result.Parsed.EstimatedCost,
		"uses_full_scan":  result.Parsed.UsesFullScan,
		"fields_read":     result.Parsed.FieldsRead,
		"search_terms":    result.Parsed.SearchTerms,
		"has_time_bounds": result.Parsed.HasTimeBounds,
	}
	if len(result.Parsed.OptimizerStats) > 0 {
		parsed["optimizer_stats"] = result.Parsed.OptimizerStats
	}
	if result.Parsed.PhysicalPlan != nil {
		parsed["physical_plan"] = result.Parsed.PhysicalPlan
	}
	if result.Parsed.ParseMS > 0 {
		parsed["parse_ms"] = result.Parsed.ParseMS
	}
	if result.Parsed.OptimizeMS > 0 {
		parsed["optimize_ms"] = result.Parsed.OptimizeMS
	}
	if result.Parsed.TotalRules > 0 {
		parsed["total_rules"] = result.Parsed.TotalRules
	}
	if len(result.Parsed.RuleDetails) > 0 {
		rules := make([]map[string]interface{}, len(result.Parsed.RuleDetails))
		for i, rd := range result.Parsed.RuleDetails {
			rules[i] = map[string]interface{}{
				"name":        rd.Name,
				"description": rd.Description,
				"count":       rd.Count,
			}
		}
		parsed["optimizer_rules"] = rules
	}
	if result.Parsed.SourceScope != nil {
		scope := map[string]interface{}{
			"type": result.Parsed.SourceScope.Type,
		}
		if len(result.Parsed.SourceScope.Sources) > 0 {
			scope["resolved_sources"] = result.Parsed.SourceScope.Sources
		}
		if result.Parsed.SourceScope.Pattern != "" {
			scope["pattern"] = result.Parsed.SourceScope.Pattern
		}
		if result.Parsed.SourceScope.TotalSourcesAvailable > 0 {
			scope["total_sources_available"] = result.Parsed.SourceScope.TotalSourcesAvailable
		}
		parsed["source_scope"] = scope
	}

	if len(result.Parsed.OptimizerMessages) > 0 {
		parsed["optimizer_messages"] = result.Parsed.OptimizerMessages
	}
	if len(result.Parsed.OptimizerWarnings) > 0 {
		parsed["optimizer_warnings"] = result.Parsed.OptimizerWarnings
	}

	return map[string]interface{}{
		"is_valid": true,
		"parsed":   parsed,
		"errors":   []interface{}{},
		"acceleration": map[string]interface{}{
			"available": result.HasMVAccel,
		},
	}
}
