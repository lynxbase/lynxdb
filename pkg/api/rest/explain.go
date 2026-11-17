package rest

import (
	"net/http"

	"github.com/lynxbase/lynxdb/pkg/lynxflow/run"
	"github.com/lynxbase/lynxdb/pkg/model"
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

	langParam := r.URL.Query().Get("language")
	if msg := validateExplicitLanguage(langParam); msg != "" {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, msg,
			WithSuggestion(`set language="lynxflow" or omit it; SPL2 was removed — see https://lynxdb.dev/docs/migration`))
		return
	}

	lang := detectQueryLanguage(q, langParam)

	// EXPLAIN ANALYZE: execute the query with profiling and return plan + stats.
	if r.URL.Query().Get("analyze") == "true" {
		s.handleExplainAnalyze(w, r, q)

		return
	}

	// Rich EXPLAIN: parse, optimize, and return a structured envelope with
	// the text plan, parsed pipeline summary, optimizer details, and MV
	// acceleration info.
	result, err := s.queryService.Explain(r.Context(), usecases.ExplainRequest{
		Query: q,
		From:  r.URL.Query().Get("from"),
		To:    r.URL.Query().Get("to"),
	})
	if err != nil {
		handlePlanError(w, err)

		return
	}

	if !result.IsValid {
		respondExplainResult(w, result, "", lang)

		return
	}

	// Also generate the text plan for backward compatibility (the previous
	// endpoint returned only {is_valid, lynxflow_plan, errors}).
	textPlan, _ := run.ExecuteExplain(q, run.Options{DefaultSource: "main"})

	respondExplainResult(w, result, textPlan, lang)
}

// handleExplainAnalyze runs both EXPLAIN and actual execution with profiling,
// returning the logical plan alongside actual execution statistics.
func (s *Server) handleExplainAnalyze(w http.ResponseWriter, r *http.Request, q string) {
	from := r.URL.Query().Get("from")
	to := r.URL.Query().Get("to")

	explainResult, err := s.queryService.Explain(r.Context(), usecases.ExplainRequest{
		Query: q, From: from, To: to,
	})
	if err != nil {
		handlePlanError(w, err)

		return
	}
	if !explainResult.IsValid {
		respondExplainResult(w, explainResult, "", langDetectResult{Language: LangLynxFlow})

		return
	}

	// Execute with full profiling.
	normalizedQuery := q
	var rewrites []model.QueryRewrite
	submitResult, err := s.queryService.Submit(r.Context(), usecases.SubmitRequest{
		Query:    normalizedQuery,
		From:     from,
		To:       to,
		Mode:     usecases.QueryModeSync,
		Profile:  "full",
		Rewrites: rewrites,
	})
	if err != nil {
		handlePlanError(w, err)

		return
	}

	if submitResult.Done {
		applyAnalyzedRangePredicates(explainResult, submitResult.Stats.RangePredicates)
	}

	// Build the combined response: plan + actual execution stats.
	textPlan, _ := run.ExecuteExplain(q, run.Options{DefaultSource: "main"})
	resp := buildExplainResponse(explainResult, textPlan)
	if submitResult.Done {
		ms := searchStatsToMeta(&submitResult.Stats)
		resp["execution"] = ms
	}

	respondData(w, http.StatusOK, resp, WithLanguage(string(LangLynxFlow)))
}

func applyAnalyzedRangePredicates(result *usecases.ExplainResult, preds []model.RangePredicate) {
	if result == nil || result.Parsed == nil || len(preds) == 0 {
		return
	}
	result.Parsed.RangePredicates = make([]usecases.ExplainRangePredicate, 0, len(preds))
	for _, pred := range preds {
		rgStrategy := "zone-map"
		rowStrategy := "per-row"
		if pred.LoweredToBSI {
			rgStrategy = "bsi"
			rowStrategy = "handled_by=bsi"
		}
		result.Parsed.RangePredicates = append(result.Parsed.RangePredicates, usecases.ExplainRangePredicate{
			Field:            pred.Field,
			Min:              pred.Min,
			Max:              pred.Max,
			LoweredToBSI:     pred.LoweredToBSI,
			RGFilterStrategy: rgStrategy,
			RowVMStrategy:    rowStrategy,
		})
	}
}

// respondExplainResult writes the standard explain response.
func respondExplainResult(w http.ResponseWriter, result *usecases.ExplainResult, textPlan string, lang langDetectResult) {
	if !result.IsValid {
		errs := make([]map[string]interface{}, len(result.Errors))
		for i, e := range result.Errors {
			errs[i] = map[string]interface{}{
				"message":    e.Message,
				"suggestion": e.Suggestion,
			}
		}
		body := map[string]interface{}{
			"is_valid": false,
			"errors":   errs,
		}
		if len(result.Rewrites) > 0 {
			body["rewrites"] = result.Rewrites
		}
		respondData(w, http.StatusOK, body, WithLanguage(string(lang.Language)))

		return
	}

	respondData(w, http.StatusOK, buildExplainResponse(result, textPlan), WithLanguage(string(lang.Language)))
}

// buildExplainResponse constructs the explain JSON response from an ExplainResult.
// The textPlan is the human-readable EXPLAIN tree; it is included as
// "lynxflow_plan" for backward compatibility with existing consumers.
func buildExplainResponse(result *usecases.ExplainResult, textPlan string) map[string]interface{} {
	stages := make([]map[string]interface{}, len(result.Parsed.Pipeline))
	for i, s := range result.Parsed.Pipeline {
		stageObj := map[string]interface{}{
			"command": s.Command,
		}
		if s.Description != "" {
			stageObj["description"] = s.Description
		}
		if len(s.FieldsAdded) > 0 {
			stageObj["fields_added"] = s.FieldsAdded
		}
		if len(s.FieldsRemoved) > 0 {
			stageObj["fields_removed"] = s.FieldsRemoved
		}
		if len(s.FieldsOut) > 0 {
			stageObj["fields_out"] = s.FieldsOut
		}
		if len(s.FieldsOptional) > 0 {
			stageObj["fields_optional"] = s.FieldsOptional
		}
		if s.FieldsUnknown {
			stageObj["fields_unknown"] = true
		}
		stages[i] = stageObj
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
	if len(result.Parsed.RangePredicates) > 0 {
		preds := make([]map[string]interface{}, 0, len(result.Parsed.RangePredicates))
		for _, pred := range result.Parsed.RangePredicates {
			p := map[string]interface{}{
				"field":              pred.Field,
				"rg_filter_strategy": pred.RGFilterStrategy,
				"row_vm_strategy":    pred.RowVMStrategy,
			}
			if pred.Min != "" {
				p["min"] = pred.Min
			}
			if pred.Max != "" {
				p["max"] = pred.Max
			}
			if pred.LoweredToBSI {
				p["lowered_to_bsi"] = true
			}
			preds = append(preds, p)
		}
		parsed["range_predicates"] = preds
	}

	if len(result.Parsed.OptimizerMessages) > 0 {
		parsed["optimizer_messages"] = result.Parsed.OptimizerMessages
	}
	if len(result.Parsed.OptimizerWarnings) > 0 {
		parsed["optimizer_warnings"] = result.Parsed.OptimizerWarnings
	}

	resp := map[string]interface{}{
		"is_valid": true,
		"parsed":   parsed,
		"errors":   []interface{}{},
	}
	// Backward compatibility: include the text plan so existing consumers
	// that read "lynxflow_plan" continue to work.
	if textPlan != "" {
		resp["lynxflow_plan"] = textPlan
	}
	if len(result.Rewrites) > 0 {
		resp["rewrites"] = result.Rewrites
	}

	return resp
}
