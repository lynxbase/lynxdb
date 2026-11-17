package rest

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/lynxflow/parser"
	"github.com/lynxbase/lynxdb/pkg/planner"
	"github.com/lynxbase/lynxdb/pkg/usecases"
)

// executeLynxFlowQuery runs a LynxFlow query via the QueryService (which
// delegates to the planner and engine for full segment-streaming scan, result
// cache, scan stats, job infrastructure, and sync/hybrid/async dispatch).
func (s *Server) executeLynxFlowQuery(w http.ResponseWriter, r *http.Request, req QueryRequest, lang langDetectResult) {
	query := req.effectiveQuery()

	skipResultCache := planner.DynamicTimeBounds(req.effectiveFrom(), req.effectiveTo()) ||
		planner.QueryUsesDynamicTimeSyntax(query)

	mode, wait := mapQueryMode(req.Wait)
	queryCfg := s.currentQueryConfig()
	limit := clampLimit(req.Limit, queryCfg)

	result, err := s.queryService.Submit(r.Context(), usecases.SubmitRequest{
		Query:           query,
		From:            req.effectiveFrom(),
		To:              req.effectiveTo(),
		Limit:           limit,
		Offset:          req.Offset,
		Mode:            mode,
		Wait:            wait,
		Profile:         req.Profile,
		NoLint:          req.Lint != nil && !*req.Lint,
		NoSuggestions:   req.Suggestions != nil && !*req.Suggestions,
		LintLimit:       req.LintLimit,
		LintFull:        req.LintFull,
		SkipResultCache: skipResultCache,
	})
	if err != nil {
		// Check for planner.ParseError with Diag for the structured error envelope.
		var pe *planner.ParseError
		if errors.As(err, &pe) && pe.Diag != nil {
			respondLynxFlowParseError(w, *pe.Diag)
			return
		}
		handlePlanError(w, err)
		return
	}

	langOpt := WithLanguage(string(lang.Language))

	if result.Done {
		if result.Error != "" {
			respondQueryError(w, result.Error, result.ErrorCode)
			return
		}

		lintsEnabled := req.Lint == nil || *req.Lint
		writeSyncResultFromUsecase(w, result, limit, req.Offset, query, queryCfg,
			lintsEnabled, req.LintLimit, req.LintFull,
			langOpt)
	} else {
		writeJobHandleFromUsecase(w, result, langOpt)
	}
}

// executeLynxFlowStream runs a LynxFlow query as a streaming NDJSON response
// via queryService.Stream (which delegates to the planner and engine for
// segment-streaming scan with hints).
func (s *Server) executeLynxFlowStream(w http.ResponseWriter, r *http.Request, query string, req QueryRequest, lang langDetectResult) {
	start := time.Now()

	iter, stats, err := s.queryService.Stream(r.Context(), usecases.StreamRequest{
		Query: query,
		From:  req.effectiveFrom(),
		To:    req.effectiveTo(),
	})
	if err != nil {
		// Check for planner.ParseError with Diag for the structured error envelope.
		var pe *planner.ParseError
		if errors.As(err, &pe) && pe.Diag != nil {
			respondLynxFlowParseError(w, *pe.Diag)
			return
		}
		handlePlanError(w, err)
		return
	}
	defer iter.Close()

	// Set streaming headers before first write.
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)

	flusher, _ := w.(http.Flusher)
	enc := json.NewEncoder(w)
	total := 0

	for {
		if err := r.Context().Err(); err != nil {
			return
		}

		batch, err := iter.Next(r.Context())
		if err != nil {
			if encErr := enc.Encode(map[string]interface{}{
				"__error": map[string]interface{}{
					"code":    "STREAM_ERROR",
					"message": err.Error(),
				},
			}); encErr != nil {
				slog.Warn("rest: stream json encode failed", "error", encErr)
			}
			if flusher != nil {
				flusher.Flush()
			}
			return
		}
		if batch == nil {
			break
		}

		for i := 0; i < batch.Len; i++ {
			row := batch.Row(i)
			out := rowToInterface(row)
			if encErr := enc.Encode(out); encErr != nil {
				slog.Warn("rest: stream json encode failed", "error", encErr)
			}
			total++
		}
		if flusher != nil {
			flusher.Flush()
		}
	}

	// Attempt to extract post-drain scan stats from the iterator.
	scanned := stats.RowsScanned
	if ss, ok := iter.(interface {
		ScanStats() *pipeline.SegmentStreamStats
	}); ok {
		if segStats := ss.ScanStats(); segStats != nil {
			scanned = segStats.EventsScanned
		}
	}

	elapsed := time.Since(start)
	if encErr := enc.Encode(map[string]interface{}{
		"__meta": map[string]interface{}{
			"total":   total,
			"scanned": scanned,
			"took_ms": elapsed.Milliseconds(),
		},
	}); encErr != nil {
		slog.Warn("rest: stream json encode failed", "error", encErr)
	}
	if flusher != nil {
		flusher.Flush()
	}
}

// respondLynxFlowParseError maps a LynxFlow parser.Diag to the structured
// error contract: {code, message, position{start,end}, expected, suggestion}.
//
// Uses the same top-level error code (INVALID_QUERY) as the SPL2 path for
// backward compatibility. The detailed diag code (e.g. E001) is carried in
// the "diag_code" field for callers that want finer-grained classification.
func respondLynxFlowParseError(w http.ResponseWriter, d parser.Diag) {
	errObj := map[string]interface{}{
		"code":      string(ErrCodeInvalidQuery),
		"diag_code": string(d.Code),
		"message":   "parse error: " + d.Message,
	}
	if d.Span.Start != 0 || d.Span.End != 0 {
		errObj["position"] = map[string]interface{}{
			"start": d.Span.Start,
			"end":   d.Span.End,
		}
	}
	if len(d.Expected) > 0 {
		errObj["expected"] = d.Expected
	}
	if d.Suggestion != "" {
		errObj["suggestion"] = d.Suggestion
	}

	respondJSON(w, http.StatusBadRequest, map[string]interface{}{"error": errObj})
}
