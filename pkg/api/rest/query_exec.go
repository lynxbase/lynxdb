package rest

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/lynxbase/lynxdb/pkg/engine/physical"
	"github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/planner"
	"github.com/lynxbase/lynxdb/pkg/ppl"
	"github.com/lynxbase/lynxdb/pkg/usecases"
)

// runQuery submits a query to the QueryService and writes its result, a job
// handle, or a structured error.
func (s *Server) runQuery(w http.ResponseWriter, r *http.Request, req QueryRequest) {
	query := req.effectiveQuery()
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
		SkipResultCache: planner.DynamicTimeBounds(req.effectiveFrom(), req.effectiveTo()),
	})
	if err != nil {
		handlePlanError(w, err)
		return
	}

	if !result.Done {
		writeJobHandleFromUsecase(w, result)
		return
	}
	if result.Error != "" {
		respondQueryError(w, result.Error, result.ErrorCode)
		return
	}
	lintsEnabled := req.Lint == nil || *req.Lint
	writeSyncResultFromUsecase(w, result, limit, req.Offset, query, queryCfg,
		lintsEnabled, req.LintLimit, req.LintFull)
}

// runQueryStream runs a query as a streaming NDJSON response.
func (s *Server) runQueryStream(w http.ResponseWriter, r *http.Request, query string, req QueryRequest) {
	start := time.Now()

	iter, stats, err := s.queryService.Stream(r.Context(), usecases.StreamRequest{
		Query: query,
		From:  req.effectiveFrom(),
		To:    req.effectiveTo(),
	})
	if err != nil {
		handlePlanError(w, err)
		return
	}
	defer iter.Close()

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
					"code":    string(ErrCodeExecutionError),
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
			if encErr := enc.Encode(rowToInterface(batch.Row(i))); encErr != nil {
				slog.Warn("rest: stream json encode failed", "error", encErr)
			}
			total++
		}
		if flusher != nil {
			flusher.Flush()
		}
	}

	scanned := stats.RowsScanned
	if ss, ok := iter.(interface {
		ScanStats() *pipeline.SegmentStreamStats
	}); ok {
		if segStats := ss.ScanStats(); segStats != nil {
			scanned = segStats.EventsScanned
		}
	}

	if encErr := enc.Encode(map[string]interface{}{
		"__meta": map[string]interface{}{
			"total":   total,
			"scanned": scanned,
			"took_ms": time.Since(start).Milliseconds(),
		},
	}); encErr != nil {
		slog.Warn("rest: stream json encode failed", "error", encErr)
	}
	if flusher != nil {
		flusher.Flush()
	}
}

// pplErrorCodes maps PPL compile error kinds to API error codes.
var pplErrorCodes = map[ppl.ErrorKind]ErrorCode{
	ppl.ErrSyntax:              ErrCodeSyntaxError,
	ppl.ErrSemantic:            ErrCodeSemanticError,
	ppl.ErrType:                ErrCodeTypeError,
	ppl.ErrUnsupportedCommand:  ErrCodeUnsupportedCommand,
	ppl.ErrUnsupportedFunction: ErrCodeUnsupportedFunction,
	ppl.ErrUnsupportedFeature:  ErrCodeUnsupportedFeature,
}

// respondCompileError writes a PPL compile error with its kind, position,
// expected tokens and hint; unsupported features answer 422, the others 400.
func respondCompileError(w http.ResponseWriter, pe *ppl.Error) {
	code, ok := pplErrorCodes[pe.Kind]
	if !ok {
		code = ErrCodeInvalidQuery
	}
	status := http.StatusBadRequest
	switch pe.Kind {
	case ppl.ErrUnsupportedCommand, ppl.ErrUnsupportedFunction, ppl.ErrUnsupportedFeature:
		status = http.StatusUnprocessableEntity
	}
	errObj := map[string]interface{}{
		"code":    string(code),
		"message": pe.Error(),
	}
	if pe.Pos.Line > 0 {
		errObj["position"] = map[string]interface{}{"line": pe.Pos.Line, "column": pe.Pos.Col}
	}
	if len(pe.Expected) > 0 {
		errObj["expected"] = pe.Expected
	}
	if pe.Hint != "" {
		errObj["suggestion"] = pe.Hint
	}
	respondJSON(w, status, map[string]interface{}{"error": errObj})
}

// respondQueryCompileError writes compile, planning and unsupported-plan
// errors; it reports whether err was one of them.
func respondQueryCompileError(w http.ResponseWriter, err error) bool {
	if pe, ok := ppl.AsError(err); ok {
		respondCompileError(w, pe)
		return true
	}
	var ue *physical.UnsupportedError
	if errors.As(err, &ue) {
		respondError(w, ErrCodeUnsupportedFeature, http.StatusUnprocessableEntity, ue.Error())
		return true
	}
	var pe *planner.PlanningError
	if errors.As(err, &pe) {
		respondError(w, ErrCodePlanningError, http.StatusBadRequest, pe.Error())
		return true
	}
	return false
}
