package rest

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/lynxbase/lynxdb/pkg/database"
	"github.com/lynxbase/lynxdb/pkg/engine/physical"
	"github.com/lynxbase/lynxdb/pkg/ppl"
	"github.com/lynxbase/lynxdb/pkg/query"
	"github.com/lynxbase/lynxdb/pkg/timerange"
)

// compiledQuery is a query ready to run with the time range of its request.
type compiledQuery struct {
	src  string
	c    *query.Compiled
	opts database.QueryOptions
}

// compileQuery compiles src and parses the from and to of its request; it
// writes the error response and reports false when either fails.
func compileQuery(w http.ResponseWriter, src, from, to string) (*compiledQuery, bool) {
	c, err := query.Compile(src)
	if err != nil {
		if _, ok := ppl.AsError(err); ok {
			respondQueryFailure(w, err)
		} else {
			respondError(w, ErrCodePlanningError, http.StatusBadRequest, err.Error())
		}
		return nil, false
	}
	opts, err := queryTimeRange(from, to)
	if err != nil {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, err.Error())
		return nil, false
	}

	return &compiledQuery{src: src, c: c, opts: opts}, true
}

// queryTimeRange parses the from and to of a request into query options.
func queryTimeRange(from, to string) (database.QueryOptions, error) {
	now := time.Now()
	tr, err := timerange.ParseOptionalRange(from, to, now)
	if err != nil || tr == nil {
		return database.QueryOptions{Now: now}, err
	}

	return database.QueryOptions{From: tr.Earliest, To: tr.Latest, Now: now}, nil
}

// submit starts a query as a job and records its duration when it ends. It
// syncs the database first, so that a query sees every event ingested before
// it.
func (s *Server) submit(cq *compiledQuery) (*database.Job, error) {
	if err := s.db.Sync(); err != nil {
		return nil, err
	}
	job, err := s.db.Submit(cq.c, cq.src, cq.opts)
	if err != nil {
		return nil, err
	}
	rt := DetectResultType(cq.c.Plan)
	go func() {
		<-job.Done()
		st := job.Status()
		errType := ""
		if st.Err != nil && !errors.Is(st.Err, context.Canceled) {
			code, _ := queryErrorCode(st.Err)
			errType = string(code)
		}
		s.promMetrics.RecordQuery(string(rt), st.Elapsed, errType)
	}()

	return job, nil
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

// queryErrorCode returns the API code and HTTP status of an error that a
// query compiled or ran into.
func queryErrorCode(err error) (ErrorCode, int) {
	if pe, ok := ppl.AsError(err); ok {
		code := pplErrorCodes[pe.Kind]
		switch pe.Kind {
		case ppl.ErrUnsupportedCommand, ppl.ErrUnsupportedFunction, ppl.ErrUnsupportedFeature:
			return code, http.StatusUnprocessableEntity
		}
		return code, http.StatusBadRequest
	}
	var ue *physical.UnsupportedError
	switch {
	case errors.As(err, &ue):
		return ErrCodeUnsupportedFeature, http.StatusUnprocessableEntity
	case errors.Is(err, database.ErrTooManyQueries):
		return ErrCodeTooManyRequests, http.StatusTooManyRequests
	}

	return ErrCodeExecutionError, http.StatusInternalServerError
}

// respondQueryFailure writes the error response for an error a query
// compiled or ran into.
func respondQueryFailure(w http.ResponseWriter, err error) {
	if pe, ok := ppl.AsError(err); ok {
		respondCompileError(w, pe)
		return
	}
	code, status := queryErrorCode(err)
	if code == ErrCodeTooManyRequests {
		// A query slot frees the instant any running query finishes.
		w.Header().Set("Retry-After", "1")
	}
	respondError(w, code, status, err.Error())
}

// respondCompileError writes a PPL compile error with its kind, position,
// expected tokens and hint; unsupported features answer 422, the others 400.
func respondCompileError(w http.ResponseWriter, pe *ppl.Error) {
	code, status := queryErrorCode(pe)
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
