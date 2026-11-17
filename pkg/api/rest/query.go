package rest

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/lynxbase/lynxdb/pkg/api/apicontracts"
	"github.com/lynxbase/lynxdb/pkg/auth"
	"github.com/lynxbase/lynxdb/pkg/colstore/variant"
	"github.com/lynxbase/lynxdb/pkg/config"
	"github.com/lynxbase/lynxdb/pkg/database"
	"github.com/lynxbase/lynxdb/pkg/engine/columnar"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/query"
)

// ResultType is the shape of the rows a query returns.
type ResultType string

const (
	ResultTypeEvents    ResultType = "events"
	ResultTypeAggregate ResultType = "aggregate"
)

// DetectResultType reports whether a plan returns events or the rows of an
// aggregation.
func DetectResultType(plan *ir.Plan) ResultType {
	if plan == nil || plan.Root == nil {
		return ResultTypeEvents
	}
	for n := plan.Root; ; {
		switch x := n.(type) {
		case *ir.Aggregate, *ir.SourceSchema, *ir.Transpose:
			return ResultTypeAggregate
		case *ir.Filter, *ir.Project, *ir.RenameColumns, *ir.Compute, *ir.Sort,
			*ir.Limit, *ir.TopN, *ir.Reverse, *ir.Dedup, *ir.FillNull, *ir.Totals:
			n = x.Inputs()[0]
		default:
			return ResultTypeEvents
		}
	}
}

// defaultResultLimit bounds the rows of a job result when the config sets
// no default.
const defaultResultLimit = 1000

func validateQueryFormat(format string) error {
	if format == "" || format == apicontracts.QueryResponseFormatJSON {
		return nil
	}

	return errors.New(apicontracts.UnsupportedQueryFormatMessage(format))
}

// handleQueryGet is the GET variant for simple queries (query params: q, from, to, limit, format).
func (s *Server) handleQueryGet(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeQuery) {
		return
	}

	q := r.URL.Query().Get("q")
	if q == "" {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, "query parameter 'q' is required")

		return
	}
	format := r.URL.Query().Get("format")
	if err := validateQueryFormat(format); err != nil {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, err.Error(),
			WithSuggestion(apicontracts.QueryFormatSuggestion))

		return
	}
	s.executeQuery(w, r, QueryRequest{
		Q:      q,
		From:   r.URL.Query().Get("from"),
		To:     r.URL.Query().Get("to"),
		Limit:  parseIntParam(r, "limit", 0),
		Offset: parseIntParam(r, "offset", 0),
		Format: format,
	})
}

// handleQuery is the three-mode query handler (sync/hybrid/async).
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeQuery) {
		return
	}
	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest, "invalid JSON")

		return
	}
	if err := validateQueryFormat(req.Format); err != nil {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, err.Error(),
			WithSuggestion(apicontracts.QueryFormatSuggestion))

		return
	}
	s.executeQuery(w, r, req)
}

// executeQuery runs a query in the mode its request asks for: sync waits for
// the result, async returns the job handle at once, and hybrid waits up to
// the wait duration before it returns the handle.
func (s *Server) executeQuery(w http.ResponseWriter, r *http.Request, req QueryRequest) {
	src := req.effectiveQuery()
	if src == "" {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, "query is required")

		return
	}
	if !s.checkQueryLength(w, src) {
		return
	}
	cq, ok := compileQuery(w, src, req.effectiveFrom(), req.effectiveTo())
	if !ok {
		return
	}
	queryCfg := s.currentQueryConfig()
	limit := clampLimit(req.Limit, queryCfg)
	if cq.c.Explain {
		respondExplainCommand(w, cq.c)
		return
	}

	job, err := s.submit(cq)
	if err != nil {
		respondQueryFailure(w, err)
		return
	}

	var wait time.Duration
	switch {
	case req.Wait == nil:
		wait = queryCfg.SyncTimeout
		if wait <= 0 {
			wait = 30 * time.Second
		}
	case *req.Wait > 0:
		wait = time.Duration(*req.Wait * float64(time.Second))
	default:
		writeJobHandle(w, job)
		return
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-job.Done():
	case <-r.Context().Done():
		job.Cancel()
		s.db.Answered(job)
		return
	case <-timer.C:
		// A job that ended as the timer fired is answered in full.
		select {
		case <-job.Done():
		default:
			writeJobHandle(w, job)
			return
		}
	}
	res, err := job.Result()
	// The request takes the whole answer, so nothing reads the job later.
	s.db.Answered(job)
	if err != nil {
		respondQueryFailure(w, err)
		return
	}
	st := job.Status()
	respondRawData(w, http.StatusOK,
		appendResultData(nil, res, DetectResultType(cq.c.Plan), limit, req.Offset),
		WithTook(st.Elapsed), WithQueryID(job.ID), WithRows(res.Len()))
}

// respondExplainCommand answers a query written with the explain command:
// one row holding the rendered plans.
func respondExplainCommand(w http.ResponseWriter, c *query.Compiled) {
	respondData(w, http.StatusOK, map[string]interface{}{
		"type":       string(ResultTypeAggregate),
		"columns":    []string{"plan"},
		"rows":       [][]interface{}{{query.Explain(c)}},
		"total_rows": 1,
		"has_more":   false,
	}, WithRows(1))
}

// writeJobHandle writes 202 Accepted with the handle of a running job.
func writeJobHandle(w http.ResponseWriter, job *database.Job) {
	respondData(w, http.StatusAccepted, map[string]interface{}{
		"type":     "job",
		"job_id":   job.ID,
		"status":   string(database.JobRunning),
		"progress": jobProgress(job.Status()),
	}, WithQueryID(job.ID))
}

func jobProgress(st database.JobStatus) map[string]interface{} {
	return map[string]interface{}{
		"status":     string(st.State),
		"elapsed_ms": st.Elapsed.Milliseconds(),
	}
}

// clampLimit normalises the requested limit to server defaults; zero leaves
// the rows unbounded.
func clampLimit(limit int, cfg config.QueryConfig) int {
	if limit <= 0 {
		limit = cfg.DefaultResultLimit
	}
	if cfg.MaxResultLimit > 0 && limit > cfg.MaxResultLimit {
		limit = cfg.MaxResultLimit
	}

	return limit
}

// checkQueryLength validates that the query string doesn't exceed the configured
// maximum length. Returns true if the query is within limits. On failure, writes
// a 400 error response and returns false.
func (s *Server) checkQueryLength(w http.ResponseWriter, q string) bool {
	maxLen := s.currentQueryConfig().MaxQueryLength
	if maxLen > 0 && len(q) > maxLen {
		respondError(w, ErrCodeQueryTooLarge, http.StatusBadRequest,
			fmt.Sprintf("query length %d exceeds maximum allowed length of %d bytes", len(q), maxLen))
		return false
	}
	return true
}

// pageRange returns the rows [start, end) that offset and limit select out
// of n, and whether rows follow them; a limit of zero selects all.
func pageRange(n, limit, offset int) (start, end int, hasMore bool) {
	start = min(max(offset, 0), n)
	end = n
	if limit > 0 && end-start > limit {
		end, hasMore = start+limit, true
	}

	return start, end, hasMore
}

// appendResultData appends the data object of a query result.
func appendResultData(dst []byte, res *columnar.Result, rt ResultType, limit, offset int) []byte {
	if rt == ResultTypeAggregate {
		return appendAggregateData(dst, res, limit, offset)
	}

	return appendEventsData(dst, res, limit, offset)
}

// appendEventsData appends {"type":"events","events":[...],"total":N,"has_more":b}
// with each event written straight from the result.
func appendEventsData(dst []byte, res *columnar.Result, limit, offset int) []byte {
	total := res.Len()
	start, end, hasMore := pageRange(total, limit, offset)
	dst = append(dst, `{"type":"events","events":[`...)
	for i := start; i < end; i++ {
		if i > start {
			dst = append(dst, ',')
		}
		dst = res.AppendJSON(dst, i)
	}
	dst = append(dst, `],"total":`...)
	dst = strconv.AppendInt(dst, int64(total), 10)
	dst = append(dst, `,"has_more":`...)
	dst = strconv.AppendBool(dst, hasMore)

	return append(dst, '}')
}

// appendAggregateData appends {"type":"aggregate","columns":[...],"rows":[[...]],
// "total_rows":N,"has_more":b}.
func appendAggregateData(dst []byte, res *columnar.Result, limit, offset int) []byte {
	rows := res.Maps()
	start, end, hasMore := pageRange(len(rows), limit, offset)
	cols := resultColumns(res.Columns, rows[start:end])
	dst = append(dst, `{"type":"aggregate","columns":[`...)
	for i, c := range cols {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = variant.AppendJSONString(dst, []byte(c))
	}
	dst = append(dst, `],"rows":[`...)
	for i, row := range rows[start:end] {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = append(dst, '[')
		for j, c := range cols {
			if j > 0 {
				dst = append(dst, ',')
			}
			v, ok := row[c]
			if !ok {
				dst = append(dst, "null"...)
				continue
			}
			dst = appendValue(dst, v)
		}
		dst = append(dst, ']')
	}
	dst = append(dst, `],"total_rows":`...)
	dst = strconv.AppendInt(dst, int64(len(rows)), 10)
	dst = append(dst, `,"has_more":`...)
	dst = strconv.AppendBool(dst, hasMore)

	return append(dst, '}')
}

// resultColumns returns the columns of rows: those the plan fixes in their
// order, then any others in the order of orderColumns.
func resultColumns(fixed []string, rows []map[string]event.Value) []string {
	listed := make(map[string]struct{}, len(fixed))
	for _, c := range fixed {
		listed[c] = struct{}{}
	}
	others := map[string]struct{}{}
	for _, row := range rows {
		for k := range row {
			if _, ok := listed[k]; !ok {
				others[k] = struct{}{}
			}
		}
	}
	cols := append([]string{}, fixed...)

	return append(cols, orderColumns(others)...)
}

// appendValue appends a value of a row as JSON: a time as text in the
// layout of RFC 3339, a duration as its nanoseconds.
func appendValue(dst []byte, v event.Value) []byte {
	switch v.Type() {
	case event.FieldTypeBool:
		return strconv.AppendBool(dst, v.AsBool())
	case event.FieldTypeInt:
		return strconv.AppendInt(dst, v.AsInt(), 10)
	case event.FieldTypeFloat:
		return variant.AppendJSONFloat(dst, v.AsFloat())
	case event.FieldTypeString:
		return variant.AppendJSONString(dst, []byte(v.AsString()))
	case event.FieldTypeTimestamp:
		dst = append(dst, '"')
		dst = v.AsTimestamp().UTC().AppendFormat(dst, time.RFC3339Nano)
		return append(dst, '"')
	case event.FieldTypeDuration:
		return strconv.AppendInt(dst, int64(v.AsDuration()), 10)
	case event.FieldTypeArray:
		dst = append(dst, '[')
		for i, e := range v.AsArray() {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = appendValue(dst, e)
		}
		return append(dst, ']')
	case event.FieldTypeObject:
		fields := v.AsObject()
		names := make([]string, 0, len(fields))
		for name := range fields {
			names = append(names, name)
		}
		sort.Strings(names)
		dst = append(dst, '{')
		for i, name := range names {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = append(variant.AppendJSONString(dst, []byte(name)), ':')
			dst = appendValue(dst, fields[name])
		}
		return append(dst, '}')
	}

	return append(dst, "null"...)
}

// builtinFieldOrder defines the canonical display order for LynxDB internal
// fields. Matches internal/output.builtinFieldOrder for consistency between
// REST API and CLI output.
var builtinFieldOrder = [...]string{
	"_time", "_raw", "index", "_source", "_sourcetype", "source", "sourcetype", "host",
}

var builtinFieldRank = func() map[string]int {
	m := make(map[string]int, len(builtinFieldOrder))
	for i, name := range builtinFieldOrder {
		m[name] = i
	}

	return m
}()

// orderColumns produces a deterministic column list: builtin fields in
// canonical order, then user-defined fields alphabetically.
func orderColumns(seen map[string]struct{}) []string {
	builtins := make([]string, 0, len(builtinFieldOrder))
	user := make([]string, 0, len(seen))

	for col := range seen {
		if _, ok := builtinFieldRank[col]; ok {
			builtins = append(builtins, col)
		} else {
			user = append(user, col)
		}
	}

	sort.Slice(builtins, func(i, j int) bool {
		return builtinFieldRank[builtins[i]] < builtinFieldRank[builtins[j]]
	})
	sort.Strings(user)

	return append(builtins, user...)
}
