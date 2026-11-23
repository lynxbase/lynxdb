package rest

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
)

// ErrorCode is a machine-readable SCREAMING_SNAKE error code.
type ErrorCode string

const (
	ErrCodeInvalidJSON         ErrorCode = "INVALID_JSON"
	ErrCodeInvalidRequest      ErrorCode = "INVALID_REQUEST"
	ErrCodeValidationError     ErrorCode = "VALIDATION_ERROR"
	ErrCodeNotFound            ErrorCode = "NOT_FOUND"
	ErrCodeConflict            ErrorCode = "CONFLICT"
	ErrCodeTooManyRequests     ErrorCode = "TOO_MANY_REQUESTS"
	ErrCodeShuttingDown        ErrorCode = "SHUTTING_DOWN"
	ErrCodeAuthRequired        ErrorCode = "AUTH_REQUIRED"
	ErrCodeInvalidToken        ErrorCode = "INVALID_TOKEN"
	ErrCodeForbidden           ErrorCode = "FORBIDDEN"
	ErrCodeLastRootKey         ErrorCode = "LAST_ROOT_KEY"
	ErrCodeInternalError       ErrorCode = "INTERNAL_ERROR"
	ErrCodeQueryTooLarge       ErrorCode = "QUERY_TOO_LARGE"
	ErrCodeUnsupportedCommand  ErrorCode = "UNSUPPORTED_COMMAND"
	ErrCodeSyntaxError         ErrorCode = "SYNTAX_ERROR"
	ErrCodeSemanticError       ErrorCode = "SEMANTIC_ERROR"
	ErrCodeTypeError           ErrorCode = "TYPE_ERROR"
	ErrCodeUnsupportedFunction ErrorCode = "UNSUPPORTED_FUNCTION"
	ErrCodeUnsupportedFeature  ErrorCode = "UNSUPPORTED_FEATURE"
	ErrCodePlanningError       ErrorCode = "PLANNING_ERROR"
	ErrCodeExecutionError      ErrorCode = "EXECUTION_ERROR"
)

// errorBody is the structured error envelope.
type errorBody struct {
	Code       ErrorCode `json:"code"`
	Message    string    `json:"message"`
	Suggestion string    `json:"suggestion,omitempty"`
}

// ErrorOpt configures optional fields on an error response.
type ErrorOpt func(*errorBody)

// WithSuggestion adds a suggestion to the error response.
func WithSuggestion(s string) ErrorOpt {
	return func(e *errorBody) {
		if s != "" {
			e.Suggestion = s
		}
	}
}

// respondError writes {"error": {"code": "...", "message": "...", "suggestion": "..."}}.
func respondError(w http.ResponseWriter, code ErrorCode, httpStatus int, msg string, opts ...ErrorOpt) {
	body := errorBody{Code: code, Message: msg}
	for _, o := range opts {
		o(&body)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	if err := json.NewEncoder(w).Encode(map[string]interface{}{"error": body}); err != nil {
		slog.Warn("rest: json encode failed", "error", err)
	}
}

// respondInternalError is shorthand for 500 + INTERNAL_ERROR.
func respondInternalError(w http.ResponseWriter, msg string) {
	respondError(w, ErrCodeInternalError, http.StatusInternalServerError, msg)
}

// metaFields holds optional metadata for success responses.
type metaFields struct {
	TookMS  *float64 `json:"took_ms,omitempty"`
	QueryID string   `json:"query_id,omitempty"`
	// Rows is the number of rows a query returned.
	Rows *int `json:"rows,omitempty"`
	// Stats says what a query read; a query that the columnar executor ran
	// only in part has none.
	Stats *queryStats `json:"stats,omitempty"`
	// Warnings tell what a query could not know to be an error.
	Warnings []string `json:"warnings,omitempty"`
}

func (m metaFields) empty() bool {
	return m.TookMS == nil && m.QueryID == "" && m.Rows == nil && m.Stats == nil && len(m.Warnings) == 0
}

// queryStats is exec.Stats as a reply shows it.
type queryStats struct {
	Blocks                int64  `json:"blocks"`
	SkippedByKey          int64  `json:"skipped_by_key"`
	SkippedByToken        int64  `json:"skipped_by_token"`
	SkippedByTime         int64  `json:"skipped_by_time"`
	SkippedAsEmpty        int64  `json:"skipped_as_empty"`
	SkippedByStats        int64  `json:"skipped_by_stats"`
	CountedByIndex        int64  `json:"counted_by_index"`
	CountedByEntries      int64  `json:"counted_by_entries"`
	CountedByTime         int64  `json:"counted_by_time"`
	TimeMissedFilter      int64  `json:"time_missed_filter"`
	TimeMissedRecord      int64  `json:"time_missed_record"`
	TimeMissedKeys        int64  `json:"time_missed_keys"`
	TimeMissedNulls       int64  `json:"time_missed_nulls"`
	ColumnsSearched       int64  `json:"columns_searched"`
	ColumnsSkippedByToken int64  `json:"columns_skipped_by_token"`
	BlocksMatched         int64  `json:"blocks_matched"`
	RowsMatched           int64  `json:"rows_matched"`
	HeldBytes             int64  `json:"held_bytes"`
	Ordered               bool   `json:"ordered"`
	HashedFor             string `json:"hashed_for,omitempty"`
}

// MetaOpt configures optional metadata fields.
type MetaOpt func(*metaFields)

// WithTook adds took_ms to the meta.
func WithTook(d time.Duration) MetaOpt {
	return func(m *metaFields) {
		ms := durationMS(d)
		m.TookMS = &ms
	}
}

// WithQueryID adds query_id to the meta.
func WithQueryID(id string) MetaOpt {
	return func(m *metaFields) {
		m.QueryID = id
	}
}

// WithRows adds the row count of a query to the meta.
func WithRows(n int) MetaOpt {
	return func(m *metaFields) {
		m.Rows = &n
	}
}

// WithWarnings adds the warnings of a query to the meta.
func WithWarnings(warnings []string) MetaOpt {
	return func(m *metaFields) {
		m.Warnings = warnings
	}
}

// WithStats adds what a query read to the meta.
func WithStats(st exec.Stats) MetaOpt {
	return func(m *metaFields) {
		m.Stats = &queryStats{
			Blocks: st.Blocks, SkippedByKey: st.SkippedByKey, SkippedByToken: st.SkippedByToken,
			SkippedByTime: st.SkippedByTime, SkippedAsEmpty: st.SkippedAsEmpty, SkippedByStats: st.SkippedByStats,
			CountedByIndex: st.CountedByIndex, CountedByEntries: st.CountedByEntries, CountedByTime: st.CountedByTime,
			TimeMissedFilter: st.TimeMissedFilter, TimeMissedRecord: st.TimeMissedRecord,
			TimeMissedKeys: st.TimeMissedKeys, TimeMissedNulls: st.TimeMissedNulls,
			ColumnsSearched: st.ColumnsSearched, ColumnsSkippedByToken: st.ColumnsSkippedByToken,
			BlocksMatched: st.BlocksMatched, RowsMatched: st.RowsMatched, HeldBytes: st.HeldBytes,
			Ordered: st.Ordered, HashedFor: st.HashedFor,
		}
	}
}

func durationMS(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

func buildMeta(opts []MetaOpt) metaFields {
	var meta metaFields
	for _, o := range opts {
		o(&meta)
	}

	return meta
}

// respondData writes {"data": payload, "meta": {...}}.
// When a query_id is present, it is also set as an X-Query-ID response header
// so the logging middleware can correlate HTTP requests with query execution.
func respondData(w http.ResponseWriter, httpStatus int, data interface{}, opts ...MetaOpt) {
	meta := buildMeta(opts)
	envelope := map[string]interface{}{"data": data}
	if !meta.empty() {
		envelope["meta"] = meta
	}
	if meta.QueryID != "" {
		w.Header().Set("X-Query-ID", meta.QueryID)
	}
	w.Header().Set("Content-Type", "application/json")
	if httpStatus != http.StatusOK {
		w.WriteHeader(httpStatus)
	}
	if err := json.NewEncoder(w).Encode(envelope); err != nil {
		slog.Warn("rest: json encode failed", "error", err)
	}
}

// respondRawData writes {"data": data, "meta": {...}} where data is JSON
// already encoded.
func respondRawData(w http.ResponseWriter, httpStatus int, data []byte, opts ...MetaOpt) {
	meta := buildMeta(opts)
	buf := make([]byte, 0, len(data)+96)
	buf = append(append(append(buf, `{"data":`...), data...), ',')
	if !meta.empty() {
		m, err := json.Marshal(meta)
		if err != nil {
			slog.Warn("rest: json encode failed", "error", err)
		} else {
			buf = append(append(append(buf, `"meta":`...), m...), ',')
		}
	}
	buf = append(buf[:len(buf)-1], '}', '\n')
	if meta.QueryID != "" {
		w.Header().Set("X-Query-ID", meta.QueryID)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	if _, err := w.Write(buf); err != nil {
		slog.Warn("rest: write failed", "error", err)
	}
}

// parseIntParam parses an integer query parameter, returning defaultVal if
// the parameter is missing or unparseable.
func parseIntParam(r *http.Request, name string, defaultVal int) int {
	s := r.URL.Query().Get(name)
	if s == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return defaultVal
	}

	return n
}

// respondJSON writes raw JSON without an envelope (for HEC Splunk-compat).
func respondJSON(w http.ResponseWriter, httpStatus int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if httpStatus != http.StatusOK {
		w.WriteHeader(httpStatus)
	}
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Warn("rest: json encode failed", "error", err)
	}
}
