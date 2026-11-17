package rest

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

// ErrorCode is a machine-readable SCREAMING_SNAKE error code.
type ErrorCode string

const (
	ErrCodeInvalidJSON         ErrorCode = "INVALID_JSON"
	ErrCodeInvalidRequest      ErrorCode = "INVALID_REQUEST"
	ErrCodeValidationError     ErrorCode = "VALIDATION_ERROR"
	ErrCodeInvalidQuery        ErrorCode = "INVALID_QUERY"
	ErrCodeNotFound            ErrorCode = "NOT_FOUND"
	ErrCodeAlreadyExists       ErrorCode = "ALREADY_EXISTS"
	ErrCodeHasDependents       ErrorCode = "HAS_DEPENDENTS"
	ErrCodeTooManyRequests     ErrorCode = "TOO_MANY_REQUESTS"
	ErrCodeBackpressure        ErrorCode = "BACKPRESSURE"
	ErrCodeShuttingDown        ErrorCode = "SHUTTING_DOWN"
	ErrCodeAuthRequired        ErrorCode = "AUTH_REQUIRED"
	ErrCodeInvalidToken        ErrorCode = "INVALID_TOKEN"
	ErrCodeForbidden           ErrorCode = "FORBIDDEN"
	ErrCodeLastRootKey         ErrorCode = "LAST_ROOT_KEY"
	ErrCodeInternalError       ErrorCode = "INTERNAL_ERROR"
	ErrCodeQueryMemoryExceeded ErrorCode = "QUERY_MEMORY_EXCEEDED"
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
}

func (m metaFields) empty() bool { return m.TookMS == nil && m.QueryID == "" && m.Rows == nil }

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
