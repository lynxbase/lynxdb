package rest

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/usecases"
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
	ErrCodeQueryPoolExhausted  ErrorCode = "QUERY_POOL_EXHAUSTED"
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

// respondQueryError maps a query error with a machine-readable code to the
// appropriate HTTP status. QUERY_MEMORY_EXCEEDED → 400 (client can narrow the
// query); QUERY_POOL_EXHAUSTED → 503 (server-wide resource contention).
// Unknown or empty codes fall back to 500 INTERNAL_ERROR.
func respondQueryError(w http.ResponseWriter, msg, code string) {
	switch ErrorCode(code) {
	case ErrCodeQueryMemoryExceeded:
		respondError(w, ErrCodeQueryMemoryExceeded, http.StatusBadRequest, msg,
			WithSuggestion("Narrow the time range, add filters, or increase query.max_query_memory_bytes."))
	case ErrCodeQueryPoolExhausted:
		respondError(w, ErrCodeQueryPoolExhausted, http.StatusServiceUnavailable, msg,
			WithSuggestion("Wait for other queries to complete, or increase query.global_query_pool_bytes."))
	default:
		respondInternalError(w, msg)
	}
}

// metaFields holds optional metadata for success responses.
type metaFields struct {
	TookMS          *float64   `json:"took_ms,omitempty"`
	Scanned         *int64     `json:"scanned,omitempty"`
	QueryID         string     `json:"query_id,omitempty"`
	SegmentsErrored *int       `json:"segments_errored,omitempty"` // E7: surface segment read errors
	SearchStats     *metaStats `json:"stats,omitempty"`            // rich query stats for CLI display
}

// metaStats holds detailed query execution statistics returned in the
// response meta block. Clients use these to display query performance
// information (segment skips, scan type, optimizations applied, etc.).
type metaStats struct {
	RowsScanned          int64               `json:"rows_scanned"`
	RowsReturned         int64               `json:"rows_returned"`
	MatchedRows          int64               `json:"matched_rows,omitempty"`
	SegmentsTotal        int                 `json:"segments_total,omitempty"`
	SegmentsScanned      int                 `json:"segments_scanned,omitempty"`
	SegmentsSkippedIdx   int                 `json:"segments_skipped_index,omitempty"`
	SegmentsSkippedTime  int                 `json:"segments_skipped_time,omitempty"`
	SegmentsSkippedStat  int                 `json:"segments_skipped_stats,omitempty"`
	SegmentsSkippedBF    int                 `json:"segments_skipped_bloom,omitempty"`
	SegmentsSkippedRange int                 `json:"segments_skipped_range,omitempty"`
	BufferedEvents       int                 `json:"buffered_events,omitempty"`
	InvertedIndexHits    int                 `json:"inverted_index_hits,omitempty"`
	IndexesUsed          []string            `json:"indexes_used,omitempty"`
	CountStarOptimized   bool                `json:"count_star_optimized,omitempty"`
	PartialAggUsed       bool                `json:"partial_agg_used,omitempty"`
	TopKUsed             bool                `json:"topk_used,omitempty"`
	PrefetchUsed         bool                `json:"prefetch_used,omitempty"`
	VectorizedFilterUsed bool                `json:"vectorized_filter_used,omitempty"`
	DictFilterUsed       bool                `json:"dict_filter_used,omitempty"`
	JoinStrategy         string              `json:"join_strategy,omitempty"`
	ScanMS               float64             `json:"scan_ms,omitempty"`
	PipelineMS           float64             `json:"pipeline_ms,omitempty"`
	AcceleratedBy        string              `json:"accelerated_by,omitempty"`
	MVStatus             string              `json:"mv_status,omitempty"`
	MVSpeedup            string              `json:"mv_speedup,omitempty"`
	MVOriginalScan       int64               `json:"mv_original_scan,omitempty"`
	CacheHit             bool                `json:"cache_hit,omitempty"`
	PipelineStages       []metaPipelineStage `json:"pipeline_stages,omitempty"`

	// Parse/optimize timing from planner.
	ParseMS    float64 `json:"parse_ms,omitempty"`
	OptimizeMS float64 `json:"optimize_ms,omitempty"`

	// Optimizer rule details from planner.
	OptimizerRules []metaOptimizerRule `json:"optimizer_rules,omitempty"`
	TotalRules     int                 `json:"total_rules,omitempty"`

	// ProcessedBytes is the total bytes of segment data touched by the query.
	ProcessedBytes int64 `json:"processed_bytes,omitempty"`

	// I/O bytes breakdown by source.
	DiskBytesRead  int64 `json:"disk_bytes_read,omitempty"`
	S3BytesRead    int64 `json:"s3_bytes_read,omitempty"`
	CacheBytesRead int64 `json:"cache_bytes_read,omitempty"`

	// Resource usage (server-side, captured via BudgetMonitor + getrusage).
	PeakMemoryBytes int64    `json:"peak_memory_bytes,omitempty"`
	MemAllocBytes   int64    `json:"mem_alloc_bytes,omitempty"`
	SpilledToDisk   bool     `json:"spilled_to_disk,omitempty"`
	SpillBytes      int64    `json:"spill_bytes,omitempty"`
	SpillFiles      int      `json:"spill_files,omitempty"`
	SpillNote       string   `json:"spill_note,omitempty"` // human-readable recommendation when spill occurs
	PoolUtilization float64  `json:"pool_utilization,omitempty"`
	Warnings        []string `json:"warnings,omitempty"`
	CPUUserMS       float64  `json:"cpu_user_ms,omitempty"`
	CPUSysMS        float64  `json:"cpu_sys_ms,omitempty"`

	// Multi-source query metadata.
	SourcesScanned []string `json:"sources_scanned,omitempty"`
	SourcesSkipped int      `json:"sources_skipped,omitempty"`

	// Query funnel: rows surviving each pipeline phase.
	RowsInRange    int64 `json:"rows_in_range,omitempty"`    // total rows in time range before filtering
	RowsAfterDedup int64 `json:"rows_after_dedup,omitempty"` // rows surviving dedup stage

	// Search selectivity: fraction of scanned rows that passed the search filter.
	// 1.0 means 100% of rows matched (low selectivity). 0.01 means 1% matched (high selectivity).
	SearchSelectivity float64 `json:"search_selectivity,omitempty"`
	// Suggestion is an actionable recommendation based on query execution analysis.
	Suggestion string `json:"suggestion,omitempty"`

	// Trace-level profiling (populated when profile=trace).
	SegmentDetails []metaSegmentDetail `json:"segment_details,omitempty"`
	VMCalls        int64               `json:"vm_calls,omitempty"`
	VMTotalNS      int64               `json:"vm_total_ns,omitempty"`
}

// metaSegmentDetail holds per-segment I/O metrics for trace-level profiling.
type metaSegmentDetail struct {
	SegmentID       string `json:"segment_id"`
	Source          string `json:"source"`
	Rows            int64  `json:"rows"`
	RowsAfterFilter int64  `json:"rows_after_filter"`
	BloomHit        bool   `json:"bloom_hit,omitempty"`
	InvertedUsed    bool   `json:"inverted_used,omitempty"`
	ReadDurationNS  int64  `json:"read_duration_ns"`
	BytesRead       int64  `json:"bytes_read,omitempty"`
}

// metaOptimizerRule describes a single optimizer rule that fired during planning.
type metaOptimizerRule struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Count       int    `json:"count"`
}

// metaPipelineStage is a single operator in the pipeline breakdown.
type metaPipelineStage struct {
	Name        string  `json:"name"`
	InputRows   int64   `json:"input_rows"`
	OutputRows  int64   `json:"output_rows"`
	DurationMS  float64 `json:"duration_ms"`
	ExclusiveMS float64 `json:"exclusive_ms,omitempty"` // self time excluding children
	MemoryBytes int64   `json:"memory_bytes,omitempty"`
	SpilledRows int64   `json:"spilled_rows,omitempty"`
	SpillBytes  int64   `json:"spill_bytes,omitempty"`
}

// MetaOpt configures optional metadata fields.
type MetaOpt func(*metaFields)

// WithTook adds took_ms to the meta.
func WithTook(d time.Duration) MetaOpt {
	return func(m *metaFields) {
		ms := float64(d.Milliseconds())
		m.TookMS = &ms
	}
}

// WithTookMS adds took_ms directly from a float64.
func WithTookMS(ms float64) MetaOpt {
	return func(m *metaFields) {
		m.TookMS = &ms
	}
}

// WithScanned adds scanned count to the meta.
func WithScanned(n int64) MetaOpt {
	return func(m *metaFields) {
		m.Scanned = &n
	}
}

// WithQueryID adds query_id to the meta.
func WithQueryID(id string) MetaOpt {
	return func(m *metaFields) {
		m.QueryID = id
	}
}

// WithSegmentsErrored adds segment error count to the meta (E7).
func WithSegmentsErrored(n int) MetaOpt {
	return func(m *metaFields) {
		if n > 0 {
			m.SegmentsErrored = &n
		}
	}
}

// WithSearchStats adds rich query execution statistics to the response meta.
// This enables CLI clients to display detailed segment/scan/optimization info.
func WithSearchStats(ss *metaStats) MetaOpt {
	return func(m *metaFields) {
		if ss != nil {
			m.SearchStats = ss
		}
	}
}

// respondData writes {"data": payload, "meta": {...}}.
// When a query_id is present, it is also set as an X-Query-ID response header
// so the logging middleware can correlate HTTP requests with query execution (O1).
func respondData(w http.ResponseWriter, httpStatus int, data interface{}, opts ...MetaOpt) {
	meta := metaFields{}
	for _, o := range opts {
		o(&meta)
	}
	envelope := map[string]interface{}{"data": data}
	// Only include meta if it has content.
	if meta.TookMS != nil || meta.Scanned != nil || meta.QueryID != "" || meta.SegmentsErrored != nil || meta.SearchStats != nil {
		envelope["meta"] = meta
	}
	// Expose query_id as a response header for logging middleware correlation.
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

// isValidationError returns true if the error is a user input validation failure
// (bad parameters) rather than an internal server error. Uses errors.Is with
// sentinel errors for reliable detection instead of string-prefix matching (U3).
func isValidationError(err error) bool {
	return errors.Is(err, usecases.ErrInvalidFrom) ||
		errors.Is(err, usecases.ErrInvalidTo) ||
		errors.Is(err, usecases.ErrFromBeforeTo)
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
