package client

import (
	"encoding/json"
	"time"
)

// Meta contains response metadata returned by the server.
type Meta struct {
	TookMS  float64 `json:"took_ms,omitempty"`
	QueryID string  `json:"query_id,omitempty"`
	// Rows is the number of rows a query returned.
	Rows int `json:"rows,omitempty"`
}

// QueryResultType identifies the kind of query result.
type QueryResultType string

const (
	ResultTypeEvents    QueryResultType = "events"
	ResultTypeAggregate QueryResultType = "aggregate"
	ResultTypeTimechart QueryResultType = "timechart"
	ResultTypeSchema    QueryResultType = "schema"
	ResultTypeJob       QueryResultType = "job"
)

// QueryRequest is the request body for POST /api/v1/query.
type QueryRequest struct {
	Q      string   `json:"q"`
	From   string   `json:"from,omitempty"`
	To     string   `json:"to,omitempty"`
	Limit  int      `json:"limit,omitempty"`
	Offset int      `json:"offset,omitempty"`
	Format string   `json:"format,omitempty"`
	Wait   *float64 `json:"wait,omitempty"`
}

// QueryResult is the polymorphic response from Query().
// Exactly one of Events, Aggregate, or Job is non-nil based on Type.
type QueryResult struct {
	Type      QueryResultType
	Events    *EventsResult
	Aggregate *AggregateResult
	Job       *JobHandle
	Meta      Meta
}

// EventsResult holds event query results.
type EventsResult struct {
	Events  []map[string]interface{} `json:"events"`
	Total   int                      `json:"total"`
	HasMore bool                     `json:"has_more"`
	Partial bool                     `json:"partial,omitempty"`
}

// AggregateResult holds aggregate/timechart query results.
type AggregateResult struct {
	Type      string                 `json:"type"`
	Columns   []string               `json:"columns"`
	Rows      [][]interface{}        `json:"rows"`
	TotalRows int                    `json:"total_rows,omitempty"`
	Interval  string                 `json:"interval,omitempty"`
	Partial   bool                   `json:"partial,omitempty"`
	RawData   map[string]interface{} `json:"-"`
}

// JobHandle is returned for async/hybrid queries (HTTP 202).
type JobHandle struct {
	JobID    string       `json:"job_id"`
	Status   string       `json:"status"`
	Progress *JobProgress `json:"progress,omitempty"`
}

// JobProgress is what is known of a running job.
type JobProgress struct {
	Status    string `json:"status"`
	ElapsedMS int64  `json:"elapsed_ms"`
}

// JobResult is the full response from GetJob().
type JobResult struct {
	Type        string           `json:"type"`
	JobID       string           `json:"job_id"`
	Status      string           `json:"status"`
	Query       string           `json:"query,omitempty"`
	CreatedAt   string           `json:"created_at,omitempty"`
	CompletedAt string           `json:"completed_at,omitempty"`
	Progress    *JobProgress     `json:"progress,omitempty"`
	Results     *json.RawMessage `json:"results,omitempty"`
	Error       *JobError        `json:"error,omitempty"`
	Meta        Meta             `json:"-"`
}

// JobError is the error detail for a failed job.
// It supports unmarshaling from both a structured object
// ({"code":"...","message":"..."}) and a plain JSON string.
type JobError struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	Suggestion string `json:"suggestion,omitempty"`
}

// UnmarshalJSON handles both structured error objects and plain strings.
func (e *JobError) UnmarshalJSON(data []byte) error {
	// Try plain string first (e.g. "error": "something went wrong").
	var s string
	if json.Unmarshal(data, &s) == nil {
		e.Code = "QUERY_ERROR"
		e.Message = s

		return nil
	}

	// Structured object — use an alias to avoid infinite recursion.
	type alias JobError
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	*e = JobError(a)

	return nil
}

// JobListResult is the response from ListJobs().
type JobListResult struct {
	Jobs []JobSummary `json:"jobs"`
}

// JobSummary is a brief job entry from ListJobs().
type JobSummary struct {
	JobID     string `json:"job_id"`
	Status    string `json:"status"`
	Query     string `json:"query"`
	CreatedAt string `json:"created_at,omitempty"`
}

// ExplainResult is the response from Explain().
type ExplainResult struct {
	IsValid bool           `json:"is_valid"`
	Parsed  *ExplainParsed `json:"parsed,omitempty"`
	Errors  []ExplainError `json:"errors"`
}

// ExplainParsed describes a compiled query: its IR plans and scan hints.
type ExplainParsed struct {
	ResultType     string              `json:"result_type"`
	EstimatedCost  string              `json:"estimated_cost"`
	UsesFullScan   bool                `json:"uses_full_scan"`
	FieldsRead     []string            `json:"fields_read"`
	SearchTerms    []string            `json:"search_terms,omitempty"`
	HasTimeBounds  bool                `json:"has_time_bounds,omitempty"`
	LogicalPlan    string              `json:"logical_plan"`
	OptimizedPlan  string              `json:"optimized_plan"`
	CompileMS      float64             `json:"compile_ms,omitempty"`
	OptimizerRules []ExplainRuleDetail `json:"optimizer_rules,omitempty"`
	SourceScope    *ExplainSourceScope `json:"source_scope,omitempty"`
}

// ExplainRuleDetail is an optimizer rule and the number of times it fired.
type ExplainRuleDetail struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

// ExplainSourceScope describes the resolved source scope.
type ExplainSourceScope struct {
	Type                  string   `json:"type"`
	Sources               []string `json:"sources,omitempty"`
	Pattern               string   `json:"pattern,omitempty"`
	TotalSourcesAvailable int      `json:"total_sources_available,omitempty"`
}

// ExplainError is a compile error returned by Explain().
type ExplainError struct {
	Kind       string         `json:"kind,omitempty"`
	Position   *ErrorPosition `json:"position,omitempty"`
	Expected   []string       `json:"expected,omitempty"`
	Message    string         `json:"message"`
	Suggestion string         `json:"suggestion,omitempty"`
}

// ErrorPosition is the line and column of a compile error, both 1-based.
type ErrorPosition struct {
	Line   int `json:"line"`
	Column int `json:"column"`
}

// IngestEvent is one event for IngestEvents: its text, time in seconds
// since the epoch, origin, index and fields.
type IngestEvent struct {
	Event      string                 `json:"event"`
	Time       *float64               `json:"time,omitempty"`
	Source     string                 `json:"source,omitempty"`
	Sourcetype string                 `json:"sourcetype,omitempty"`
	Host       string                 `json:"host,omitempty"`
	Index      string                 `json:"index,omitempty"`
	Fields     map[string]interface{} `json:"fields,omitempty"`
}

// IngestResult is the response from the ingest endpoints.
type IngestResult struct {
	Accepted  int    `json:"accepted"`
	Failed    int    `json:"failed"`
	Truncated bool   `json:"truncated,omitempty"`
	Warning   string `json:"warning,omitempty"`
}

// IngestOpts configures IngestRaw().
type IngestOpts struct {
	Source      string
	Sourcetype  string
	Index       string
	ContentType string
}

// FieldInfo describes a field and the indexes that hold it.
type FieldInfo struct {
	Name    string   `json:"name"`
	Indexes []string `json:"indexes,omitempty"`
}

// FieldValuesResult is the response from FieldValues().
type FieldValuesResult struct {
	Field  string       `json:"field"`
	Values []FieldValue `json:"values"`
	Meta   Meta         `json:"-"`
}

// FieldValue is a value of a field and the number of events that hold it.
type FieldValue struct {
	Value interface{} `json:"value"`
	Count int64       `json:"count"`
}

// SourceInfo describes an index as a source of events.
type SourceInfo struct {
	Name       string `json:"name"`
	EventCount int64  `json:"event_count"`
	FirstEvent string `json:"first_event,omitempty"`
	LastEvent  string `json:"last_event,omitempty"`
}

// ServerStatus is the response from Status().
type ServerStatus struct {
	Version       string           `json:"version"`
	UptimeSeconds int              `json:"uptime_seconds"`
	Health        string           `json:"health"`
	Storage       StatusStorage    `json:"storage"`
	Events        StatusEvents     `json:"events"`
	Queries       StatusQueries    `json:"queries"`
	Retention     *StatusRetention `json:"retention,omitempty"`
}

// StatusStorage holds storage usage info.
type StatusStorage struct {
	UsedBytes int64 `json:"used_bytes"`
}

// StatusEvents holds event counts.
type StatusEvents struct {
	Total int64 `json:"total"`
}

// StatusQueries holds query stats.
type StatusQueries struct {
	Active int `json:"active"`
}

// StatusRetention holds retention policy info.
type StatusRetention struct {
	Policy      string `json:"policy,omitempty"`
	OldestEvent string `json:"oldest_event,omitempty"`
}

// HealthResult is the response from Health().
type HealthResult struct {
	Status string `json:"status"`
}

// StatsResult is the response from Stats().
type StatsResult struct {
	UptimeSeconds int            `json:"uptime_seconds"`
	StorageBytes  int64          `json:"storage_bytes"`
	TotalEvents   int64          `json:"total_events"`
	IndexCount    int            `json:"index_count"`
	PartCount     int            `json:"part_count"`
	OldestEvent   string         `json:"oldest_event,omitempty"`
	NewestEvent   string         `json:"newest_event,omitempty"`
	Indexes       []IndexInfo    `json:"indexes,omitempty"`
	Jobs          map[string]int `json:"jobs,omitempty"`
}

// IndexInfo describes an index from the server.
type IndexInfo struct {
	Name       string `json:"name"`
	Events     int64  `json:"events"`
	Bytes      int64  `json:"bytes"`
	Parts      int    `json:"parts"`
	FirstEvent string `json:"first_event,omitempty"`
	LastEvent  string `json:"last_event,omitempty"`
}

// HistogramResult is the response from Histogram().
type HistogramResult struct {
	Interval string            `json:"interval"`
	Buckets  []HistogramBucket `json:"buckets"`
	Total    int64             `json:"total"`
	Meta     Meta              `json:"-"`
}

// HistogramBucket is a single time bucket.
type HistogramBucket struct {
	Time  time.Time `json:"time"`
	Count int64     `json:"count"`
}

// SSEEvent is a parsed Server-Sent Event.
type SSEEvent struct {
	Event string
	Data  json.RawMessage
}

// StreamMeta is the __meta line from NDJSON streaming.
type StreamMeta struct {
	Total  int   `json:"total"`
	TookMS int64 `json:"took_ms"`
}

// ConfigResult is the response from GetConfig().
type ConfigResult map[string]interface{}

// ConfigPatch is the request body for PatchConfig().
type ConfigPatch struct {
	Retention        *string `json:"retention,omitempty"`
	MaxQueryMemoryMB *int    `json:"max_query_memory_mb,omitempty"`
	Listen           *string `json:"listen,omitempty"`
	AuthEnabled      *bool   `json:"auth_enabled,omitempty"`
	OTLPEnabled      *bool   `json:"otlp_enabled,omitempty"`
	SyslogEnabled    *bool   `json:"syslog_enabled,omitempty"`
}

// ESBulkResult is the response from ESBulk().
type ESBulkResult struct {
	Took   int          `json:"took"`
	Errors bool         `json:"errors"`
	Items  []ESBulkItem `json:"items"`
}

// ESBulkItem is a single item result from ES bulk.
type ESBulkItem struct {
	Index ESBulkItemDetail `json:"index"`
}

// ESBulkItemDetail is the detail of a bulk item result.
type ESBulkItemDetail struct {
	ID     string `json:"_id"`
	Status int    `json:"status"`
}

// ESClusterInfoResult is the response from ESClusterInfo().
type ESClusterInfoResult struct {
	Name        string `json:"name"`
	ClusterName string `json:"cluster_name"`
	Version     struct {
		Number string `json:"number"`
	} `json:"version"`
}
