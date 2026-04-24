package client

import (
	"encoding/json"
	"time"
)

// Meta contains response metadata returned by the server.
type Meta struct {
	TookMS          float64      `json:"took_ms,omitempty"`
	Scanned         int64        `json:"scanned,omitempty"`
	QueryID         string       `json:"query_id,omitempty"`
	SegmentsErrored int          `json:"segments_errored,omitempty"`
	Stats           *SearchStats `json:"stats,omitempty"`
}

// SearchStats holds detailed query execution statistics from the server.
// These are used by the CLI to display rich query performance information.
type SearchStats struct {
	RowsScanned          int64           `json:"rows_scanned"`
	RowsReturned         int64           `json:"rows_returned"`
	MatchedRows          int64           `json:"matched_rows,omitempty"`
	SegmentsTotal        int             `json:"segments_total,omitempty"`
	SegmentsScanned      int             `json:"segments_scanned,omitempty"`
	SegmentsSkippedIdx   int             `json:"segments_skipped_index,omitempty"`
	SegmentsSkippedTime  int             `json:"segments_skipped_time,omitempty"`
	SegmentsSkippedStat  int             `json:"segments_skipped_stats,omitempty"`
	SegmentsSkippedBF    int             `json:"segments_skipped_bloom,omitempty"`
	SegmentsSkippedRange int             `json:"segments_skipped_range,omitempty"`
	BufferedEvents       int             `json:"buffered_events,omitempty"`
	InvertedIndexHits    int             `json:"inverted_index_hits,omitempty"`
	IndexesUsed          []string        `json:"indexes_used,omitempty"`
	CountStarOptimized   bool            `json:"count_star_optimized,omitempty"`
	PartialAggUsed       bool            `json:"partial_agg_used,omitempty"`
	TopKUsed             bool            `json:"topk_used,omitempty"`
	PrefetchUsed         bool            `json:"prefetch_used,omitempty"`
	VectorizedFilterUsed bool            `json:"vectorized_filter_used,omitempty"`
	DictFilterUsed       bool            `json:"dict_filter_used,omitempty"`
	JoinStrategy         string          `json:"join_strategy,omitempty"`
	ScanMS               float64         `json:"scan_ms,omitempty"`
	PipelineMS           float64         `json:"pipeline_ms,omitempty"`
	AcceleratedBy        string          `json:"accelerated_by,omitempty"`
	MVStatus             string          `json:"mv_status,omitempty"`
	MVSpeedup            string          `json:"mv_speedup,omitempty"`
	MVOriginalScan       int64           `json:"mv_original_scan,omitempty"`
	CacheHit             bool            `json:"cache_hit,omitempty"`
	PipelineStages       []PipelineStage `json:"pipeline_stages,omitempty"`

	// Parse/optimize timing from planner.
	ParseMS    float64 `json:"parse_ms,omitempty"`
	OptimizeMS float64 `json:"optimize_ms,omitempty"`

	// Optimizer rule details from planner.
	OptimizerRules []OptimizerRuleStat `json:"optimizer_rules,omitempty"`
	TotalRules     int                 `json:"total_rules,omitempty"`

	// ProcessedBytes is the total bytes of segment data touched by the query.
	ProcessedBytes int64 `json:"processed_bytes,omitempty"`

	// I/O bytes breakdown by source.
	DiskBytesRead  int64 `json:"disk_bytes_read,omitempty"`
	S3BytesRead    int64 `json:"s3_bytes_read,omitempty"`
	CacheBytesRead int64 `json:"cache_bytes_read,omitempty"`

	// Resource usage (server-side, captured via BudgetAdapter + getrusage).
	PeakMemoryBytes int64    `json:"peak_memory_bytes,omitempty"`
	MemAllocBytes   int64    `json:"mem_alloc_bytes,omitempty"`
	SpilledToDisk   bool     `json:"spilled_to_disk,omitempty"`
	SpillBytes      int64    `json:"spill_bytes,omitempty"`
	SpillFiles      int      `json:"spill_files,omitempty"`
	PoolUtilization float64  `json:"pool_utilization,omitempty"`
	Warnings        []string `json:"warnings,omitempty"`
	CPUUserMS       float64  `json:"cpu_user_ms,omitempty"`
	CPUSysMS        float64  `json:"cpu_sys_ms,omitempty"`

	// Per-operator memory budget statistics from the MemoryCoordinator.
	// Populated when profile level is "full" or "trace" and the query uses
	// coordinator-managed spillable operators.
	OperatorBudgets []OperatorBudgetStat `json:"operator_budgets,omitempty"`

	// Trace-level profiling (populated when profile=trace).
	SegmentDetails []SegmentDetailStat `json:"segment_details,omitempty"`
	VMCalls        int64               `json:"vm_calls,omitempty"`
	VMTotalNS      int64               `json:"vm_total_ns,omitempty"`
}

// SegmentDetailStat holds per-segment I/O metrics for trace-level profiling.
type SegmentDetailStat struct {
	SegmentID       string `json:"segment_id"`
	Source          string `json:"source"`
	Rows            int64  `json:"rows"`
	RowsAfterFilter int64  `json:"rows_after_filter"`
	BloomHit        bool   `json:"bloom_hit,omitempty"`
	InvertedUsed    bool   `json:"inverted_used,omitempty"`
	ReadDurationNS  int64  `json:"read_duration_ns"`
	BytesRead       int64  `json:"bytes_read,omitempty"`
}

// OptimizerRuleStat describes a single optimizer rule that fired during planning.
type OptimizerRuleStat struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Count       int    `json:"count"`
}

// OperatorBudgetStat holds per-operator memory budget statistics from the
// MemoryCoordinator, showing how memory was distributed across spillable operators.
type OperatorBudgetStat struct {
	Label     string `json:"label"`
	SoftLimit int64  `json:"soft_limit"`
	PeakBytes int64  `json:"peak_bytes"`
	Spilled   bool   `json:"spilled"`
	Phase     string `json:"phase"`
}

// PipelineStage holds per-operator metrics from the server-side pipeline execution.
type PipelineStage struct {
	Name        string  `json:"name"`
	InputRows   int64   `json:"input_rows"`
	OutputRows  int64   `json:"output_rows"`
	DurationMS  float64 `json:"duration_ms"`
	MemoryBytes int64   `json:"memory_bytes,omitempty"`
	SpilledRows int64   `json:"spilled_rows,omitempty"`
	SpillBytes  int64   `json:"spill_bytes,omitempty"`
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
	Q       string   `json:"q"`
	From    string   `json:"from,omitempty"`
	To      string   `json:"to,omitempty"`
	Limit   int      `json:"limit,omitempty"`
	Offset  int      `json:"offset,omitempty"`
	Format  string   `json:"format,omitempty"`
	Wait    *float64 `json:"wait,omitempty"`
	Profile string   `json:"profile,omitempty"` // "basic", "full", "trace"
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
	AccelBy   *AcceleratedBy         `json:"-"`
	RawData   map[string]interface{} `json:"-"`
}

// AcceleratedBy indicates MV acceleration was used.
type AcceleratedBy struct {
	View            string  `json:"view"`
	OriginalScan    int64   `json:"original_scan"`
	Speedup         string  `json:"speedup"`
	Status          string  `json:"status"`
	CoveragePercent float64 `json:"coverage_percent,omitempty"`
}

// JobHandle is returned for async/hybrid queries (HTTP 202).
type JobHandle struct {
	JobID    string       `json:"job_id"`
	Status   string       `json:"status"`
	Progress *JobProgress `json:"progress,omitempty"`
}

// JobProgress describes the progress of an async job.
type JobProgress struct {
	Phase         string  `json:"phase"`
	Scanned       int64   `json:"scanned,omitempty"`
	TotalEstimate int64   `json:"total_estimate,omitempty"`
	Percent       float64 `json:"percent"`
	EventsMatched int64   `json:"events_matched,omitempty"`
	ElapsedMS     int64   `json:"elapsed_ms,omitempty"`
	EtaMS         *int64  `json:"eta_ms,omitempty"`
	// Granular segment progress (used by TUI).
	SegmentsTotal        int   `json:"segments_total,omitempty"`
	SegmentsScanned      int   `json:"segments_scanned,omitempty"`
	SegmentsDispatched   int   `json:"segments_dispatched,omitempty"`
	SegmentsSkippedIndex int   `json:"segments_skipped_index,omitempty"`
	SegmentsSkippedTime  int   `json:"segments_skipped_time,omitempty"`
	SegmentsSkippedStats int   `json:"segments_skipped_stats,omitempty"`
	SegmentsSkippedBloom int   `json:"segments_skipped_bloom,omitempty"`
	BufferedEvents       int   `json:"buffered_events,omitempty"`
	RowsReadSoFar        int64 `json:"rows_read_so_far,omitempty"`
}

// JobResult is the full response from GetJob().
type JobResult struct {
	Type        string           `json:"type"`
	JobID       string           `json:"job_id"`
	Status      string           `json:"status"`
	Query       string           `json:"query,omitempty"`
	From        string           `json:"from,omitempty"`
	To          string           `json:"to,omitempty"`
	CreatedAt   string           `json:"created_at,omitempty"`
	CompletedAt string           `json:"completed_at,omitempty"`
	FailedAt    string           `json:"failed_at,omitempty"`
	ExpiresAt   string           `json:"expires_at,omitempty"`
	Progress    *JobProgress     `json:"progress,omitempty"`
	Results     *json.RawMessage `json:"results,omitempty"`
	Error       *JobError        `json:"error,omitempty"`
	Meta        Meta             `json:"-"`
}

// JobError is the error detail for a failed job.
// It supports unmarshaling from both a structured object
// ({"code":"...","message":"..."}) and a plain JSON string
// for backward compatibility with older server versions.
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
	Meta struct {
		MaxConcurrent int `json:"max_concurrent,omitempty"`
		Active        int `json:"active,omitempty"`
	} `json:"meta,omitempty"`
}

// JobSummary is a brief job entry from ListJobs().
type JobSummary struct {
	JobID       string       `json:"job_id"`
	Status      string       `json:"status"`
	Query       string       `json:"query"`
	From        string       `json:"from,omitempty"`
	To          string       `json:"to,omitempty"`
	CreatedAt   string       `json:"created_at,omitempty"`
	CompletedAt string       `json:"completed_at,omitempty"`
	ExpiresAt   string       `json:"expires_at,omitempty"`
	Progress    *JobProgress `json:"progress,omitempty"`
}

// ExplainResult is the response from Explain().
type ExplainResult struct {
	IsValid      bool           `json:"is_valid"`
	Parsed       *ExplainParsed `json:"parsed,omitempty"`
	Errors       []ExplainError `json:"errors"`
	Acceleration *ExplainAccel  `json:"acceleration,omitempty"`
}

// ExplainParsed contains the parsed query analysis.
type ExplainParsed struct {
	Pipeline      []ExplainStage `json:"pipeline"`
	ResultType    string         `json:"result_type"`
	EstimatedCost string         `json:"estimated_cost"`
	UsesFullScan  bool           `json:"uses_full_scan"`
	FieldsRead    []string       `json:"fields_read"`
	SearchTerms   []string       `json:"search_terms,omitempty"`
	HasTimeBounds bool           `json:"has_time_bounds,omitempty"`

	// Optimizer details (rich explain output).
	OptimizerStats    map[string]int       `json:"optimizer_stats,omitempty"`
	OptimizerMessages []string             `json:"optimizer_messages,omitempty"`
	OptimizerWarnings []string             `json:"optimizer_warnings,omitempty"`
	PhysicalPlan      *ExplainPhysicalPlan `json:"physical_plan,omitempty"`
	ParseMS           float64              `json:"parse_ms,omitempty"`
	OptimizeMS        float64              `json:"optimize_ms,omitempty"`
	TotalRules        int                  `json:"total_rules,omitempty"`
	OptimizerRules    []ExplainRuleDetail  `json:"optimizer_rules,omitempty"`
	SourceScope       *ExplainSourceScope  `json:"source_scope,omitempty"`
}

// ExplainPhysicalPlan describes the runtime execution strategy.
type ExplainPhysicalPlan struct {
	CountStarOnly bool   `json:"count_star_only,omitempty"`
	PartialAgg    bool   `json:"partial_agg,omitempty"`
	TopKAgg       bool   `json:"topk_agg,omitempty"`
	TopK          int    `json:"topk,omitempty"`
	JoinStrategy  string `json:"join_strategy,omitempty"`
}

// ExplainRuleDetail describes a single optimizer rule.
type ExplainRuleDetail struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Count       int    `json:"count"`
}

// ExplainSourceScope describes the resolved source scope.
type ExplainSourceScope struct {
	Type                  string   `json:"type"`
	Sources               []string `json:"resolved_sources,omitempty"`
	Pattern               string   `json:"pattern,omitempty"`
	TotalSourcesAvailable int      `json:"total_sources_available,omitempty"`
}

// ExplainStage is a single pipeline stage.
type ExplainStage struct {
	Command string `json:"command"`
}

// ExplainError is a parse error returned by Explain().
type ExplainError struct {
	Position   int    `json:"position,omitempty"`
	Length     int    `json:"length,omitempty"`
	Message    string `json:"message"`
	Suggestion string `json:"suggestion,omitempty"`
}

// ExplainAccel describes MV acceleration availability.
type ExplainAccel struct {
	Available        bool   `json:"available"`
	View             string `json:"view,omitempty"`
	Reason           string `json:"reason,omitempty"`
	EstimatedSpeedup string `json:"estimated_speedup,omitempty"`
}

// IngestEvent is a single structured event payload for POST /api/v1/ingest.
type IngestEvent struct {
	Event      string                 `json:"event"`
	Time       *float64               `json:"time,omitempty"`
	Source     string                 `json:"source,omitempty"`
	Sourcetype string                 `json:"sourcetype,omitempty"`
	Host       string                 `json:"host,omitempty"`
	Index      string                 `json:"index,omitempty"`
	Fields     map[string]interface{} `json:"fields,omitempty"`
}

// IngestResult is the response from structured and raw ingest endpoints.
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

// FieldInfo describes a field from the field catalog.
type FieldInfo struct {
	Name      string     `json:"name"`
	Type      string     `json:"type"`
	Count     int64      `json:"count"`
	Coverage  float64    `json:"coverage"`
	Min       *float64   `json:"min,omitempty"`
	Max       *float64   `json:"max,omitempty"`
	Avg       *float64   `json:"avg,omitempty"`
	P50       *float64   `json:"p50,omitempty"`
	P99       *float64   `json:"p99,omitempty"`
	TopValues []TopValue `json:"top_values,omitempty"`
}

// TopValue is a frequent field value.
type TopValue struct {
	Value interface{} `json:"value"`
	Count int64       `json:"count"`
}

// FieldValuesResult is the response from FieldValues().
type FieldValuesResult struct {
	Field       string       `json:"field"`
	Type        string       `json:"type,omitempty"`
	Values      []FieldValue `json:"values"`
	UniqueCount int          `json:"unique_count,omitempty"`
	TotalCount  int          `json:"total_count,omitempty"`
	Meta        Meta         `json:"-"`
}

// FieldValue is a single value from FieldValues().
type FieldValue struct {
	Value   interface{} `json:"value"`
	Count   int64       `json:"count"`
	Percent float64     `json:"percent,omitempty"`
}

// SourceInfo describes a data source.
type SourceInfo struct {
	Name         string  `json:"name"`
	EventCount   int64   `json:"event_count"`
	FirstEvent   string  `json:"first_event,omitempty"`
	LastEvent    string  `json:"last_event,omitempty"`
	Rate         float64 `json:"rate,omitempty"`
	StorageBytes int64   `json:"storage_bytes,omitempty"`
}

// ServerStatus is the response from Status().
type ServerStatus struct {
	Version       string            `json:"version"`
	UptimeSeconds int               `json:"uptime_seconds"`
	Health        string            `json:"health"`
	Storage       StatusStorage     `json:"storage"`
	Events        StatusEvents      `json:"events"`
	Queries       StatusQueries     `json:"queries"`
	Views         StatusViews       `json:"views"`
	Retention     *StatusRetention  `json:"retention,omitempty"`
	Tail          *StatusTail       `json:"tail,omitempty"`
	MemoryPool    *StatusMemoryPool `json:"memory_pool,omitempty"`
}

// StatusMemoryPool holds unified memory pool statistics.
type StatusMemoryPool struct {
	TotalBytes        int64 `json:"total_pool_bytes"`
	QueryAllocated    int64 `json:"query_allocated_bytes"`
	CacheAllocated    int64 `json:"cache_allocated_bytes"`
	CacheReserveFloor int64 `json:"cache_reserve_floor_bytes"`
	FreeBytes         int64 `json:"free_bytes"`
	CacheEvictions    int64 `json:"cache_eviction_count"`
	CacheEvictedBytes int64 `json:"cache_eviction_bytes"`
	QueryRejections   int64 `json:"query_rejections"`
}

// StatusTail holds live tail session info.
type StatusTail struct {
	ActiveSessions int `json:"active_sessions"`
}

// StatusStorage holds storage usage info.
type StatusStorage struct {
	UsedBytes    int64   `json:"used_bytes"`
	TotalBytes   int64   `json:"total_bytes,omitempty"`
	UsagePercent float64 `json:"usage_percent,omitempty"`
}

// StatusEvents holds event counts.
type StatusEvents struct {
	Total      int64   `json:"total"`
	Today      int64   `json:"today"`
	IngestRate float64 `json:"ingest_rate,omitempty"`
}

// StatusQueries holds query stats.
type StatusQueries struct {
	Active        int     `json:"active"`
	AvgDurationMS float64 `json:"avg_duration_ms,omitempty"`
}

// StatusViews holds MV summary info.
type StatusViews struct {
	Total        int   `json:"total"`
	Active       int   `json:"active"`
	Backfilling  int   `json:"backfilling,omitempty"`
	StorageBytes int64 `json:"storage_bytes,omitempty"`
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
	UptimeSeconds  int           `json:"uptime_seconds"`
	StorageBytes   int64         `json:"storage_bytes"`
	TotalEvents    int64         `json:"total_events"`
	EventsToday    int64         `json:"events_today"`
	IndexCount     int           `json:"index_count"`
	SegmentCount   int           `json:"segment_count"`
	BufferedEvents int           `json:"buffered_events"`
	Sources        []StatsSource `json:"sources,omitempty"`
	OldestEvent    string        `json:"oldest_event,omitempty"`
}

// StatsSource describes a data source in stats results.
type StatsSource struct {
	Name  string `json:"name"`
	Count int64  `json:"count"`
}

// IndexInfo describes an index from the server.
type IndexInfo struct {
	Name              string `json:"name"`
	RetentionPeriod   string `json:"retention_period,omitempty"`
	ReplicationFactor int    `json:"replication_factor,omitempty"`
}

// CacheStatsResult is the response from CacheStats().
type CacheStatsResult map[string]interface{}

// MetricsResult is the response from Metrics().
type MetricsResult map[string]interface{}

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
	Total   int   `json:"total"`
	Scanned int64 `json:"scanned"`
	TookMS  int64 `json:"took_ms"`
}

// View is a materialized view summary.
type View struct {
	Name         string            `json:"name"`
	Kind         string            `json:"kind"`
	Query        string            `json:"query"`
	Retention    string            `json:"retention,omitempty"`
	Status       string            `json:"status"`
	Version      int               `json:"version,omitempty"`
	Rows         int64             `json:"rows,omitempty"`
	Segments     int               `json:"segments,omitempty"`
	StorageBytes int64             `json:"storage_bytes,omitempty"`
	LagMS        *int64            `json:"lag_ms,omitempty"`
	Backfill     *BackfillProgress `json:"backfill,omitempty"`
	CreatedAt    string            `json:"created_at,omitempty"`
	LastEvent    *string           `json:"last_event,omitempty"`
}

// BackfillProgress tracks MV backfill.
type BackfillProgress struct {
	Phase           string  `json:"phase"`
	SegmentsTotal   int     `json:"segments_total"`
	SegmentsScanned int     `json:"segments_scanned"`
	SegmentsSkipped int     `json:"segments_skipped"`
	RowsScanned     int64   `json:"rows_scanned"`
	ElapsedMS       float64 `json:"elapsed_ms"`
}

// ViewDetail extends View with schema info.
type ViewDetail struct {
	View
	Columns      []ViewColumn `json:"columns,omitempty"`
	GroupBy      []string     `json:"group_by,omitempty"`
	Aggregations []string     `json:"aggregations,omitempty"`
	SourceView   *string      `json:"source_view,omitempty"`
}

// ViewColumn describes a column in a materialized view.
// Type is serialized as a numeric FieldType by the server (uint8).
type ViewColumn struct {
	Name        string      `json:"name"`
	Type        interface{} `json:"type"`
	Encoding    string      `json:"encoding,omitempty"`
	DerivedFrom []string    `json:"derived_from,omitempty"`
}

// ViewInput is the request body for creating a view.
type ViewInput struct {
	Name        string `json:"name"`
	Q           string `json:"query"`
	Retention   string `json:"retention,omitempty"`
	PartitionBy string `json:"partition_by,omitempty"`
}

// ViewPatchInput is the request body for patching a view.
type ViewPatchInput struct {
	Retention *string `json:"retention,omitempty"`
	Paused    *bool   `json:"paused,omitempty"`
}

// SavedQuery is a saved query definition.
type SavedQuery struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Q         string `json:"q"`
	From      string `json:"from,omitempty"`
	CreatedAt string `json:"created_at,omitempty"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

// SavedQueryInput is the request body for creating/updating a saved query.
type SavedQueryInput struct {
	Name string `json:"name"`
	Q    string `json:"q"`
	From string `json:"from,omitempty"`
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
