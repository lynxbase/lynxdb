package usecases

import (
	"time"

	"github.com/lynxbase/lynxdb/pkg/server"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/storage/views"
)

// QueryMode controls sync/async/hybrid dispatch.
type QueryMode int

const (
	QueryModeSync   QueryMode = iota // wait for completion (default)
	QueryModeAsync                   // return job handle immediately
	QueryModeHybrid                  // wait up to WaitDuration, then return handle
)

// SubmitRequest is the domain input for query submission.
type SubmitRequest struct {
	Query   string
	From    string
	To      string
	Limit   int
	Offset  int
	Mode    QueryMode
	Wait    time.Duration // used in hybrid mode
	Profile string        // "basic", "full", "trace" — passed to engine for profiling
}

// SubmitResult is the domain output for query submission.
type SubmitResult struct {
	Done       bool
	ResultType server.ResultType
	Results    []spl2.ResultRow
	Stats      server.SearchStats
	QueryID    string

	// Returned when the job is not yet done (async / hybrid timeout).
	JobID    string
	Status   string
	Progress *server.SearchProgress
	Error    string

	// ErrorCode is a machine-readable code for the error (e.g., QUERY_MEMORY_EXCEEDED).
	// Empty when no error or no specific code applies.
	ErrorCode string
}

// StreamRequest is the domain input for streaming queries.
type StreamRequest struct {
	Query string
	From  string
	To    string
}

// ExplainRequest is the domain input for query explanation.
type ExplainRequest struct {
	Query string
	From  string
	To    string
}

// ExplainResult is the domain output for query explanation.
type ExplainResult struct {
	IsValid    bool
	Errors     []ExplainError
	Parsed     *ExplainParsed
	HasMVAccel bool
}

// ExplainError represents a parse/validation error in an explain response.
type ExplainError struct {
	Message    string
	Suggestion string
}

// ExplainParsed holds the breakdown of a successfully parsed query.
type ExplainParsed struct {
	Pipeline       []PipelineStage
	ResultType     string
	EstimatedCost  string
	UsesFullScan   bool
	FieldsRead     []string
	SearchTerms    []string
	HasTimeBounds  bool
	OptimizerStats map[string]int
	PhysicalPlan   *PhysicalPlan // physical plan with optimizer annotations
	SourceScope    *ExplainSourceScope

	// Planner timing and optimizer rule details.
	ParseMS     float64
	OptimizeMS  float64
	RuleDetails []ExplainRuleDetail
	TotalRules  int

	// Optimizer diagnostic messages and warnings.
	OptimizerMessages []string
	OptimizerWarnings []string
}

// ExplainSourceScope describes the resolved source scope in an explain response.
type ExplainSourceScope struct {
	Type                  string   `json:"type"`
	Sources               []string `json:"sources,omitempty"`
	Pattern               string   `json:"pattern,omitempty"`
	TotalSourcesAvailable int      `json:"total_sources_available,omitempty"`
}

// ExplainRuleDetail describes a single optimizer rule for the explain response.
type ExplainRuleDetail struct {
	Name        string
	Description string
	Count       int
}

// PhysicalPlan describes the runtime execution strategy chosen by the optimizer.
// This surfaces information that the logical pipeline stages alone do not show,
// such as partial aggregation pushdown, count(*) metadata shortcut, or topK heap merge.
type PhysicalPlan struct {
	CountStarOnly bool   `json:"count_star_only,omitempty"`
	PartialAgg    bool   `json:"partial_agg,omitempty"`
	TopKAgg       bool   `json:"topk_agg,omitempty"`
	TopK          int    `json:"topk,omitempty"`
	JoinStrategy  string `json:"join_strategy,omitempty"`
}

// PipelineStage describes a single command in the query pipeline.
type PipelineStage struct {
	Command string
}

// HistogramRequest is the domain input for histogram generation.
type HistogramRequest struct {
	From    string
	To      string
	Buckets int
	Index   string
}

// HistogramBucket is a single histogram time bucket.
type HistogramBucket struct {
	Time  time.Time
	Count int
}

// HistogramResult is the domain output for histogram.
type HistogramResult struct {
	Interval string
	Buckets  []HistogramBucket
	Total    int
}

// FieldValuesRequest is the domain input for field value listing.
type FieldValuesRequest struct {
	FieldName string
	Limit     int
	From      string
	To        string
	Index     string
}

// FieldValue represents a single field value with count.
type FieldValue struct {
	Value   string
	Count   int
	Percent float64
}

// FieldValuesResult is the domain output for field value listing.
type FieldValuesResult struct {
	Field       string
	Values      []FieldValue
	UniqueCount int
	TotalCount  int
}

// SourceInfo describes an event source.
type SourceInfo struct {
	Name       string
	EventCount int
	FirstEvent time.Time
	LastEvent  time.Time
}

// SourcesResult is the domain output for source listing.
type SourcesResult struct {
	Sources []SourceInfo
}

// CreateViewRequest is the domain input for creating a materialized view.
type CreateViewRequest struct {
	Name      string
	Query     string
	Retention string
}

// PatchViewRequest is the domain input for updating a materialized view.
type PatchViewRequest struct {
	Retention *string
	Paused    *bool
}

// ViewSummary is a brief representation of a materialized view.
type ViewSummary struct {
	Name      string
	Status    views.ViewStatus
	Query     string
	Type      views.ViewType
	CreatedAt time.Time
	UpdatedAt time.Time
}

// ViewDetail is the full representation of a materialized view.
type ViewDetail struct {
	ViewSummary
	Filter           string
	Columns          []views.ColumnDef
	Retention        time.Duration
	BackfillProgress *server.BackfillProgressInfo // non-nil when backfill is active
}
