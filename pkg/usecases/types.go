package usecases

import (
	"time"

	"github.com/lynxbase/lynxdb/pkg/model"
	"github.com/lynxbase/lynxdb/pkg/server"
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
	Query           string
	From            string
	To              string
	Limit           int
	Offset          int
	Mode            QueryMode
	Wait            time.Duration        // used in hybrid mode
	Profile         string               // "basic", "full", "trace" — passed to engine for profiling
	NoLint          bool                 // disables advisory query lints
	NoSuggestions   bool                 // disables advisory query suggestions
	LintLimit       int                  // max lints to return; default 5
	LintFull        bool                 // true returns all advisory lints
	Rewrites        []model.QueryRewrite // visible query normalizer rewrites
	SkipResultCache bool                 // true when request syntax depends on now
}

// SubmitResult is the domain output for query submission.
type SubmitResult struct {
	Done       bool
	ResultType server.ResultType
	Results    []model.ResultRow
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

	// Warnings holds user-facing warnings about the query (e.g., blocking operators).
	Warnings []string

	// Lints holds post-parse query warnings with stable RFC lint codes.
	Lints []model.QueryLint

	// Suggestions holds advisory query edit/template suggestions.
	Suggestions []model.QuerySuggestion

	// Rewrites holds visible query normalizer rewrites.
	Rewrites []model.QueryRewrite
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
	IsValid bool
	Errors  []ExplainError
	Parsed  *ExplainParsed
}

// ExplainError is a compile error in an explain response.
type ExplainError struct {
	Kind       string
	Message    string
	Suggestion string
	Line       int
	Column     int
	Expected   []string
}

// ExplainParsed describes a compiled query.
type ExplainParsed struct {
	ResultType    string
	EstimatedCost string
	UsesFullScan  bool
	FieldsRead    []string
	SearchTerms   []string
	HasTimeBounds bool
	SourceScope   *ExplainSourceScope
	// LogicalPlan and OptimizedPlan render the IR before and after
	// optimization.
	LogicalPlan   string
	OptimizedPlan string
	CompileMS     float64
	RuleDetails   []ExplainRuleDetail
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

// HistogramRequest is the domain input for histogram generation.
type HistogramRequest struct {
	From    string
	To      string
	Buckets int
	Index   string
	GroupBy string // optional: field to group counts by (e.g. "level")
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

// GroupedHistogramBucket is a single histogram time bucket with counts broken down by a field value.
type GroupedHistogramBucket struct {
	Time   time.Time
	Counts map[string]int
}

// GroupedHistogramResult is the domain output for grouped histogram.
type GroupedHistogramResult struct {
	Interval string
	Buckets  []GroupedHistogramBucket
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
