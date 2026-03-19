package server

import (
	"context"
	crypto_rand "crypto/rand"
	"encoding/hex"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lynxbase/lynxdb/pkg/model"
	"github.com/lynxbase/lynxdb/pkg/optimizer"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/storage/segment"
	"github.com/lynxbase/lynxdb/pkg/storage/segment/index"
)

// segmentHandle replaces segmentData to support disk-backed segments.
type segmentHandle struct {
	reader      *segment.Reader      // always non-nil for hot; nil for evicted warm/cold
	mmap        *segment.MmapSegment // non-nil for disk-backed hot segments
	meta        model.SegmentMeta
	index       string
	bloom       *index.BloomFilter     // cached for query skip
	invertedIdx *index.SerializedIndex // cached inverted index for bitmap lookups

	// pendingDelete holds file paths to delete when refs reaches 0 (after
	// mmap.Close). This defers file deletion until no epoch references the
	// segment, preventing SIGSEGV on macOS arm64 where the kernel can revoke
	// page protections for unlinked file-backed mappings under memory pressure.
	pendingDelete []string

	// refs is an epoch-based reference count. Each epoch that includes this
	// segment increments refs via incRef(); when the epoch drains it decrements
	// via decRef(). The mmap is closed only when refs reaches 0, ensuring that
	// segments shared across consecutive epochs (e.g., E(1) and E(2)) are not
	// unmapped while any epoch still references them.
	//
	// This fixes a use-after-unmap SIGSEGV where intermediate epochs with 0
	// readers would drain immediately and close mmaps still in use by a long-
	// running query pinned to an earlier epoch.
	refs atomic.Int32
}

// incRef increments the reference count. Called when a segment is included
// in a new epoch via advanceEpoch.
func (sh *segmentHandle) incRef() {
	sh.refs.Add(1)
}

// decRef decrements the reference count. When it reaches 0, closes the mmap.
// Returns true if this call triggered the close. Safe to call concurrently.
func (sh *segmentHandle) decRef() bool {
	new := sh.refs.Add(-1)
	if new < 0 {
		// Defensive: log and recover from double-decRef rather than crashing.
		slog.Default().Error("segmentHandle.decRef: refs went negative",
			"segment_id", sh.meta.ID, "refs", new)
		return false
	}
	if new == 0 {
		if sh.mmap != nil {
			sh.mmap.Close() // idempotent via atomic.CompareAndSwap inside MmapSegment.Close
		}
		for _, p := range sh.pendingDelete {
			if err := os.Remove(p); err == nil {
				os.Remove(filepath.Dir(p)) // clean up empty partition dir; fails on non-empty (ENOTEMPTY)
			}
		}
		return true
	}
	return false
}

// SearchStats holds execution statistics for a search job.
type SearchStats struct {
	RowsScanned          int64           `json:"rows_scanned"`
	RowsReturned         int64           `json:"rows_returned"`
	MatchedRows          int64           `json:"matched_rows,omitempty"`
	ElapsedMS            float64         `json:"elapsed_ms"`
	IndexesUsed          []string        `json:"indexes_used"`
	SegmentsTotal        int             `json:"segments_total"`
	SegmentsScanned      int             `json:"segments_scanned"`
	SegmentsSkippedIdx   int             `json:"segments_skipped_index"`
	SegmentsSkippedTime  int             `json:"segments_skipped_time"`
	SegmentsSkippedStat  int             `json:"segments_skipped_stats"`
	SegmentsSkippedBF    int             `json:"segments_skipped_bloom"`
	SegmentsSkippedRange int             `json:"segments_skipped_range"`
	BufferedEvents       int             `json:"buffered_events"`
	InvertedIndexHits    int             `json:"inverted_index_hits"`
	BloomsChecked        int             `json:"blooms_checked"`
	CountStarOptimized   bool            `json:"count_star_optimized"`
	PartialAggUsed       bool            `json:"partial_agg_used"`
	TopKUsed             bool            `json:"topk_used"`
	SegmentsErrored      int             `json:"segments_errored"`
	ScanMS               float64         `json:"scan_ms"`
	PipelineMS           float64         `json:"pipeline_ms"`
	PrefetchUsed         bool            `json:"prefetch_used"`
	VectorizedFilterUsed bool            `json:"vectorized_filter_used"`
	DictFilterUsed       bool            `json:"dict_filter_used"`
	JoinStrategy         string          `json:"join_strategy,omitempty"`
	AcceleratedBy        string          `json:"accelerated_by,omitempty"`
	MVStatus             string          `json:"mv_status,omitempty"`
	MVSpeedup            string          `json:"mv_speedup,omitempty"`
	MVOriginalScan       int64           `json:"mv_original_scan,omitempty"`
	CacheHit             bool            `json:"cache_hit,omitempty"`
	PipelineStages       []PipelineStage `json:"pipeline_stages,omitempty"`

	// Parse/optimize timing (populated from planner).
	ParseMS    float64 `json:"parse_ms,omitempty"`
	OptimizeMS float64 `json:"optimize_ms,omitempty"`

	// Optimizer rule details (populated from planner).
	OptimizerRules []OptimizerRuleStat `json:"optimizer_rules,omitempty"`
	TotalRules     int                 `json:"total_rules,omitempty"`

	// ProcessedBytes is the total bytes of segment data touched by the query.
	// Sum of SizeBytes for all scanned segments plus buffered event byte estimate.
	ProcessedBytes int64 `json:"processed_bytes,omitempty"`

	// I/O bytes breakdown by source.
	DiskBytesRead  int64 `json:"disk_bytes_read,omitempty"`
	S3BytesRead    int64 `json:"s3_bytes_read,omitempty"`
	CacheBytesRead int64 `json:"cache_bytes_read,omitempty"`

	// Trace-level profiling (populated when ProfileLevel == "trace").
	SegmentDetails []SegmentDetailStat `json:"segment_details,omitempty"`
	VMCalls        int64               `json:"vm_calls,omitempty"`
	VMTotalNS      int64               `json:"vm_total_ns,omitempty"`

	// Search selectivity: fraction of scanned rows that passed the search filter.
	// 1.0 means 100% of rows matched (low selectivity). 0.01 means 1% matched (high selectivity).
	SearchSelectivity float64 `json:"search_selectivity,omitempty"`
	// Suggestion is an actionable recommendation based on query execution analysis.
	Suggestion string `json:"suggestion,omitempty"`

	// Resource usage (captured via BudgetMonitor + getrusage snapshots).
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

	// Per-operator memory budget statistics from the MemoryCoordinator.
	// Populated when profile level is "full" or "trace" and the query has
	// multiple spillable operators with coordinator-managed budgets.
	OperatorBudgets []OperatorBudgetStat `json:"operator_budgets,omitempty"`

	// Multi-source query metadata.
	SourcesScanned []string `json:"sources_scanned,omitempty"`
	SourcesSkipped int      `json:"sources_skipped,omitempty"`

	// Query funnel: rows surviving each pipeline phase.
	// RowsInRange = total rows across segments in the query time range.
	// RowsAfterSearch = rows that passed the search/filter stage.
	// RowsAfterDedup = rows surviving dedup (0 if no dedup stage).
	// RowsInResult = final result row count.
	RowsInRange    int64 `json:"rows_in_range,omitempty"`
	RowsAfterDedup int64 `json:"rows_after_dedup,omitempty"`

	// ResultTypeLabel is the query result type ("events", "aggregate", "timechart").
	// Used by Prometheus to label query duration histograms.
	ResultTypeLabel string `json:"-"`

	// Distributed query shard metadata (populated only in cluster mode).
	ShardsTotal    int  `json:"shards_total,omitempty"`
	ShardsSuccess  int  `json:"shards_success,omitempty"`
	ShardsFailed   int  `json:"shards_failed,omitempty"`
	ShardsTimedOut int  `json:"shards_timed_out,omitempty"`
	ShardsPartial  bool `json:"shards_partial,omitempty"`

	// SlowQuery is true when the query exceeded the slow query threshold.
	// Set before calling OnQueryComplete so Prometheus can increment the counter.
	SlowQuery bool `json:"-"`

	// ErrorType is the machine-readable error classification for failed queries.
	// Values: "parse", "execution", "timeout", "memory". Empty for successful queries.
	// Used by Prometheus to label the query error counter.
	ErrorType string `json:"-"`
}

// PipelineStage holds per-operator metrics collected during pipeline execution.
// Populated when pipeline instrumentation is enabled.
type PipelineStage struct {
	Name        string  `json:"name"`
	InputRows   int64   `json:"input_rows"`
	OutputRows  int64   `json:"output_rows"`
	DurationMS  float64 `json:"duration_ms"`
	ExclusiveMS float64 `json:"exclusive_ms,omitempty"` // self time excluding children
	MemoryBytes int64   `json:"memory_bytes,omitempty"`
	SpilledRows int64   `json:"spilled_rows,omitempty"`
	SpillBytes  int64   `json:"spill_bytes,omitempty"`
}

// SegmentDetailStat holds per-segment I/O metrics for trace-level profiling.
type SegmentDetailStat struct {
	SegmentID        string `json:"segment_id"`
	Source           string `json:"source"` // "disk", "cache", "s3"
	Rows             int64  `json:"rows"`
	RowsAfterFilter  int64  `json:"rows_after_filter"`
	BloomHit         bool   `json:"bloom_hit"`
	InvertedUsed     bool   `json:"inverted_used"`
	ReadDurationNS   int64  `json:"read_duration_ns"`
	BytesRead        int64  `json:"bytes_read"`
	ColumnsProjected int    `json:"columns_projected,omitempty"`
}

// OperatorBudgetStat holds per-operator memory budget statistics from the
// MemoryCoordinator. Surfaced in query profiles to show how memory was
// distributed and consumed across spillable operators.
type OperatorBudgetStat struct {
	Label     string `json:"label"`
	SoftLimit int64  `json:"soft_limit"`
	PeakBytes int64  `json:"peak_bytes"`
	Spilled   bool   `json:"spilled"`
	Phase     string `json:"phase"`
}

// OptimizerRuleStat describes a single optimizer rule that fired during query planning.
type OptimizerRuleStat struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Count       int    `json:"count"`
}

// storeStats holds stats collected during buildIndexStore.
type storeStats struct {
	SegmentsTotal        int
	SegmentsScanned      int
	SegmentsErrored      int // segments that failed to read
	SegmentsSkippedIdx   int // skipped by index name
	SegmentsSkippedTime  int // skipped by time range
	SegmentsSkippedStat  int // skipped by column stats
	SegmentsSkippedBF    int // skipped by bloom filter
	SegmentsSkippedRange int // skipped by range predicates
	BufferedEvents       int
	InvertedIndexHits    int // segments that used inverted index
	PrefetchUsed         bool
	DictFilterUsed       bool // at least one segment used dict-encoded filter
	BloomsChecked        int  // total bloom filter consultations (from RG filter evaluator)
	// TotalBytesRead is the sum of SizeBytes for all scanned segments.
	// Always accumulated (not trace-only) to support ClickHouse-style throughput display.
	TotalBytesRead int64
	// Per-segment I/O details (trace level only).
	SegmentDetails []SegmentDetailStat
}

// Job status constants for SearchJob.Status.
const (
	JobStatusRunning  = "running"
	JobStatusDone     = "done"
	JobStatusError    = "error"
	JobStatusCanceled = "canceled"
)

// SearchPhase describes the current execution phase of a search job.
type SearchPhase string

const (
	PhaseParsing           SearchPhase = "parsing"
	PhaseBufferScan        SearchPhase = "scanning_buffer"
	PhaseFilteringSegments SearchPhase = "filtering_segments"
	PhaseScanningSegments  SearchPhase = "scanning_segments"
	PhaseExecutingPipeline SearchPhase = "executing_pipeline"
)

// SearchProgress holds real-time progress metrics for a running search job.
type SearchProgress struct {
	Phase                SearchPhase `json:"phase"`
	SegmentsTotal        int         `json:"segments_total"`
	SegmentsScanned      int         `json:"segments_scanned"`
	SegmentsDispatched   int         `json:"segments_dispatched"`
	SegmentsSkippedIdx   int         `json:"segments_skipped_index"`
	SegmentsSkippedTime  int         `json:"segments_skipped_time"`
	SegmentsSkippedStat  int         `json:"segments_skipped_stats"`
	SegmentsSkippedBF    int         `json:"segments_skipped_bloom"`
	SegmentsSkippedRange int         `json:"segments_skipped_range"`
	BufferedEvents       int         `json:"buffered_events"`
	RowsReadSoFar        int64       `json:"rows_read_so_far"`
	ElapsedMS            float64     `json:"elapsed_ms"`
}

// SearchJob holds the state and results of a search job.
type SearchJob struct {
	ID         string                         `json:"id"` // "qry_" + 8 hex bytes
	Query      string                         `json:"query"`
	CreatedAt  time.Time                      `json:"created_at"`
	DoneAt     time.Time                      `json:"-"`
	ResultType ResultType                     `json:"-"`
	Progress   atomic.Pointer[SearchProgress] `json:"-"`

	mu        sync.Mutex       // protects Status, Results, Stats, Error, ErrorCode
	Status    string           `json:"status"` // JobStatusRunning, JobStatusDone, JobStatusError, JobStatusCanceled
	Results   []spl2.ResultRow `json:"-"`
	Stats     SearchStats      `json:"-"`
	Error     string           `json:"error,omitempty"`
	ErrorCode string           `json:"-"` // machine-readable error code (e.g., QUERY_MEMORY_EXCEEDED)

	cancel context.CancelFunc // cancels the job's context
	detach func()             // stops parent context propagation (sync→async promotion)
	doneCh chan struct{}      // closed when job completes
}

func newSearchJob(query string, rt ResultType) *SearchJob {
	b := make([]byte, 8)
	_, _ = crypto_rand.Read(b)

	return &SearchJob{
		ID:         "qry_" + hex.EncodeToString(b),
		Query:      query,
		Status:     JobStatusRunning,
		ResultType: rt,
		CreatedAt:  time.Now(),
		doneCh:     make(chan struct{}),
	}
}

func (j *SearchJob) complete(status string) {
	j.DoneAt = time.Now()
	j.Status = status
	close(j.doneCh)
}

// Done returns a channel that is closed when the job completes.
func (j *SearchJob) Done() <-chan struct{} {
	return j.doneCh
}

// Snapshot returns a thread-safe snapshot of the job's current state.
func (j *SearchJob) Snapshot() JobSnapshot {
	j.mu.Lock()
	defer j.mu.Unlock()

	return JobSnapshot{
		ID:         j.ID,
		Query:      j.Query,
		Status:     j.Status,
		Results:    j.Results,
		Stats:      j.Stats,
		Error:      j.Error,
		ErrorCode:  j.ErrorCode,
		ResultType: j.ResultType,
		CreatedAt:  j.CreatedAt,
		DoneAt:     j.DoneAt,
	}
}

// Cancel cancels the job and marks it as canceled.
func (j *SearchJob) Cancel() {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.Status == JobStatusRunning {
		j.cancel()
		j.Error = "canceled by user"
		j.complete(JobStatusCanceled)
	}
}

// Detach re-parents the job's context so it no longer inherits cancellation
// from the original HTTP request context. Called when a sync query is promoted
// to async (202 job handle returned) — ensures the job survives client disconnect.
func (j *SearchJob) Detach() {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.detach != nil {
		j.detach()
		j.detach = nil
	}
}

// JobSnapshot is a point-in-time snapshot of a SearchJob's state.
type JobSnapshot struct {
	ID         string
	Query      string
	Status     string
	Results    []spl2.ResultRow
	Stats      SearchStats
	Error      string
	ErrorCode  string // machine-readable error code (e.g., QUERY_MEMORY_EXCEEDED)
	ResultType ResultType
	CreatedAt  time.Time
	DoneAt     time.Time
}

// JobInfo is a lightweight summary of a job for listing.
type JobInfo struct {
	ID        string    `json:"job_id"`
	Query     string    `json:"query"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// HistogramBucket is a single time bucket for histogram queries.
type HistogramBucket struct {
	Time  time.Time
	Count int
}

// GroupedHistogramBucket is a single time bucket with counts broken down by a field value.
type GroupedHistogramBucket struct {
	Time   time.Time
	Counts map[string]int
}

// FieldValue represents a single field value with count.
type FieldValue struct {
	Value   string
	Count   int
	Percent float64
}

// FieldValuesResult is the result of a field values query.
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

// SourcesResult is the result of a list sources query.
type SourcesResult struct {
	Sources []SourceInfo
}

// ResultType describes the shape of query results.
type ResultType string

const (
	ResultTypeEvents      ResultType = "events"
	ResultTypeAggregate   ResultType = "aggregate"
	ResultTypeTimechart   ResultType = "timechart"
	ResultTypeViewCreated ResultType = "view_created"
)

// QueryParams holds the parameters needed to submit a query.
type QueryParams struct {
	Query              string
	Program            *spl2.Program
	Hints              *spl2.QueryHints // pre-extracted by planner; avoids duplicate extraction
	ExternalTimeBounds *spl2.TimeBounds
	ResultType         ResultType
	ProfileLevel       string // "", "basic", "full", "trace"

	// Planner timing and optimizer rule details (populated by planner for profiling).
	ParseDuration    time.Duration
	OptimizeDuration time.Duration
	RuleDetails      []optimizer.RuleDetail
	TotalRules       int
}
