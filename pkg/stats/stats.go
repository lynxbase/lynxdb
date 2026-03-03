// Package stats provides query execution statistics collected during
// query parsing, optimization, and execution. It serves as the single
// source of truth for all query metrics displayed in the CLI, returned
// via the REST API, and logged for observability.
//
// QueryStats is populated incrementally by the engine and read by the
// CLI formatter at the end. For server-mode queries, use FromSearchStats
// to convert from the server.SearchStats struct.
package stats

import "time"

// Scan type constants used in QueryStats.ScanType.
const (
	ScanTypeMetadataOnly = "metadata_only"
	ScanTypeIndexScan    = "index_scan"
	ScanTypeFilteredScan = "filtered_scan"
	ScanTypeFullScan     = "full_scan"
	ScanTypeEphemeral    = "ephemeral"
)

// QueryStats holds all metrics collected during query execution.
// Populated incrementally by the engine; read by the CLI formatter at the end.
type QueryStats struct {
	// Timing

	// TotalDuration is end-to-end from query parse start to final result written.
	TotalDuration time.Duration
	// ParseDuration is the time spent in the SPL2 lexer + parser + AST build.
	ParseDuration time.Duration
	// OptimizeDuration is the time spent applying optimizer rules.
	OptimizeDuration time.Duration
	// ExecDuration is the pipeline execution time (scan + filter + aggregate + sort).
	ExecDuration time.Duration
	// SerializeDuration is the time spent formatting results (JSON/table/CSV).
	SerializeDuration time.Duration
	// ScanDuration is the time spent scanning segments (server queries).
	// Populated from SearchStats.ScanMS; zero for ephemeral queries.
	ScanDuration time.Duration
	// PipelineDuration is the time spent in pipeline execution (server queries).
	// Populated from SearchStats.PipelineMS; zero for ephemeral queries.
	PipelineDuration time.Duration

	// Scan

	// TotalSegments is the number of segment files in the query time range.
	TotalSegments int
	// ScannedSegments is the number of segments actually opened and read.
	ScannedSegments int
	// BloomSkippedSegments is segments skipped because bloom filter said "term not present".
	BloomSkippedSegments int
	// TimeSkippedSegments is segments skipped because their time range doesn't overlap the query.
	TimeSkippedSegments int
	// IndexSkippedSegments is segments skipped because the index name didn't match.
	IndexSkippedSegments int
	// StatSkippedSegments is segments skipped via column statistics (min/max pruning).
	StatSkippedSegments int
	// RangeSkippedSegments is segments skipped via range predicates.
	RangeSkippedSegments int
	// SegmentsErrored is segments that failed to read.
	SegmentsErrored int
	// TotalRowsInRange is the total event count across all segments in the query time range.
	TotalRowsInRange int64
	// ScannedRows is rows actually read from disk/cache (after segment skipping).
	ScannedRows int64
	// MatchedRows is rows that passed all WHERE/SEARCH filters.
	MatchedRows int64
	// ResultRows is final output rows (after head/limit/dedup/aggregation).
	ResultRows int64
	// BufferedRowsScanned is rows scanned from in-memory buffer (not yet flushed to segments).
	BufferedRowsScanned int64

	// Index utilization

	// InvertedIndexHits is segments that used inverted index for term search.
	InvertedIndexHits int
	// ScanType is one of: "index_scan", "filtered_scan", "full_scan", "ephemeral".
	ScanType string
	// IndexesUsed lists which indexes were queried.
	IndexesUsed []string

	// Optimizations

	// CountStarOptimized is true when COUNT(*) metadata-only shortcut was used.
	CountStarOptimized bool
	// PartialAggUsed is true when two-phase aggregation was used.
	PartialAggUsed bool
	// TopKUsed is true when TopK heap merge was used.
	TopKUsed bool
	// PrefetchUsed is true when segment prefetch was enabled.
	PrefetchUsed bool
	// VectorizedFilterUsed is true when SIMD filter was applied.
	VectorizedFilterUsed bool
	// DictFilterUsed is true when dictionary encoding filter was applied.
	DictFilterUsed bool
	// JoinStrategy describes which join algorithm was used (hash, in_list, bloom_semi).
	JoinStrategy string

	// Throughput

	// ProcessedBytes is the total bytes of segment data touched by the query.
	// For disk-backed queries, this is the sum of SizeBytes for all scanned segments
	// plus a buffered event byte estimate. For ephemeral queries, estimated from event count.
	ProcessedBytes int64

	// Cache

	// CacheHit is true when the entire result was served from query cache.
	CacheHit bool
	// CacheBytesRead is the number of bytes served from cache.
	CacheBytesRead int64
	// DiskBytesRead is the number of bytes read from local disk.
	DiskBytesRead int64
	// S3BytesRead is the number of bytes fetched from object storage.
	S3BytesRead int64

	// Resources

	// PeakMemoryBytes is the high-water mark of tracked operator buffer memory
	// during query execution. Includes aggregation hash tables, sort buffers,
	// join build sides, and scan event buffers. Does NOT include small per-row
	// allocations in streaming operators (filter, eval, project).
	// Populated from BudgetMonitor.MaxAllocated().
	PeakMemoryBytes int64
	// MemAllocBytes is the total tracked allocation bytes during query execution.
	// Equal to PeakMemoryBytes when using BudgetMonitor (high-water mark).
	MemAllocBytes int64
	// SpilledToDisk is true when at least one operator spilled data to disk
	// during query execution (sort, aggregate, or join).
	SpilledToDisk bool
	// SpillBytes is the total bytes written to spill files during execution.
	SpillBytes int64
	// SpillFiles is the number of spill files created during execution.
	SpillFiles int
	// PoolUtilization is the fraction of the global memory pool in use at
	// query completion (0.0–1.0). Populated from RootMonitor when available.
	PoolUtilization float64
	// Warnings holds user-visible warning messages generated during execution.
	// Examples: scan truncation, approximate results from spill merge.
	Warnings []string
	// CPUTimeUser is user-space CPU time consumed (from syscall.Getrusage).
	CPUTimeUser time.Duration
	// CPUTimeSys is kernel CPU time consumed (from syscall.Getrusage).
	CPUTimeSys time.Duration

	// MV acceleration (zero values mean "not used")

	// AcceleratedBy is the name of the MV used (empty if none).
	AcceleratedBy string
	// MVSpeedup is a human-readable speedup estimate, e.g. "~400x".
	MVSpeedup string
	// MVStatus is the MV state: "active", "backfilling", or "partial".
	MVStatus string
	// MVCoveragePercent is the fraction of the time range covered by the MV (0-100).
	MVCoveragePercent float64
	// MVOriginalScan is the estimated rows that would have been scanned without MV.
	MVOriginalScan int64

	// Pipeline breakdown (populated when --verbose)

	// Stages holds per-operator metrics for the pipeline breakdown.
	Stages []StageStats

	// Profile (populated when --analyze is used)

	// OptimizerRules lists details of optimizer rules that fired.
	OptimizerRules []OptimizerRuleDetail
	// OptimizerTotalRules is the total number of rules registered in the optimizer.
	OptimizerTotalRules int
	// Recommendations holds actionable suggestions based on profile analysis.
	Recommendations []Recommendation

	// OperatorBudgets holds per-operator memory budget statistics from the
	// MemoryCoordinator. Populated when --analyze is used and the query has
	// multiple spillable operators with coordinator-managed budgets.
	OperatorBudgets []OperatorBudgetStats

	// Trace-level profiling (populated when --analyze=trace)

	// SegmentDetails holds per-segment I/O breakdown (trace level only).
	SegmentDetails []SegmentDetail
	// VMCalls is the total number of VM.Execute invocations across all operators.
	VMCalls int64
	// VMTotalTime is the cumulative time spent inside VM.Execute calls.
	VMTotalTime time.Duration

	// Mode

	// Ephemeral is true when the query ran against the ephemeral in-memory engine
	// (CLI pipe/file mode, no server).
	Ephemeral bool
}

// OptimizerRuleDetail describes a single optimizer rule application.
type OptimizerRuleDetail struct {
	Name        string
	Description string
	Count       int
}

// Recommendation is an actionable suggestion based on profile analysis.
type Recommendation struct {
	Category         string // "performance", "correctness", "info"
	Priority         string // "high", "medium", "low", "info"
	Message          string
	EstimatedSpeedup string // e.g., "~10x" (optional, trace-level)
	SuggestedAction  string // e.g., "lynxdb mv create ..." (optional)
}

// OperatorBudgetStats holds per-operator memory budget statistics from the
// MemoryCoordinator, providing visibility into how the query memory budget
// was distributed and consumed across spillable operators.
type OperatorBudgetStats struct {
	Name      string `json:"name"`
	SoftLimit int64  `json:"soft_limit"`
	PeakBytes int64  `json:"peak_bytes"`
	Phase     string `json:"phase"`
	Spilled   bool   `json:"spilled"`
}

// StageStats holds per-operator metrics for the pipeline breakdown.
type StageStats struct {
	Name        string
	InputRows   int64
	OutputRows  int64
	Duration    time.Duration
	ExclusiveNS int64 // exclusive time in nanoseconds (self only, excludes children)
	VMCalls     int64 // VM.Execute invocations in this stage (trace only)
	VMTimeNS    int64 // total nanoseconds in VM.Execute (trace only)
	MemoryBytes int64 // current tracked memory for this operator (from BoundAccount)
	SpilledRows int64 // rows spilled to disk by this operator (0 = no spill)
	SpillBytes  int64 // bytes written to spill files by this operator
}

// ComputeExclusiveTiming populates ExclusiveNS for each stage. In a Volcano
// (pull-based) pipeline, each operator's inclusive time contains all children
// below it. Exclusive = inclusive - max(child inclusive). The leaf operator
// (last in the slice) has exclusive == inclusive.
func ComputeExclusiveTiming(stages []StageStats) {
	if len(stages) == 0 {
		return
	}
	// Leaf operator: exclusive == inclusive.
	last := len(stages) - 1
	stages[last].ExclusiveNS = stages[last].Duration.Nanoseconds()

	// Non-leaf operators: exclusive = inclusive - child inclusive.
	// i+1 is always in bounds because we iterate up to last-1.
	for i := range stages[:last] {
		inclusive := stages[i].Duration.Nanoseconds()
		childInclusive := stages[i+1].Duration.Nanoseconds() //nolint:gosec // i < last, so i+1 <= last which is in bounds
		exclusive := inclusive - childInclusive
		if exclusive < 0 {
			exclusive = 0
		}
		stages[i].ExclusiveNS = exclusive
	}
}

// SegmentDetail holds per-segment I/O metrics collected during trace-level profiling.
type SegmentDetail struct {
	SegmentID       string
	Source          string // "disk", "cache", "s3", "mmap"
	Rows            int64
	RowsAfterFilter int64
	BloomHit        bool
	InvertedUsed    bool
	ReadDuration    time.Duration
	BytesRead       int64
}

// FromSearchStats converts a server.SearchStats into a QueryStats for CLI display.
// This bridges the server-side statistics collected during disk-backed query execution
// with the unified CLI formatting. The caller should set TotalDuration separately
// (it includes network round-trip which the server doesn't know about).
func FromSearchStats(
	rowsScanned int64,
	rowsReturned int64,
	elapsedMS float64,
	segmentsTotal int,
	segmentsScanned int,
	segmentsSkippedIdx int,
	segmentsSkippedTime int,
	segmentsSkippedStat int,
	segmentsSkippedBF int,
	segmentsSkippedRange int,
	segmentsErrored int,
	memtableEvents int,
	invertedIndexHits int,
	countStarOptimized bool,
	partialAggUsed bool,
	topKUsed bool,
	prefetchUsed bool,
	vectorizedFilterUsed bool,
	dictFilterUsed bool,
	joinStrategy string,
	scanMS float64,
	pipelineMS float64,
	indexesUsed []string,
	acceleratedBy string,
) *QueryStats {
	st := &QueryStats{
		ExecDuration:         timeDurFromMS(elapsedMS),
		ScannedRows:          rowsScanned,
		ResultRows:           rowsReturned,
		TotalSegments:        segmentsTotal,
		ScannedSegments:      segmentsScanned,
		IndexSkippedSegments: segmentsSkippedIdx,
		TimeSkippedSegments:  segmentsSkippedTime,
		StatSkippedSegments:  segmentsSkippedStat,
		BloomSkippedSegments: segmentsSkippedBF,
		RangeSkippedSegments: segmentsSkippedRange,
		SegmentsErrored:      segmentsErrored,
		BufferedRowsScanned:  int64(memtableEvents),
		InvertedIndexHits:    invertedIndexHits,
		CountStarOptimized:   countStarOptimized,
		PartialAggUsed:       partialAggUsed,
		TopKUsed:             topKUsed,
		PrefetchUsed:         prefetchUsed,
		VectorizedFilterUsed: vectorizedFilterUsed,
		DictFilterUsed:       dictFilterUsed,
		JoinStrategy:         joinStrategy,
		IndexesUsed:          indexesUsed,
		AcceleratedBy:        acceleratedBy,
	}

	// Derive scan type from available information.
	switch {
	case countStarOptimized:
		st.ScanType = ScanTypeMetadataOnly
	case invertedIndexHits > 0:
		st.ScanType = ScanTypeIndexScan
	case segmentsSkippedBF > 0 || segmentsSkippedTime > 0 || segmentsSkippedStat > 0:
		st.ScanType = ScanTypeFilteredScan
	case segmentsTotal > 0:
		st.ScanType = ScanTypeFullScan
	default:
		st.ScanType = ScanTypeFullScan
	}

	// Compute TotalRowsInRange as scanned + skipped segment event estimates.
	// We approximate by assuming skipped segments have similar density to scanned ones.
	st.TotalRowsInRange = rowsScanned
	if segmentsScanned > 0 {
		totalSkipped := segmentsSkippedIdx + segmentsSkippedTime + segmentsSkippedStat +
			segmentsSkippedBF + segmentsSkippedRange
		avgRowsPerSegment := rowsScanned / int64(segmentsScanned)
		st.TotalRowsInRange += int64(totalSkipped) * avgRowsPerSegment
	}

	return st
}

// FromMeta is a simplified constructor for building QueryStats from REST API
// response metadata (took_ms, scanned count, and optional rich stats).
// Use this to convert client-side API responses into displayable stats.
func FromMeta(tookMS float64, scanned, rowsReturned int64) *QueryStats {
	return &QueryStats{
		ExecDuration: timeDurFromMS(tookMS),
		ScannedRows:  scanned,
		ResultRows:   rowsReturned,
		ScanType:     ScanTypeFullScan,
	}
}

// timeDurFromMS converts milliseconds (float64) to time.Duration.
func timeDurFromMS(ms float64) time.Duration {
	return time.Duration(ms * float64(time.Millisecond))
}

// Computed helpers

// ScanRateRowsPerSec returns the scan throughput in rows per second.
func (s *QueryStats) ScanRateRowsPerSec() float64 {
	if s.ExecDuration <= 0 {
		return 0
	}

	return float64(s.ScannedRows) / s.ExecDuration.Seconds()
}

// BytesPerSec returns the data processing throughput in bytes per second.
// Uses ExecDuration (pipeline execution time) as the denominator to match
// the same time base as ScanRateRowsPerSec.
func (s *QueryStats) BytesPerSec() float64 {
	if s.ExecDuration <= 0 || s.ProcessedBytes <= 0 {
		return 0
	}

	return float64(s.ProcessedBytes) / s.ExecDuration.Seconds()
}

// Selectivity returns the fraction of scanned rows that matched filters.
// Lower selectivity = more selective query = better filtering.
// Returns 0 if no rows were scanned.
func (s *QueryStats) Selectivity() float64 {
	if s.ScannedRows == 0 {
		return 0
	}

	return float64(s.MatchedRows) / float64(s.ScannedRows)
}

// SkipRatio returns the fraction of total rows the engine avoided reading.
// Higher = better index utilization.
// Returns 0 if TotalRowsInRange is 0.
func (s *QueryStats) SkipRatio() float64 {
	if s.TotalRowsInRange == 0 {
		return 0
	}

	return 1.0 - float64(s.ScannedRows)/float64(s.TotalRowsInRange)
}

// CacheHitRatio returns 1.0 if the result was served from cache, 0.0 otherwise.
func (s *QueryStats) CacheHitRatio() float64 {
	if s.CacheHit {
		return 1.0
	}

	return 0.0
}

// EventsPerSec returns the filter throughput in matched rows per second.
func (s *QueryStats) EventsPerSec() float64 {
	if s.ExecDuration <= 0 {
		return 0
	}

	return float64(s.MatchedRows) / s.ExecDuration.Seconds()
}

// SkippedSegments returns the total number of segments skipped for any reason.
func (s *QueryStats) SkippedSegments() int {
	return s.BloomSkippedSegments + s.TimeSkippedSegments +
		s.IndexSkippedSegments + s.StatSkippedSegments +
		s.RangeSkippedSegments
}

// SkipPercent returns the percentage of segments that were skipped.
func (s *QueryStats) SkipPercent() float64 {
	if s.TotalSegments == 0 {
		return 0
	}

	return float64(s.TotalSegments-s.ScannedSegments) / float64(s.TotalSegments) * 100
}
