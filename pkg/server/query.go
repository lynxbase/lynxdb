package server

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lynxbase/lynxdb/pkg/cache"
	enginepipeline "github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/memgov"
	"github.com/lynxbase/lynxdb/pkg/optimizer"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/stats"
	"github.com/lynxbase/lynxdb/pkg/timerange"
)

// DetectResultType walks the pipeline backwards, skipping pass-through commands
// (head, tail, sort, fields, table, rename, fillnull, topn) to find the last
// result-type-defining command.
func DetectResultType(prog *spl2.Program) ResultType {
	if prog.Main == nil || len(prog.Main.Commands) == 0 {
		return ResultTypeEvents
	}
	for i := len(prog.Main.Commands) - 1; i >= 0; i-- {
		cmd := prog.Main.Commands[i]
		switch cmd.(type) {
		case *spl2.HeadCommand, *spl2.TailCommand, *spl2.SortCommand,
			*spl2.FieldsCommand, *spl2.TableCommand, *spl2.RenameCommand,
			*spl2.FillnullCommand, *spl2.TopNCommand:
			continue // transparent — check previous command
		case *spl2.TimechartCommand:
			return ResultTypeTimechart
		case *spl2.StatsCommand, *spl2.TopCommand, *spl2.RareCommand,
			*spl2.XYSeriesCommand:
			return ResultTypeAggregate
		case *spl2.EventstatsCommand:
			// EVENTSTATS is an enrichment command — it adds computed fields
			// to every input event while preserving the original row count.
			// When it's the terminal command, the result is still Events format,
			// not Aggregate. Treating it as Aggregate would misclassify queries
			// like "FROM idx | EVENTSTATS count BY group".
			return ResultTypeEvents
		case *spl2.MaterializeCommand:
			return ResultTypeViewCreated
		case *spl2.GlimpseCommand:
			return ResultTypeGlimpse
		default:
			return ResultTypeEvents
		}
	}

	return ResultTypeEvents
}

// ParseTimeBounds parses from/to time strings into TimeBounds.
func ParseTimeBounds(from, to string) *spl2.TimeBounds {
	if from == "" && to == "" {
		return nil
	}
	tb := &spl2.TimeBounds{}
	now := time.Now()
	if from != "" {
		if t, err := parseTimeValue(from, now); err == nil {
			tb.Earliest = t
		}
	}
	if to != "" {
		if t, err := parseTimeValue(to, now); err == nil {
			tb.Latest = t
		}
	}

	return tb
}

func parseTimeValue(s string, now time.Time) (time.Time, error) {
	s = strings.TrimSpace(s)
	if strings.EqualFold(s, "now") {
		return now, nil
	}
	if strings.HasPrefix(s, "-") {
		dur, err := timerange.ParseRelative(s[1:])
		if err != nil {
			return time.Time{}, err
		}

		return now.Add(-dur), nil
	}
	// Try RFC3339Nano first (exact format from CLI flags).
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}

	return timerange.ParseAbsolute(s)
}

// ErrTooManyQueries is returned when the concurrency limit is exceeded.
var ErrTooManyQueries = errors.New("too many concurrent queries")

// ErrQueryMemoryExceeded is returned when a query exceeds its memory budget.
var ErrQueryMemoryExceeded = errors.New("query memory budget exceeded")

// ErrQueryTimeout is returned when a query exceeds the max_query_runtime limit.
var ErrQueryTimeout = errors.New("query exceeded maximum runtime")

// SubmitQuery creates a SearchJob and starts executing the query in a goroutine.
// It atomically checks and increments the active job count to enforce the
// concurrency limit without races. Returns ErrTooManyQueries if over limit.
//
// The parent context controls query cancellation: sync callers should pass the HTTP
// request context so that client disconnects cancel the query; async callers should
// pass context.Background() since jobs outlive the HTTP request.
func (e *Engine) SubmitQuery(ctx context.Context, params QueryParams) (*SearchJob, error) {
	// Atomic concurrency check: CAS loop ensures no two goroutines can
	// both pass the limit check and increment past maxConcurrent.
	maxConcur := int64(e.maxConcur.Load())
	for {
		current := e.activeJobs.Load()
		if current >= maxConcur {
			return nil, ErrTooManyQueries
		}
		if e.activeJobs.CompareAndSwap(current, current+1) {
			break
		}
	}

	job := newSearchJob(params.Query, params.ResultType)

	// Create a detachable context: jobCtx is independent (from Background),
	// with a goroutine that propagates parent (HTTP) cancellation. Calling
	// Detach() closes detachCh, stopping propagation so the job survives
	// client disconnect after sync→async promotion.
	//
	// Always enforce a hard deadline so no query can run unbounded.
	// Uses MaxQueryRuntime from config, falling back to a 10-minute safety
	// net when the config value is 0 (prevents runaway queries from
	// exhausting memory, CPU, and file descriptors indefinitely).
	const defaultMaxQueryRuntime = 10 * time.Minute
	var jobCtx context.Context
	var jobCancel context.CancelFunc
	if maxRT := e.queryCfg.Load().MaxQueryRuntime; maxRT > 0 {
		jobCtx, jobCancel = context.WithTimeout(context.Background(), maxRT)
	} else {
		jobCtx, jobCancel = context.WithTimeout(context.Background(), defaultMaxQueryRuntime)
	}
	job.cancel = jobCancel

	detachCh := make(chan struct{})
	job.detach = func() { close(detachCh) }

	go func() {
		select {
		case <-ctx.Done():
			jobCancel() // parent (HTTP) canceled — cancel job
		case <-detachCh:
			// Detached — stop propagating parent cancellation.
		case <-jobCtx.Done():
			// Job finished — nothing to propagate.
		}
	}()

	e.jobs.Store(job.ID, job)
	e.metrics.QueryTotal.Add(1)

	go e.executeQuery(jobCtx, job, params)

	return job, nil
}

// ActiveJobCount returns the number of currently active jobs.
func (e *Engine) ActiveJobCount() int64 {
	return e.activeJobs.Load()
}

// executeQuery runs the query pipeline for a SearchJob. Called in a goroutine.
//
//nolint:maintidx // large orchestration function; splitting would lose readability of the query execution flow
func (e *Engine) executeQuery(ctx context.Context, job *SearchJob, params QueryParams) {
	defer e.activeJobs.Add(-1)

	// Track query errors and timeouts in metrics on completion.
	defer func() {
		// Handle timeout: if the context deadline fired and the job is still
		// running, mark it as a timeout error.
		if ctx.Err() == context.DeadlineExceeded {
			job.mu.Lock()
			if job.Status == JobStatusRunning {
				job.Error = ErrQueryTimeout.Error()
				job.Stats.ErrorType = "timeout"
				job.Stats.ResultTypeLabel = string(params.ResultType)
				job.complete(JobStatusError)
			}
			job.mu.Unlock()
			e.metrics.QueryTimeouts.Add(1)

			// Record timeout in Prometheus via the query-complete hook.
			if fn := e.onQueryComplete; fn != nil {
				job.mu.Lock()
				ss := job.Stats
				job.mu.Unlock()
				fn(&ss)
			}
		}
		// Count errors from any source (timeout, OOM, pipeline failure, etc.).
		job.mu.Lock()
		isError := job.Status == JobStatusError
		job.mu.Unlock()
		if isError {
			e.metrics.QueryErrors.Add(1)
		}
	}()

	logger := e.logger.With("query_id", job.ID)
	prog := params.Program
	externalTimeBounds := params.ExternalTimeBounds
	start := time.Now()

	// Wrap onProgress to preserve counters across phase transitions (Bug 2 fix).
	// Phase-only updates (all counters zero) merge with previous progress so
	// the TUI never sees counters reset to zero during transitions.
	var lastProgress atomic.Pointer[SearchProgress]
	onProgress := func(p *SearchProgress) {
		prev := lastProgress.Load()
		if prev != nil && p.SegmentsTotal == 0 && p.RowsReadSoFar == 0 && p.BufferedEvents == 0 {
			merged := *prev
			merged.Phase = p.Phase
			merged.ElapsedMS = float64(time.Since(start).Milliseconds())
			p = &merged
		}
		p.ElapsedMS = float64(time.Since(start).Milliseconds())
		lastProgress.Store(p)
		job.Progress.Store(p)
	}
	onProgress(&SearchProgress{Phase: PhaseParsing})

	// Use pre-extracted hints from planner when available; fall back to extraction.
	hints := params.Hints
	if hints == nil {
		hints = spl2.ExtractQueryHints(prog)
	}
	// Merge external time bounds.
	if externalTimeBounds != nil {
		if hints.TimeBounds == nil {
			hints.TimeBounds = externalTimeBounds
		} else {
			if !externalTimeBounds.Earliest.IsZero() &&
				(hints.TimeBounds.Earliest.IsZero() || externalTimeBounds.Earliest.After(hints.TimeBounds.Earliest)) {
				hints.TimeBounds.Earliest = externalTimeBounds.Earliest
			}
			if !externalTimeBounds.Latest.IsZero() &&
				(hints.TimeBounds.Latest.IsZero() || externalTimeBounds.Latest.Before(hints.TimeBounds.Latest)) {
				hints.TimeBounds.Latest = externalTimeBounds.Latest
			}
		}
	}

	// Build cache key from query + time bounds + ingest generation.
	// Including the generation counter ensures that queries after new ingest
	// always miss the cache, even if the query and time range are identical (E3).
	var earliest, latest int64
	if hints.TimeBounds != nil {
		if !hints.TimeBounds.Earliest.IsZero() {
			earliest = hints.TimeBounds.Earliest.UnixNano()
		}
		if !hints.TimeBounds.Latest.IsZero() {
			latest = hints.TimeBounds.Latest.UnixNano()
		}
	}
	gen := e.ingestGen.Load()
	cacheKey := cache.Key{
		QueryHash:  cache.HashQuery(job.Query),
		Generation: uint64(gen),
		TimeRange:  [2]int64{earliest, latest},
	}

	// Check cache.
	if cached, cacheErr := e.cache.Get(ctx, cacheKey); cacheErr != nil {
		logger.Debug("cache get failed", "error", cacheErr)
	} else if cached != nil {
		e.metrics.QueryCacheHits.Add(1)
		rows := cachedResultToResultRows(cached)
		elapsed := time.Since(start)
		job.mu.Lock()
		job.Results = rows
		job.Stats = SearchStats{
			RowsReturned: int64(len(rows)),
			ElapsedMS:    float64(elapsed.Milliseconds()),
			CacheHit:     true,
		}
		job.complete(JobStatusDone)
		job.mu.Unlock()

		return
	}

	onProgress(&SearchProgress{Phase: PhaseBufferScan})

	// Check for countStarOnly annotation — metadata-only count shortcut.
	if prog.Main != nil {
		if _, ok := prog.Main.GetAnnotation("countStarOnly"); ok {
			count := e.countStarFromMetadata(hints)
			elapsed := time.Since(start)
			// Use the alias from the AST if present (e.g., "stats count AS total"),
			// otherwise default to "count" to match Splunk convention.
			countAlias := "count"
			if stats := findStatsCommand(prog.Main); stats != nil && len(stats.Aggregations) == 1 {
				if stats.Aggregations[0].Alias != "" {
					countAlias = stats.Aggregations[0].Alias
				}
			}
			rows := []spl2.ResultRow{{Fields: map[string]interface{}{countAlias: count}}}
			job.mu.Lock()
			job.Results = rows
			job.Stats = SearchStats{
				RowsReturned:       int64(len(rows)),
				ElapsedMS:          float64(elapsed.Milliseconds()),
				CountStarOptimized: true,
			}
			job.complete(JobStatusDone)
			job.mu.Unlock()
			if err := e.cache.Put(ctx, cacheKey, resultRowsToCachedResult(rows)); err != nil {
				logger.Debug("cache put failed", "error", err)
			}

			return
		}
	}

	// Check for aggregation pushdown annotation.
	var aggSpec *enginepipeline.PartialAggSpec
	if prog.Main != nil {
		if ann, ok := prog.Main.GetAnnotation("partialAgg"); ok {
			aggSpec = ann.(*enginepipeline.PartialAggSpec)
		}
	}

	ann := extractAnnotations(prog)

	// Governor handles memory enforcement. No per-query budget monitor needed.
	cpuBefore := stats.TakeCPUSnapshot()
	scanStart := time.Now()

	// Preview callback: stores a sample of materialized rows on the job for
	// progressive SSE delivery while the pipeline is still running.
	previewSize := e.queryCfg.Load().PreviewSize
	if previewSize == 0 {
		previewSize = 50
	}
	var previewSeq int64
	var onPreview func(totalRows int, rows []map[string]event.Value)
	if previewSize > 0 && job.ResultType != ResultTypeAggregate && job.ResultType != ResultTypeTimechart {
		var lastPreview time.Time
		onPreview = func(total int, rows []map[string]event.Value) {
			now := time.Now()
			if now.Sub(lastPreview) < time.Second && total > 0 {
				return // throttle: at most 1/sec after first batch
			}
			lastPreview = now
			sample := sampleRowsForPreview(rows, previewSize)
			ver := atomic.AddInt64(&previewSeq, 1)
			job.Preview.Store(&PreviewSnapshot{
				Version: ver,
				Total:   int64(total),
				Rows:    sample,
			})
		}
	}

	qr, err := e.runQueryPipeline(ctx, prog, hints, params, aggSpec, ann, onProgress, onPreview, scanStart)
	if err != nil {
		errCode := classifyQueryError(err)
		job.mu.Lock()
		job.Error = err.Error()
		job.ErrorCode = errCode
		job.Stats = SearchStats{
			ElapsedMS:       float64(time.Since(start).Milliseconds()),
			ResultTypeLabel: string(params.ResultType),
			ErrorType:       classifyErrorType(err),
		}
		job.complete(JobStatusError)
		job.mu.Unlock()

		// Invoke the query-complete hook for error queries too so Prometheus
		// can track error counts and latency distributions for failed queries.
		if fn := e.onQueryComplete; fn != nil {
			fn(&job.Stats)
		}

		return
	}

	// Check for scope hint: warn Splunk users who search all sources without
	// narrowing by source/index when many sources exist.
	if scopeHint := spl2.DetectScopeHint(job.Query, e.sourceRegistry.Count()); scopeHint != nil {
		qr.warnings = append(qr.warnings, scopeHint.Suggestion)
	}

	cpuAfter := stats.TakeCPUSnapshot()
	elapsed := time.Since(start)
	job.mu.Lock()
	job.Results = qr.rows
	job.Stats = SearchStats{
		RowsScanned:          qr.rowsScanned,
		RowsReturned:         int64(len(qr.rows)),
		MatchedRows:          qr.matchedRows,
		ElapsedMS:            float64(elapsed.Milliseconds()),
		ScanMS:               qr.scanMS,
		PipelineMS:           qr.pipelineMS,
		ParseMS:              float64(params.ParseDuration.Microseconds()) / 1000,
		OptimizeMS:           float64(params.OptimizeDuration.Microseconds()) / 1000,
		IndexesUsed:          qr.indexesUsed,
		SegmentsTotal:        qr.ss.SegmentsTotal,
		SegmentsScanned:      qr.ss.SegmentsScanned,
		SegmentsSkippedIdx:   qr.ss.SegmentsSkippedIdx,
		SegmentsSkippedTime:  qr.ss.SegmentsSkippedTime,
		SegmentsSkippedStat:  qr.ss.SegmentsSkippedStat,
		SegmentsSkippedBF:    qr.ss.SegmentsSkippedBF,
		SegmentsSkippedRange: qr.ss.SegmentsSkippedRange,
		SegmentsErrored:      qr.ss.SegmentsErrored,
		BufferedEvents:       qr.ss.BufferedEvents,
		InvertedIndexHits:    qr.ss.InvertedIndexHits,
		BloomsChecked:        qr.ss.BloomsChecked,
		PrefetchUsed:         qr.ss.PrefetchUsed,
		PartialAggUsed:       aggSpec != nil || hasTransformPartialAgg(prog),
		TopKUsed:             qr.topKUsed,
		VectorizedFilterUsed: qr.vectorizedFilterUsed,
		DictFilterUsed:       qr.ss.DictFilterUsed,
		JoinStrategy:         ann.joinStrategy,
		AcceleratedBy:        ann.acceleratedBy,
		MVStatus:             ann.mvStatus,
		MVSpeedup:            estimateMVSpeedup(ann.mvRows, qr.rowsScanned),
		MVOriginalScan:       ann.mvRows,
		PipelineStages:       qr.pipelineStages,
		Warnings:             qr.warnings,
		SegmentDetails:       qr.ss.SegmentDetails,
		VMCalls:              qr.vmCalls,
		VMTotalNS:            qr.vmTimeNS,
	}

	// Populate multi-source metadata from resolved hints.
	switch hints.SourceScopeType {
	case spl2.SourceScopeList:
		if len(hints.SourceScopeSources) > 0 {
			job.Stats.SourcesScanned = hints.SourceScopeSources
		}
	case spl2.SourceScopeSingle:
		if len(hints.SourceScopeSources) > 0 {
			job.Stats.SourcesScanned = hints.SourceScopeSources
		} else if hints.IndexName != "" {
			job.Stats.SourcesScanned = []string{hints.IndexName}
		}
	case spl2.SourceScopeAll, "":
		if e.sourceRegistry.Count() > 0 {
			job.Stats.SourcesScanned = e.sourceRegistry.List()
		}
	}

	// Compute SourcesSkipped: total available minus scanned.
	if totalSources := e.sourceRegistry.Count(); totalSources > 0 {
		scannedCount := len(job.Stats.SourcesScanned)
		if scannedCount < totalSources {
			job.Stats.SourcesSkipped = totalSources - scannedCount
		}
	}

	// Populate per-operator memory budget statistics from coordinator.
	if len(qr.operatorBudgets) > 0 {
		budgets := make([]OperatorBudgetStat, len(qr.operatorBudgets))
		for i, b := range qr.operatorBudgets {
			budgets[i] = OperatorBudgetStat{
				Label:     b.Name,
				SoftLimit: b.SoftLimit,
				PeakBytes: b.PeakBytes,
				Spilled:   b.Spilled,
				Phase:     b.Phase,
			}
		}
		job.Stats.OperatorBudgets = budgets
	}

	// Populate optimizer rule details from planner.
	if len(params.RuleDetails) > 0 {
		rules := make([]OptimizerRuleStat, len(params.RuleDetails))
		for i, rd := range params.RuleDetails {
			rules[i] = OptimizerRuleStat{
				Name:        rd.Name,
				Description: rd.Description,
				Count:       rd.Count,
			}
		}
		job.Stats.OptimizerRules = rules
		job.Stats.TotalRules = params.TotalRules
	}

	// ProcessedBytes: segment bytes + buffered event byte estimate.
	// Buffered events don't have segment metadata, so we use a conservative
	// estimate (256 B/event) that accounts for struct overhead + avg field payload.
	const bufferedEventEstimate int64 = 256
	job.Stats.ProcessedBytes = qr.ss.TotalBytesRead + int64(qr.ss.BufferedEvents)*bufferedEventEstimate

	// Aggregate bytes read by source from segment details.
	for _, sd := range qr.ss.SegmentDetails {
		switch sd.Source {
		case "disk", "mmap":
			job.Stats.DiskBytesRead += sd.BytesRead
		case "cache":
			job.Stats.CacheBytesRead += sd.BytesRead
		case "s3":
			job.Stats.S3BytesRead += sd.BytesRead
		}
	}

	applyBudgetAndCPUDelta(&job.Stats, qr.govBudget, cpuBefore, cpuAfter)
	// Close the governor budget adapter after stats are collected.
	// Operator accounts are already closed by iterator.Close(); this releases
	// any remaining QueryBudget leases.
	if qr.govBudget != nil {
		qr.govBudget.Close()
	}
	applySpillAndPoolStats(&job.Stats, qr.pipelineStages)
	computeSearchSelectivity(&job.Stats)
	computeQueryFunnel(&job.Stats, qr.pipelineStages)

	// Set result type label for Prometheus histogram labeling.
	job.Stats.ResultTypeLabel = string(params.ResultType)

	// Zero-result guidance: when the query returns no results, generate actionable
	// suggestions based on field predicates and segment skip reasons.
	if len(qr.rows) == 0 {
		if suggestions := zeroResultSuggestions(hints, qr.ss); len(suggestions) > 0 {
			job.Stats.Warnings = append(job.Stats.Warnings, suggestions...)
		}
	}

	// Determine slow query flag BEFORE calling onQueryComplete so Prometheus
	// can increment the slow query counter in RecordQuery.
	threshold := e.queryCfg.Load().SlowQueryThresholdMs
	if threshold > 0 && elapsed.Milliseconds() > threshold {
		job.Stats.SlowQuery = true
	}

	job.complete(JobStatusDone)
	job.mu.Unlock()

	// Invoke the query-complete hook (e.g., Prometheus histogram observations).
	if fn := e.onQueryComplete; fn != nil {
		fn(&job.Stats)
	}

	// Slow query log: emit a structured warning when the query exceeds the threshold.
	// SlowQueryThresholdMs == 0 disables slow query logging.
	if job.Stats.SlowQuery {
		e.metrics.QuerySlowTotal.Add(1)
		logger.Warn("slow query",
			"query", job.Query,
			"duration_ms", elapsed.Milliseconds(),
			"scanned_rows", qr.rowsScanned,
			"result_rows", len(qr.rows),
			"scan_ms", qr.scanMS,
			"pipeline_ms", qr.pipelineMS,
			"segments_scanned", qr.ss.SegmentsScanned,
			"segments_total", qr.ss.SegmentsTotal,
			"search_selectivity", job.Stats.SearchSelectivity,
		)
	}

	// Store in cache (non-fatal; log failures for observability).
	if err := e.cache.Put(ctx, cacheKey, resultRowsToCachedResult(qr.rows)); err != nil {
		logger.Debug("cache put failed", "error", err)
	}
}

// queryAnnotations holds pre-extracted optimizer annotations from the AST.
type queryAnnotations struct {
	joinStrategy  string
	acceleratedBy string
	mvStatus      string
	mvRows        int64
}

// extractAnnotations pulls optimizer annotations from the program AST.
func extractAnnotations(prog *spl2.Program) queryAnnotations {
	var ann queryAnnotations
	if prog.Main == nil {
		return ann
	}
	if a, ok := prog.Main.GetAnnotation("joinStrategy"); ok {
		if ja, ok := a.(interface{ GetStrategy() string }); ok {
			ann.joinStrategy = ja.GetStrategy()
		}
	}
	if a, ok := prog.Main.GetAnnotation("mvAccelerated"); ok {
		switch v := a.(type) {
		case *optimizer.MVAccelAnnotation:
			ann.acceleratedBy = v.ViewName
			ann.mvStatus = v.Status
			ann.mvRows = v.MVRows
		case string:
			ann.acceleratedBy = v
		}
	}

	return ann
}

// sampleRowsForPreview returns the last maxN rows from the accumulated slice,
// converted to JSON-compatible maps. Used for progressive preview in SSE events.
func sampleRowsForPreview(rows []map[string]event.Value, maxN int) []map[string]interface{} {
	start := 0
	if len(rows) > maxN {
		start = len(rows) - maxN
	}
	out := make([]map[string]interface{}, 0, len(rows)-start)
	for i := start; i < len(rows); i++ {
		row := make(map[string]interface{}, len(rows[i]))
		for k, v := range rows[i] {
			row[k] = v.Interface()
		}
		out = append(out, row)
	}
	return out
}

// queryPipelineResult holds the output of runQueryPipeline.
type queryPipelineResult struct {
	rows                 []spl2.ResultRow
	ss                   storeStats
	rowsScanned          int64
	matchedRows          int64
	indexesUsed          []string
	scanMS               float64
	pipelineMS           float64
	topKUsed             bool
	vectorizedFilterUsed bool
	pipelineStages       []PipelineStage
	warnings             []string
	vmCalls              int64
	vmTimeNS             int64
	operatorBudgets      []stats.OperatorBudgetStats
	govBudget            *memgov.BudgetAdapter // non-nil when governor v2 path was used
}

// runQueryPipeline executes either the partial-agg or standard pipeline path.
func (e *Engine) runQueryPipeline(
	ctx context.Context,
	prog *spl2.Program,
	hints *spl2.QueryHints,
	params QueryParams,
	aggSpec *enginepipeline.PartialAggSpec,
	ann queryAnnotations,
	onProgress func(*SearchProgress),
	onPreview func(totalRows int, rows []map[string]event.Value),
	scanStart time.Time,
) (*queryPipelineResult, error) {
	// Distributed query path: if a cluster coordinator is configured,
	// delegate to scatter-gather execution across the cluster.
	if e.clusterCoordinator != nil {
		return e.runDistributedPipeline(ctx, prog, hints, onProgress, scanStart)
	}

	if aggSpec != nil {
		return e.runPartialAggPipeline(ctx, prog, hints, aggSpec, onProgress, scanStart)
	}

	// Check for transform+partial-agg annotation (rex/eval/where + stats).
	if prog.Main != nil {
		if tAnn, ok := prog.Main.GetAnnotation("transformPartialAgg"); ok {
			if tSpec, ok := tAnn.(*optimizer.TransformPartialAggAnnotation); ok {
				return e.runTransformPartialAggPipeline(ctx, prog, hints, tSpec, onProgress, scanStart)
			}
		}
	}

	return e.runStandardPipeline(ctx, prog, hints, params, onProgress, onPreview, scanStart)
}

// runDistributedPipeline delegates query execution to the cluster coordinator
// for scatter-gather execution across all relevant shards.
func (e *Engine) runDistributedPipeline(
	ctx context.Context,
	prog *spl2.Program,
	hints *spl2.QueryHints,
	onProgress func(*SearchProgress),
	scanStart time.Time,
) (*queryPipelineResult, error) {
	onProgress(&SearchProgress{Phase: PhaseScanningSegments})

	result, err := e.clusterCoordinator.ExecuteQuery(ctx, prog, hints)
	if err != nil {
		return nil, err
	}

	qr := &queryPipelineResult{
		scanMS:     result.ScanMS,
		pipelineMS: result.MergeMS,
	}

	// Convert rows to ResultRows.
	qr.rows = pipelineRowsToResultRows(result.Rows)

	// Surface shard metadata as warnings.
	if result.Meta.Partial {
		qr.warnings = append(qr.warnings, result.Meta.Warnings...)
		qr.warnings = append(qr.warnings, fmt.Sprintf(
			"partial results: %d/%d shards succeeded",
			result.Meta.ShardsSuccess, result.Meta.ShardsTotal))
	}

	return qr, nil
}

// runPartialAggPipeline handles the partial aggregation path.
func (e *Engine) runPartialAggPipeline(
	ctx context.Context,
	prog *spl2.Program,
	hints *spl2.QueryHints,
	aggSpec *enginepipeline.PartialAggSpec,
	onProgress func(*SearchProgress),
	scanStart time.Time,
) (*queryPipelineResult, error) {
	qr := &queryPipelineResult{}
	partials, pss := e.buildPartialAggStore(ctx, hints, aggSpec, onProgress)
	qr.ss = pss

	// Check for topKAgg annotation for heap-based merge.
	var mergedRows []map[string]event.Value
	if prog.Main != nil {
		if ann, ok := prog.Main.GetAnnotation("topKAgg"); ok {
			if topK, ok := ann.(*optimizer.TopKAggAnnotation); ok {
				qr.topKUsed = true
				sortFields := make([]enginepipeline.SortField, len(topK.SortFields))
				for i, sf := range topK.SortFields {
					sortFields[i] = enginepipeline.SortField{Name: sf.Name, Desc: sf.Desc}
				}
				mergedRows = enginepipeline.MergePartialAggsTopK(partials, aggSpec, topK.K, sortFields)
			}
		}
	}
	if mergedRows == nil {
		mergedRows = enginepipeline.MergePartialAggs(partials, aggSpec)
	}

	qr.scanMS = float64(time.Since(scanStart).Milliseconds())
	pipelineStart := time.Now()
	onProgress(&SearchProgress{Phase: PhaseExecutingPipeline})

	if len(prog.Main.Commands) > 1 {
		events := mergedRowsToEvents(mergedRows)
		pipeStore := &enginepipeline.ServerIndexStore{
			Events: map[string][]*event.Event{"_partial": events},
		}
		subQuery := &spl2.Query{
			Source:   &spl2.SourceClause{Index: "_partial"},
			Commands: prog.Main.Commands[1:],
		}
		iter, pipeErr := enginepipeline.BuildPipelineWithStats(ctx, subQuery, pipeStore, 0)
		if pipeErr != nil {
			return nil, pipeErr
		}
		pipeRows, collectErr := enginepipeline.CollectAll(ctx, iter)
		if collectErr != nil {
			return nil, collectErr
		}
		stages := enginepipeline.CollectStageStats(iter)
		qr.pipelineStages = convertStageStats(stages)
		qr.warnings = enginepipeline.CollectWarnings(iter)
		qr.rows = pipelineRowsToResultRows(pipeRows)
	} else {
		qr.rows = pipelineRowsToResultRows(mergedRows)
	}
	qr.pipelineMS = float64(time.Since(pipelineStart).Milliseconds())

	return qr, nil
}

// runTransformPartialAggPipeline handles the transform+stats partial agg path.
// Per-segment workers run the full transform mini-pipeline (rex, eval, etc.)
// followed by partial aggregation. Results are merged exactly like the
// standard partial agg path.
func (e *Engine) runTransformPartialAggPipeline(
	ctx context.Context,
	prog *spl2.Program,
	hints *spl2.QueryHints,
	tSpec *optimizer.TransformPartialAggAnnotation,
	onProgress func(*SearchProgress),
	scanStart time.Time,
) (*queryPipelineResult, error) {
	qr := &queryPipelineResult{}
	partials, pss := e.buildTransformPartialAggStore(ctx, hints, tSpec, onProgress)
	qr.ss = pss

	aggSpec := tSpec.AggSpec

	// Check for topKAgg annotation for heap-based merge.
	var mergedRows []map[string]event.Value
	if prog.Main != nil {
		if ann, ok := prog.Main.GetAnnotation("topKAgg"); ok {
			if topK, ok := ann.(*optimizer.TopKAggAnnotation); ok {
				qr.topKUsed = true
				sortFields := make([]enginepipeline.SortField, len(topK.SortFields))
				for i, sf := range topK.SortFields {
					sortFields[i] = enginepipeline.SortField{Name: sf.Name, Desc: sf.Desc}
				}
				mergedRows = enginepipeline.MergePartialAggsTopK(partials, aggSpec, topK.K, sortFields)
			}
		}
	}
	if mergedRows == nil {
		mergedRows = enginepipeline.MergePartialAggs(partials, aggSpec)
	}

	qr.scanMS = float64(time.Since(scanStart).Milliseconds())
	pipelineStart := time.Now()
	onProgress(&SearchProgress{Phase: PhaseExecutingPipeline})

	// Apply post-stats commands (sort, head, etc.) if present.
	if len(tSpec.PostStatsCommands) > 0 {
		events := mergedRowsToEvents(mergedRows)
		pipeStore := &enginepipeline.ServerIndexStore{
			Events: map[string][]*event.Event{"_partial": events},
		}
		subQuery := &spl2.Query{
			Source:   &spl2.SourceClause{Index: "_partial"},
			Commands: tSpec.PostStatsCommands,
		}
		iter, pipeErr := enginepipeline.BuildPipelineWithStats(ctx, subQuery, pipeStore, 0)
		if pipeErr != nil {
			return nil, pipeErr
		}
		pipeRows, collectErr := enginepipeline.CollectAll(ctx, iter)
		if collectErr != nil {
			return nil, collectErr
		}
		stages := enginepipeline.CollectStageStats(iter)
		qr.pipelineStages = convertStageStats(stages)
		qr.warnings = enginepipeline.CollectWarnings(iter)
		qr.rows = pipelineRowsToResultRows(pipeRows)
	} else {
		qr.rows = pipelineRowsToResultRows(mergedRows)
	}
	qr.pipelineMS = float64(time.Since(pipelineStart).Milliseconds())

	return qr, nil
}

// runStandardPipeline handles the standard query pipeline path.
//
// All server-mode queries use the streaming path (runStreamingPipeline) which
// reads segment row groups on-demand — memory footprint is O(one row group ≈
// 65K events) instead of O(all matching events). Non-streamable operators
// (sort, tail, join, dedup, eventstats) have spill-to-disk support that
// activates when the memory budget is exceeded during pipeline execution.
//
// The batch materialization path (buildColumnarStore) is only used when an
// external IndexStore is injected for testing.
func (e *Engine) runStandardPipeline(
	ctx context.Context,
	prog *spl2.Program,
	hints *spl2.QueryHints,
	params QueryParams,
	onProgress func(*SearchProgress),
	onPreview func(totalRows int, rows []map[string]event.Value),
	scanStart time.Time,
) (*queryPipelineResult, error) {
	// Resolve glob patterns against the source registry before segment filtering.
	var scopeWarnings []string
	hints, scopeWarnings = e.resolveSourceScope(hints)

	// Always use the streaming scan path for server-mode queries. The streaming
	// path reads segment row groups on-demand via SegmentStreamIterator, bounding
	// scan memory to one row group (~65K events). Non-streamable operators (sort,
	// tail, join, dedup, eventstats) have spill-to-disk support that activates
	// when the memory budget is exceeded — but only if they receive data through
	// the streaming path. The batch materialization path (buildColumnarStore) loads
	// ALL data upfront, which causes OOM before operators get a chance to spill.
	//
	// Fall back to batch materialization only when an external IndexStore is
	// injected (test path), since tests provide pre-built event maps.
	useStreaming := true
	e.mu.RLock()
	hasExternalStore := e.indexStore != nil
	e.mu.RUnlock()
	if hasExternalStore {
		useStreaming = false
	}

	if useStreaming {
		qr, err := e.runStreamingPipeline(ctx, prog, hints, params, onProgress, onPreview, scanStart)
		if err == nil && len(scopeWarnings) > 0 {
			qr.warnings = append(scopeWarnings, qr.warnings...)
		}

		return qr, err
	}

	// Batch materialization path: reads all matching segments upfront.
	// For multi-index queries (APPEND, JOIN, MULTISEARCH), clear the index
	// filter so buildColumnarStore includes segments from all indexes.
	qr := &queryPipelineResult{}
	qr.warnings = append(qr.warnings, scopeWarnings...)
	storeHints := hints
	allIndexes := spl2.CollectAllIndexNames(prog)
	if len(allIndexes) > 1 {
		hintsCopy := *hints
		hintsCopy.IndexName = ""
		storeHints = &hintsCopy
	}
	batchMap, ess, memErr := e.buildColumnarStore(ctx, storeHints, onProgress, params.ProfileLevel == "trace")
	if memErr != nil {
		return nil, memErr
	}
	qr.ss = ess
	for name, batches := range batchMap {
		for _, b := range batches {
			qr.rowsScanned += int64(b.Len)
		}
		qr.indexesUsed = append(qr.indexesUsed, name)
	}
	sort.Strings(qr.indexesUsed)
	qr.scanMS = float64(time.Since(scanStart).Milliseconds())
	stdPipelineStart := time.Now()

	if onProgress != nil {
		onProgress(&SearchProgress{Phase: PhaseExecutingPipeline})
	}

	pipeStore := &enginepipeline.ColumnarBatchStore{Batches: batchMap}
	buildResult, pipeErr := e.buildProgramPipeline(ctx, prog, pipeStore, params.ProfileLevel)
	if pipeErr != nil {
		return nil, pipeErr
	}
	iter := buildResult.Iterator
	pipeRows, collectErr := enginepipeline.CollectAll(ctx, iter)
	if collectErr != nil {
		return nil, collectErr
	}
	qr.vectorizedFilterUsed = enginepipeline.CheckVectorizedFilter(iter)
	stages := enginepipeline.CollectStageStats(iter)
	qr.pipelineStages = convertStageStats(stages)
	qr.warnings = enginepipeline.CollectWarnings(iter)
	qr.matchedRows = extractMatchedRowsFromStages(stages)
	for _, stage := range stages {
		qr.vmCalls += stage.VMCalls
		qr.vmTimeNS += stage.VMTimeNS
	}
	qr.operatorBudgets = enginepipeline.CollectOperatorBudgets(buildResult.Coordinator)
	qr.rows = pipelineRowsToResultRows(pipeRows)
	qr.pipelineMS = float64(time.Since(stdPipelineStart).Milliseconds())

	return qr, nil
}

// runStreamingPipeline handles the streaming scan path for streamable queries.
//
// Instead of materializing all matching events upfront (buildColumnarStore),
// this path creates a SegmentStreamIterator that reads segment row groups
// on-demand as the pipeline pulls data. Memory footprint is bounded to
// O(one row group ≈ 65K events ≈ 13MB) plus pipeline working set.
//
// In streaming mode, the scan and pipeline execution phases are interleaved:
// there is no separate "scan" phase — the pipeline pulls batches which triggers
// lazy row group reads from segment files.
func (e *Engine) runStreamingPipeline(
	ctx context.Context,
	prog *spl2.Program,
	hints *spl2.QueryHints,
	params QueryParams,
	onProgress func(*SearchProgress),
	onPreview func(totalRows int, rows []map[string]event.Value),
	scanStart time.Time,
) (*queryPipelineResult, error) {
	qr := &queryPipelineResult{}

	// Warn when a single source doesn't exist in the registry, with fuzzy matching.
	qr.warnings = append(qr.warnings, e.checkSourceWarnings(hints)...)

	// Flush buffered events so they are visible to the query scan.
	if e.batcher != nil && e.batcher.BufferedEvents() > 0 {
		if err := e.batcher.Flush(); err != nil {
			e.logger.Warn("pre-query batcher flush failed", "error", err)
		}
	}

	// Pin the current epoch to prevent retired segments from being munmap'd
	// while the streaming pipeline reads from them. The pipeline reads
	// lazily during CollectAll; unpin after all reads complete.
	ep := e.pinEpoch()
	defer ep.unpin()
	segs := make([]*segmentHandle, len(ep.segments))
	copy(segs, ep.segments)

	// Detect multi-index query (APPEND, JOIN, MULTISEARCH with different indexes).
	// For multi-index queries, we don't pre-filter by index — each
	// SegmentStreamIterator filters by its own IndexName at scan time.
	allIndexes := spl2.CollectAllIndexNames(prog)
	isMultiIndex := len(allIndexes) > 1

	segFilterHints := hints
	if isMultiIndex {
		hintsCopy := *hints
		hintsCopy.IndexName = ""
		// Clear source scope so buildSegmentSources doesn't skip segments
		// belonging to sub-query indexes (e.g., MULTISEARCH [FROM backend ...]).
		// Each SegmentStreamIterator applies its own per-call IndexName filter.
		hintsCopy.SourceScopeType = ""
		hintsCopy.SourceScopeSources = nil
		hintsCopy.SourceScopePattern = ""
		segFilterHints = &hintsCopy
	}

	// No separate buffer to scan — all data is in segments (parts).
	var memEvents []*event.Event

	if onProgress != nil {
		onProgress(&SearchProgress{
			Phase:          PhaseFilteringSegments,
			BufferedEvents: 0,
		})
	}

	// Sort segments by MaxTime descending (newest first).
	sort.Slice(segs, func(i, j int) bool {
		return segs[i].meta.MaxTime.After(segs[j].meta.MaxTime)
	})

	// Build segment sources with pre-filtering and skip stats.
	// shouldSkipSegment populates ss with segment-level skip counters.
	// For multi-index queries, segFilterHints has IndexName="" so no
	// segments are skipped by index — each iterator filters its own.
	var ss storeStats
	ss.SegmentsTotal = len(segs)
	ss.BufferedEvents = len(memEvents)
	sources := e.buildSegmentSources(ctx, segs, segFilterHints, &ss)

	// Propagate segment skip counts to global pruning metrics.
	e.recordPruningMetrics(&ss)

	// Build streaming hints from query hints.
	// For multi-index queries, clear IndexName and SourceScope* — each
	// GetEventIterator call sets its own IndexName for per-index filtering.
	// If we leave SourceScopeType="single" with the main query's source,
	// sub-query iterators (e.g., FROM backend inside MULTISEARCH) would be
	// rejected by matchesStreamSourceScope before IndexName is checked.
	streamHints := buildStreamHints(segFilterHints, e.queryCfg.Load().BitmapSelectivityThreshold)
	if isMultiIndex {
		streamHints.IndexName = ""
		streamHints.SourceScopeType = ""
		streamHints.SourceScopeSources = nil
		streamHints.SourceScopePattern = ""
		streamHints.SourceIndices = nil
		streamHints.SourceGlob = ""
	}

	store := &StreamingServerStore{
		segments:     sources,
		allMemEvents: memEvents,
		baseHints:    streamHints,
		batchSize:    0, // use default (DefaultBatchSize = 1024)
		gov:          e.governor,
	}

	// Wire progress reporting through the aggregator which sums across all
	// iterators. This fixes the APPEND/MULTISEARCH flickering bug (Bug 1)
	// and incorrect skip breakdown (Bug 4/5).
	if onProgress != nil {
		store.aggregator = &progressAggregator{
			baseSS:    ss,
			memEvents: len(memEvents),
			startTime: scanStart,
			onReport:  onProgress,
		}
	}

	// In streaming mode, scan and pipeline execution are interleaved.
	// The pipeline pulls batches, which triggers lazy row group reads.
	if onProgress != nil {
		onProgress(&SearchProgress{Phase: PhaseExecutingPipeline})
	}
	pipelineStart := time.Now()

	streamBuildResult, pipeErr := e.buildProgramPipeline(ctx, prog, store, params.ProfileLevel)
	if pipeErr != nil {
		return nil, pipeErr
	}
	streamIter := streamBuildResult.Iterator

	// Wire preview callback into CollectAll if provided.
	var collectOpts []enginepipeline.CollectOptions
	if onPreview != nil {
		collectOpts = append(collectOpts, enginepipeline.CollectOptions{
			OnBatch: func(total int, rows []map[string]event.Value) {
				onPreview(total, rows)
			},
		})
	}
	pipeRows, collectErr := enginepipeline.CollectAll(ctx, streamIter, collectOpts...)
	if collectErr != nil {
		return nil, collectErr
	}

	// Harvest scan statistics from ALL iterators (APPEND creates multiple).
	if aggSt := store.AggregatedStats(); aggSt != nil {
		ss.SegmentsScanned += aggSt.SegmentsScanned
		ss.InvertedIndexHits += int(aggSt.BitmapHits)
		ss.TotalBytesRead += aggSt.BytesRead
		ss.BloomsChecked += aggSt.RGBloomsChecked
		qr.rowsScanned = aggSt.EventsScanned + int64(len(memEvents))
	}

	// Populate indexes used.
	if isMultiIndex {
		qr.indexesUsed = allIndexes
	} else if hints.IndexName != "" {
		qr.indexesUsed = []string{hints.IndexName}
	} else {
		qr.indexesUsed = []string{DefaultIndexName}
	}

	qr.ss = ss
	qr.vectorizedFilterUsed = enginepipeline.CheckVectorizedFilter(streamIter)
	stages := enginepipeline.CollectStageStats(streamIter)
	qr.pipelineStages = convertStageStats(stages)
	qr.warnings = enginepipeline.CollectWarnings(streamIter)
	qr.matchedRows = extractMatchedRowsFromStages(stages)
	for _, stage := range stages {
		qr.vmCalls += stage.VMCalls
		qr.vmTimeNS += stage.VMTimeNS
	}
	qr.operatorBudgets = enginepipeline.CollectOperatorBudgets(streamBuildResult.Coordinator)
	qr.govBudget = streamBuildResult.GovBudget
	qr.rows = pipelineRowsToResultRows(pipeRows)
	qr.pipelineMS = float64(time.Since(pipelineStart).Milliseconds())

	return qr, nil
}

// parallelConfig constructs a ParallelConfig from the server's query config.
// Branch-level parallelism is always enabled in server mode; the config knob
// controls the goroutine limit. Returns nil when parallelism is disabled
// (currently never, but the nil check in the pipeline is defensive).
func (e *Engine) parallelConfig() *enginepipeline.ParallelConfig {
	return &enginepipeline.ParallelConfig{
		MaxBranchParallelism: e.queryCfg.Load().MaxBranchParallelism,
		Enabled:              true,
	}
}

// buildProgramPipeline builds the query pipeline using the governor for memory
// accounting. Operator accounts are backed by the governor for process-wide
// enforcement with class-based accounting and pressure callbacks.
func (e *Engine) buildProgramPipeline(
	ctx context.Context,
	prog *spl2.Program,
	store enginepipeline.IndexStore,
	profileLevel string,
) (*enginepipeline.BuildResult, error) {
	parallelCfg := e.parallelConfig()
	sysOpt := enginepipeline.WithSystemTables(&systemTableResolver{engine: e})
	qc := e.queryCfg.Load()

	return enginepipeline.BuildProgramWithGovernor(
		ctx, prog, store, e, e, 0,
		profileLevel, e.governor, int64(qc.MaxQueryMemory),
		e.spillMgr, qc.DedupExact,
		parallelCfg,
		sysOpt,
	)
}

// countStarFromMetadata counts events using segment metadata and buffered event count,
// avoiding reading any column data. Used when the optimizer detects countStarOnly.
func (e *Engine) countStarFromMetadata(hints *spl2.QueryHints) int64 {
	// Flush buffered events so they are visible to the metadata count.
	if e.batcher != nil && e.batcher.BufferedEvents() > 0 {
		if err := e.batcher.Flush(); err != nil {
			e.logger.Warn("pre-query batcher flush failed", "error", err)
		}
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	var total int64

	// Sum segment event counts from metadata.
	var ss storeStats
	for _, seg := range e.currentEpoch.Load().segments {
		if shouldSkipSegment(seg, hints, &ss) {
			continue
		}
		total += seg.meta.EventCount
	}

	return total
}

// findStatsCommand returns the first StatsCommand in the query pipeline, or nil.
func findStatsCommand(q *spl2.Query) *spl2.StatsCommand {
	for _, cmd := range q.Commands {
		if stats, ok := cmd.(*spl2.StatsCommand); ok {
			return stats
		}
	}

	return nil
}

// mergedRowsToEvents converts merged aggregation result rows to events
// for feeding into a sub-pipeline (e.g., sort, head after stats).
func mergedRowsToEvents(rows []map[string]event.Value) []*event.Event {
	events := make([]*event.Event, len(rows))
	for i, row := range rows {
		fields := make(map[string]event.Value, len(row))
		for k, v := range row {
			fields[k] = v
		}
		events[i] = &event.Event{Fields: fields}
	}

	return events
}

// convertStageStats converts pipeline stats.StageStats to server PipelineStage.
// It also computes exclusive (self) timing for each stage using the Volcano
// model property: exclusive = inclusive - child inclusive.
func convertStageStats(stages []stats.StageStats) []PipelineStage {
	if len(stages) == 0 {
		return nil
	}

	// Compute exclusive timing on the stats slice (mutates ExclusiveNS in place).
	stats.ComputeExclusiveTiming(stages)

	result := make([]PipelineStage, len(stages))
	for i, s := range stages {
		result[i] = PipelineStage{
			Name:        s.Name,
			InputRows:   s.InputRows,
			OutputRows:  s.OutputRows,
			DurationMS:  float64(s.Duration.Microseconds()) / 1000,
			ExclusiveMS: float64(s.ExclusiveNS) / 1e6,
			MemoryBytes: s.MemoryBytes,
			SpilledRows: s.SpilledRows,
			SpillBytes:  s.SpillBytes,
		}
	}

	return result
}

// estimateMVSpeedup returns a human-readable speedup estimate (e.g., "~400x")
// by comparing the original scan row count to the MV row count. Returns "" if
// no meaningful estimate can be computed (e.g., no MV rows or no original scan).
func estimateMVSpeedup(originalRows, mvRows int64) string {
	if originalRows <= 0 || mvRows <= 0 {
		return ""
	}
	ratio := float64(originalRows) / float64(mvRows)
	if ratio < 2 {
		return ""
	}

	return fmt.Sprintf("~%.0fx", ratio)
}

// applyBudgetAndCPUDelta populates SearchStats from a governor BudgetAdapter
// and CPU snapshots. PeakMemoryBytes comes from the BudgetAdapter.
func applyBudgetAndCPUDelta(ss *SearchStats, govBudget *memgov.BudgetAdapter, cpuBefore, cpuAfter stats.CPUSnapshot) {
	if govBudget != nil {
		ss.PeakMemoryBytes = govBudget.MaxAllocated()
		ss.MemAllocBytes = govBudget.MaxAllocated()
	}

	// CPU delta: compute user/sys time consumed during this query.
	var tmp stats.QueryStats
	stats.ApplyCPUStats(&tmp, cpuBefore, cpuAfter)
	ss.CPUUserMS = float64(tmp.CPUTimeUser.Microseconds()) / 1000
	ss.CPUSysMS = float64(tmp.CPUTimeSys.Microseconds()) / 1000
}

// computeSearchSelectivity derives search selectivity and an actionable
// suggestion from the query execution statistics. Selectivity is the fraction
// of scanned rows that passed the search filter (MatchedRows / RowsScanned).
// A value of 1.0 means every row matched (low selectivity); 0.01 means only
// 1% matched (high selectivity). When selectivity is poor (>90% match rate)
// on a large scan (>10K rows), a suggestion is generated to help the user
// narrow the query.
func computeSearchSelectivity(ss *SearchStats) {
	if ss.RowsScanned <= 0 {
		return
	}

	matched := ss.MatchedRows
	if matched <= 0 {
		// No filter stage was present — all scanned rows were returned.
		matched = ss.RowsScanned
	}

	ss.SearchSelectivity = float64(matched) / float64(ss.RowsScanned)

	// Generate suggestion for low-selectivity queries on large datasets.
	const (
		lowSelectivityThreshold = 0.9
		minRowsForSuggestion    = 10_000
	)

	if ss.SearchSelectivity >= lowSelectivityThreshold && ss.RowsScanned >= minRowsForSuggestion {
		ss.Suggestion = "Query scanned most rows without filtering. Add a WHERE clause, narrow the time range, or use a more specific search term to improve performance."
	}
}

// applySpillAndPoolStats populates spill file statistics on SearchStats
// from the pipeline stage breakdown. Spill stats are aggregated across all
// pipeline stages that spilled data.
func applySpillAndPoolStats(ss *SearchStats, stages []PipelineStage) {
	var spillingOperators []string
	for _, stage := range stages {
		if stage.SpillBytes > 0 {
			ss.SpilledToDisk = true
			ss.SpillBytes += stage.SpillBytes
			ss.SpillFiles++ // one spill file per spilling stage (approximation)
			spillingOperators = append(spillingOperators, stage.Name)
		}
	}

	// Generate a human-readable performance note when spill occurs.
	if ss.SpilledToDisk {
		ss.SpillNote = generateSpillNote(spillingOperators)
	}
}

// generateSpillNote produces a context-aware performance recommendation based
// on which pipeline operators spilled to disk during query execution.
func generateSpillNote(operators []string) string {
	if len(operators) == 0 {
		return ""
	}

	// Map operator names to actionable advice.
	for _, op := range operators {
		switch op {
		case "Sort":
			return "Query spilled to disk due to unbounded sort. Consider adding a LIMIT, narrowing the time range, or increasing max_query_memory_bytes."
		case "Aggregate":
			return "Query spilled to disk due to high cardinality GROUP BY. Consider adding filters to reduce cardinality or increase max_query_memory_bytes."
		case "Dedup":
			return "Query spilled to disk due to high cardinality dedup field. Consider narrowing the time range, adding filters, or increasing max_query_memory_bytes."
		case "EventStats":
			return "Query spilled to disk due to large row set in eventstats. Consider narrowing the time range or increasing max_query_memory_bytes."
		}
	}

	// Generic fallback for unknown operator names.
	return "Query spilled to disk due to memory pressure. Consider adding filters or increasing max_query_memory_bytes."
}

// computeQueryFunnel derives the row funnel from pipeline stage breakdown.
// RowsInRange = Scan stage output (total rows entering the pipeline).
// RowsAfterDedup = Dedup stage output (rows surviving deduplication).
func computeQueryFunnel(ss *SearchStats, stages []PipelineStage) {
	for _, stage := range stages {
		switch stage.Name {
		case "Scan":
			ss.RowsInRange = stage.OutputRows
		case "Dedup":
			ss.RowsAfterDedup = stage.OutputRows
		}
	}
	// Fallback: if no Scan stage found, use RowsScanned.
	if ss.RowsInRange == 0 {
		ss.RowsInRange = ss.RowsScanned
	}
}

// classifyQueryError maps a pipeline error to a machine-readable error code.
// Returns "" for errors that don't have a specific code (generic failures).
func classifyQueryError(err error) string {
	if memgov.IsMemoryExhausted(err) {
		return "QUERY_MEMORY_EXCEEDED"
	}

	return ""
}

// classifyErrorType maps a pipeline error to a Prometheus error type label.
// Returns one of: "timeout", "memory", "execution". Used for the
// lynxdb_query_errors_total counter's "type" label.
func classifyErrorType(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if memgov.IsMemoryExhausted(err) {
		return "memory"
	}

	return "execution"
}

// extractMatchedRowsFromStages derives MatchedRows from the first Filter/Search
// stage output rows. Returns 0 if no filter stage is found.
func extractMatchedRowsFromStages(stages []stats.StageStats) int64 {
	for _, stage := range stages {
		if stage.Name == "Filter" || stage.Name == "Search" {
			return stage.OutputRows
		}
	}

	return 0
}

// hasTransformPartialAgg checks whether the program has a transformPartialAgg annotation.
func hasTransformPartialAgg(prog *spl2.Program) bool {
	if prog.Main == nil {
		return false
	}

	_, ok := prog.Main.GetAnnotation("transformPartialAgg")

	return ok
}
