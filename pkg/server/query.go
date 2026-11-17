package server

import (
	"context"
	"errors"
	"fmt"
	"path"
	"runtime/debug"
	"sort"
	"sync/atomic"
	"time"

	"github.com/lynxbase/lynxdb/pkg/cache"
	"github.com/lynxbase/lynxdb/pkg/engine/physical"
	enginepipeline "github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/memgov"
	"github.com/lynxbase/lynxdb/pkg/model"
	"github.com/lynxbase/lynxdb/pkg/stats"
	"github.com/lynxbase/lynxdb/pkg/storage/segment"
	"github.com/lynxbase/lynxdb/pkg/timerange"
)

// DetectResultType reports whether a plan returns events or a table: it
// looks through row-preserving operators for an aggregation or schema.
func DetectResultType(plan *ir.Plan) ResultType {
	if plan == nil || plan.Root == nil {
		return ResultTypeEvents
	}
	for n := plan.Root; ; {
		switch x := n.(type) {
		case *ir.Aggregate, *ir.SourceSchema, *ir.Transpose:
			return ResultTypeAggregate
		case *ir.Filter, *ir.Project, *ir.RenameColumns, *ir.Compute, *ir.Sort,
			*ir.Limit, *ir.TopN, *ir.Reverse, *ir.Dedup, *ir.FillNull, *ir.Totals:
			n = x.Inputs()[0]
		default:
			return ResultTypeEvents
		}
	}
}

// ParseTimeBounds parses from/to time strings into TimeBounds.
func ParseTimeBounds(from, to string) *model.TimeBounds {
	tb, _ := ParseTimeBoundsStrict(from, to)
	return tb
}

// ErrInvalidTimeBounds is returned when query from/to parameters cannot be parsed.
var ErrInvalidTimeBounds = errors.New("invalid query time bounds")

// ParseTimeBoundsStrict parses from/to time strings into TimeBounds and returns
// explicit errors instead of silently dropping invalid bounds.
func ParseTimeBoundsStrict(from, to string) (*model.TimeBounds, error) {
	tr, err := timerange.ParseOptionalRange(from, to, time.Now())
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidTimeBounds, err)
	}
	if tr == nil {
		return nil, nil
	}

	return &model.TimeBounds{
		Earliest: tr.Earliest,
		Latest:   tr.Latest,
	}, nil
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

	e.jobsWG.Add(1)
	go func() {
		defer e.jobsWG.Done()
		defer jobCancel()
		e.executeQuery(jobCtx, job, params)
	}()

	return job, nil
}

// ActiveJobCount returns the number of currently active jobs.
func (e *Engine) ActiveJobCount() int64 {
	return e.activeJobs.Load()
}

// failJobOnPanic turns a panic in the query goroutine into a failed job so one
// bad query cannot terminate the server process. It must be deferred directly.
func (e *Engine) failJobOnPanic(job *SearchJob) {
	r := recover()
	if r == nil {
		return
	}
	e.logger.Error("panic recovered in query execution",
		"query_id", job.ID,
		"panic", fmt.Sprint(r),
		"stack", string(debug.Stack()),
	)
	job.mu.Lock()
	defer job.mu.Unlock()
	if job.Status != JobStatusRunning {
		return
	}
	job.Error = fmt.Sprintf("internal error: query execution panicked: %v", r)
	job.ErrorCode = "QUERY_INTERNAL_ERROR"
	job.Stats.ErrorType = "execution"
	job.Stats.ResultTypeLabel = string(job.ResultType)
	job.complete(JobStatusError)
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
	defer e.failJobOnPanic(job)

	logger := e.logger.With("query_id", job.ID)
	prog := params.Plan
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
		hints = &model.QueryHints{}
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

	if !params.SkipResultCache {
		if cached, cacheErr := e.cache.Get(ctx, cacheKey); cacheErr != nil {
			logger.Debug("cache get failed", "error", cacheErr)
		} else if cached != nil {
			e.metrics.QueryCacheHits.Add(1)
			rows := cachedResultToResultRows(cached)
			if params.ResultType == ResultTypeEvents {
				canonicalizeEventMetadataFields(rows, true)
			}
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
	}

	var aggSpec *enginepipeline.PartialAggSpec

	var ann queryAnnotations

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
		RangeBSIChecks:       qr.ss.RangeBSIChecks,
		RangeBSISkips:        qr.ss.RangeBSISkips,
		RangeBSIMaskBytes:    qr.ss.RangeBSIMaskBytes,
		PrewhereUsed:         qr.ss.PrewhereUsed,
		PrewhereSteps:        qr.ss.PrewhereSteps,
		PrewhereColumns:      append([]string(nil), qr.ss.PrewhereColumns...),
		PrewhereRowsIn:       qr.ss.PrewhereRowsIn,
		PrewhereRowsOut:      qr.ss.PrewhereRowsOut,
		PrewhereRGSkipped:    qr.ss.PrewhereRGSkipped,
		PrewhereBytesRead:    qr.ss.PrewhereBytesRead,
		PrewhereBytesAvoided: qr.ss.PrewhereBytesAvoided,
		RangePredicates:      append([]model.RangePredicate(nil), qr.rangePredicates...),
		PrefetchUsed:         qr.ss.PrefetchUsed,
		PartialAggUsed:       aggSpec != nil,
		TopKUsed:             qr.topKUsed,
		DictFilterUsed:       qr.ss.DictFilterUsed,
		JoinStrategy:         ann.joinStrategy,
		PipelineStages:       qr.pipelineStages,
		Warnings:             qr.warnings,
		SegmentDetails:       qr.ss.SegmentDetails,
		VMCalls:              qr.vmCalls,
		VMTotalNS:            qr.vmTimeNS,
	}

	// Populate multi-source metadata from resolved hints.
	switch hints.SourceScopeType {
	case model.SourceScopeList:
		if len(hints.SourceScopeSources) > 0 {
			job.Stats.SourcesScanned = hints.SourceScopeSources
		}
	case model.SourceScopeSingle:
		if len(hints.SourceScopeSources) > 0 {
			job.Stats.SourcesScanned = hints.SourceScopeSources
		} else if hints.IndexName != "" {
			job.Stats.SourcesScanned = []string{hints.IndexName}
		}
	case model.SourceScopeAll, "":
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
				Name:        rd.Rule,
				Description: "",
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
	if params.ResultType == ResultTypeEvents {
		canonicalizeEventMetadataFields(qr.rows, queryAllowsDefaultEventMetadata(params.Plan))
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
	if job.Stats.SpilledToDisk {
		operators, spilledRows := spillSummary(job.Stats.PipelineStages)
		triggerOperator := ""
		if len(operators) > 0 {
			triggerOperator = operators[0]
		}
		logger.Warn("query spilled to disk",
			"query", job.Query,
			"trigger_operator", triggerOperator,
			"operators", operators,
			"spill_bytes", job.Stats.SpillBytes,
			"spill_files", job.Stats.SpillFiles,
			"spilled_rows", spilledRows,
			"peak_memory_bytes", job.Stats.PeakMemoryBytes,
			"duration_ms", elapsed.Milliseconds(),
		)
	}

	// Store in cache (non-fatal; log failures for observability).
	if !params.SkipResultCache {
		if err := e.cache.Put(ctx, cacheKey, resultRowsToCachedResult(qr.rows)); err != nil {
			logger.Debug("cache put failed", "error", err)
		}
	}
}

func (e *Engine) bufferedEventsForQuery() []*event.Event {
	if e.batcher == nil {
		return nil
	}
	return e.batcher.SnapshotEvents()
}

func (e *Engine) bufferedEventsForHints(hints *model.QueryHints) []*event.Event {
	events := e.bufferedEventsForQuery()
	if len(events) == 0 || hints == nil {
		return events
	}
	filtered := events[:0]
	for _, ev := range events {
		idx := ev.Index
		if idx == "" {
			idx = DefaultIndexName
		}
		if !matchesSourceScope(idx, hints) {
			continue
		}
		if hints.TimeBounds != nil {
			if !hints.TimeBounds.Earliest.IsZero() && ev.Time.Before(hints.TimeBounds.Earliest) {
				continue
			}
			if !hints.TimeBounds.Latest.IsZero() && ev.Time.After(hints.TimeBounds.Latest) {
				continue
			}
		}
		filtered = append(filtered, ev)
	}
	return filtered
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
	rows            []model.ResultRow
	ss              storeStats
	rowsScanned     int64
	matchedRows     int64
	indexesUsed     []string
	scanMS          float64
	pipelineMS      float64
	topKUsed        bool
	pipelineStages  []PipelineStage
	rangePredicates []model.RangePredicate
	warnings        []string
	vmCalls         int64
	vmTimeNS        int64
	operatorBudgets []stats.OperatorBudgetStats
	govBudget       *memgov.BudgetAdapter // non-nil when governor v2 path was used
}

// runQueryPipeline executes either the distributed or standard pipeline path.
// When a cluster coordinator is configured, queries are dispatched via
// scatter-gather across shards. Otherwise, local execution is used.
func (e *Engine) runQueryPipeline(
	ctx context.Context,
	prog *ir.Plan,
	hints *model.QueryHints,
	params QueryParams,
	aggSpec *enginepipeline.PartialAggSpec,
	ann queryAnnotations,
	onProgress func(*SearchProgress),
	onPreview func(totalRows int, rows []map[string]event.Value),
	scanStart time.Time,
) (*queryPipelineResult, error) {
	if e.clusterCoordinator != nil {
		return e.runDistributedPipeline(ctx, params.Query, prog, hints, onProgress, scanStart)
	}

	return e.runStandardPipeline(ctx, prog, hints, params, onProgress, onPreview, scanStart)
}

// runDistributedPipeline delegates query execution to the cluster coordinator
// for scatter-gather execution across all relevant shards.
func (e *Engine) runDistributedPipeline(
	ctx context.Context,
	query string,
	prog *ir.Plan,
	hints *model.QueryHints,
	onProgress func(*SearchProgress),
	scanStart time.Time,
) (*queryPipelineResult, error) {
	onProgress(&SearchProgress{Phase: PhaseScanningSegments})

	result, err := e.clusterCoordinator.ExecuteQuery(ctx, query, prog, hints)
	if err != nil {
		return nil, fmt.Errorf("runDistributedPipeline: %w", err)
	}

	qr := &queryPipelineResult{
		scanMS:     result.ScanMS,
		pipelineMS: result.MergeMS,
	}
	qr.rows = pipelineRowsToResultRows(result.Rows)

	if result.Meta.Partial {
		qr.warnings = append(qr.warnings, result.Meta.Warnings...)
		qr.warnings = append(qr.warnings, fmt.Sprintf(
			"partial results: %d/%d shards succeeded",
			result.Meta.ShardsSuccess, result.Meta.ShardsTotal))
	}

	return qr, nil
}

func (e *Engine) runStandardPipeline(
	ctx context.Context,
	prog *ir.Plan,
	hints *model.QueryHints,
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
	// scan memory to one row group (~65K events). Blocking operators (sort, tail,
	// join, aggregate, eventstats, transaction, sessionize) are wired with
	// governor-backed memory accounts and a SpillManager via physical.Build: when
	// their coordinated budget is exceeded, they spill to disk automatically.
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
	allIndexes := collectIndexNamesFromPlan(prog)
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

	// Cap event queries at MaxResultLimit in the batch path too.
	if params.ResultType == ResultTypeEvents && !planHasRootLimit(prog) {
		maxRL := e.queryCfg.Load().MaxResultLimit
		if maxRL > 0 {
			iter = enginepipeline.NewLimitIterator(iter, maxRL)
		}
	}

	pipeRows, collectErr := enginepipeline.CollectAll(ctx, iter)
	if collectErr != nil {
		return nil, collectErr
	}
	stages := enginepipeline.CollectStageStats(iter)
	qr.pipelineStages = convertStageStats(stages)
	qr.warnings = enginepipeline.CollectWarnings(iter)
	qr.matchedRows = extractMatchedRowsFromStages(stages)
	for _, stage := range stages {
		qr.vmCalls += stage.VMCalls
		qr.vmTimeNS += stage.VMTimeNS
	}
	qr.operatorBudgets = enginepipeline.CollectOperatorBudgets(buildResult.Coordinator)
	qr.govBudget = buildResult.GovBudget
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
	prog *ir.Plan,
	hints *model.QueryHints,
	params QueryParams,
	onProgress func(*SearchProgress),
	onPreview func(totalRows int, rows []map[string]event.Value),
	scanStart time.Time,
) (*queryPipelineResult, error) {
	qr := &queryPipelineResult{}

	// Warn when a single source doesn't exist in the registry, with fuzzy matching.
	qr.warnings = append(qr.warnings, e.checkSourceWarnings(hints)...)

	memEvents := e.bufferedEventsForQuery()

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
	allIndexes := collectIndexNamesFromPlan(prog)
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

	if onProgress != nil {
		onProgress(&SearchProgress{
			Phase:          PhaseFilteringSegments,
			BufferedEvents: len(memEvents),
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

	// For multi-index queries, clear IndexName and SourceScope* — each
	// GetEventIterator call sets its own IndexName for per-index filtering.
	// If we leave SourceScopeType="single" with the main query's source,
	// sub-query iterators (e.g., FROM backend inside MULTISEARCH) would be
	// rejected by matchesStreamSourceScope before IndexName is checked.
	streamHints := buildStreamHints(segFilterHints, e.queryCfg.Load().BitmapSelectivityThreshold)
	if isMultiIndex {
		streamHints.SourceIndices = nil
		streamHints.SourceGlob = ""
		streamHints.SourceIncludeGlobs = nil
		streamHints.SourceExcludeGlobs = nil
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

	// For non-aggregate event queries that lack an explicit pipeline-level
	// limit, cap the collected output at MaxResultLimit to prevent unbounded
	// memory growth during collection. Aggregate/timechart queries are NOT
	// capped because their row count is determined by group cardinality, not
	// by the input size.
	if params.ResultType == ResultTypeEvents && !planHasRootLimit(prog) {
		maxRL := e.queryCfg.Load().MaxResultLimit
		if maxRL > 0 {
			streamIter = enginepipeline.NewLimitIterator(streamIter, maxRL)
		}
	}

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
		ss.RangeBSIChecks += aggSt.RGRangeBSIChecks
		ss.RangeBSISkips += aggSt.RGRangeBSISkips
		ss.RangeBSIMaskBytes += aggSt.RGRangeBSIMaskBytes
		ss.PrewhereUsed = aggSt.PrewhereUsed
		ss.PrewhereSteps = aggSt.PrewhereSteps
		ss.PrewhereColumns = append([]string(nil), aggSt.PrewhereColumns...)
		ss.PrewhereRowsIn = aggSt.PrewhereRowsIn
		ss.PrewhereRowsOut = aggSt.PrewhereRowsOut
		ss.PrewhereRGSkipped = aggSt.PrewhereRowGroupsSkipped
		ss.PrewhereBytesRead = aggSt.PrewhereBytesRead
		ss.PrewhereBytesAvoided = aggSt.PrewhereBytesAvoided
		qr.rowsScanned = aggSt.EventsScanned + int64(len(memEvents))
		e.metrics.SegmentReads.Add(int64(aggSt.SegmentsScanned))
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
	if hints != nil {
		qr.rangePredicates = append([]model.RangePredicate(nil), hints.RangePredicates...)
	}
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

type segmentSourceSet []*enginepipeline.SegmentSource

func (s segmentSourceSet) Segments() []*segment.Reader {
	readers := make([]*segment.Reader, 0, len(s))
	for _, src := range s {
		if src != nil && src.Reader != nil {
			readers = append(readers, src.Reader)
		}
	}

	return readers
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

// buildProgramPipeline plans an IR plan onto pipeline iterators reading the
// given store. Blocking operators get a per-query memory coordinator from
// the engine governor and the engine's spill manager; both are returned in
// BuildResult so operator budgets surface in query stats.
func (e *Engine) buildProgramPipeline(
	ctx context.Context,
	prog *ir.Plan,
	store enginepipeline.IndexStore,
	_ string,
) (*enginepipeline.BuildResult, error) {
	source := indexStoreToSource(store, DefaultIndexName)

	// Create per-query memory coordinator and budget from the engine governor.
	var coordinator *enginepipeline.MemoryCoordinator
	var govBudget *memgov.BudgetAdapter
	if e.governor != nil {
		queryBudget := memgov.NewQueryBudget(e.governor, "query")
		maxQueryMem := int64(e.queryCfg.Load().MaxQueryMemory)
		govBudget = memgov.NewBudgetAdapterWithLimit(queryBudget, e.governor, maxQueryMem)

		// Budget for the coordinator: use per-query limit if set, otherwise
		// 25% of total governor limit as a reasonable default.
		coordBudget := maxQueryMem
		if coordBudget <= 0 {
			total := e.governor.TotalUsage()
			if total.Limit > 0 {
				coordBudget = total.Limit / 4
			} else {
				coordBudget = 256 << 20 // 256MB fallback
			}
		}
		coordinator = enginepipeline.NewMemoryCoordinator(coordBudget, 0.10)
	}

	iter, err := physical.Build(prog, physical.Options{
		Source:       source,
		Describe:     e.describeSource,
		Now:          time.Now(),
		Context:      ctx,
		Coordinator:  coordinator,
		SpillManager: e.spillMgr,
		GovBudget:    govBudget,
	})
	if err != nil {
		// Close the budget if build fails to avoid leaking the query budget lease.
		if govBudget != nil {
			govBudget.Close()
		}
		return nil, fmt.Errorf("physical.Build: %w", err)
	}

	// Finalize the coordinator's initial sub-limit distribution now that all
	// operators have been registered during Build.
	if coordinator != nil {
		coordinator.Finalize()
	}

	return &enginepipeline.BuildResult{
		Iterator:    iter,
		Coordinator: coordinator,
		GovBudget:   govBudget,
	}, nil
}

// indexStoreToSource adapts an IndexStore to a physical.Source callback.
//
// For StreamingServerStore, this returns a SegmentStreamIterator that lazily
// reads buffered events and flushed segment data on demand.
//
// For ColumnarBatchStore, which holds pre-materialized batches, this converts
// the batches to rows.
func indexStoreToSource(store enginepipeline.IndexStore, defaultIndex string) func(*ir.Scan) (enginepipeline.Iterator, error) {
	switch st := store.(type) {
	case *StreamingServerStore:
		return func(scan *ir.Scan) (enginepipeline.Iterator, error) {
			index, match := scanIndexes(scan, defaultIndex)
			return filterIndexes(st.GetEventIterator(index), match), nil
		}
	case *enginepipeline.ColumnarBatchStore:
		return func(scan *ir.Scan) (enginepipeline.Iterator, error) {
			index, match := scanIndexes(scan, defaultIndex)
			var rows []map[string]event.Value
			for name, batches := range st.Batches {
				if (index != "" && name != index) || (match != nil && !match(name)) {
					continue
				}
				for _, batch := range batches {
					for i := 0; i < batch.Len; i++ {
						rows = append(rows, batch.Row(i))
					}
				}
			}
			return enginepipeline.NewRowScanIterator(rows, enginepipeline.DefaultBatchSize), nil
		}
	}
	return func(scan *ir.Scan) (enginepipeline.Iterator, error) {
		index, match := scanIndexes(scan, defaultIndex)
		if index == "" {
			index = "*"
		}
		events, err := store.MaterializeEvents(context.Background(), index)
		if err != nil {
			return nil, fmt.Errorf("MaterializeEvents(%s): %w", index, err)
		}
		iter := enginepipeline.NewRowScanIterator(eventsToValueRows(events), enginepipeline.DefaultBatchSize)
		return filterIndexes(iter, match), nil
	}
}

// scanIndexes returns the one index a scan reads, or "" with a matcher of
// index names for a scan of several sources or patterns. A nil matcher
// with "" reads every index.
func scanIndexes(scan *ir.Scan, defaultIndex string) (string, func(string) bool) {
	if len(scan.Sources) == 0 {
		return defaultIndex, nil
	}
	if len(scan.Sources) == 1 && !scan.Sources[0].Pattern && scan.Sources[0].Name != "*" {
		return scan.Sources[0].Name, nil
	}
	for _, s := range scan.Sources {
		if s.Name == "*" {
			return "", nil
		}
	}
	sources := scan.Sources
	return "", func(index string) bool {
		for _, s := range sources {
			if !s.Pattern && s.Name == index {
				return true
			}
			if ok, _ := path.Match(s.Name, index); s.Pattern && ok {
				return true
			}
		}
		return false
	}
}

// FilterScanIndexes keeps the rows of it that belong to an index the scan
// reads.
func FilterScanIndexes(scan *ir.Scan, it enginepipeline.Iterator) enginepipeline.Iterator {
	index, match := scanIndexes(scan, DefaultIndexName)
	if index != "" {
		return filterIndexes(it, func(name string) bool { return name == index })
	}
	return filterIndexes(it, match)
}

// filterIndexes keeps the rows whose index column satisfies match; rows
// without one belong to the default index.
func filterIndexes(it enginepipeline.Iterator, match func(string) bool) enginepipeline.Iterator {
	if match == nil {
		return it
	}
	return &indexFilterIterator{Iterator: it, match: match}
}

type indexFilterIterator struct {
	enginepipeline.Iterator
	match func(string) bool
}

func (f *indexFilterIterator) Next(ctx context.Context) (*enginepipeline.Batch, error) {
	for {
		b, err := f.Iterator.Next(ctx)
		if err != nil || b == nil {
			return b, err
		}
		keep := make([]int, 0, b.Len)
		for i := 0; i < b.Len; i++ {
			index := DefaultIndexName
			if v := b.Value("index", i); v.Type() == event.FieldTypeString && v.AsString() != "" {
				index = v.AsString()
			}
			if f.match(index) {
				keep = append(keep, i)
			}
		}
		if len(keep) == b.Len {
			return b, nil
		}
		if len(keep) > 0 {
			return b.PermuteSlice(keep), nil
		}
	}
}

// describeSource lists the columns the field catalog knows, for describe.
func (e *Engine) describeSource(src ir.Source) ([]map[string]event.Value, error) {
	fields := e.ListFields()
	out := make([]map[string]event.Value, len(fields))
	for i, f := range fields {
		out[i] = map[string]event.Value{
			"TABLE_NAME":       event.StringValue(src.Name),
			"COLUMN_NAME":      event.StringValue(f.Name),
			"TYPE_NAME":        event.StringValue(describeTypeName(f.Type)),
			"ORDINAL_POSITION": event.IntValue(int64(i + 1)),
		}
	}
	return out, nil
}

func describeTypeName(catalogType string) string {
	switch catalogType {
	case "int":
		return "bigint"
	case "float":
		return "double"
	case "bool":
		return "boolean"
	case "duration":
		return "interval"
	case "object":
		return "struct"
	}
	return catalogType
}

// eventsToValueRows converts []*event.Event to []map[string]event.Value
// for use with NewRowScanIterator.
func eventsToValueRows(events []*event.Event) []map[string]event.Value {
	rows := make([]map[string]event.Value, len(events))
	for i, ev := range events {
		row := make(map[string]event.Value, len(ev.Fields)+4)
		for k, v := range ev.Fields {
			row[k] = v
		}
		if !ev.Time.IsZero() {
			row["_time"] = event.TimestampValue(ev.Time)
		}
		if ev.Raw != "" {
			row["_raw"] = event.StringValue(ev.Raw)
		}
		if ev.Source != "" {
			row["_source"] = event.StringValue(ev.Source)
		}
		if ev.SourceType != "" {
			row["_sourcetype"] = event.StringValue(ev.SourceType)
		}
		if ev.Index != "" {
			row["index"] = event.StringValue(ev.Index)
		}
		rows[i] = row
	}
	return rows
}

// countStarFromMetadata counts events using segment metadata and buffered event count,
// avoiding reading any column data. Used when the optimizer detects countStarOnly.
func (e *Engine) countStarFromMetadata(hints *model.QueryHints) int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var total int64
	total += int64(len(e.bufferedEventsForHints(hints)))

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
	spillingOperators, _ := spillSummary(stages)
	for _, stage := range stages {
		if stage.SpillBytes <= 0 {
			continue
		}
		ss.SpilledToDisk = true
		ss.SpillBytes += stage.SpillBytes
		ss.SpillFiles++ // one spill file per spilling stage (approximation)
	}

	// Generate a human-readable performance note when spill occurs.
	if ss.SpilledToDisk {
		ss.SpillNote = generateSpillNote(spillingOperators)
	}
}

func spillSummary(stages []PipelineStage) ([]string, int64) {
	var operators []string
	var spilledRows int64
	for _, stage := range stages {
		if stage.SpillBytes <= 0 && stage.SpilledRows <= 0 {
			continue
		}
		operators = append(operators, stage.Name)
		spilledRows += stage.SpilledRows
	}

	return operators, spilledRows
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

// planHasRootLimit reports whether the plan ends in an explicit row limit,
// so the server does not add its own result cap on top.
func planHasRootLimit(plan *ir.Plan) bool {
	if plan == nil || plan.Root == nil {
		return false
	}
	switch plan.Root.(type) {
	case *ir.Limit, *ir.TopN:
		return true
	}
	return false
}

// collectIndexNamesFromPlan lists the distinct indexes the plan's scans name
// literally; patterns and the all-indexes source are left out.
func collectIndexNamesFromPlan(plan *ir.Plan) []string {
	var names []string
	seen := map[string]bool{}
	for _, scan := range physical.Scans(plan) {
		for _, s := range scan.Sources {
			if s.Pattern || s.Name == "*" || seen[s.Name] {
				continue
			}
			seen[s.Name] = true
			names = append(names, s.Name)
		}
	}
	return names
}

type queryAnnotations struct {
	joinStrategy string
}
