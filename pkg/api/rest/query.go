package rest

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/api/apicontracts"
	"github.com/lynxbase/lynxdb/pkg/auth"
	"github.com/lynxbase/lynxdb/pkg/config"
	"github.com/lynxbase/lynxdb/pkg/logical/physical"
	"github.com/lynxbase/lynxdb/pkg/model"
	"github.com/lynxbase/lynxdb/pkg/planner"
	"github.com/lynxbase/lynxdb/pkg/server"
	"github.com/lynxbase/lynxdb/pkg/usecases"
)

func validateQueryFormat(format string) error {
	if format == "" || format == apicontracts.QueryResponseFormatJSON {
		return nil
	}

	return errors.New(apicontracts.UnsupportedQueryFormatMessage(format))
}

// handleQueryGet is the GET variant for simple queries (query params: q, from, to, limit, format).
func (s *Server) handleQueryGet(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeQuery) {
		return
	}

	q := r.URL.Query().Get("q")
	if q == "" {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, "query parameter 'q' is required")

		return
	}
	if !s.checkQueryLength(w, q) {
		return
	}
	format := r.URL.Query().Get("format")
	if err := validateQueryFormat(format); err != nil {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, err.Error(),
			WithSuggestion(apicontracts.QueryFormatSuggestion))

		return
	}
	limit := parseIntParam(r, "limit", 0)
	req := QueryRequest{
		Q:        q,
		From:     r.URL.Query().Get("from"),
		To:       r.URL.Query().Get("to"),
		Limit:    limit,
		Format:   format,
		Language: r.URL.Query().Get("language"),
	}
	s.executeQuery(w, r, req)
}

// handleQuery is the three-mode query handler (sync/hybrid/async).
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeQuery) {
		return
	}
	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest, "invalid JSON")

		return
	}
	if err := validateQueryFormat(req.Format); err != nil {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, err.Error(),
			WithSuggestion(apicontracts.QueryFormatSuggestion))

		return
	}
	s.executeQuery(w, r, req)
}

// executeQuery is the shared execution logic for POST and GET /query.
func (s *Server) executeQuery(w http.ResponseWriter, r *http.Request, req QueryRequest) {
	query := req.effectiveQuery()
	if query == "" {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, "query is required")

		return
	}
	if !s.checkQueryLength(w, query) {
		return
	}

	if msg := validateExplicitLanguage(req.Language); msg != "" {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, msg,
			WithSuggestion(`set language="lynxflow" or omit it; SPL2 was removed — see https://lynxdb.dev/docs/migration`))
		return
	}

	// Language routing: post-RFC-002, always LynxFlow.
	lang := detectQueryLanguage(query, req.Language)

	s.executeLynxFlowQuery(w, r, req, lang)
}

// executeSPL2Query is removed (RFC-002 Phase 10). All queries now execute
// through the LynxFlow path. Callers requesting language=spl2 receive a 400
// error with migration guidance.

// mapQueryMode converts the HTTP Wait parameter to a QueryMode + duration.
func mapQueryMode(wait *float64) (usecases.QueryMode, time.Duration) {
	if wait == nil {
		return usecases.QueryModeSync, 0
	}
	if *wait == 0 {
		return usecases.QueryModeAsync, 0
	}

	return usecases.QueryModeHybrid, time.Duration(*wait * float64(time.Second))
}

// clampLimit normalises the requested limit to server defaults.
func clampLimit(limit int, cfg config.QueryConfig) int {
	if limit <= 0 {
		limit = cfg.DefaultResultLimit
	}
	if cfg.MaxResultLimit > 0 && limit > cfg.MaxResultLimit {
		limit = cfg.MaxResultLimit
	}

	return limit
}

// checkQueryLength validates that the query string doesn't exceed the configured
// maximum length. Returns true if the query is within limits. On failure, writes
// a 400 error response and returns false.
func (s *Server) checkQueryLength(w http.ResponseWriter, q string) bool {
	maxLen := s.currentQueryConfig().MaxQueryLength
	if maxLen > 0 && len(q) > maxLen {
		respondError(w, ErrCodeQueryTooLarge, http.StatusBadRequest,
			fmt.Sprintf("query length %d exceeds maximum allowed length of %d bytes", len(q), maxLen))
		return false
	}
	return true
}

// handlePlanError maps domain errors to HTTP responses.
func handlePlanError(w http.ResponseWriter, err error) {
	var pe *planner.ParseError
	if errors.As(err, &pe) {
		if pe.Diag != nil {
			respondLynxFlowParseError(w, *pe.Diag)
		} else {
			respondError(w, ErrCodeInvalidQuery, http.StatusBadRequest, "parse error: "+pe.Message,
				WithSuggestion(pe.Suggestion))
		}
		return
	}
	if errors.Is(err, usecases.ErrTooManyQueries) {
		// A query slot frees the instant any in-flight query finishes, so hint a
		// short backoff rather than letting clients hammer the endpoint.
		w.Header().Set("Retry-After", "1")
		respondError(w, ErrCodeTooManyRequests, http.StatusTooManyRequests, err.Error())

		return
	}
	if errors.Is(err, server.ErrInvalidTimeBounds) {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, err.Error())

		return
	}

	// NotYetImplementedError: a logical node has no physical mapping yet.
	// Return 422 UNSUPPORTED_COMMAND so clients get a clear, non-500 signal.
	var nie *physical.NotYetImplementedError
	if errors.As(err, &nie) {
		respondError(w, ErrCodeUnsupportedCommand, http.StatusUnprocessableEntity, nie.Error(),
			WithSuggestion("This feature is not yet available. Check the docs for the current command set."))
		return
	}

	respondInternalError(w, err.Error())
}

// writeSyncResultFromUsecase writes 200 with full results from a SubmitResult.
func writeSyncResultFromUsecase(w http.ResponseWriter, result *usecases.SubmitResult, limit, offset int, query string, queryCfg config.QueryConfig, lintsEnabled bool, lintLimit int, lintFull bool, extraOpts ...MetaOpt) {
	var data interface{}
	switch result.ResultType {
	case server.ResultTypeAggregate, server.ResultTypeTimechart:
		data = buildAggregateResponse(result.ResultType, result.Results, limit, offset)
	case server.ResultTypeGlimpse:
		data = buildGlimpseResponse(result.Results)
	default:
		data = buildEventsResponse(result.Results, limit, offset)
	}

	lints := lintsWithBroadScope(result.Lints, query, &result.Stats, queryCfg, lintsEnabled, lintLimit, lintFull)
	opts := []MetaOpt{
		WithTookMS(result.Stats.ElapsedMS),
		WithScanned(result.Stats.RowsScanned),
		WithQueryID(result.QueryID),
		WithSegmentsErrored(result.Stats.SegmentsErrored),
		WithSearchStats(searchStatsToMeta(&result.Stats)),
		WithWarnings(result.Warnings),
		WithLints(lints),
		WithSuggestions(result.Suggestions),
		WithRewrites(result.Rewrites),
		WithExplain(explainFromSearchStats(&result.Stats, query)),
	}
	opts = append(opts, extraOpts...)
	respondData(w, http.StatusOK, data, opts...)
}

const restDefaultLintLimit = 5

func lintsWithBroadScope(lints []model.QueryLint, query string, stats *server.SearchStats, queryCfg config.QueryConfig, enabled bool, limit int, full bool) []model.QueryLint {
	if !enabled || stats == nil {
		return lints
	}
	extra := broadScopeLints(query, stats, queryCfg)
	if len(extra) == 0 {
		return lints
	}

	combined := append([]model.QueryLint(nil), lints...)
	for _, lint := range extra {
		if !hasLintCode(combined, lint.Code) {
			combined = append(combined, lint)
		}
	}
	combined = model.PrepareQueryLints(combined)
	if full {
		return combined
	}
	if limit <= 0 {
		limit = restDefaultLintLimit
	}
	if len(combined) <= limit {
		return combined
	}

	return append([]model.QueryLint(nil), combined[:limit]...)
}

func broadScopeLints(query string, stats *server.SearchStats, queryCfg config.QueryConfig) []model.QueryLint {
	allSources, hasSearch := broadScopeQueryShape(query)
	if !allSources {
		return nil
	}
	sourceCount := sourceScopeCount(stats)
	segmentCount := stats.SegmentsTotal
	sourceThreshold, segmentThreshold := broadLintThresholds(queryCfg)
	if hasSearch && ((sourceThreshold > 0 && sourceCount >= sourceThreshold) || (segmentThreshold > 0 && segmentCount >= segmentThreshold)) {
		return []model.QueryLint{{
			Code:     model.LintBroadSearch,
			Message:  fmt.Sprintf("Broad search over %d sources; narrow with `FROM`, `source=`, or a time range", sourceCount),
			Position: 0,
		}}
	}
	if sourceThreshold > 0 && sourceCount >= sourceThreshold {
		return []model.QueryLint{{
			Code:     model.LintAllSourcesHigh,
			Message:  "Narrow the source with `FROM <source>` or `source=<name>`",
			Position: 0,
		}}
	}

	return nil
}

func broadLintThresholds(queryCfg config.QueryConfig) (sourceThreshold, segmentThreshold int) {
	defaults := config.DefaultConfig().Query
	sourceThreshold = queryCfg.BroadSourceLintThreshold
	if sourceThreshold == 0 {
		sourceThreshold = defaults.BroadSourceLintThreshold
	}
	segmentThreshold = queryCfg.BroadSegmentLintThreshold
	if segmentThreshold == 0 {
		segmentThreshold = defaults.BroadSegmentLintThreshold
	}

	return sourceThreshold, segmentThreshold
}

func broadScopeQueryShape(query string) (allSources bool, hasSearch bool) {
	// Heuristic string-based detection after spl2 removal (RFC-002 Phase 10).
	// The lynxflow path produces lints via lint.Run; this function only provides
	// a fallback for the broad-scope warning on the response envelope.
	upper := strings.ToUpper(query)
	lower := strings.ToLower(query)
	allSources = strings.Contains(upper, "FROM *")
	hasSearch = queryUsesRegex(query) || strings.Contains(lower, "| search")
	return allSources, hasSearch
}

func sourceScopeCount(stats *server.SearchStats) int {
	if len(stats.SourcesScanned) > 0 {
		return len(stats.SourcesScanned)
	}

	return len(stats.IndexesUsed)
}

func hasLintCode(lints []model.QueryLint, code string) bool {
	for _, lint := range lints {
		if lint.Code == code {
			return true
		}
	}

	return false
}

func explainFromSearchStats(ss *server.SearchStats, query string) *metaExplain {
	if ss == nil {
		return nil
	}

	sources := ss.SourcesScanned
	if len(sources) == 0 {
		sources = ss.IndexesUsed
	}
	skipped := ss.SegmentsSkippedIdx + ss.SegmentsSkippedTime + ss.SegmentsSkippedStat +
		ss.SegmentsSkippedBF + ss.SegmentsSkippedRange
	hasSegments := ss.SegmentsTotal > 0 || ss.SegmentsScanned > 0 || skipped > 0
	if len(sources) == 0 && !hasSegments && ss.RowsScanned == 0 && ss.ProcessedBytes == 0 && ss.ElapsedMS == 0 {
		return nil
	}

	candidates := ss.RowsScanned
	if ss.RowsInRange > 0 {
		candidates = ss.RowsInRange
	}
	literalExtraction := ss.InvertedIndexHits > 0
	explain := &metaExplain{
		CandidateRows:     &candidates,
		LiteralExtraction: &literalExtraction,
		WallClockMS:       ss.ElapsedMS,
		ScannedBytes:      ss.ProcessedBytes,
	}
	if len(sources) > 0 {
		explain.SourceScope = &metaExplainSourceScope{
			Selected: append([]string(nil), sources...),
			Count:    len(sources),
		}
	}
	if hasSegments {
		explain.Segments = &metaExplainSegments{
			Total:        ss.SegmentsTotal,
			Scanned:      ss.SegmentsScanned,
			Skipped:      skipped,
			SkippedIndex: ss.SegmentsSkippedIdx,
			SkippedTime:  ss.SegmentsSkippedTime,
			SkippedStats: ss.SegmentsSkippedStat,
			SkippedBloom: ss.SegmentsSkippedBF,
			SkippedRange: ss.SegmentsSkippedRange,
		}
	}
	if queryUsesRegex(query) {
		explain.RegexEngine = "linear"
	}

	return explain
}

func queryUsesRegex(query string) bool {
	lower := strings.ToLower(query)

	return strings.Contains(lower, "| regex ") ||
		strings.Contains(lower, " regex ") ||
		strings.Contains(query, "=~") ||
		strings.Contains(query, "!~") ||
		strings.Contains(lower, "matches(") ||
		strings.Contains(lower, "extract(")
}

// searchStatsToMeta converts a server.SearchStats to the REST meta stats struct.
func searchStatsToMeta(ss *server.SearchStats) *metaStats {
	if ss == nil {
		return nil
	}

	ms := &metaStats{
		RowsScanned:          ss.RowsScanned,
		RowsReturned:         ss.RowsReturned,
		MatchedRows:          ss.MatchedRows,
		SegmentsTotal:        ss.SegmentsTotal,
		SegmentsScanned:      ss.SegmentsScanned,
		SegmentsSkippedIdx:   ss.SegmentsSkippedIdx,
		SegmentsSkippedTime:  ss.SegmentsSkippedTime,
		SegmentsSkippedStat:  ss.SegmentsSkippedStat,
		SegmentsSkippedBF:    ss.SegmentsSkippedBF,
		SegmentsSkippedRange: ss.SegmentsSkippedRange,
		BufferedEvents:       ss.BufferedEvents,
		InvertedIndexHits:    ss.InvertedIndexHits,
		RangeBSIChecks:       ss.RangeBSIChecks,
		RangeBSISkips:        ss.RangeBSISkips,
		RangeBSIMaskBytes:    ss.RangeBSIMaskBytes,
		IndexesUsed:          ss.IndexesUsed,
		CountStarOptimized:   ss.CountStarOptimized,
		PartialAggUsed:       ss.PartialAggUsed,
		TopKUsed:             ss.TopKUsed,
		PrefetchUsed:         ss.PrefetchUsed,
		VectorizedFilterUsed: ss.VectorizedFilterUsed,
		DictFilterUsed:       ss.DictFilterUsed,
		JoinStrategy:         ss.JoinStrategy,
		ScanMS:               ss.ScanMS,
		PipelineMS:           ss.PipelineMS,
		CacheHit:             ss.CacheHit,
		PeakMemoryBytes:      ss.PeakMemoryBytes,
		MemAllocBytes:        ss.MemAllocBytes,
		SpilledToDisk:        ss.SpilledToDisk,
		SpillBytes:           ss.SpillBytes,
		SpillFiles:           ss.SpillFiles,
		SpillNote:            ss.SpillNote,
		PoolUtilization:      ss.PoolUtilization,
		Warnings:             ss.Warnings,
		CPUUserMS:            ss.CPUUserMS,
		CPUSysMS:             ss.CPUSysMS,
	}
	// Parse/optimize timing and optimizer rule details.
	ms.ParseMS = ss.ParseMS
	ms.OptimizeMS = ss.OptimizeMS
	ms.TotalRules = ss.TotalRules
	for _, r := range ss.OptimizerRules {
		ms.OptimizerRules = append(ms.OptimizerRules, metaOptimizerRule{
			Name:        r.Name,
			Description: r.Description,
			Count:       r.Count,
		})
	}

	// Multi-source query metadata.
	ms.SourcesScanned = ss.SourcesScanned
	ms.SourcesSkipped = ss.SourcesSkipped

	// Query funnel fields.
	ms.RowsInRange = ss.RowsInRange
	ms.RowsAfterDedup = ss.RowsAfterDedup

	// Search selectivity and actionable suggestion.
	ms.SearchSelectivity = ss.SearchSelectivity
	ms.Suggestion = ss.Suggestion

	// Total processed bytes for throughput display.
	ms.ProcessedBytes = ss.ProcessedBytes

	// I/O bytes breakdown.
	ms.DiskBytesRead = ss.DiskBytesRead
	ms.S3BytesRead = ss.S3BytesRead
	ms.CacheBytesRead = ss.CacheBytesRead

	for _, ps := range ss.PipelineStages {
		ms.PipelineStages = append(ms.PipelineStages, metaPipelineStage{
			Name:        ps.Name,
			InputRows:   ps.InputRows,
			OutputRows:  ps.OutputRows,
			DurationMS:  ps.DurationMS,
			ExclusiveMS: ps.ExclusiveMS,
			MemoryBytes: ps.MemoryBytes,
			SpilledRows: ps.SpilledRows,
			SpillBytes:  ps.SpillBytes,
		})
	}

	// Trace-level profiling fields.
	ms.VMCalls = ss.VMCalls
	ms.VMTotalNS = ss.VMTotalNS
	for _, sd := range ss.SegmentDetails {
		ms.SegmentDetails = append(ms.SegmentDetails, metaSegmentDetail{
			SegmentID:       sd.SegmentID,
			Source:          sd.Source,
			Rows:            sd.Rows,
			RowsAfterFilter: sd.RowsAfterFilter,
			BloomHit:        sd.BloomHit,
			InvertedUsed:    sd.InvertedUsed,
			ReadDurationNS:  sd.ReadDurationNS,
			BytesRead:       sd.BytesRead,
		})
	}

	// Distributed query shard metadata (cluster mode only).
	ms.ShardsTotal = ss.ShardsTotal
	ms.ShardsSuccess = ss.ShardsSuccess
	ms.ShardsFailed = ss.ShardsFailed
	ms.ShardsTimedOut = ss.ShardsTimedOut
	ms.ShardsPartial = ss.ShardsPartial

	return ms
}

// writeJobHandleFromUsecase writes 202 Accepted with a job handle from a SubmitResult.
func writeJobHandleFromUsecase(w http.ResponseWriter, result *usecases.SubmitResult, extraOpts ...MetaOpt) {
	data := map[string]interface{}{
		"type":   "job",
		"job_id": result.JobID,
		"status": result.Status,
	}
	if result.Progress != nil {
		data["progress"] = result.Progress
	}
	opts := []MetaOpt{
		WithQueryID(result.JobID),
		WithWarnings(result.Warnings),
		WithLints(result.Lints),
		WithSuggestions(result.Suggestions),
		WithRewrites(result.Rewrites),
	}
	opts = append(opts, extraOpts...)
	respondData(w, http.StatusAccepted, data, opts...)
}

func buildEventsResponse(rows []model.ResultRow, limit, offset int) map[string]interface{} {
	total := len(rows)
	if offset > 0 && offset < len(rows) {
		rows = rows[offset:]
	} else if offset >= len(rows) {
		rows = nil
	}
	hasMore := len(rows) > limit
	if limit > 0 && limit < len(rows) {
		rows = rows[:limit]
	}
	events := make([]map[string]interface{}, len(rows))
	for i, row := range rows {
		events[i] = row.Fields
	}

	return map[string]interface{}{
		"type": "events", "events": events,
		"total": total, "has_more": hasMore,
	}
}

// buildGlimpseResponse extracts the structured glimpse result from __glimpse_result column.
func buildGlimpseResponse(rows []model.ResultRow) map[string]interface{} {
	if len(rows) == 0 {
		return map[string]interface{}{
			"type": "schema", "fields": []interface{}{}, "sampled": 0,
		}
	}

	// Extract the structured JSON from the first row's __glimpse_result field.
	raw, ok := rows[0].Fields["__glimpse_result"]
	if !ok {
		// Fallback: return as events if structured column is missing.
		return buildEventsResponse(rows, 0, 0)
	}

	var result map[string]interface{}
	if err := json.Unmarshal([]byte(fmt.Sprintf("%v", raw)), &result); err != nil {
		return buildEventsResponse(rows, 0, 0)
	}

	result["type"] = "schema"

	return result
}

func buildAggregateResponse(rt server.ResultType, rows []model.ResultRow, limit, offset int) map[string]interface{} {
	if len(rows) == 0 {
		return map[string]interface{}{
			"type": string(rt), "columns": []string{}, "rows": [][]interface{}{}, "total_rows": 0, "has_more": false,
		}
	}

	// Capture total before slicing for pagination.
	totalRows := len(rows)
	if offset > 0 && offset < len(rows) {
		rows = rows[offset:]
	} else if offset >= len(rows) {
		rows = rows[:0]
	}
	hasMore := limit > 0 && len(rows) > limit
	if limit > 0 && limit < len(rows) {
		rows = rows[:limit]
	}

	seen := map[string]struct{}{}
	for _, row := range rows {
		for k := range row.Fields {
			seen[k] = struct{}{}
		}
	}
	// Deterministic column ordering: builtin fields first (canonical order),
	// then user-defined fields alphabetically. Matches CLI output ordering.
	cols := orderColumns(seen)

	tableRows := make([][]interface{}, len(rows))
	for i, row := range rows {
		r := make([]interface{}, len(cols))
		for j, col := range cols {
			r[j] = row.Fields[col]
		}
		tableRows[i] = r
	}

	return map[string]interface{}{
		"type": string(rt), "columns": cols, "rows": tableRows, "total_rows": totalRows, "has_more": hasMore,
	}
}

// builtinFieldOrder defines the canonical display order for LynxDB internal
// fields. Matches internal/output.builtinFieldOrder for consistency between
// REST API and CLI output.
var builtinFieldOrder = [...]string{
	"_time", "_raw", "index", "_source", "_sourcetype", "source", "sourcetype", "host",
}

var builtinFieldRank = func() map[string]int {
	m := make(map[string]int, len(builtinFieldOrder))
	for i, name := range builtinFieldOrder {
		m[name] = i
	}

	return m
}()

// orderColumns produces a deterministic column list: builtin fields in
// canonical order, then user-defined fields alphabetically.
func orderColumns(seen map[string]struct{}) []string {
	builtins := make([]string, 0, len(builtinFieldOrder))
	user := make([]string, 0, len(seen))

	for col := range seen {
		if _, ok := builtinFieldRank[col]; ok {
			builtins = append(builtins, col)
		} else {
			user = append(user, col)
		}
	}

	sort.Slice(builtins, func(i, j int) bool {
		return builtinFieldRank[builtins[i]] < builtinFieldRank[builtins[j]]
	})
	sort.Strings(user)

	return append(builtins, user...)
}
