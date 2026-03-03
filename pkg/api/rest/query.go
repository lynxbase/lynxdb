package rest

import (
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/config"
	"github.com/OrlovEvgeny/Lynxdb/pkg/planner"
	"github.com/OrlovEvgeny/Lynxdb/pkg/server"
	"github.com/OrlovEvgeny/Lynxdb/pkg/spl2"
	"github.com/OrlovEvgeny/Lynxdb/pkg/usecases"
)

// handleQueryGet is the GET variant for simple queries (query params: q, from, to, limit, format).
func (s *Server) handleQueryGet(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	if q == "" {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, "query parameter 'q' is required")

		return
	}
	limit := parseIntParam(r, "limit", 0)
	req := QueryRequest{
		Q:      q,
		From:   r.URL.Query().Get("from"),
		To:     r.URL.Query().Get("to"),
		Limit:  limit,
		Format: r.URL.Query().Get("format"),
	}
	s.executeQuery(w, r, req)
}

// handleQuery is the three-mode query handler (sync/hybrid/async).
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest, "invalid JSON")

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

	mode, wait := mapQueryMode(req.Wait)
	limit := clampLimit(req.Limit, s.queryCfg)

	result, err := s.queryService.Submit(r.Context(), usecases.SubmitRequest{
		Query:   query,
		From:    req.effectiveFrom(),
		To:      req.effectiveTo(),
		Limit:   limit,
		Offset:  req.Offset,
		Mode:    mode,
		Wait:    wait,
		Profile: req.Profile,
	})
	if err != nil {
		handlePlanError(w, err)

		return
	}

	if result.Done {
		if result.Error != "" {
			respondQueryError(w, result.Error, result.ErrorCode)

			return
		}
		writeSyncResultFromUsecase(w, result, limit, req.Offset)
	} else {
		writeJobHandleFromUsecase(w, result)
	}
}

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

// handlePlanError maps domain errors to HTTP responses.
func handlePlanError(w http.ResponseWriter, err error) {
	if planner.IsParseError(err) {
		pe := func() *planner.ParseError {
			target := &planner.ParseError{}
			_ = errors.As(err, &target)

			return target
		}()
		respondError(w, ErrCodeInvalidQuery, http.StatusBadRequest, "parse error: "+pe.Message,
			WithSuggestion(pe.Suggestion))

		return
	}
	if errors.Is(err, usecases.ErrTooManyQueries) {
		respondError(w, ErrCodeTooManyRequests, http.StatusTooManyRequests, err.Error())

		return
	}
	respondInternalError(w, err.Error())
}

// writeSyncResultFromUsecase writes 200 with full results from a SubmitResult.
func writeSyncResultFromUsecase(w http.ResponseWriter, result *usecases.SubmitResult, limit, offset int) {
	var data interface{}
	switch result.ResultType {
	case server.ResultTypeAggregate, server.ResultTypeTimechart:
		data = buildAggregateResponse(result.ResultType, result.Results)
	default:
		data = buildEventsResponse(result.Results, limit, offset)
	}

	respondData(w, http.StatusOK, data,
		WithTookMS(result.Stats.ElapsedMS),
		WithScanned(result.Stats.RowsScanned),
		WithQueryID(result.QueryID),
		WithSegmentsErrored(result.Stats.SegmentsErrored),
		WithSearchStats(searchStatsToMeta(&result.Stats)))
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
		AcceleratedBy:        ss.AcceleratedBy,
		MVStatus:             ss.MVStatus,
		MVSpeedup:            ss.MVSpeedup,
		MVOriginalScan:       ss.MVOriginalScan,
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

	return ms
}

// writeJobHandleFromUsecase writes 202 Accepted with a job handle from a SubmitResult.
func writeJobHandleFromUsecase(w http.ResponseWriter, result *usecases.SubmitResult) {
	data := map[string]interface{}{
		"type":   "job",
		"job_id": result.JobID,
		"status": result.Status,
	}
	if result.Progress != nil {
		data["progress"] = result.Progress
	}
	respondData(w, http.StatusAccepted, data, WithQueryID(result.JobID))
}

func buildEventsResponse(rows []spl2.ResultRow, limit, offset int) map[string]interface{} {
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

func buildAggregateResponse(rt server.ResultType, rows []spl2.ResultRow) map[string]interface{} {
	if len(rows) == 0 {
		return map[string]interface{}{
			"type": string(rt), "columns": []string{}, "rows": [][]interface{}{}, "total_rows": 0,
		}
	}
	// Collect unique column names from all rows.
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
		"type": string(rt), "columns": cols, "rows": tableRows, "total_rows": len(rows),
	}
}

// builtinFieldOrder defines the canonical display order for LynxDB internal
// fields. Matches internal/output.builtinFieldOrder for consistency between
// REST API and CLI output.
var builtinFieldOrder = [...]string{
	"_time", "_raw", "index", "source", "_source", "sourcetype", "_sourcetype", "host",
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
