package usecases

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lynxbase/lynxdb/pkg/config"
	enginepipeline "github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/optimizer"
	"github.com/lynxbase/lynxdb/pkg/planner"
	"github.com/lynxbase/lynxdb/pkg/server"
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

// QueryService orchestrates query planning and execution.
type QueryService struct {
	planner  planner.Planner
	engine   QueryEngine
	queryCfg config.QueryConfig
}

// NewQueryService creates a QueryService.
func NewQueryService(p planner.Planner, engine QueryEngine, cfg config.QueryConfig) *QueryService {
	return &QueryService{
		planner:  p,
		engine:   engine,
		queryCfg: cfg,
	}
}

// Explain parses and analyses a query without executing it.
func (s *QueryService) Explain(_ context.Context, req ExplainRequest) (*ExplainResult, error) {
	plan, err := s.planner.Plan(planner.PlanRequest{
		Query: req.Query,
		From:  req.From,
		To:    req.To,
	})
	if err != nil {
		var pe *planner.ParseError
		if errors.As(err, &pe) {
			return &ExplainResult{
				IsValid: false,
				Errors: []ExplainError{{
					Message:    pe.Message,
					Suggestion: pe.Suggestion,
				}},
			}, nil
		}

		return nil, err
	}

	stages := make([]PipelineStage, 0)
	if plan.Program.Main != nil {
		for _, cmd := range plan.Program.Main.Commands {
			stages = append(stages, PipelineStage{
				Command: commandName(cmd),
			})
		}
	}

	// Account for external time bounds when evaluating cost.
	hasTimeBounds := plan.Hints.TimeBounds != nil || plan.ExternalTimeBounds != nil
	cost := "low"
	if !hasTimeBounds && len(plan.Hints.SearchTerms) == 0 {
		cost = "high"
	} else if !hasTimeBounds || len(plan.Hints.SearchTerms) == 0 {
		cost = "medium"
	}

	usesFullScan := !hasTimeBounds && len(plan.Hints.SearchTerms) == 0

	// Build physical plan from optimizer annotations on the AST.
	physPlan := extractPhysicalPlan(plan.Program)

	// Convert optimizer rule details for the explain response.
	var ruleDetails []ExplainRuleDetail
	for _, rd := range plan.RuleDetails {
		ruleDetails = append(ruleDetails, ExplainRuleDetail{
			Name:        rd.Name,
			Description: rd.Description,
			Count:       rd.Count,
		})
	}

	// Build source scope from hints.
	var sourceScope *ExplainSourceScope
	if plan.Hints.SourceScopeType != "" {
		var totalAvailable int
		if s.engine != nil {
			totalAvailable = s.engine.SourceCount()
		}
		sourceScope = &ExplainSourceScope{
			Type:                  plan.Hints.SourceScopeType,
			Sources:               plan.Hints.SourceScopeSources,
			Pattern:               plan.Hints.SourceScopePattern,
			TotalSourcesAvailable: totalAvailable,
		}
	}

	// Extract optimizer diagnostic messages and warnings from AST annotations.
	var optMessages, optWarnings []string
	if plan.Program.Main != nil {
		if v, ok := plan.Program.Main.GetAnnotation("optimizerMessages"); ok {
			if msgs, ok := v.([]string); ok {
				optMessages = msgs
			}
		}
		if v, ok := plan.Program.Main.GetAnnotation("optimizerWarnings"); ok {
			if msgs, ok := v.([]string); ok {
				optWarnings = msgs
			}
		}
	}

	return &ExplainResult{
		IsValid: true,
		Errors:  nil,
		Parsed: &ExplainParsed{
			Pipeline:          stages,
			ResultType:        string(plan.ResultType),
			EstimatedCost:     cost,
			UsesFullScan:      usesFullScan,
			FieldsRead:        plan.Hints.RequiredCols,
			SearchTerms:       plan.Hints.SearchTerms,
			HasTimeBounds:     hasTimeBounds,
			OptimizerStats:    plan.OptimizerStats,
			PhysicalPlan:      physPlan,
			SourceScope:       sourceScope,
			ParseMS:           float64(plan.ParseDuration.Microseconds()) / 1000,
			OptimizeMS:        float64(plan.OptimizeDuration.Microseconds()) / 1000,
			RuleDetails:       ruleDetails,
			TotalRules:        plan.TotalRules,
			OptimizerMessages: optMessages,
			OptimizerWarnings: optWarnings,
		},
		HasMVAccel: false,
	}, nil
}

// commandName returns a human-readable name for a pipeline command.
func commandName(cmd spl2.Command) string {
	switch cmd.(type) {
	case *spl2.SearchCommand:
		return "search"
	case *spl2.WhereCommand:
		return "where"
	case *spl2.StatsCommand:
		return "stats"
	case *spl2.EvalCommand:
		return "eval"
	case *spl2.HeadCommand:
		return "head"
	case *spl2.TailCommand:
		return "tail"
	case *spl2.SortCommand:
		return "sort"
	case *spl2.FieldsCommand:
		return "fields"
	case *spl2.TableCommand:
		return "table"
	case *spl2.RenameCommand:
		return "rename"
	case *spl2.DedupCommand:
		return "dedup"
	case *spl2.TimechartCommand:
		return "timechart"
	case *spl2.RexCommand:
		return "rex"
	case *spl2.BinCommand:
		return "bin"
	case *spl2.StreamstatsCommand:
		return "streamstats"
	case *spl2.EventstatsCommand:
		return "eventstats"
	case *spl2.JoinCommand:
		return "join"
	case *spl2.AppendCommand:
		return "append"
	case *spl2.MultisearchCommand:
		return "multisearch"
	case *spl2.TransactionCommand:
		return "transaction"
	case *spl2.XYSeriesCommand:
		return "xyseries"
	case *spl2.TopCommand:
		return "top"
	case *spl2.RareCommand:
		return "rare"
	case *spl2.FillnullCommand:
		return "fillnull"
	case *spl2.TopNCommand:
		return "topn"
	case *spl2.MaterializeCommand:
		return "materialize"
	case *spl2.FromCommand:
		return "from"
	case *spl2.ViewsCommand:
		return "views"
	case *spl2.DropviewCommand:
		return "dropview"
	default:
		return fmt.Sprintf("unknown(%T)", cmd)
	}
}

// extractPhysicalPlan inspects optimizer annotations on the AST to build
// a PhysicalPlan that describes the runtime execution strategy. This surfaces
// optimizations that are invisible in the logical pipeline stages (e.g.,
// count(*) metadata shortcut, partial aggregation pushdown, topK heap merge).
func extractPhysicalPlan(prog *spl2.Program) *PhysicalPlan {
	if prog == nil || prog.Main == nil {
		return nil
	}
	q := prog.Main
	pp := &PhysicalPlan{}
	hasAnnotation := false

	if _, ok := q.GetAnnotation("countStarOnly"); ok {
		pp.CountStarOnly = true
		hasAnnotation = true
	}
	if _, ok := q.GetAnnotation("partialAgg"); ok {
		pp.PartialAgg = true
		hasAnnotation = true
	}
	if ann, ok := q.GetAnnotation("topKAgg"); ok {
		pp.TopKAgg = true
		hasAnnotation = true
		if topK, ok := ann.(*optimizer.TopKAggAnnotation); ok {
			pp.TopK = topK.K
		}
	}
	if ann, ok := q.GetAnnotation("joinStrategy"); ok {
		if s, ok := ann.(string); ok {
			pp.JoinStrategy = s
			hasAnnotation = true
		}
	}

	if !hasAnnotation {
		return nil
	}

	return pp
}

// Submit plans and executes a query with sync/hybrid/async dispatch.
func (s *QueryService) Submit(ctx context.Context, req SubmitRequest) (*SubmitResult, error) {
	plan, err := s.planner.Plan(planner.PlanRequest{
		Query: req.Query,
		From:  req.From,
		To:    req.To,
	})
	if err != nil {
		return nil, err
	}

	// Sync/hybrid queries derive from the caller's context so client disconnect
	// cancels the query. Async queries use Background since they outlive the request.
	queryCtx := ctx
	if req.Mode == QueryModeAsync {
		queryCtx = context.Background()
	}

	// Concurrency limit is enforced atomically inside SubmitQuery (CAS loop).
	job, err := s.engine.SubmitQuery(queryCtx, server.QueryParams{
		Query:              plan.RawQuery,
		Program:            plan.Program,
		Hints:              plan.Hints,
		ExternalTimeBounds: plan.ExternalTimeBounds,
		ResultType:         plan.ResultType,
		ProfileLevel:       req.Profile,
		ParseDuration:      plan.ParseDuration,
		OptimizeDuration:   plan.OptimizeDuration,
		RuleDetails:        plan.RuleDetails,
		TotalRules:         plan.TotalRules,
	})
	if err != nil {
		return nil, err
	}

	limit := req.Limit
	if limit <= 0 {
		limit = s.queryCfg.DefaultResultLimit
	}
	if s.queryCfg.MaxResultLimit > 0 && limit > s.queryCfg.MaxResultLimit {
		limit = s.queryCfg.MaxResultLimit
	}

	switch req.Mode {
	case QueryModeSync:
		syncTimeout := s.queryCfg.SyncTimeout
		if syncTimeout == 0 {
			syncTimeout = 30 * time.Second
		}
		timer := time.NewTimer(syncTimeout)
		defer timer.Stop()
		select {
		case <-job.Done():
			return buildSyncResult(job, limit, req.Offset), nil
		case <-timer.C:
			// Promoted to async — detach from HTTP context so job survives disconnect.
			job.Detach()

			return buildJobHandle(job), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}

	case QueryModeHybrid:
		timer := time.NewTimer(req.Wait)
		defer timer.Stop()
		select {
		case <-job.Done():
			return buildSyncResult(job, limit, req.Offset), nil
		case <-timer.C:
			// Promoted to async — detach from HTTP context so job survives disconnect.
			job.Detach()

			return buildJobHandle(job), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}

	case QueryModeAsync:
		return buildJobHandle(job), nil
	}

	return buildJobHandle(job), nil
}

// Stream plans a query and returns a streaming iterator.
func (s *QueryService) Stream(ctx context.Context, req StreamRequest) (enginepipeline.Iterator, server.StreamingStats, error) {
	plan, err := s.planner.Plan(planner.PlanRequest{
		Query: req.Query,
		From:  req.From,
		To:    req.To,
	})
	if err != nil {
		return nil, server.StreamingStats{}, err
	}

	return s.engine.BuildStreamingPipeline(ctx, plan.Program, plan.ExternalTimeBounds)
}

// Histogram computes event count buckets over a time range.
// It uses segment metadata (zone maps) to estimate bucket counts without
// loading all events into memory, then scans memtable events individually.
func (s *QueryService) Histogram(ctx context.Context, req HistogramRequest) (*HistogramResult, error) {
	now := time.Now()
	fromStr := req.From
	if fromStr == "" {
		fromStr = "-1h"
	}
	toStr := req.To
	if toStr == "" {
		toStr = "now"
	}

	fromTime, err := ParseTimeParam(fromStr, now)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidFrom, err)
	}
	toTime, err := ParseTimeParam(toStr, now)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidTo, err)
	}

	bucketCount := req.Buckets
	if bucketCount <= 0 {
		bucketCount = 60
	}

	totalDuration := toTime.Sub(fromTime)
	if totalDuration <= 0 {
		return nil, ErrFromBeforeTo
	}
	intervalNs := totalDuration.Nanoseconds() / int64(bucketCount)
	interval := SnapInterval(time.Duration(intervalNs))

	srvBuckets := make([]server.HistogramBucket, bucketCount)
	for i := 0; i < bucketCount; i++ {
		srvBuckets[i] = server.HistogramBucket{
			Time: fromTime.Add(time.Duration(i) * interval),
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	total, err := s.engine.HistogramFromMetadata(ctx, req.Index, fromTime, toTime, interval, srvBuckets)
	if err != nil {
		return nil, err
	}

	buckets := make([]HistogramBucket, len(srvBuckets))
	for i, b := range srvBuckets {
		buckets[i] = HistogramBucket{Time: b.Time, Count: b.Count}
	}

	return &HistogramResult{
		Interval: interval.String(),
		Buckets:  buckets,
		Total:    total,
	}, nil
}

// FieldValues returns the top values for a given field name.
// Uses streaming scan with context cancellation instead of loading all events.
func (s *QueryService) FieldValues(ctx context.Context, req FieldValuesRequest) (*FieldValuesResult, error) {
	now := time.Now()
	var from, to time.Time
	if req.From != "" {
		if t, err := ParseTimeParam(req.From, now); err == nil {
			from = t
		}
	}
	if req.To != "" {
		if t, err := ParseTimeParam(req.To, now); err == nil {
			to = t
		}
	}

	srvResult, err := s.engine.FieldValuesFromMetadata(ctx, req.FieldName, req.Index, from, to, req.Limit)
	if err != nil {
		return nil, err
	}

	values := make([]FieldValue, len(srvResult.Values))
	for i, v := range srvResult.Values {
		values[i] = FieldValue{
			Value:   v.Value,
			Count:   v.Count,
			Percent: v.Percent,
		}
	}

	return &FieldValuesResult{
		Field:       srvResult.Field,
		Values:      values,
		UniqueCount: srvResult.UniqueCount,
		TotalCount:  srvResult.TotalCount,
	}, nil
}

// ListSources returns all distinct event sources.
// Uses streaming scan with context cancellation instead of loading all events.
func (s *QueryService) ListSources(ctx context.Context) (*SourcesResult, error) {
	srvResult, err := s.engine.ListSourcesFromMetadata(ctx, "", time.Time{}, time.Time{})
	if err != nil {
		return nil, err
	}

	result := make([]SourceInfo, len(srvResult.Sources))
	for i, si := range srvResult.Sources {
		result[i] = SourceInfo{
			Name:       si.Name,
			EventCount: si.EventCount,
			FirstEvent: si.FirstEvent,
			LastEvent:  si.LastEvent,
		}
	}

	return &SourcesResult{Sources: result}, nil
}

func buildSyncResult(job *server.SearchJob, limit, offset int) *SubmitResult {
	snap := job.Snapshot()
	if snap.Status == "error" {
		return &SubmitResult{
			Done:      true,
			Error:     snap.Error,
			ErrorCode: snap.ErrorCode,
			QueryID:   snap.ID,
		}
	}

	return &SubmitResult{
		Done:       true,
		ResultType: snap.ResultType,
		Results:    snap.Results,
		Stats:      snap.Stats,
		QueryID:    snap.ID,
	}
}

func buildJobHandle(job *server.SearchJob) *SubmitResult {
	r := &SubmitResult{
		Done:   false,
		JobID:  job.ID,
		Status: "running",
	}
	if p := job.Progress.Load(); p != nil {
		r.Progress = p
	}

	return r
}
