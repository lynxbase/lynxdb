package usecases

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/lynxbase/lynxdb/pkg/config"
	enginepipeline "github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/model"
	"github.com/lynxbase/lynxdb/pkg/planner"
	"github.com/lynxbase/lynxdb/pkg/ppl"
	"github.com/lynxbase/lynxdb/pkg/query"
	"github.com/lynxbase/lynxdb/pkg/server"
)

// QueryService orchestrates query planning and execution.
type QueryService struct {
	planner  planner.Planner
	engine   QueryEngine
	queryCfg atomic.Pointer[config.QueryConfig]
}

// NewQueryService creates a QueryService.
func NewQueryService(p planner.Planner, engine QueryEngine, cfg config.QueryConfig) *QueryService {
	svc := &QueryService{
		planner: p,
		engine:  engine,
	}
	svc.ReloadConfig(cfg)

	return svc
}

// ReloadConfig swaps the live query config snapshot used for new requests.
func (s *QueryService) ReloadConfig(cfg config.QueryConfig) {
	cfgCopy := cfg
	s.queryCfg.Store(&cfgCopy)
}

func (s *QueryService) currentQueryConfig() config.QueryConfig {
	if cfg := s.queryCfg.Load(); cfg != nil {
		return *cfg
	}

	return config.QueryConfig{}
}

// Explain compiles a query without executing it and describes its logical
// and optimized IR plans and the storage hints the scan uses.
func (s *QueryService) Explain(_ context.Context, req ExplainRequest) (*ExplainResult, error) {
	plan, err := s.planner.Plan(planner.PlanRequest{
		Query: req.Query,
		From:  req.From,
		To:    req.To,
	})
	if err != nil {
		if pe, ok := ppl.AsError(err); ok {
			return &ExplainResult{Errors: []ExplainError{{
				Kind:       string(pe.Kind),
				Message:    pe.Msg,
				Suggestion: pe.Hint,
				Line:       pe.Pos.Line,
				Column:     pe.Pos.Col,
				Expected:   pe.Expected,
			}}}, nil
		}
		return nil, err
	}

	hints := plan.Hints
	hasTimeBounds := hints.TimeBounds != nil || plan.ExternalTimeBounds != nil
	hasSearchHints := len(hints.SearchTerms) > 0 || len(hints.TokenGlobs) > 0 || len(hints.FieldPredicates) > 0
	cost := "low"
	switch {
	case !hasTimeBounds && !hasSearchHints:
		cost = "high"
	case !hasTimeBounds || !hasSearchHints:
		cost = "medium"
	}

	var rules []ExplainRuleDetail
	for _, rd := range plan.RuleDetails {
		rules = append(rules, ExplainRuleDetail{Name: rd.Rule, Count: rd.Count})
	}

	var scope *ExplainSourceScope
	if hints.SourceScopeType != "" {
		var total int
		if s.engine != nil {
			total = s.engine.SourceCount()
		}
		scope = &ExplainSourceScope{
			Type:                  hints.SourceScopeType,
			Sources:               hints.SourceScopeSources,
			Pattern:               hints.SourceScopePattern,
			TotalSourcesAvailable: total,
		}
	}

	return &ExplainResult{
		IsValid: true,
		Parsed: &ExplainParsed{
			ResultType:    string(server.DetectResultType(plan.Plan)),
			EstimatedCost: cost,
			UsesFullScan:  !hasTimeBounds && !hasSearchHints,
			FieldsRead:    hints.RequiredCols,
			SearchTerms:   hints.SearchTerms,
			HasTimeBounds: hasTimeBounds,
			SourceScope:   scope,
			LogicalPlan:   ir.Render(plan.Compiled.Logical),
			OptimizedPlan: ir.Render(plan.Plan),
			CompileMS:     float64(plan.CompileDuration.Microseconds()) / 1000,
			RuleDetails:   rules,
		},
	}, nil
}

// Submit plans and executes a query with sync/hybrid/async dispatch.
func (s *QueryService) Submit(ctx context.Context, req SubmitRequest) (*SubmitResult, error) {
	queryCfg := s.currentQueryConfig()

	plan, err := s.planner.Plan(planner.PlanRequest{
		Query: req.Query,
		From:  req.From,
		To:    req.To,
	})
	if err != nil {
		return nil, err
	}
	if plan.Compiled.Explain {
		return explainResult(plan), nil
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
		Plan:               plan.Plan,
		Hints:              plan.Hints,
		ExternalTimeBounds: plan.ExternalTimeBounds,
		SkipResultCache:    plan.SkipResultCache || req.SkipResultCache,
		ResultType:         server.DetectResultType(plan.Plan),
		ProfileLevel:       req.Profile,
		ParseDuration:      plan.CompileDuration,
		RuleDetails:        plan.RuleDetails,
		TotalRules:         len(plan.RuleDetails),
	})
	if err != nil {
		return nil, err
	}
	job.SetLintOptions(!req.NoLint, req.LintLimit, req.LintFull)

	limit := req.Limit
	if limit <= 0 {
		limit = queryCfg.DefaultResultLimit
	}
	if queryCfg.MaxResultLimit > 0 && limit > queryCfg.MaxResultLimit {
		limit = queryCfg.MaxResultLimit
	}

	var warnings []string
	if plan.Hints != nil && len(plan.Hints.Warnings) > 0 {
		warnings = plan.Hints.Warnings
	}
	rewrites := append([]model.QueryRewrite(nil), req.Rewrites...)
	job.SetAdvisoryMetadata(warnings, nil, nil, rewrites)

	buildSync := func() *SubmitResult {
		r := buildSyncResult(job, limit, req.Offset)
		r.Warnings = warnings
		r.Rewrites = rewrites

		return r
	}

	// promoteToAsync hands back a job handle after the wait budget expires. It
	// first re-checks job.Done() without blocking: select picks randomly among
	// ready cases, so a job that completed at the same instant the timer fired
	// would otherwise return a 202 and force the client to poll for a result
	// that is already available.
	promoteToAsync := func() *SubmitResult {
		select {
		case <-job.Done():
			return buildSync()
		default:
		}
		// Detach from the HTTP context so the job survives client disconnect.
		job.Detach()

		return buildJobHandle(job)
	}

	switch req.Mode {
	case QueryModeSync:
		syncTimeout := queryCfg.SyncTimeout
		if syncTimeout == 0 {
			syncTimeout = 30 * time.Second
		}
		timer := time.NewTimer(syncTimeout)
		defer timer.Stop()
		select {
		case <-job.Done():
			return buildSync(), nil
		case <-timer.C:
			return promoteToAsync(), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}

	case QueryModeHybrid:
		timer := time.NewTimer(req.Wait)
		defer timer.Stop()
		select {
		case <-job.Done():
			return buildSync(), nil
		case <-timer.C:
			return promoteToAsync(), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return buildJobHandle(job), nil
}

// explainResult answers a query written with the explain command: one row
// holding the rendered plans.
func explainResult(plan *planner.PlanResult) *SubmitResult {
	return &SubmitResult{
		Done:       true,
		ResultType: server.ResultTypeAggregate,
		Results: []model.ResultRow{{Fields: map[string]interface{}{
			"plan": query.Explain(plan.Compiled),
		}}},
	}
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

	return s.engine.BuildStreamingPipeline(ctx, plan.Plan, plan.Hints, plan.ExternalTimeBounds)
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

const defaultLintOutputLimit = 5

func applyLintOutputLimit(lints []model.QueryLint, limit int, full bool) []model.QueryLint {
	if full || len(lints) == 0 {
		return lints
	}
	if limit <= 0 {
		limit = defaultLintOutputLimit
	}
	if len(lints) <= limit {
		return lints
	}

	return append([]model.QueryLint(nil), lints[:limit]...)
}

func buildJobHandle(job *server.SearchJob) *SubmitResult {
	snap := job.Snapshot()
	r := &SubmitResult{
		Done:        false,
		JobID:       job.ID,
		Status:      "running",
		Warnings:    snap.Warnings,
		Lints:       snap.Lints,
		Suggestions: snap.Suggestions,
		Rewrites:    snap.Rewrites,
	}
	if p := job.Progress.Load(); p != nil {
		r.Progress = p
	}

	return r
}
