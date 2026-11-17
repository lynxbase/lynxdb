package usecases

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/lynxbase/lynxdb/pkg/config"
	enginepipeline "github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/logical"
	"github.com/lynxbase/lynxdb/pkg/lynxflow/ast"
	"github.com/lynxbase/lynxdb/pkg/lynxflow/sema"
	"github.com/lynxbase/lynxdb/pkg/model"
	"github.com/lynxbase/lynxdb/pkg/planner"
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

// Explain parses and analyses a query without executing it.
func (s *QueryService) Explain(_ context.Context, req ExplainRequest) (*ExplainResult, error) {
	query := req.Query // RFC-002: spl2 normalization removed
	plan, err := s.planner.Plan(planner.PlanRequest{
		Query: query,
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

	var catalogFields []string
	if s.engine != nil {
		catalogFields = s.engine.ListFieldNames()
	}
	stages := annotatePipelineFields(plan.Program, catalogFields)

	// Account for external time bounds when evaluating cost.
	hasTimeBounds := plan.Hints.TimeBounds != nil || plan.ExternalTimeBounds != nil
	hasSearchHints := len(plan.Hints.SearchTerms) > 0 || len(plan.Hints.TokenGlobs) > 0
	cost := "low"
	if !hasTimeBounds && !hasSearchHints {
		cost = "high"
	} else if !hasTimeBounds || !hasSearchHints {
		cost = "medium"
	}

	usesFullScan := !hasTimeBounds && !hasSearchHints

	// Build physical plan from optimizer annotations on the AST.
	physPlan := extractPhysicalPlan(plan.Program)

	// Convert optimizer rule details for the explain response.
	var ruleDetails []ExplainRuleDetail
	for _, rd := range plan.RuleDetails {
		ruleDetails = append(ruleDetails, ExplainRuleDetail{
			Name:        rd.Rule,
			Description: "",
			Count:       rd.Count,
		})
	}

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
	if plan.Program != nil {
		// RFC-002: annotations removed.
	}

	return &ExplainResult{
		IsValid:  true,
		Errors:   nil,
		Rewrites: plan.Rewrites,
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
			RangePredicates:   explainRangePredicates(plan.Hints.RangePredicates),
			ParseMS:           float64(plan.ParseDuration.Microseconds()) / 1000,
			OptimizeMS:        float64(plan.OptimizeDuration.Microseconds()) / 1000,
			RuleDetails:       ruleDetails,
			TotalRules:        plan.TotalRules,
			OptimizerMessages: optMessages,
			OptimizerWarnings: optWarnings,
		},
	}, nil
}

func explainRangePredicates(preds []model.RangePredicate) []ExplainRangePredicate {
	if len(preds) == 0 {
		return nil
	}
	out := make([]ExplainRangePredicate, 0, len(preds))
	for _, pred := range preds {
		rgStrategy := "zone-map"
		rowStrategy := "per-row"
		if pred.LoweredToBSI {
			rgStrategy = "bsi"
			rowStrategy = "handled_by=bsi"
		}
		out = append(out, ExplainRangePredicate{
			Field:            pred.Field,
			Min:              pred.Min,
			Max:              pred.Max,
			LoweredToBSI:     pred.LoweredToBSI,
			RGFilterStrategy: rgStrategy,
			RowVMStrategy:    rowStrategy,
		})
	}

	return out
}

// commandName returns the lynxflow operator name for a logical IR node.
func commandName(n logical.Node) string {
	switch nd := n.(type) {
	case *logical.Scan:
		return "from"
	case *logical.Empty:
		return "empty"
	case *logical.Filter:
		return "where"
	case *logical.Extend:
		return "extend"
	case *logical.Project:
		return projectCommandName(nd)
	case *logical.Aggregate:
		return aggregateCommandName(nd)
	case *logical.Sort:
		return "sort"
	case *logical.TopK:
		return "top"
	case *logical.Limit:
		if nd.Tail {
			return "tail"
		}
		return "head"
	case *logical.Offset:
		return "offset"
	case *logical.Dedup:
		return "dedup"
	case *logical.Parse:
		return "parse"
	case *logical.Join:
		return "join"
	case *logical.Union:
		return "union"
	case *logical.Describe:
		return "describe"
	case *logical.Explode:
		return "explode"
	case *logical.Helper:
		return nd.Name
	case *logical.Materialize:
		return "materialize"
	case *logical.Tee:
		return "tee"
	default:
		return "unknown"
	}
}

// projectCommandName distinguishes keep, drop, and rename for a Project node.
func projectCommandName(p *logical.Project) string {
	hasKeep := false
	hasDrop := false
	hasRename := false
	for _, c := range p.Cols {
		switch c.Action {
		case logical.ProjectKeep:
			hasKeep = true
		case logical.ProjectDrop:
			hasDrop = true
		case logical.ProjectRename:
			hasRename = true
		}
	}
	if hasRename && !hasKeep && !hasDrop {
		return "rename"
	}
	if hasDrop && !hasKeep {
		return "drop"
	}
	if hasKeep {
		return "keep"
	}
	return "project"
}

// aggregateCommandName picks stats/eventstats/streamstats based on the window variant.
func aggregateCommandName(a *logical.Aggregate) string {
	if a.Window != nil {
		switch a.Window.Variant {
		case logical.WindowEventstats:
			return "eventstats"
		case logical.WindowStreamstats:
			return "streamstats"
		}
	}
	return "stats"
}

// annotatePipelineFields walks a query's commands sequentially, maintaining a
// running ordered field set, and computes field additions/removals per command.
// The result drives the Lynx Flow sidebar's per-stage field tracking.
func annotatePipelineFields(plan *logical.Plan, catalogFields []string) []PipelineStage {
	if plan == nil || plan.Root == nil {
		return nil
	}
	chain := linearizePlan(plan.Root)
	if len(chain) == 0 {
		return nil
	}

	// Running field set as an ordered slice of names.
	var fields []string

	stages := make([]PipelineStage, 0, len(chain))
	for _, node := range chain {
		stage := buildPipelineStage(node, fields, catalogFields)
		fields = stage.FieldsOut
		stages = append(stages, stage)
	}
	return stages
}

// linearizePlan walks the plan tree from root to leaf and returns nodes in
// pipeline order (leaf first). Same algorithm as logical.linearize.
func linearizePlan(root logical.Node) []logical.Node {
	if root == nil {
		return nil
	}
	var chain []logical.Node
	cur := root
	for cur != nil {
		chain = append(chain, cur)
		children := cur.Children()
		if len(children) == 0 {
			break
		}
		cur = children[0]
	}
	// Reverse to get pipeline order (Scan first).
	for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
		chain[i], chain[j] = chain[j], chain[i]
	}
	return chain
}

// buildPipelineStage creates a PipelineStage for a single logical node,
// computing field additions, removals, and the resulting field set.
func buildPipelineStage(node logical.Node, prevFields []string, catalogFields []string) PipelineStage {
	name := commandName(node)
	desc := truncateDesc(node.String(), 120)

	switch nd := node.(type) {
	case *logical.Scan:
		// The source stage: we know catalog fields exist but the full schema
		// is unknown at explain time (schema-on-read). We seed from the node's
		// OutputSchema if populated, otherwise mark as unknown.
		outFields := schemaFieldNames(nd.Schema())
		if len(outFields) == 0 && len(catalogFields) > 0 {
			outFields = make([]string, len(catalogFields))
			copy(outFields, catalogFields)
		}
		return PipelineStage{
			Command:       name,
			Description:   desc,
			FieldsOut:     outFields,
			FieldsUnknown: true, // schema-on-read: field set is never complete
		}

	case *logical.Empty:
		return PipelineStage{
			Command:     name,
			Description: desc,
			FieldsOut:   copyStrings(prevFields),
		}

	case *logical.Filter:
		// Filter does not change the field set.
		return PipelineStage{
			Command:     name,
			Description: desc,
			FieldsOut:   copyStrings(prevFields),
		}

	case *logical.Extend:
		added := make([]string, 0, len(nd.Assignments))
		out := copyStrings(prevFields)
		for _, a := range nd.Assignments {
			if !containsField(out, a.Name) {
				added = append(added, a.Name)
				out = append(out, a.Name)
			}
			// If it already exists, extend overwrites — not added, not removed.
		}
		return PipelineStage{
			Command:     name,
			Description: desc,
			FieldsAdded: nilIfEmpty(added),
			FieldsOut:   out,
		}

	case *logical.Project:
		outSchema := schemaFieldNames(nd.Schema())
		added := diffFields(outSchema, prevFields)
		removed := diffFields(prevFields, outSchema)
		return PipelineStage{
			Command:       name,
			Description:   desc,
			FieldsAdded:   nilIfEmpty(added),
			FieldsRemoved: nilIfEmpty(removed),
			FieldsOut:     outSchema,
		}

	case *logical.Aggregate:
		outSchema := schemaFieldNames(nd.Schema())
		added := diffFields(outSchema, prevFields)
		removed := diffFields(prevFields, outSchema)
		return PipelineStage{
			Command:       name,
			Description:   desc,
			FieldsAdded:   nilIfEmpty(added),
			FieldsRemoved: nilIfEmpty(removed),
			FieldsOut:     outSchema,
		}

	case *logical.TopK:
		// TopK is a fused sort+limit — does not change the schema.
		outSchema := schemaFieldNames(nd.Schema())
		added := diffFields(outSchema, prevFields)
		removed := diffFields(prevFields, outSchema)
		return PipelineStage{
			Command:       name,
			Description:   desc,
			FieldsAdded:   nilIfEmpty(added),
			FieldsRemoved: nilIfEmpty(removed),
			FieldsOut:     outSchema,
		}

	case *logical.Parse:
		outSchema := schemaFieldNames(nd.Schema())
		added := diffFields(outSchema, prevFields)
		return PipelineStage{
			Command:     name,
			Description: desc,
			FieldsAdded: nilIfEmpty(added),
			FieldsOut:   outSchema,
		}

	case *logical.Sort, *logical.Limit, *logical.Offset, *logical.Dedup:
		// These do not change the field set.
		return PipelineStage{
			Command:     name,
			Description: desc,
			FieldsOut:   copyStrings(prevFields),
		}

	case *logical.Join:
		outSchema := schemaFieldNames(nd.Schema())
		added := diffFields(outSchema, prevFields)
		return PipelineStage{
			Command:     name,
			Description: desc,
			FieldsAdded: nilIfEmpty(added),
			FieldsOut:   outSchema,
		}

	case *logical.Union:
		outSchema := schemaFieldNames(nd.Schema())
		added := diffFields(outSchema, prevFields)
		return PipelineStage{
			Command:     name,
			Description: desc,
			FieldsAdded: nilIfEmpty(added),
			FieldsOut:   outSchema,
		}

	case *logical.Describe:
		outSchema := schemaFieldNames(nd.Schema())
		removed := diffFields(prevFields, outSchema)
		return PipelineStage{
			Command:       name,
			Description:   desc,
			FieldsAdded:   outSchema,
			FieldsRemoved: nilIfEmpty(removed),
			FieldsOut:     outSchema,
		}

	default:
		// Passthrough default for Helper, Explode, Materialize, Tee, etc.
		outSchema := schemaFieldNames(node.Schema())
		if len(outSchema) == 0 {
			outSchema = copyStrings(prevFields)
		}
		added := diffFields(outSchema, prevFields)
		removed := diffFields(prevFields, outSchema)
		return PipelineStage{
			Command:       name,
			Description:   desc,
			FieldsAdded:   nilIfEmpty(added),
			FieldsRemoved: nilIfEmpty(removed),
			FieldsOut:     outSchema,
		}
	}
}

// schemaFieldNames extracts field names from a sema.Field slice.
func schemaFieldNames(fields []sema.Field) []string {
	if len(fields) == 0 {
		return nil
	}
	names := make([]string, len(fields))
	for i, f := range fields {
		names[i] = f.Name
	}
	return names
}

// copyStrings returns a defensive copy of a string slice.
func copyStrings(s []string) []string {
	if s == nil {
		return nil
	}
	out := make([]string, len(s))
	copy(out, s)
	return out
}

// containsField checks if a field name exists in the slice.
func containsField(fields []string, name string) bool {
	for _, f := range fields {
		if f == name {
			return true
		}
	}
	return false
}

// nilIfEmpty returns nil when the slice is empty, otherwise returns it as-is.
func nilIfEmpty(s []string) []string {
	if len(s) == 0 {
		return nil
	}
	return s
}

// aggOutputName returns the output field name for an aggregation expression,
// following the same convention as the pipeline builder in pkg/engine/pipeline.
func aggOutputName(_ interface{}) string { return "" }

// truncateDesc truncates a description to maxLen characters, appending "..."
// if truncation occurred.
func truncateDesc(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

// diffFields returns elements in 'from' that are not in 'keep'.
func diffFields(from []string, keep []string) []string {
	keepSet := make(map[string]struct{}, len(keep))
	for _, f := range keep {
		keepSet[f] = struct{}{}
	}
	var diff []string
	for _, f := range from {
		if _, ok := keepSet[f]; !ok {
			diff = append(diff, f)
		}
	}
	return diff
}

// declareUnpackFields creates a parser for the given UnpackCommand and returns
// its FieldDeclaration, or nil if the parser cannot be created or does not
// implement FieldDeclarer.
func declareUnpackFields(_ interface{}) interface{} {
	return nil
}

// applyPrefix prepends a prefix (with dot separator) to each field name.
// Returns the original slice unchanged if prefix is empty.
func applyPrefix(prefix string, fields []string) []string {
	if prefix == "" {
		return fields
	}
	result := make([]string, len(fields))
	for i, f := range fields {
		result[i] = prefix + f
	}
	return result
}

// extractPhysicalPlan inspects the logical plan tree to build a PhysicalPlan
// that describes the runtime execution strategy. This surfaces optimizations
// that are invisible in the logical pipeline stages (e.g., partial aggregation
// pushdown, topK heap merge, reverse scan).
func extractPhysicalPlan(plan *logical.Plan) *PhysicalPlan {
	if plan == nil || plan.Root == nil {
		return nil
	}
	chain := linearizePlan(plan.Root)

	var pp PhysicalPlan
	hasAnything := false

	for _, node := range chain {
		switch nd := node.(type) {
		case *logical.Aggregate:
			if nd.Partial {
				pp.PartialAgg = true
				hasAnything = true
			}
			if nd.TopK != nil {
				pp.TopKAgg = true
				pp.TopK = int(nd.TopK.K)
				hasAnything = true
			}
			// Detect count(*) only: single agg, no keys, function is count().
			if len(nd.Aggs) == 1 && len(nd.Keys) == 0 && nd.TimeBin == nil {
				if call, ok := nd.Aggs[0].Func.(*ast.Call); ok {
					if call.Callee == "count" && len(call.Args) == 0 {
						pp.CountStarOnly = true
						hasAnything = true
					}
				}
			}
		case *logical.TopK:
			pp.TopKAgg = true
			pp.TopK = int(nd.K)
			hasAnything = true
		case *logical.Join:
			pp.JoinStrategy = nd.Type
			hasAnything = true
		case *logical.Scan:
			if len(nd.Pushdown.RawTerms) > 0 || len(nd.Pushdown.BloomTerms) > 0 {
				hasAnything = true
			}
		}
	}

	if queryHasRexLiteralPreFilter(plan) {
		pp.RexLiteralPreFilter = true
		hasAnything = true
	}

	if !hasAnything {
		return nil
	}
	return &pp
}

// queryHasRexLiteralPreFilter checks whether the optimizer has extracted
// bloom/raw terms from a parse (rex) node's regex literals and pushed them
// down to the Scan node. This is indicated by the presence of both a Parse
// node in the plan and RawTerms/BloomTerms in the Scan pushdown.
func queryHasRexLiteralPreFilter(plan *logical.Plan) bool {
	if plan == nil || plan.Root == nil {
		return false
	}
	chain := linearizePlan(plan.Root)
	hasParse := false
	hasTermPushdown := false
	for _, node := range chain {
		switch nd := node.(type) {
		case *logical.Parse:
			hasParse = true
		case *logical.Scan:
			if len(nd.Pushdown.RawTerms) > 0 || len(nd.Pushdown.BloomTerms) > 0 {
				hasTermPushdown = true
			}
		}
	}
	return hasParse && hasTermPushdown
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
		SkipResultCache:    plan.SkipResultCache || req.SkipResultCache,
		ResultType:         server.ResultType(plan.ResultType),
		ProfileLevel:       req.Profile,
		ParseDuration:      plan.ParseDuration,
		OptimizeDuration:   plan.OptimizeDuration,
		RuleDetails:        plan.RuleDetails,
		TotalRules:         plan.TotalRules,
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

	// Collect any user-facing warnings from query hints.
	var warnings []string
	if plan.Hints != nil && len(plan.Hints.Warnings) > 0 {
		warnings = plan.Hints.Warnings
	}
	var analysisLints []model.QueryLint
	if !req.NoLint || !req.NoSuggestions {
		// Lints come from the planner (sema + lint passes on the desugared AST).
		analysisLints = model.PrepareQueryLints(plan.Lints)
	}
	var lints []model.QueryLint
	if !req.NoLint {
		lints = applyLintOutputLimit(analysisLints, req.LintLimit, req.LintFull)
	}
	var suggestions []model.QuerySuggestion
	if !req.NoSuggestions {
		suggestions = model.SuggestionsFromLints(analysisLints)
	}
	// Merge planner rewrites (desugar) with request rewrites (user-facing normalizer).
	allRewrites := append([]model.QueryRewrite(nil), plan.Rewrites...)
	allRewrites = append(allRewrites, req.Rewrites...)
	job.SetAdvisoryMetadata(warnings, lints, suggestions, allRewrites)

	// buildSync builds an inline result from the (completed) job, attaching the
	// advisory metadata consistently across every synchronous return path.
	buildSync := func() *SubmitResult {
		r := buildSyncResult(job, limit, req.Offset)
		r.Warnings = warnings
		r.Lints = lints
		r.Suggestions = suggestions
		r.Rewrites = allRewrites

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

	return s.engine.BuildStreamingPipeline(ctx, plan.Program, plan.Hints, plan.ExternalTimeBounds)
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

// GroupedHistogram computes event count buckets grouped by a field value.
// Uses ReadEventsWithColumns to extract both timestamps and the grouping field.
func (s *QueryService) GroupedHistogram(ctx context.Context, req HistogramRequest) (*GroupedHistogramResult, error) {
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

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	srvBuckets, total, err := s.engine.HistogramByFieldFromMetadata(
		ctx, req.Index, fromTime, toTime, interval, req.GroupBy, bucketCount,
	)
	if err != nil {
		return nil, err
	}

	buckets := make([]GroupedHistogramBucket, len(srvBuckets))
	for i, b := range srvBuckets {
		buckets[i] = GroupedHistogramBucket{Time: b.Time, Counts: b.Counts}
	}

	return &GroupedHistogramResult{
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
