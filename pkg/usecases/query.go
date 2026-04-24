package usecases

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/lynxbase/lynxdb/pkg/config"
	enginepipeline "github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/engine/unpack"
	"github.com/lynxbase/lynxdb/pkg/optimizer"
	"github.com/lynxbase/lynxdb/pkg/planner"
	"github.com/lynxbase/lynxdb/pkg/server"
	"github.com/lynxbase/lynxdb/pkg/spl2"
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

	var catalogFields []string
	if s.engine != nil {
		catalogFields = s.engine.ListFieldNames()
	}
	stages := annotatePipelineFields(plan.Program.Main, catalogFields)

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
	case *spl2.UnpackCommand:
		return "parse"
	case *spl2.JsonCommand:
		return "json"
	case *spl2.UnrollCommand:
		return "explode"
	case *spl2.SelectCommand:
		return "select"
	case *spl2.PackJsonCommand:
		return "pack"
	default:
		return fmt.Sprintf("unknown(%T)", cmd)
	}
}

// annotatePipelineFields walks a query's commands sequentially, maintaining a
// running ordered field set, and computes field additions/removals per command.
// The result drives the Lynx Flow sidebar's per-stage field tracking.
func annotatePipelineFields(query *spl2.Query, catalogFields []string) []PipelineStage {
	if query == nil {
		return nil
	}

	// orderedSet tracks the current field set in insertion order.
	// We use a slice for order and a map for O(1) membership testing.
	type orderedSet struct {
		order []string
		index map[string]struct{}
	}
	newSet := func() *orderedSet {
		return &orderedSet{index: make(map[string]struct{})}
	}
	setAdd := func(s *orderedSet, fields ...string) {
		for _, f := range fields {
			if _, exists := s.index[f]; !exists {
				s.order = append(s.order, f)
				s.index[f] = struct{}{}
			}
		}
	}
	setRemove := func(s *orderedSet, fields ...string) {
		rm := make(map[string]struct{}, len(fields))
		for _, f := range fields {
			rm[f] = struct{}{}
		}
		newOrder := make([]string, 0, len(s.order))
		for _, f := range s.order {
			if _, removed := rm[f]; !removed {
				newOrder = append(newOrder, f)
			}
		}
		s.order = newOrder
		s.index = make(map[string]struct{}, len(newOrder))
		for _, f := range newOrder {
			s.index[f] = struct{}{}
		}
	}
	setReplace := func(s *orderedSet, fields ...string) {
		s.order = make([]string, 0, len(fields))
		s.index = make(map[string]struct{}, len(fields))
		setAdd(s, fields...)
	}
	setSnapshot := func(s *orderedSet) []string {
		if len(s.order) == 0 {
			return nil
		}
		cp := make([]string, len(s.order))
		copy(cp, s.order)
		return cp
	}
	setContains := func(s *orderedSet, field string) bool {
		_, ok := s.index[field]
		return ok
	}

	fields := newSet()
	fieldsUnknown := false

	// Synthetic source stage: the initial field set from schema-on-read data.
	// We mark it unknown because at parse time we cannot enumerate all fields.
	var sourceDesc string
	if query.Source != nil {
		sourceDesc = fmt.Sprintf("from %s", query.Source.Index)
	} else {
		sourceDesc = "from (default)"
	}
	baseFields := []string{"_time", "_raw", "_source"}
	setAdd(fields, baseFields...)
	for _, f := range catalogFields {
		setAdd(fields, f)
	}
	fieldsUnknown = true

	stages := make([]PipelineStage, 0, len(query.Commands)+1)

	// Emit the synthetic source stage.
	stages = append(stages, PipelineStage{
		Command:       "source",
		Description:   sourceDesc,
		FieldsAdded:   setSnapshot(fields),
		FieldsOut:     setSnapshot(fields),
		FieldsUnknown: true,
	})

	for _, cmd := range query.Commands {
		stage := PipelineStage{
			Command: commandName(cmd),
		}

		var added, removed []string

		switch c := cmd.(type) {
		case *spl2.SearchCommand:
			stage.Description = truncateDesc(c.String(), 80)
			// search does not change the field set.

		case *spl2.WhereCommand:
			stage.Description = truncateDesc(fmt.Sprintf("where %s", c.Expr), 80)
			// where does not change the field set.

		case *spl2.EvalCommand:
			// The parser always populates Assignments (including the first
			// field/expr pair), so we derive the field list from Assignments
			// to avoid double-counting c.Field.
			var evalFields []string
			if len(c.Assignments) > 0 {
				for _, a := range c.Assignments {
					evalFields = append(evalFields, a.Field)
				}
			} else if c.Field != "" {
				evalFields = append(evalFields, c.Field)
			}
			for _, f := range evalFields {
				if !setContains(fields, f) {
					added = append(added, f)
				}
			}
			setAdd(fields, evalFields...)
			stage.Description = truncateDesc(c.String(), 80)

		case *spl2.StatsCommand:
			// Stats replaces the entire field set with groupby fields + agg outputs.
			var newFields []string
			newFields = append(newFields, c.GroupBy...)
			for _, agg := range c.Aggregations {
				newFields = append(newFields, aggOutputName(agg))
			}
			removed = diffFields(fields.order, newFields)
			added = newFields
			setReplace(fields, newFields...)
			fieldsUnknown = false
			stage.Description = truncateDesc(c.String(), 80)

		case *spl2.TimechartCommand:
			// Timechart replaces the entire field set with _time + agg outputs + optional split-by.
			var newFields []string
			newFields = append(newFields, "_time")
			for _, agg := range c.Aggregations {
				newFields = append(newFields, aggOutputName(agg))
			}
			newFields = append(newFields, c.GroupBy...)
			removed = diffFields(fields.order, newFields)
			added = newFields
			setReplace(fields, newFields...)
			fieldsUnknown = false
			stage.Description = truncateDesc(c.String(), 80)

		case *spl2.RexCommand:
			groups := spl2.ExtractNamedGroupsFromPattern(c.Pattern)
			for _, g := range groups {
				if !setContains(fields, g) {
					added = append(added, g)
				}
			}
			setAdd(fields, groups...)
			stage.Description = truncateDesc(c.String(), 80)

		case *spl2.UnpackCommand:
			if len(c.Fields) > 0 {
				prefixed := applyPrefix(c.Prefix, c.Fields)
				for _, f := range prefixed {
					if !setContains(fields, f) {
						added = append(added, f)
					}
				}
				setAdd(fields, prefixed...)
			} else {
				decl := declareUnpackFields(c)
				if decl != nil {
					allDeclared := applyPrefix(c.Prefix, append(decl.Known, decl.Optional...))
					for _, f := range allDeclared {
						if !setContains(fields, f) {
							added = append(added, f)
						}
					}
					setAdd(fields, allDeclared...)
					if len(decl.Optional) > 0 {
						stage.FieldsOptional = applyPrefix(c.Prefix, decl.Optional)
					}
					if decl.Dynamic {
						fieldsUnknown = true
					}
				} else {
					fieldsUnknown = true
				}
			}
			stage.Description = truncateDesc(c.String(), 80)

		case *spl2.JsonCommand:
			var jsonFields []string
			for _, jp := range c.Paths {
				jsonFields = append(jsonFields, jp.OutputName())
			}
			if len(jsonFields) > 0 {
				for _, f := range jsonFields {
					if !setContains(fields, f) {
						added = append(added, f)
					}
				}
				setAdd(fields, jsonFields...)
			} else {
				// No explicit paths = extract all from JSON — unknown fields.
				fieldsUnknown = true
			}
			stage.Description = truncateDesc(c.String(), 80)

		case *spl2.FieldsCommand:
			if spl2.FieldListHasGlob(c.Fields) {
				// Glob patterns — fields are runtime-dependent.
				fieldsUnknown = true
			} else if c.Remove {
				// fields - field1, field2: remove listed fields.
				for _, f := range c.Fields {
					if setContains(fields, f) {
						removed = append(removed, f)
					}
				}
				setRemove(fields, c.Fields...)
			} else {
				// fields field1, field2: keep only listed fields.
				removed = diffFields(fields.order, c.Fields)
				setReplace(fields, c.Fields...)
				fieldsUnknown = false
			}
			stage.Description = truncateDesc(c.String(), 80)

		case *spl2.TableCommand:
			if spl2.FieldListHasGlob(c.Fields) {
				// Glob patterns — fields are runtime-dependent.
				fieldsUnknown = true
			} else {
				// Table keeps only listed fields in specified order.
				removed = diffFields(fields.order, c.Fields)
				setReplace(fields, c.Fields...)
				fieldsUnknown = false
			}
			stage.Description = truncateDesc(c.String(), 80)

		case *spl2.SelectCommand:
			var selectFields []string
			for _, col := range c.Columns {
				if col.Alias != "" {
					selectFields = append(selectFields, col.Alias)
				} else {
					selectFields = append(selectFields, col.Name)
				}
			}
			removed = diffFields(fields.order, selectFields)
			added = diffFields(selectFields, fields.order)
			setReplace(fields, selectFields...)
			fieldsUnknown = false
			stage.Description = truncateDesc(c.String(), 80)

		case *spl2.RenameCommand:
			for _, r := range c.Renames {
				if setContains(fields, r.Old) {
					removed = append(removed, r.Old)
					added = append(added, r.New)
					setRemove(fields, r.Old)
					setAdd(fields, r.New)
				}
			}
			stage.Description = truncateDesc(c.String(), 80)

		case *spl2.SortCommand:
			stage.Description = truncateDesc(c.String(), 80)

		case *spl2.HeadCommand:
			stage.Description = fmt.Sprintf("head %d", c.Count)

		case *spl2.TailCommand:
			stage.Description = fmt.Sprintf("tail %d", c.Count)

		case *spl2.DedupCommand:
			stage.Description = truncateDesc(c.String(), 80)

		case *spl2.BinCommand:
			outField := c.Alias
			if outField == "" {
				outField = c.Field
			}
			if outField != c.Field && !setContains(fields, outField) {
				added = append(added, outField)
			}
			setAdd(fields, outField)
			stage.Description = truncateDesc(c.String(), 80)

		case *spl2.StreamstatsCommand:
			for _, agg := range c.Aggregations {
				name := aggOutputName(agg)
				if !setContains(fields, name) {
					added = append(added, name)
				}
			}
			setAdd(fields, added...)
			stage.Description = truncateDesc(c.String(), 80)

		case *spl2.EventstatsCommand:
			for _, agg := range c.Aggregations {
				name := aggOutputName(agg)
				if !setContains(fields, name) {
					added = append(added, name)
				}
			}
			setAdd(fields, added...)
			stage.Description = truncateDesc(c.String(), 80)

		case *spl2.TopCommand:
			var newFields []string
			newFields = append(newFields, c.Field)
			if c.ByField != "" {
				newFields = append(newFields, c.ByField)
			}
			newFields = append(newFields, "count", "percent")
			removed = diffFields(fields.order, newFields)
			added = newFields
			setReplace(fields, newFields...)
			fieldsUnknown = false
			stage.Description = truncateDesc(c.String(), 80)

		case *spl2.RareCommand:
			var newFields []string
			newFields = append(newFields, c.Field)
			if c.ByField != "" {
				newFields = append(newFields, c.ByField)
			}
			newFields = append(newFields, "count", "percent")
			removed = diffFields(fields.order, newFields)
			added = newFields
			setReplace(fields, newFields...)
			fieldsUnknown = false
			stage.Description = truncateDesc(c.String(), 80)

		case *spl2.JoinCommand:
			// Join can introduce unknown fields from the subquery.
			fieldsUnknown = true
			stage.Description = truncateDesc(c.String(), 80)

		case *spl2.AppendCommand:
			// Append can introduce unknown fields from the subquery.
			fieldsUnknown = true
			stage.Description = truncateDesc(c.String(), 80)

		case *spl2.MultisearchCommand:
			fieldsUnknown = true
			stage.Description = truncateDesc(c.String(), 80)

		case *spl2.TransactionCommand:
			// Transaction adds duration and eventcount fields.
			txAdded := []string{"duration", "eventcount"}
			for _, f := range txAdded {
				if !setContains(fields, f) {
					added = append(added, f)
				}
			}
			setAdd(fields, txAdded...)
			stage.Description = truncateDesc(c.String(), 80)

		case *spl2.XYSeriesCommand:
			// XYSeries pivots data — output fields are unknowable at parse time.
			fieldsUnknown = true
			stage.Description = truncateDesc(c.String(), 80)

		case *spl2.FillnullCommand:
			stage.Description = truncateDesc(c.String(), 80)

		case *spl2.UnrollCommand:
			outField := c.Alias
			if outField == "" {
				outField = c.Field
			}
			if outField != c.Field && !setContains(fields, outField) {
				added = append(added, outField)
			}
			setAdd(fields, outField)
			for _, f := range c.ExtraFields {
				setAdd(fields, f)
			}
			stage.Description = truncateDesc(c.String(), 80)

		case *spl2.PackJsonCommand:
			if !setContains(fields, c.Target) {
				added = append(added, c.Target)
			}
			setAdd(fields, c.Target)
			stage.Description = truncateDesc(c.String(), 80)

		case *spl2.FromCommand:
			// FROM a materialized view — fields are unknowable.
			fieldsUnknown = true
			stage.Description = fmt.Sprintf("from %s", c.ViewName)

		case *spl2.TopNCommand:
			// Internal optimizer command (sort+head fused). Does not change fields.
			stage.Description = truncateDesc(c.String(), 80)

		case *spl2.MaterializeCommand:
			stage.Description = truncateDesc(c.String(), 80)

		case *spl2.ViewsCommand:
			stage.Description = truncateDesc(c.String(), 80)

		case *spl2.DropviewCommand:
			stage.Description = truncateDesc(c.String(), 80)

		default:
			stage.Description = truncateDesc(fmt.Sprintf("%s", cmd), 80)
		}

		stage.FieldsAdded = added
		stage.FieldsRemoved = removed
		stage.FieldsOut = setSnapshot(fields)
		stage.FieldsUnknown = fieldsUnknown

		stages = append(stages, stage)
	}

	return stages
}

// aggOutputName returns the output field name for an aggregation expression,
// following the same convention as the pipeline builder in pkg/engine/pipeline.
func aggOutputName(agg spl2.AggExpr) string {
	if agg.Alias != "" {
		return agg.Alias
	}
	if len(agg.Args) > 0 {
		return fmt.Sprintf("%s(%s)", agg.Func, agg.Args[0])
	}
	return agg.Func
}

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
func declareUnpackFields(c *spl2.UnpackCommand) *unpack.FieldDeclaration {
	var parser unpack.FormatParser
	var err error

	switch c.Format {
	case "pattern":
		if c.Pattern == "" {
			return nil
		}
		parser, err = unpack.NewPatternParser(c.Pattern)
	case "w3c":
		if c.Header != "" {
			parser = unpack.NewW3CParser(c.Header)
		} else {
			parser, err = unpack.NewParser(c.Format)
		}
	default:
		parser, err = unpack.NewParser(c.Format)
	}
	if err != nil || parser == nil {
		return nil
	}

	declarer, ok := parser.(unpack.FieldDeclarer)
	if !ok {
		return nil
	}
	decl := declarer.DeclareFields()
	return &decl
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
			r := buildSyncResult(job, limit, req.Offset)
			r.Warnings = warnings
			return r, nil
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
			r := buildSyncResult(job, limit, req.Offset)
			r.Warnings = warnings
			return r, nil
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
