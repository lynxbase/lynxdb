package planner

import (
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/optimizer"
	"github.com/OrlovEvgeny/Lynxdb/pkg/server"
	"github.com/OrlovEvgeny/Lynxdb/pkg/spl2"
)

// Planner parses, optimizes, and classifies SPL2 queries.
type Planner interface {
	Plan(req PlanRequest) (*Plan, error)
}

// FieldStatsProvider provides per-field statistics for selectivity estimation.
type FieldStatsProvider interface {
	GetFieldStats() map[string]optimizer.FieldStatInfo
}

// Option configures the planner.
type Option func(*plannerImpl)

// WithViewCatalog enables MV query rewrite using the given ViewLister.
func WithViewCatalog(lister ViewLister) Option {
	return func(p *plannerImpl) {
		p.viewCatalog = NewViewCatalog(lister)
	}
}

// WithFieldStats enables stat-based predicate reordering.
func WithFieldStats(provider FieldStatsProvider) Option {
	return func(p *plannerImpl) {
		p.fieldStatsProvider = provider
	}
}

// WithPlanCache enables query plan caching.
func WithPlanCache(cache *PlanCache) Option {
	return func(p *plannerImpl) {
		p.planCache = cache
	}
}

// New creates a Planner with the given options.
func New(opts ...Option) Planner {
	p := &plannerImpl{}
	for _, o := range opts {
		o(p)
	}

	return p
}

type plannerImpl struct {
	viewCatalog        optimizer.ViewCatalog
	fieldStatsProvider FieldStatsProvider
	planCache          *PlanCache
}

func (p *plannerImpl) Plan(req PlanRequest) (*Plan, error) {
	// Normalize implicit search syntax (e.g. "level=error" -> "search level=error")
	// before parsing so all clients benefit from bare field=value support.
	query := spl2.NormalizeQuery(req.Query)

	// Check plan cache first.
	if p.planCache != nil {
		if cached, ok := p.planCache.Get(query); ok {
			// Deep clone to prevent mutation of cached plan.
			plan := cached.Clone()
			// Always apply fresh external time bounds.
			plan.ExternalTimeBounds = server.ParseTimeBounds(req.From, req.To)

			return plan, nil
		}
	}

	parseStart := time.Now()

	prog, err := spl2.ParseProgram(query)
	if err != nil {
		return nil, &ParseError{
			Message:    err.Error(),
			Suggestion: spl2.SuggestFix(err.Error(), nil),
			Wrapped:    err,
		}
	}

	parseDuration := time.Since(parseStart)
	optimizeStart := time.Now()

	// Build optimizer options.
	var optOpts []optimizer.OptOption
	if p.viewCatalog != nil {
		optOpts = append(optOpts, optimizer.WithCatalog(p.viewCatalog))
	}
	if p.fieldStatsProvider != nil {
		stats := p.fieldStatsProvider.GetFieldStats()
		optOpts = append(optOpts, optimizer.WithFieldStats(stats))
	}
	opt := optimizer.New(optOpts...)

	prog.Main = opt.Optimize(prog.Main)
	for i := range prog.Datasets {
		prog.Datasets[i].Query = opt.Optimize(prog.Datasets[i].Query)
	}

	optimizeDuration := time.Since(optimizeStart)

	resultType := server.DetectResultType(prog)
	hints := spl2.ExtractQueryHints(prog)
	externalTB := server.ParseTimeBounds(req.From, req.To)

	plan := &Plan{
		RawQuery:           query,
		Program:            prog,
		ResultType:         resultType,
		Hints:              hints,
		OptimizerStats:     opt.Stats,
		ExternalTimeBounds: externalTB,
		ParseDuration:      parseDuration,
		OptimizeDuration:   optimizeDuration,
		RuleDetails:        opt.GetRuleDetails(),
		TotalRules:         opt.TotalRules(),
	}

	// Cache the plan (without external time bounds).
	if p.planCache != nil {
		cachePlan := plan.Clone()
		cachePlan.ExternalTimeBounds = nil
		p.planCache.Put(query, cachePlan)
	}

	return plan, nil
}
