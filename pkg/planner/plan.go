package planner

import (
	"time"

	"github.com/lynxbase/lynxdb/pkg/optimizer"
	"github.com/lynxbase/lynxdb/pkg/server"
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

// PlanRequest is a transport-agnostic input for planning a query.
type PlanRequest struct {
	Query string // SPL2 query text
	From  string // optional: ISO 8601 or relative ("-1h")
	To    string // optional: ISO 8601 or relative, or "now"
}

// Plan is the immutable result of parse + optimize + classify.
type Plan struct {
	RawQuery           string
	Program            *spl2.Program
	ResultType         server.ResultType
	Hints              *spl2.QueryHints
	OptimizerStats     map[string]int
	ExternalTimeBounds *spl2.TimeBounds

	// Profiling: populated by the planner for timing and optimizer rule details.
	ParseDuration    time.Duration
	OptimizeDuration time.Duration
	RuleDetails      []optimizer.RuleDetail
	TotalRules       int
}
