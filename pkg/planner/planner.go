// Package planner compiles query text into an optimized IR plan together
// with the storage hints the server uses to select segments.
package planner

import (
	"errors"
	"fmt"
	"time"

	"github.com/lynxbase/lynxdb/pkg/engine/physical"
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/ir/opt"
	"github.com/lynxbase/lynxdb/pkg/model"
	"github.com/lynxbase/lynxdb/pkg/ppl"
	"github.com/lynxbase/lynxdb/pkg/query"
	"github.com/lynxbase/lynxdb/pkg/timerange"
)

// DefaultSource is the index a scan without sources reads.
const DefaultSource = "main"

// Planner compiles queries.
type Planner interface {
	Plan(req PlanRequest) (*PlanResult, error)
}

// PlanRequest is the input to Plan. From and To bound the query by event
// time in addition to any bounds in the query text.
type PlanRequest struct {
	Query string
	From  string
	To    string
}

// PlanResult is a compiled query ready for execution.
type PlanResult struct {
	RawQuery string
	Compiled *query.Compiled
	// Plan is the optimized plan.
	Plan               *ir.Plan
	Hints              *model.QueryHints
	ExternalTimeBounds *model.TimeBounds
	// SkipResultCache is set for plans whose results depend on the time of
	// execution.
	SkipResultCache bool
	CompileDuration time.Duration
	RuleDetails     []opt.Applied
}

// PlanningError is a query the frontend accepted but the planner could
// not turn into an executable plan.
type PlanningError struct {
	Err error
}

func (e *PlanningError) Error() string { return "planning error: " + e.Err.Error() }
func (e *PlanningError) Unwrap() error { return e.Err }

// New returns the PPL planner.
func New() Planner { return pplPlanner{} }

type pplPlanner struct{}

func (pplPlanner) Plan(req PlanRequest) (*PlanResult, error) {
	now := time.Now()
	start := time.Now()
	c, err := query.Compile(req.Query)
	if err != nil {
		if _, ok := ppl.AsError(err); ok {
			return nil, err
		}
		return nil, &PlanningError{Err: err}
	}
	res := &PlanResult{
		RawQuery:        req.Query,
		Compiled:        c,
		Plan:            c.Plan,
		Hints:           physical.PlanHints(c.Plan, DefaultSource, now),
		SkipResultCache: UsesCurrentTime(c.Plan),
		CompileDuration: time.Since(start),
		RuleDetails:     c.Applied,
	}
	if req.From != "" || req.To != "" {
		tr, err := timerange.ParseOptionalRange(req.From, req.To, now)
		if err != nil {
			return nil, &PlanningError{Err: fmt.Errorf("time range: %w", err)}
		}
		if tr != nil {
			res.ExternalTimeBounds = &model.TimeBounds{Earliest: tr.Earliest, Latest: tr.Latest}
		}
	}
	return res, nil
}

// UsesCurrentTime reports whether a plan's result depends on the time it
// runs: relative time bounds or volatile functions such as now().
func UsesCurrentTime(plan *ir.Plan) bool {
	found := false
	ir.WalkPlan(plan.Root, func(n ir.Node) bool {
		if s, ok := n.(*ir.Scan); ok && s.Time != nil {
			for _, b := range []*ir.TimeBound{s.Time.Earliest, s.Time.Latest} {
				if b != nil && b.IsRelative() {
					found = true
				}
			}
		}
		for _, e := range ir.NodeExprs(n) {
			ir.WalkExpr(e, func(x ir.Expr) bool {
				switch v := x.(type) {
				case *ir.RelativeTime:
					found = true
				case *ir.Call:
					if !v.Func.Deterministic() {
						found = true
					}
				}
				return !found
			})
		}
		return !found
	})
	return found
}

// DynamicTimeBounds reports whether request time bounds were given; they may
// be relative to the current time.
func DynamicTimeBounds(from, to string) bool {
	return from != "" || to != ""
}

// IsQueryError reports whether err is a compile or planning error rather
// than a failure to execute.
func IsQueryError(err error) bool {
	if _, ok := ppl.AsError(err); ok {
		return true
	}
	var pe *PlanningError
	return errors.As(err, &pe)
}
