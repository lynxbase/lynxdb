package columnar

import (
	"context"
	"math"
	"time"

	"github.com/lynxbase/lynxdb/pkg/ir"
)

// QueryPage reads up to bound output rows. If a job needs the complete
// result after its request times out, accept returns false and execution
// continues against the same pinned sources.
func (db *DB) QueryPage(ctx context.Context, plan *ir.Plan, opts Options, bound int, accept func() bool) (*Result, error) {
	if opts.Now.IsZero() {
		opts.Now = time.Now()
	}
	page, capRows := pagePlan(plan, bound)
	if page == nil {
		return db.Query(ctx, plan, opts)
	}
	sources, err := db.querySources(ctx, plan, opts.DefaultSource)
	defer sources.release()
	if err != nil {
		return nil, err
	}
	res, err := Run(ctx, page, sources, opts)
	if err != nil || res.Len() < bound {
		return res, err
	}
	res.partial = true
	if res.table != nil && res.table.MatchedAll {
		res.total = int(min(res.table.Stats.RowsMatched, capRows))
		res.partial = false
	}
	if res.partial || res.total > res.Len() {
		if accept != nil && !accept() {
			return Run(ctx, plan, sources, opts)
		}
	}
	return res, nil
}

// pagePlan bounds the output of a single event pipeline. capRows is its
// original maximum after PPL head. Aggregates and combining commands keep
// their complete result, including its count.
func pagePlan(plan *ir.Plan, bound int) (*ir.Plan, int64) {
	if bound <= 0 {
		return nil, 0
	}
	capRows := int64(math.MaxInt64)
	for n := plan.Root; ; n = n.Inputs()[0] {
		switch x := n.(type) {
		case *ir.Limit:
			if x.Offset != 0 {
				return nil, 0
			}
			if x.Count >= 0 {
				capRows = min(capRows, x.Count)
			}
		case *ir.TopN:
			if x.Offset != 0 {
				return nil, 0
			}
			capRows = min(capRows, x.Count)
		case *ir.Project, *ir.Filter, *ir.Compute, *ir.RenameColumns, *ir.Sort, *ir.Extract, *ir.FillNull:
		case *ir.Scan:
			if capRows <= int64(bound) {
				return nil, 0
			}
			if s, ok := plan.Root.(*ir.Sort); ok {
				return &ir.Plan{Root: &ir.TopN{Input: s.Input, Keys: s.Keys, Count: int64(bound)}}, capRows
			}
			return &ir.Plan{Root: &ir.Limit{Input: plan.Root, Count: int64(bound)}}, capRows
		default:
			return nil, 0
		}
	}
}

// Total returns the number of rows before API pagination and whether the
// count is exact. An early stop reports the number known to exist.
func (r *Result) Total() (int, bool) {
	return max(r.total, r.Len()), !r.partial
}
