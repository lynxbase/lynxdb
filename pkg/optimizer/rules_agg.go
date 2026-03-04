package optimizer

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

type countStarOptimizationRule struct{}

func (r *countStarOptimizationRule) Name() string { return "CountStarOptimization" }
func (r *countStarOptimizationRule) Description() string {
	return "Answers unfiltered COUNT(*) from segment metadata without scanning"
}
func (r *countStarOptimizationRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	if q.Annotations != nil {
		if _, done := q.Annotations["countStarOnly"]; done {
			return q, false
		}
	}
	// Detect: pipeline is just "stats count" with no preceding filter or group-by.
	// The preceding commands must be only SearchCommand with no filter (i.e., search *)
	// or no commands at all before stats.
	statsIdx := -1
	for i, cmd := range q.Commands {
		if _, ok := cmd.(*spl2.StatsCommand); ok {
			statsIdx = i
		}
	}
	if statsIdx < 0 {
		return q, false
	}
	stats := q.Commands[statsIdx].(*spl2.StatsCommand)
	if len(stats.GroupBy) > 0 {
		return q, false
	}
	if len(stats.Aggregations) != 1 || !strings.EqualFold(stats.Aggregations[0].Func, "count") {
		return q, false
	}
	if len(stats.Aggregations[0].Args) > 0 {
		return q, false // count(field) is different from count
	}
	// Check no filtering or cardinality-changing commands before stats.
	for i := 0; i < statsIdx; i++ {
		switch cmd := q.Commands[i].(type) {
		case *spl2.WhereCommand:
			return q, false
		case *spl2.UnrollCommand:
			return q, false // unroll changes cardinality (1→N rows)
		case *spl2.SearchCommand:
			if cmd.Term != "" || cmd.Expression != nil {
				return q, false // has a search filter
			}
		}
	}
	q.Annotate("countStarOnly", true)

	return q, true
}

type aggregationPushdownRule struct{}

func (r *aggregationPushdownRule) Name() string { return "AggregationPushdown" }
func (r *aggregationPushdownRule) Description() string {
	return "Pushes aggregation into per-segment partial execution"
}
func (r *aggregationPushdownRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	if q.Annotations != nil {
		if _, done := q.Annotations["partialAgg"]; done {
			return q, false
		}
	}

	// Must have a source clause (not a CTE variable).
	if q.Source == nil || q.Source.IsVariable {
		return q, false
	}

	// Find first StatsCommand, allowing SearchCommand and WhereCommand before it.
	if len(q.Commands) == 0 {
		return q, false
	}
	statsIdx := -1
	if _, ok := q.Commands[0].(*spl2.StatsCommand); ok {
		statsIdx = 0
	}
	if statsIdx < 0 {
		return q, false
	}
	stats, ok := q.Commands[statsIdx].(*spl2.StatsCommand)
	if !ok {
		return q, false
	}

	// All aggs must be pushable with no nested eval.
	spec := &pipeline.PartialAggSpec{GroupBy: stats.GroupBy}
	for _, agg := range stats.Aggregations {
		if !pipeline.IsPushableAgg(agg.Func) {
			return q, false
		}
		// Nested eval check: args must be nil (count) or FieldExpr.
		field := ""
		if len(agg.Args) > 0 {
			if _, ok := agg.Args[0].(*spl2.FieldExpr); !ok {
				return q, false
			}
			field = agg.Args[0].String()
		}
		// Compute alias the same way as convertAggs.
		alias := agg.Alias
		if alias == "" {
			if len(agg.Args) > 0 {
				alias = fmt.Sprintf("%s(%s)", agg.Func, agg.Args[0])
			} else {
				alias = agg.Func
			}
		}
		spec.Funcs = append(spec.Funcs, pipeline.PartialAggFunc{
			Name:  agg.Func,
			Field: field,
			Alias: alias,
		})
	}

	q.Annotate("partialAgg", spec)

	return q, true
}

// TransformPartialAggAnnotation is the annotation value for "transformPartialAgg".
// It carries the streamable transform commands and the partial agg spec so the
// server can build per-segment mini-pipelines (transforms → partial agg).
type TransformPartialAggAnnotation struct {
	TransformCommands []spl2.Command
	AggSpec           *pipeline.PartialAggSpec
	// PostStatsCommands are the commands after STATS (sort, head, etc.)
	// that must be applied after merging partials.
	PostStatsCommands []spl2.Command
}

// transformAggPushdownRule extends partial aggregation to handle streamable
// transforms followed by stats (e.g., rex | eval | stats count by x). Each
// segment runs the full transform + stats mini-pipeline in a worker goroutine,
// producing per-segment partial agg groups that are merged identically to the
// existing partialAgg path.
//
// This rule runs AFTER aggregationPushdownRule so the simpler pattern
// (stats with no preceding transforms) takes priority.
type transformAggPushdownRule struct{}

func (r *transformAggPushdownRule) Name() string { return "TransformAggPushdown" }
func (r *transformAggPushdownRule) Description() string {
	return "Pushes streamable transforms + aggregation into per-segment parallel execution"
}

func (r *transformAggPushdownRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	// Skip if already annotated by simpler partialAgg rule or by this rule.
	if q.Annotations != nil {
		if _, done := q.Annotations["partialAgg"]; done {
			return q, false
		}
		if _, done := q.Annotations["transformPartialAgg"]; done {
			return q, false
		}
	}

	// Must have a source clause (not a CTE variable).
	if q.Source == nil || q.Source.IsVariable {
		return q, false
	}

	if len(q.Commands) < 2 {
		return q, false
	}

	// Find STATS command. All commands before it must be streamable transforms.
	statsIdx := -1
	for i, cmd := range q.Commands {
		if _, ok := cmd.(*spl2.StatsCommand); ok {
			statsIdx = i

			break
		}
	}
	// statsIdx=0 handled by existing aggregationPushdownRule; <0 = no stats
	if statsIdx <= 0 {
		return q, false
	}

	// Verify all commands before STATS are stateless, per-row transforms.
	for i := 0; i < statsIdx; i++ {
		if !isStreamableTransform(q.Commands[i]) {
			return q, false
		}
	}

	// Build agg spec (same logic as existing aggregationPushdownRule).
	stats := q.Commands[statsIdx].(*spl2.StatsCommand)
	spec := &pipeline.PartialAggSpec{GroupBy: stats.GroupBy}
	for _, agg := range stats.Aggregations {
		if !pipeline.IsPushableAgg(agg.Func) {
			return q, false
		}
		field := ""
		if len(agg.Args) > 0 {
			if _, ok := agg.Args[0].(*spl2.FieldExpr); !ok {
				return q, false
			}
			field = agg.Args[0].String()
		}
		alias := agg.Alias
		if alias == "" {
			if len(agg.Args) > 0 {
				alias = fmt.Sprintf("%s(%s)", agg.Func, agg.Args[0])
			} else {
				alias = agg.Func
			}
		}
		spec.Funcs = append(spec.Funcs, pipeline.PartialAggFunc{
			Name:  agg.Func,
			Field: field,
			Alias: alias,
		})
	}

	ann := &TransformPartialAggAnnotation{
		TransformCommands: q.Commands[:statsIdx],
		AggSpec:           spec,
	}
	if statsIdx+1 < len(q.Commands) {
		ann.PostStatsCommands = q.Commands[statsIdx+1:]
	}

	q.Annotate("transformPartialAgg", ann)

	return q, true
}

// isStreamableTransform returns true if the command is a stateless, per-row
// transform that can safely execute independently on each segment's data.
func isStreamableTransform(cmd spl2.Command) bool {
	switch cmd.(type) {
	case *spl2.RexCommand, *spl2.EvalCommand, *spl2.WhereCommand,
		*spl2.FieldsCommand, *spl2.RenameCommand, *spl2.FillnullCommand,
		*spl2.BinCommand, *spl2.SearchCommand,
		*spl2.UnpackCommand, *spl2.JsonCommand,
		*spl2.PackJsonCommand:
		return true
	default:
		return false
	}
}

type partialAggregationRule struct{}

func (r *partialAggregationRule) Name() string { return "PartialAggregation" }
func (r *partialAggregationRule) Description() string {
	return "Enables two-phase partial aggregation for distributed execution"
}
func (r *partialAggregationRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	return q, false // Pipeline builder handles per-segment partial aggregation
}

type earlyLimitRule struct{}

func (r *earlyLimitRule) Name() string { return "EarlyLimit" }
func (r *earlyLimitRule) Description() string {
	return "Converts sort+head and sort+tail into TopN and pushes limits through transforms"
}
func (r *earlyLimitRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	changed := false

	for i := 1; i < len(q.Commands); i++ {
		head, isHead := q.Commands[i].(*spl2.HeadCommand)
		if !isHead {
			continue
		}

		prev := q.Commands[i-1]
		// Detect sort + head → replace with TopNCommand.
		if sortCmd, ok := prev.(*spl2.SortCommand); ok {
			topn := &spl2.TopNCommand{
				Fields: sortCmd.Fields,
				Limit:  head.Count,
			}
			// Replace sort+head with single topn command.
			newCmds := make([]spl2.Command, 0, len(q.Commands)-1)
			newCmds = append(newCmds, q.Commands[:i-1]...)
			newCmds = append(newCmds, topn)
			newCmds = append(newCmds, q.Commands[i+1:]...)
			q.Commands = newCmds

			return q, true
		}

		// Push head through non-filtering, non-reordering commands.
		if canPushHeadThrough(prev) {
			q.Commands[i-1] = head
			q.Commands[i] = prev
			changed = true
		}
	}

	// Second pass: sort + tail → TopN with inverted sort directions.
	// sort -x | tail N ≡ sort x | head N ≡ TopN(asc x, N)
	for i := 1; i < len(q.Commands); i++ {
		tail, isTail := q.Commands[i].(*spl2.TailCommand)
		if !isTail {
			continue
		}
		sortCmd, ok := q.Commands[i-1].(*spl2.SortCommand)
		if !ok {
			continue
		}

		// Invert every sort direction: desc→asc, asc→desc.
		inverted := make([]spl2.SortField, len(sortCmd.Fields))
		for j, f := range sortCmd.Fields {
			inverted[j] = spl2.SortField{Name: f.Name, Desc: !f.Desc}
		}
		topn := &spl2.TopNCommand{Fields: inverted, Limit: tail.Count}

		// Replace sort+tail with single topn command.
		newCmds := make([]spl2.Command, 0, len(q.Commands)-1)
		newCmds = append(newCmds, q.Commands[:i-1]...)
		newCmds = append(newCmds, topn)
		newCmds = append(newCmds, q.Commands[i+1:]...)
		q.Commands = newCmds

		return q, true
	}

	return q, changed
}

// canPushHeadThrough returns true if head can safely be pushed before cmd.
// Head can pass through eval, rename, fillnull (non-filtering, non-reordering).
func canPushHeadThrough(cmd spl2.Command) bool {
	switch cmd.(type) {
	case *spl2.EvalCommand:
		return true
	case *spl2.RenameCommand:
		return true
	case *spl2.FillnullCommand:
		return true
	default:
		return false
	}
}

type commonSubexprElimRule struct{}

func (r *commonSubexprElimRule) Name() string { return "CommonSubexprElim" }
func (r *commonSubexprElimRule) Description() string {
	return "Caches repeated subexpressions to avoid redundant evaluation"
}
func (r *commonSubexprElimRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	return q, false // VM cache handles this
}

type strengthReductionRule struct{}

func (r *strengthReductionRule) Name() string { return "StrengthReduction" }
func (r *strengthReductionRule) Description() string {
	return "Replaces expensive operations with cheaper equivalents (x*2 -> x+x)"
}
func (r *strengthReductionRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	changed := false
	for _, cmd := range q.Commands {
		if c, ok := cmd.(*spl2.EvalCommand); ok {
			if c.Expr != nil {
				newExpr, reduced := reduceStrength(c.Expr)
				if reduced {
					c.Expr = newExpr
					changed = true
				}
			}
		}
	}

	return q, changed
}

func reduceStrength(expr spl2.Expr) (spl2.Expr, bool) {
	arith, ok := expr.(*spl2.ArithExpr)
	if !ok {
		return expr, false
	}
	rLit, rOk := arith.Right.(*spl2.LiteralExpr)
	if !rOk {
		return expr, false
	}
	n, err := strconv.ParseInt(rLit.Value, 10, 64)
	if err != nil {
		return expr, false
	}
	// x * 2 → x + x
	if arith.Op == "*" && n == 2 {
		return &spl2.ArithExpr{Left: arith.Left, Op: "+", Right: arith.Left}, true
	}

	return expr, false
}

type regexLiteralExtractionRule struct{}

func (r *regexLiteralExtractionRule) Name() string { return "RegexLiteralExtraction" }
func (r *regexLiteralExtractionRule) Description() string {
	return "Extracts literal strings from regex for pre-filter acceleration"
}
func (r *regexLiteralExtractionRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	if q.Annotations != nil {
		if _, done := q.Annotations["rexPreFilter"]; done {
			return q, false
		}
	}
	var literals []string
	for _, cmd := range q.Commands {
		if rex, ok := cmd.(*spl2.RexCommand); ok {
			lits := extractRegexLiterals(rex.Pattern)
			literals = append(literals, lits...)
		}
	}
	if len(literals) == 0 {
		return q, false
	}
	q.Annotate("rexPreFilter", literals)

	return q, true
}

// extractRegexLiterals walks a regex pattern and extracts non-meta character
// runs of 3+ characters that can be used for pre-filtering.
func extractRegexLiterals(pattern string) []string {
	var result []string
	var current strings.Builder
	i := 0
	for i < len(pattern) {
		c := pattern[i]
		switch c {
		case '.', '*', '+', '?', '(', ')', '[', ']', '{', '}', '|', '^', '$':
			if current.Len() >= 3 {
				result = append(result, current.String())
			}
			current.Reset()
			i++
		case '\\':
			// Escaped character
			if i+1 < len(pattern) {
				next := pattern[i+1]
				// Literal escape: \/ \. \- \_
				if next == '/' || next == '.' || next == '-' || next == '_' ||
					next == '\\' || next == '"' || next == '\'' || next == ':' ||
					next == ' ' || next == '@' {
					current.WriteByte(next)
					i += 2

					continue
				}
			}
			// Metaclass like \d, \w, \s — break literal run.
			if current.Len() >= 3 {
				result = append(result, current.String())
			}
			current.Reset()
			i += 2
		default:
			current.WriteByte(c)
			i++
		}
	}
	if current.Len() >= 3 {
		result = append(result, current.String())
	}

	return result
}

type predicateFusionRule struct{}

func (r *predicateFusionRule) Name() string { return "PredicateFusion" }
func (r *predicateFusionRule) Description() string {
	return "Merges consecutive WHERE commands into a single AND predicate"
}

// Apply merges consecutive WHERE commands into a single WHERE with AND.
// Example: WHERE a>1 | WHERE b<5 → WHERE a>1 AND b<5
// Only fuses when no intervening transform commands (eval, rex, rename)
// create fields referenced by the second predicate.
func (r *predicateFusionRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	if len(q.Commands) < 2 {
		return q, false
	}
	changed := false
	result := make([]spl2.Command, 0, len(q.Commands))
	i := 0
	for i < len(q.Commands) {
		if i+1 < len(q.Commands) {
			fused, ok := tryFusePredicates(q.Commands[i], q.Commands[i+1])
			if ok {
				result = append(result, fused)
				i += 2
				changed = true

				continue
			}
		}
		result = append(result, q.Commands[i])
		i++
	}
	if changed {
		q.Commands = result
	}

	return q, changed
}

// tryFusePredicates tries to merge two consecutive filter commands.
func tryFusePredicates(a, b spl2.Command) (spl2.Command, bool) {
	wa, aIsWhere := a.(*spl2.WhereCommand)
	wb, bIsWhere := b.(*spl2.WhereCommand)
	if aIsWhere && bIsWhere {
		return &spl2.WhereCommand{
			Expr: &spl2.BinaryExpr{Left: wa.Expr, Op: "and", Right: wb.Expr},
		}, true
	}

	return nil, false
}

// tailScanOptimizationRule annotates queries where a terminal `tail N` can be
// served by a reverse (newest-first) scan with early termination at the storage
// layer. This is safe only when no preceding command reorders events or changes
// the event window (e.g., sort, stats, head, join).
type tailScanOptimizationRule struct{}

func (r *tailScanOptimizationRule) Name() string { return "TailScanOptimization" }
func (r *tailScanOptimizationRule) Description() string {
	return "Enables reverse scan with early termination for terminal tail N queries"
}

func (r *tailScanOptimizationRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	if q.Annotations != nil {
		if _, done := q.Annotations["tailScanOptimization"]; done {
			return q, false
		}
	}

	if len(q.Commands) == 0 {
		return q, false
	}

	// Last command must be TailCommand.
	tc, ok := q.Commands[len(q.Commands)-1].(*spl2.TailCommand)
	if !ok || tc.Count <= 0 {
		return q, false
	}

	// Every preceding command must be safe for reverse scan pushdown.
	// Commands that reorder events or change the event window block this optimization.
	for _, cmd := range q.Commands[:len(q.Commands)-1] {
		if !isTailScanSafe(cmd) {
			return q, false
		}
	}

	q.Annotate("tailScanOptimization", tc.Count)

	return q, true
}

// isTailScanSafe returns true if the command preserves event ordering and does
// not change the event window, making reverse scan pushdown safe.
//
// Allowed: search, where, eval, fields, table, rename, rex, bin, fillnull
// — these filter, transform, or project without reordering.
//
// NOT allowed: sort, stats, head, limit, join, eventstats, dedup, transaction,
// streamstats, xyseries, top, rare, timechart, append, multisearch, topn
// — these reorder, materialize, or change the event window.
func isTailScanSafe(cmd spl2.Command) bool {
	switch cmd.(type) {
	case *spl2.SearchCommand, *spl2.WhereCommand, *spl2.EvalCommand,
		*spl2.FieldsCommand, *spl2.TableCommand, *spl2.RenameCommand,
		*spl2.RexCommand, *spl2.BinCommand, *spl2.FillnullCommand,
		*spl2.UnpackCommand, *spl2.JsonCommand, *spl2.PackJsonCommand:
		return true
	default:
		return false
	}
}

type joinReorderingRule struct{}

func (r *joinReorderingRule) Name() string { return "JoinReordering" }
func (r *joinReorderingRule) Description() string {
	return "Selects build/probe side for hash join based on input sizes"
}
func (r *joinReorderingRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	return q, false // Pipeline builder handles build/probe side selection
}
