package optimizer

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/spl2"
)

// Rule is a single optimization rule.
type Rule interface {
	Name() string
	Description() string
	Apply(q *spl2.Query) (*spl2.Query, bool)
}

// RuleDetail describes a single optimizer rule application for profile output.
type RuleDetail struct {
	Name        string
	Description string
	Count       int
}

// FieldStatInfo holds per-field statistics for selectivity estimation.
type FieldStatInfo struct {
	Cardinality int64   // number of distinct values
	TotalCount  int64   // total non-null occurrences
	MinValue    string  // min value as string
	MaxValue    string  // max value as string
	NullFrac    float64 // fraction of null values (0.0 - 1.0)
}

// Optimizer applies rules in fixed-point iteration.
type Optimizer struct {
	rules []Rule
	Stats map[string]int
}

// OptOption configures the optimizer.
type OptOption func(*Optimizer)

// WithCatalog enables MV query rewrite.
func WithCatalog(catalog ViewCatalog) OptOption {
	return func(o *Optimizer) {
		o.rules = append(o.rules, NewMVRewriteRule(catalog))
	}
}

// WithFieldStats enables stat-based predicate reordering.
func WithFieldStats(stats map[string]FieldStatInfo) OptOption {
	return func(o *Optimizer) {
		// Replace the default predicateReorderingRule with one that uses stats.
		for i, r := range o.rules {
			if r.Name() == "PredicateReordering" {
				o.rules[i] = &predicateReorderingRule{stats: stats}

				return
			}
		}
	}
}

// New creates an optimizer with all built-in rules and optional configuration.
func New(opts ...OptOption) *Optimizer {
	o := &Optimizer{
		rules: allRules(),
		Stats: make(map[string]int),
	}
	for _, opt := range opts {
		opt(o)
	}

	return o
}

// Optimize applies all rules until no more changes occur.
func (o *Optimizer) Optimize(q *spl2.Query) *spl2.Query {
	const maxIterations = 10
	changed := true
	iter := 0
	for ; changed && iter < maxIterations; iter++ {
		changed = false
		for _, rule := range o.rules {
			newQ, applied := rule.Apply(q)
			if applied {
				q = newQ
				changed = true
				o.Stats[rule.Name()]++
			}
		}
	}
	if iter == maxIterations && changed {
		slog.Warn("optimizer: iteration cap reached, plan may be suboptimal",
			"max_iterations", maxIterations)
	}

	return q
}

// Explain returns a string describing the applied optimizations.
func (o *Optimizer) Explain() string {
	var sb strings.Builder
	sb.WriteString("=== Applied Rules ===\n")
	for name, count := range o.Stats {
		fmt.Fprintf(&sb, "%s: applied %d time(s)\n", name, count)
	}

	return sb.String()
}

// GetRuleDetails returns details for all rules that fired during optimization.
// Only rules with Stats[name] > 0 are included.
func (o *Optimizer) GetRuleDetails() []RuleDetail {
	if len(o.Stats) == 0 {
		return nil
	}

	// Build name→description index from active rules.
	descMap := make(map[string]string, len(o.rules))
	for _, r := range o.rules {
		descMap[r.Name()] = r.Description()
	}

	details := make([]RuleDetail, 0, len(o.Stats))
	for name, count := range o.Stats {
		details = append(details, RuleDetail{
			Name:        name,
			Description: descMap[name],
			Count:       count,
		})
	}

	return details
}

// TotalRules returns the total number of rules registered in the optimizer.
func (o *Optimizer) TotalRules() int {
	return len(o.rules)
}

func allRules() []Rule {
	return []Rule{
		// Expression simplification.
		&constantFoldingRule{},
		&constantPropagationRule{},
		&predicateSimplificationRule{},
		&negationPushdownRule{},
		&deadCodeEliminationRule{},
		&rangeMergeRule{},
		&sourceORtoINRule{},
		&inListRewriteRule{},
		&flattenNestedAppendRule{},
		// Predicate & projection pushdown.
		&predicatePushdownRule{},
		&predicateReorderingRule{},
		&projectionPushdownRule{},
		&columnPruningRule{},
		&unpackFieldPruningRule{},
		&filterPushdownIntoJoinRule{},
		&unionFilterPushdownRule{},
		// Scan optimization.
		&timeRangePruningRule{},
		&bloomFilterPruningRule{},
		&columnStatsPruningRule{},
		&invertedIndexPruningRule{},
		&partitionPruningRule{},
		&sourceScopeAnnotationRule{},
		// Ordering optimization — must run BEFORE earlyLimitRule/topKAggRule
		// so dead sorts are eliminated before they get fused into TopN commands.
		&removeDeadSortRule{},
		&removeRedundantSortRule{},
		&removeSortOnScanOrderRule{},
		&sortStatsReorderRule{},
		// Aggregation optimization.
		&countStarOptimizationRule{},
		&aggregationPushdownRule{},
		&transformAggPushdownRule{},
		&partialAggregationRule{},
		// topKAggRule must run before earlyLimitRule because earlyLimitRule
		// converts sort+head into TopNCommand, destroying the stats+sort+head
		// pattern that topKAggRule needs. topKAggRule only annotates (no AST
		// mutation), so earlyLimitRule still fires afterward.
		&topKAggRule{},
		&relaxAppendOrderingRule{},
		&earlyLimitRule{},
		// tailScanOptimizationRule annotates terminal tail N queries for reverse
		// scan pushdown. Runs after earlyLimit so head+tail interactions are resolved.
		&tailScanOptimizationRule{},
		// Expression optimization.
		&commonSubexprElimRule{},
		&strengthReductionRule{},
		&regexLiteralExtractionRule{},
		&predicateFusionRule{},
		// Join optimization.
		&joinReorderingRule{},
		&joinSizeEstimationRule{},
	}
}
