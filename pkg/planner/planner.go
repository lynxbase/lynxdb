// Package planner provides a thin wrapper around the LynxFlow parser,
// desugarer, semantic analyzer, and logical optimizer. It presents the
// same Planner interface that the rest of the server stack expects.
//
// RFC-002 Phase 10: this package was previously the SPL2 planner.
// It now delegates entirely to the LynxFlow pipeline.
package planner

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/logical"
	"github.com/lynxbase/lynxdb/pkg/logical/opt"
	"github.com/lynxbase/lynxdb/pkg/lynxflow/ast"
	"github.com/lynxbase/lynxdb/pkg/lynxflow/desugar"
	"github.com/lynxbase/lynxdb/pkg/lynxflow/lint"
	"github.com/lynxbase/lynxdb/pkg/lynxflow/parser"
	"github.com/lynxbase/lynxdb/pkg/lynxflow/sema"
	"github.com/lynxbase/lynxdb/pkg/model"
	"github.com/lynxbase/lynxdb/pkg/timerange"
)

// Planner parses and optimizes queries.
type Planner interface {
	Plan(req PlanRequest) (*PlanResult, error)
}

// PlanRequest is the input to Plan.
type PlanRequest struct {
	Query string
	From  string
	To    string
}

// PlanResult is the output of Plan.
type PlanResult struct {
	RawQuery           string
	Program            *logical.Plan
	Hints              *model.QueryHints
	ExternalTimeBounds *model.TimeBounds
	ResultType         string
	SkipResultCache    bool
	ParseDuration      time.Duration
	OptimizeDuration   time.Duration
	RuleDetails        []opt.Applied
	TotalRules         int
	OptimizerStats     map[string]int
	Count              int // for tail: requested event count

	// Lints carries advisory diagnostics from sema.Analyze and lint.Run.
	Lints []model.QueryLint

	// Rewrites carries desugaring rewrites from desugar.Desugar.
	Rewrites []model.QueryRewrite
}

// ParseError represents a query parse error.
type ParseError struct {
	Message    string
	Suggestion string
	Diag       *parser.Diag // full first error diagnostic (nil if unavailable)
}

func (e *ParseError) Error() string { return e.Message }

// IsParseError returns true if err wraps a ParseError.
func IsParseError(err error) bool {
	var pe *ParseError
	return errors.As(err, &pe)
}

// TailValidationError represents a tail query validation error.
type TailValidationError struct {
	Message string
}

func (e *TailValidationError) Error() string { return e.Message }

// ValidateForTail checks whether a plan is valid for live tail.
// Blocking (accumulator) stages like stats, sort, and top cannot operate on
// an unbounded live stream, so they are rejected.
func ValidateForTail(plan *logical.Plan) error {
	if plan == nil || plan.Root == nil {
		return nil
	}
	if cmd := findBlockingStage(plan.Root); cmd != "" {
		return &TailValidationError{
			Message: fmt.Sprintf("command %q is not supported in live tail (it requires all data before producing output)", cmd),
		}
	}
	return nil
}

// findBlockingStage walks the logical plan tree and returns the name of the
// first non-streaming (accumulator) stage, or "" if all stages are streaming.
func findBlockingStage(n logical.Node) string {
	if n == nil {
		return ""
	}
	switch nd := n.(type) {
	case *logical.Aggregate:
		if nd.Window == nil {
			return "stats"
		}
	case *logical.Sort:
		return "sort"
	case *logical.TopK:
		return "top"
	}
	for _, child := range n.Children() {
		if cmd := findBlockingStage(child); cmd != "" {
			return cmd
		}
	}
	return ""
}

// DynamicTimeBounds returns true when from/to contain relative time syntax.
func DynamicTimeBounds(from, to string) bool {
	return from != "" || to != "" /* RFC-002: simplified */
}

// QueryUsesDynamicTimeSyntax returns true for queries containing now() or similar.
func QueryUsesDynamicTimeSyntax(_ string) bool {
	return false
}

// New creates a new Planner backed by the LynxFlow pipeline.
func New() Planner {
	return &lynxFlowPlanner{}
}

type lynxFlowPlanner struct{}

func (p *lynxFlowPlanner) Plan(req PlanRequest) (*PlanResult, error) {
	parseStart := time.Now()

	q, diags := parser.Parse(req.Query)
	for _, d := range diags {
		if d.Severity == parser.SeverityError {
			diagCopy := d
			return nil, &ParseError{
				Message:    d.Message,
				Suggestion: d.Suggestion,
				Diag:       &diagCopy,
			}
		}
	}

	// Run pre-desugar lint rules (e.g. LF09 shortcut detection) on the
	// parsed AST before desugaring expands sugar stages. This prevents
	// false positives: users who write `top 5 service` would otherwise
	// be flagged because the desugarer expands it to the long form.
	preDesugarLints := lint.RunPreDesugar(q)

	desugared, dsRewrites := desugar.Desugar(q, desugar.Options{DefaultSource: "main"})
	parseDuration := time.Since(parseStart)

	optStart := time.Now()
	plan, lowerDiags := logical.Lower(desugared, logical.Options{DefaultSource: "main"})
	for _, d := range lowerDiags {
		if d.Severity == parser.SeverityError {
			diagCopy := d
			return nil, &ParseError{
				Message: d.Message,
				Diag:    &diagCopy,
			}
		}
	}

	rt := detectResultType(plan)

	plan, applied := opt.Optimize(plan)
	optimizeDuration := time.Since(optStart)

	// Build hints from the logical plan pushdown.
	hints := hintsFromPlan(plan)

	// Apply external time bounds.
	var externalTB *model.TimeBounds
	if req.From != "" || req.To != "" {
		tr, trErr := timerange.ParseOptionalRange(req.From, req.To, time.Now())
		if trErr == nil && tr != nil {
			externalTB = &model.TimeBounds{Earliest: tr.Earliest, Latest: tr.Latest}
		}
	}

	stats := make(map[string]int)
	totalRules := 0
	for _, a := range applied {
		stats[a.Rule] += a.Count
		totalRules += a.Count
	}

	// Collect desugar rewrites.
	var rewrites []model.QueryRewrite
	for _, rw := range dsRewrites {
		rewrites = append(rewrites, model.QueryRewrite{
			Before: rw.Before,
			After:  rw.After,
			Reason: rw.Reason,
		})
	}
	for _, rw := range plan.SchemaRewrites {
		rewrites = append(rewrites, model.QueryRewrite{
			Before: rw.Before,
			After:  rw.After,
			Reason: rw.Reason,
		})
	}

	// Run sema + lint on the desugared AST to collect advisory lints.
	var lints []model.QueryLint
	semaResult := sema.Analyze(desugared, sema.MapCatalog{})
	for _, d := range semaResult.Diags {
		if d.Severity == parser.SeverityWarning {
			lints = append(lints, model.QueryLint{
				Code:     string(d.Code),
				Message:  d.Message,
				Position: d.Span.Start,
			})
		}
	}
	for _, l := range lint.Run(desugared) {
		lints = append(lints, model.QueryLint{
			Code:     l.Code,
			Message:  l.Message,
			Reason:   l.Reason,
			Position: l.Span.Start,
		})
	}
	// Merge pre-desugar lints (e.g. LF09 shortcut detection).
	for _, l := range preDesugarLints {
		lints = append(lints, model.QueryLint{
			Code:     l.Code,
			Message:  l.Message,
			Reason:   l.Reason,
			Position: l.Span.Start,
		})
	}

	return &PlanResult{
		RawQuery:           req.Query,
		Program:            plan,
		Hints:              hints,
		ExternalTimeBounds: externalTB,
		ResultType:         rt,
		ParseDuration:      parseDuration,
		OptimizeDuration:   optimizeDuration,
		RuleDetails:        applied,
		TotalRules:         totalRules,
		OptimizerStats:     stats,
		Lints:              lints,
		Rewrites:           rewrites,
	}, nil
}

// hintsFromPlan extracts QueryHints from the logical plan's Scan pushdown.
// It maps multi-source patterns, time bounds, bloom/raw terms, columns,
// field predicates, reverse scan, and limit hints from the logical tree.
func hintsFromPlan(plan *logical.Plan) *model.QueryHints {
	hints := &model.QueryHints{}
	if plan == nil || plan.Root == nil {
		return hints
	}

	// Walk tree to find the first Scan node — its pushdown carries storage hints.
	var scan *logical.Scan
	walkNodes(plan.Root, func(n logical.Node) {
		if scan != nil {
			return
		}
		if s, ok := n.(*logical.Scan); ok {
			scan = s
		}
	})
	if scan == nil {
		return hints
	}

	// --- Source scope ---
	mapSourceScope(scan, hints)

	// --- Time bounds ---
	// Time bounds may come from two places:
	// 1. Scan.TimeRange — from bracket syntax: from main[-1h]
	// 2. Scan.Pushdown.TimeBounds — from optimizer time pushdown
	// We try both, preferring whichever resolves to concrete timestamps.
	if scan.TimeRange != nil {
		tb := resolveTimeBoundsFromExprs(scan.TimeRange)
		if tb != nil {
			hints.TimeBounds = tb
		}
	}
	if scan.Pushdown.TimeBounds != nil {
		tb := resolveTimeBoundsFromExprs(scan.Pushdown.TimeBounds)
		if tb != nil {
			if hints.TimeBounds == nil {
				hints.TimeBounds = tb
			} else {
				// Intersect: keep tightest bounds.
				if !tb.Earliest.IsZero() &&
					(hints.TimeBounds.Earliest.IsZero() || tb.Earliest.After(hints.TimeBounds.Earliest)) {
					hints.TimeBounds.Earliest = tb.Earliest
				}
				if !tb.Latest.IsZero() &&
					(hints.TimeBounds.Latest.IsZero() || tb.Latest.Before(hints.TimeBounds.Latest)) {
					hints.TimeBounds.Latest = tb.Latest
				}
			}
		}
	}

	// --- Search terms ---
	// Merge BloomTerms and RawTerms, dedup and lowercase to match inverted index
	// tokenizer behavior (the tokenizer lowercases all indexed tokens).
	hints.SearchTerms = mergeAndLowerTerms(scan.Pushdown.BloomTerms, scan.Pushdown.RawTerms)

	// --- Token globs ---
	// Already lowercased by the optimizer (extractHasGlobPattern). They are
	// served by FST term-dictionary expansion and MUST stay out of
	// SearchTerms: bloom filters and exact FST lookups on a glob pattern
	// would falsely prune segments.
	hints.TokenGlobs = append([]string(nil), scan.Pushdown.TokenGlobs...)

	// --- Required columns ---
	// nil means "all columns" — preserve nil explicitly.
	if scan.Pushdown.Columns != nil {
		hints.RequiredCols = append([]string(nil), scan.Pushdown.Columns...)
	}

	// --- Field predicates ---
	// Convert literal-only pushdown predicates to model types for segment
	// pruning. Non-literal predicates are skipped (correctness over pruning).
	for _, expr := range scan.Pushdown.FieldPredicates {
		convertFieldPredicate(expr, hints)
	}

	// --- Reverse scan ---
	hints.ReverseScan = scan.Reverse

	// --- Limit pushdown ---
	// Only push a limit hint when a Limit node sits directly above a chain
	// of streaming-only, non-filtering nodes (Project/Extend/Scan). This
	// ensures early-stop does not truncate rows that would otherwise be
	// filtered or reordered by intermediate stages.
	hints.Limit = extractLimitHint(plan.Root)

	return hints
}

// mapSourceScope populates source scope fields on hints from the Scan's Sources.
func mapSourceScope(scan *logical.Scan, hints *model.QueryHints) {
	if len(scan.Sources) == 0 {
		return
	}

	// Classify sources.
	var names []string
	var globs []string
	var excludeGlobs []string
	hasStar := false
	hasCTE := false

	for _, src := range scan.Sources {
		switch src.Kind {
		case ast.SourceStar:
			hasStar = true
		case ast.SourceName:
			names = append(names, src.Name)
		case ast.SourceGlob:
			globs = append(globs, src.Pattern)
		case ast.SourceNegated:
			excludeGlobs = append(excludeGlobs, src.Pattern)
		case ast.SourceCTE:
			hasCTE = true
		}
	}

	hints.SourceExcludeGlobs = excludeGlobs

	if hasStar {
		hints.SourceScopeType = model.SourceScopeAll
		return
	}

	if hasCTE && len(names) == 0 && len(globs) == 0 {
		// Pure CTE reference — no storage source scope.
		return
	}

	// Glob sources.
	if len(globs) > 0 {
		if len(globs) == 1 && len(names) == 0 {
			hints.SourceScopeType = model.SourceScopeGlob
			hints.SourceScopePattern = globs[0]
			hints.SourceGlob = globs[0]
		} else {
			// Multiple globs or mixed names+globs.
			hints.SourceIncludeGlobs = globs
			if len(names) > 0 {
				hints.SourceScopeType = model.SourceScopeList
				hints.SourceScopeSources = names
				hints.SourceIndices = names
			} else {
				hints.SourceScopeType = model.SourceScopeGlob
				hints.SourceScopePattern = globs[0]
			}
		}
		return
	}

	// Named sources only.
	switch len(names) {
	case 0:
		// Nothing to set.
	case 1:
		hints.IndexName = names[0]
		hints.SourceScopeType = model.SourceScopeSingle
		hints.SourceScopeSources = names
	default:
		hints.SourceScopeType = model.SourceScopeList
		hints.SourceScopeSources = names
		hints.SourceIndices = names
	}
}

// resolveTimeBoundsFromExprs attempts to extract concrete time.Time values
// from the logical TimeBounds Start/End AST expressions. Only literal
// timestamps and durations (relative to "now") are resolved.
func resolveTimeBoundsFromExprs(tb *logical.TimeBounds) *model.TimeBounds {
	if tb == nil {
		return nil
	}
	now := time.Now()
	var earliest, latest time.Time

	if tb.Start != nil {
		if t, ok := resolveTimeExpr(tb.Start, now); ok {
			earliest = t
		}
	}
	if tb.End != nil {
		if t, ok := resolveTimeExpr(tb.End, now); ok {
			latest = t
		}
	}

	if earliest.IsZero() && latest.IsZero() {
		return nil
	}
	return &model.TimeBounds{Earliest: earliest, Latest: latest}
}

// resolveTimeExpr tries to resolve an AST expression to a time.Time.
// Handles negative duration literals (e.g., -1h → now - 1h) and
// direct timestamp values.
func resolveTimeExpr(expr ast.Expr, now time.Time) (time.Time, bool) {
	switch e := expr.(type) {
	case *ast.Literal:
		switch e.Kind {
		case ast.LitDuration:
			if d, ok := e.Value.(time.Duration); ok {
				return now.Add(d), true
			}
		case ast.LitInt:
			// Epoch seconds or nanos — skip, too ambiguous.
		case ast.LitString:
			// Could be an ISO timestamp, but parsing here would add
			// complexity. Leave for external time bounds.
		}
	case *ast.Unary:
		if e.Op == ast.OpNeg {
			if lit, ok := e.Operand.(*ast.Literal); ok && lit.Kind == ast.LitDuration {
				if d, okD := lit.Value.(time.Duration); okD {
					return now.Add(-d), true
				}
			}
		}
	}
	return time.Time{}, false
}

// mergeAndLowerTerms merges two term slices, deduplicates, and lowercases.
func mergeAndLowerTerms(a, b []string) []string {
	if len(a) == 0 && len(b) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(a)+len(b))
	var out []string
	for _, s := range a {
		low := strings.ToLower(s)
		if _, ok := seen[low]; !ok {
			seen[low] = struct{}{}
			out = append(out, low)
		}
	}
	for _, s := range b {
		low := strings.ToLower(s)
		if _, ok := seen[low]; !ok {
			seen[low] = struct{}{}
			out = append(out, low)
		}
	}
	return out
}

// convertFieldPredicate inspects a pushdown field predicate AST expression
// and converts it to model types when it is a safe, literal-only pattern.
// Patterns handled:
//   - field == literal   → FieldPredicate
//   - field op numeric   → RangePredicate (for <, <=, >, >=)
//   - field in [lit...]  → InPredicate
//
// Non-literal or complex expressions are silently skipped (correctness over pruning).
func convertFieldPredicate(expr ast.Expr, hints *model.QueryHints) {
	switch e := expr.(type) {
	case *ast.Binary:
		field, fieldOK := identName(e.Left)
		if !fieldOK {
			return
		}
		litVal, litOK := literalString(e.Right)
		if !litOK {
			return
		}

		switch e.Op {
		case ast.OpEq:
			hints.FieldPredicates = append(hints.FieldPredicates, model.FieldPredicate{
				Field: field, Op: "=", Value: litVal,
			})
		case ast.OpNotEq:
			hints.FieldPredicates = append(hints.FieldPredicates, model.FieldPredicate{
				Field: field, Op: "!=", Value: litVal,
			})
		case ast.OpLt:
			hints.RangePredicates = append(hints.RangePredicates, model.RangePredicate{
				Field: field, Max: litVal, MaxInclusive: false,
			})
		case ast.OpLtEq:
			hints.RangePredicates = append(hints.RangePredicates, model.RangePredicate{
				Field: field, Max: litVal, MaxInclusive: true,
			})
		case ast.OpGt:
			hints.RangePredicates = append(hints.RangePredicates, model.RangePredicate{
				Field: field, Min: litVal, MinInclusive: false,
			})
		case ast.OpGtEq:
			hints.RangePredicates = append(hints.RangePredicates, model.RangePredicate{
				Field: field, Min: litVal, MinInclusive: true,
			})
		}

	case *ast.In:
		field, fieldOK := identName(e.LHS)
		if !fieldOK {
			return
		}
		arr, arrOK := e.RHS.(*ast.Array)
		if !arrOK {
			return
		}
		var values []string
		for _, elem := range arr.Elems {
			v, vOK := literalString(elem)
			if !vOK {
				return // skip entire predicate if any element is non-literal
			}
			values = append(values, v)
		}
		hints.InPredicates = append(hints.InPredicates, model.InPredicate{
			Field: field, Values: values,
		})
	}
}

// identName extracts a field name from an *ast.Ident.
func identName(e ast.Expr) (string, bool) {
	if id, ok := e.(*ast.Ident); ok {
		return id.Name, true
	}
	return "", false
}

// literalString extracts a string representation from an *ast.Literal.
func literalString(e ast.Expr) (string, bool) {
	lit, ok := e.(*ast.Literal)
	if !ok {
		return "", false
	}
	switch lit.Kind {
	case ast.LitString, ast.LitRawString:
		if s, sOK := lit.Value.(string); sOK {
			return s, true
		}
	case ast.LitInt:
		if v, vOK := lit.Value.(int64); vOK {
			return fmt.Sprintf("%d", v), true
		}
	case ast.LitFloat:
		if v, vOK := lit.Value.(float64); vOK {
			return fmt.Sprintf("%g", v), true
		}
	case ast.LitBool:
		if v, vOK := lit.Value.(bool); vOK {
			if v {
				return "true", true
			}
			return "false", true
		}
	}
	return "", false
}

// extractLimitHint walks from the plan root downward. If a Limit node
// sits directly above a chain of streaming-only, non-filtering nodes
// (Project, Extend) leading to a Scan, the limit is safe to push down.
func extractLimitHint(root logical.Node) int {
	// Walk top-down: root may be Limit(Project(Scan)) etc.
	lim, ok := root.(*logical.Limit)
	if !ok || lim.Tail {
		return 0
	}
	n := lim.Input
	for n != nil {
		switch nd := n.(type) {
		case *logical.Project:
			n = nd.Input
		case *logical.Extend:
			n = nd.Input
		case *logical.Scan:
			return int(lim.N)
		default:
			return 0 // intervening non-streamable node
		}
	}
	return 0
}

func walkNodes(n logical.Node, f func(logical.Node)) {
	if n == nil {
		return
	}
	f(n)
	for _, child := range n.Children() {
		walkNodes(child, f)
	}
}

func detectResultType(plan *logical.Plan) string {
	if plan == nil || plan.Root == nil {
		return "events"
	}
	return detectNodeResultType(plan.Root)
}

func detectNodeResultType(n logical.Node) string {
	switch nd := n.(type) {
	case *logical.Aggregate:
		if nd.Window != nil {
			return "events" // windowed = events
		}
		return "aggregate"
	case *logical.TopK:
		return "aggregate"
	case *logical.Describe:
		return "aggregate"
	case *logical.Limit:
		return detectNodeResultType(nd.Input)
	case *logical.Offset:
		return detectNodeResultType(nd.Input)
	case *logical.Sort:
		return detectNodeResultType(nd.Input)
	case *logical.Project:
		return detectNodeResultType(nd.Input)
	default:
		return "events"
	}
}

// Utility for compile-time check.
var _ Planner = (*lynxFlowPlanner)(nil)

// Error wrapping for parse errors.
func FormatParseError(err error, _ string) string {
	return fmt.Sprintf("parse error: %v", err)
}

func scanSourceName(scan *logical.Scan) string {
	if len(scan.Sources) == 0 {
		return ""
	}
	return scan.Sources[0].Name
}
