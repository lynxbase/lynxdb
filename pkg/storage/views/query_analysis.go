package views

import (
	"fmt"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

// QueryAnalysis holds the result of analyzing an MV query for pipeline splitting.
// It separates the query into insert-time streaming commands, a partial aggregation
// spec, and query-time commands that run on finalized results.
type QueryAnalysis struct {
	// SourceIndex is the FROM clause index name (e.g., "main", "nginx").
	SourceIndex string

	// IsAggregation is true if the query contains a terminal aggregation command
	// (stats, timechart, top, rare).
	IsAggregation bool

	// StreamingCmds are pre-aggregation commands that run at insert time
	// (rex, eval, where, bin, search, fields, rename).
	StreamingCmds []spl2.Command

	// AggSpec describes the partial aggregation to compute at insert time.
	// Nil for projection views.
	AggSpec *pipeline.PartialAggSpec

	// QueryCmds are post-aggregation commands that run at query time
	// (sort, head, table, eval, where after stats, etc.).
	QueryCmds []spl2.Command

	// GroupBy holds the group-by field names from the aggregation command.
	GroupBy []string
}

// AnalyzeQuery parses an SPL2 query string and splits it into insert-time and
// query-time components for materialized view processing.
//
// Pipeline splitting rules:
//   - Walk commands looking for the first terminal aggregation (stats, timechart, top, rare).
//   - Everything before the split point is insert-time (streaming).
//   - The aggregation itself becomes a PartialAggSpec.
//   - Everything after the split point is query-time.
//   - If no aggregation is found, all commands are insert-time (projection view).
//
// Returns an error for unsupported patterns (eventstats, streamstats, transaction,
// join, dedup before stats, unsupported agg functions).
func AnalyzeQuery(query string) (*QueryAnalysis, error) {
	normalized := spl2.NormalizeQuery(query)
	prog, err := spl2.ParseProgram(normalized)
	if err != nil {
		return nil, fmt.Errorf("views.AnalyzeQuery: parse: %w", err)
	}

	if prog.Main == nil {
		return nil, fmt.Errorf("views.AnalyzeQuery: empty query")
	}

	result := &QueryAnalysis{}

	// Extract source index from FROM clause.
	if prog.Main.Source != nil {
		if prog.Main.Source.IsVariable {
			return nil, fmt.Errorf("views.AnalyzeQuery: variable sources ($%s) not supported for MV", prog.Main.Source.Index)
		}
		if prog.Main.Source.IsGlob {
			return nil, fmt.Errorf("views.AnalyzeQuery: glob sources not supported for MV; use an explicit index name")
		}
		result.SourceIndex = prog.Main.Source.Index
	}

	// Find the first terminal aggregation command and split.
	splitIdx := -1
	for i, cmd := range prog.Main.Commands {
		// Reject unsupported commands before we find the split point.
		if err := validateCommandForMV(cmd); err != nil {
			return nil, err
		}

		if isTerminalAggregation(cmd) {
			splitIdx = i

			break
		}
	}

	if splitIdx == -1 {
		// No aggregation found — projection view. All commands are streaming.
		result.IsAggregation = false
		result.StreamingCmds = prog.Main.Commands

		return result, nil
	}

	// Split: [0, splitIdx) = streaming, [splitIdx] = agg, (splitIdx, end) = query-time.
	result.IsAggregation = true
	result.StreamingCmds = prog.Main.Commands[:splitIdx]

	aggCmd := prog.Main.Commands[splitIdx]
	spec, extraStreaming, queryCmds, groupBy, err := buildAggSpec(aggCmd)
	if err != nil {
		return nil, err
	}

	result.AggSpec = spec
	result.GroupBy = groupBy

	// Append extra streaming commands from the aggregation decomposition.
	// For example, timechart injects a BinCommand to bucket _time before
	// partial aggregation groups by the bucketed _time.
	if len(extraStreaming) > 0 {
		result.StreamingCmds = append(result.StreamingCmds, extraStreaming...)
	}

	// Everything after the split point is query-time, prepended by any
	// implicit commands from the aggregation decomposition (e.g., xyseries
	// from timechart, sort+head from top).
	result.QueryCmds = append(queryCmds, prog.Main.Commands[splitIdx+1:]...)

	return result, nil
}

// isTerminalAggregation returns true for commands that perform terminal
// aggregation and can be split into partial agg + merge.
func isTerminalAggregation(cmd spl2.Command) bool {
	switch cmd.(type) {
	case *spl2.StatsCommand, *spl2.TimechartCommand, *spl2.TopCommand, *spl2.RareCommand:
		return true
	default:
		return false
	}
}

// validateCommandForMV checks if a command is allowed in an MV query.
// Returns a descriptive error for unsupported commands.
func validateCommandForMV(cmd spl2.Command) error {
	switch cmd.(type) {
	case *spl2.EventstatsCommand:
		return fmt.Errorf("views.AnalyzeQuery: eventstats not supported for MV; use stats + join or a projection view")
	case *spl2.StreamstatsCommand:
		return fmt.Errorf("views.AnalyzeQuery: streamstats not supported for MV (order-dependent running aggregate)")
	case *spl2.TransactionCommand:
		return fmt.Errorf("views.AnalyzeQuery: transaction not supported for MV (requires cross-event session state)")
	case *spl2.JoinCommand:
		return fmt.Errorf("views.AnalyzeQuery: join not supported for MV (cross-source correlation)")
	case *spl2.DedupCommand:
		return fmt.Errorf("views.AnalyzeQuery: dedup before aggregation not supported for MV " +
			"(per-batch dedup is semantically incorrect; use dc() or a projection view)")
	default:
		return nil
	}
}

// validateAggForMV checks if an aggregation function is supported for MV partial state.
func validateAggForMV(agg spl2.AggExpr) error {
	fn := strings.ToLower(agg.Func)

	// Check for eval expression arguments: count(eval(status>=500)).
	// AggExpr.Args[0] is not a simple FieldExpr → reject.
	if len(agg.Args) > 0 {
		if _, ok := agg.Args[0].(*spl2.FieldExpr); !ok {
			// Allow LiteralExpr (count("*") etc.) but reject function calls.
			if _, isLiteral := agg.Args[0].(*spl2.LiteralExpr); !isLiteral {
				return fmt.Errorf("views.AnalyzeQuery: materialized views do not support expression arguments in %s(); "+
					"pre-compute with eval: | eval err=if(status>=500,1,0) | stats sum(err) as errors", fn)
			}
		}
	}

	switch fn {
	case "count", "sum", "avg", "min", "max", "dc":
		return nil
	case "values":
		return fmt.Errorf("views.AnalyzeQuery: values() not supported for MV (unbounded memory); use dc() or pre-filter")
	case "earliest", "latest":
		return fmt.Errorf("views.AnalyzeQuery: earliest()/latest() not supported for MV (needs {value,_time} state)")
	case "stdev":
		return fmt.Errorf("views.AnalyzeQuery: stdev() not supported for MV (needs Welford's {sum,sum_sq,count} state)")
	case "perc50", "perc75", "perc90", "perc95", "perc99":
		return fmt.Errorf("views.AnalyzeQuery: percentile functions not supported for MV (needs t-digest/DDSketch state)")
	default:
		return fmt.Errorf("views.AnalyzeQuery: unsupported aggregation function %q for materialized views", fn)
	}
}

// buildAggSpec converts a terminal aggregation command into a PartialAggSpec
// plus any implicit query-time commands and extra streaming commands.
// Returns (spec, extraStreaming, queryCmds, groupBy, err).
//
// ExtraStreaming contains commands that must be appended to StreamingCmds
// at the caller. For example, timechart injects a BinCommand to bucket _time
// before partial aggregation groups by _time.
func buildAggSpec(cmd spl2.Command) (*pipeline.PartialAggSpec, []spl2.Command, []spl2.Command, []string, error) {
	switch c := cmd.(type) {
	case *spl2.StatsCommand:
		spec, queryCmds, groupBy, err := buildStatsSpec(c)

		return spec, nil, queryCmds, groupBy, err
	case *spl2.TimechartCommand:
		return buildTimechartSpec(c)
	case *spl2.TopCommand:
		spec, queryCmds, groupBy, err := buildTopSpec(c)

		return spec, nil, queryCmds, groupBy, err
	case *spl2.RareCommand:
		spec, queryCmds, groupBy, err := buildRareSpec(c)

		return spec, nil, queryCmds, groupBy, err
	default:
		return nil, nil, nil, nil, fmt.Errorf("views.AnalyzeQuery: unexpected aggregation command: %T", cmd)
	}
}

// buildStatsSpec converts a StatsCommand into a PartialAggSpec.
func buildStatsSpec(c *spl2.StatsCommand) (*pipeline.PartialAggSpec, []spl2.Command, []string, error) {
	funcs, err := convertAggExprs(c.Aggregations)
	if err != nil {
		return nil, nil, nil, err
	}

	spec := &pipeline.PartialAggSpec{
		GroupBy: c.GroupBy,
		Funcs:   funcs,
	}

	return spec, nil, c.GroupBy, nil
}

// buildTimechartSpec decomposes timechart into bin + stats + implicit sort.
// Insert-time: returns BinCommand as extraStreaming to bucket _time before agg.
// AggSpec: stats <aggs> by _time [, groupBy...].
// Query-time: implicit sort by _time (timechart output is time-ordered).
func buildTimechartSpec(c *spl2.TimechartCommand) (*pipeline.PartialAggSpec, []spl2.Command, []spl2.Command, []string, error) {
	funcs, err := convertAggExprs(c.Aggregations)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	groupBy := append([]string{"_time"}, c.GroupBy...)

	spec := &pipeline.PartialAggSpec{
		GroupBy: groupBy,
		Funcs:   funcs,
	}

	// BinCommand must run at insert time (streaming) so _time is bucketed
	// before partial aggregation groups by it. Without this, each event's
	// nanosecond-precision _time creates its own group — zero aggregation.
	extraStreaming := []spl2.Command{
		&spl2.BinCommand{Field: "_time", Span: c.Span},
	}

	// Timechart implicitly sorts by _time at query time.
	queryCmds := []spl2.Command{
		&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time", Desc: false}}},
	}

	return spec, extraStreaming, queryCmds, groupBy, nil
}

// buildTopSpec decomposes top into stats count + sort desc + head N.
func buildTopSpec(c *spl2.TopCommand) (*pipeline.PartialAggSpec, []spl2.Command, []string, error) {
	groupBy := []string{c.Field}
	if c.ByField != "" {
		groupBy = append(groupBy, c.ByField)
	}

	spec := &pipeline.PartialAggSpec{
		GroupBy: groupBy,
		Funcs:   []pipeline.PartialAggFunc{{Name: "count", Alias: "count"}},
	}

	n := c.N
	if n <= 0 {
		n = 10
	}

	queryCmds := []spl2.Command{
		&spl2.SortCommand{Fields: []spl2.SortField{{Name: "count", Desc: true}}},
		&spl2.HeadCommand{Count: n},
	}

	return spec, queryCmds, groupBy, nil
}

// buildRareSpec decomposes rare into stats count + sort asc + head N.
func buildRareSpec(c *spl2.RareCommand) (*pipeline.PartialAggSpec, []spl2.Command, []string, error) {
	groupBy := []string{c.Field}
	if c.ByField != "" {
		groupBy = append(groupBy, c.ByField)
	}

	spec := &pipeline.PartialAggSpec{
		GroupBy: groupBy,
		Funcs:   []pipeline.PartialAggFunc{{Name: "count", Alias: "count"}},
	}

	n := c.N
	if n <= 0 {
		n = 10
	}

	queryCmds := []spl2.Command{
		&spl2.SortCommand{Fields: []spl2.SortField{{Name: "count", Desc: false}}},
		&spl2.HeadCommand{Count: n},
	}

	return spec, queryCmds, groupBy, nil
}

// convertAggExprs validates and converts SPL2 AggExpr slices to PartialAggFunc slices.
func convertAggExprs(aggs []spl2.AggExpr) ([]pipeline.PartialAggFunc, error) {
	funcs := make([]pipeline.PartialAggFunc, 0, len(aggs))
	for _, agg := range aggs {
		if err := validateAggForMV(agg); err != nil {
			return nil, err
		}

		alias := agg.Alias
		if alias == "" {
			if len(agg.Args) > 0 {
				alias = fmt.Sprintf("%s(%s)", agg.Func, agg.Args[0])
			} else {
				alias = agg.Func
			}
		}

		field := ""
		if len(agg.Args) > 0 {
			field = agg.Args[0].String()
		}

		funcs = append(funcs, pipeline.PartialAggFunc{
			Name:  strings.ToLower(agg.Func),
			Field: field,
			Alias: alias,
		})
	}

	return funcs, nil
}
