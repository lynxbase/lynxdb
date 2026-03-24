package optimizer

import (
	"fmt"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/spl2"
)

// sortStatsReorderRule rewrites sort-before-stats into stats-then-sort when
// the sort fields are a prefix of the group-by fields. This is vastly cheaper
// because stats produces one row per group (often orders of magnitude fewer
// than input rows), so the sort operates on the aggregated result instead of
// materializing all input rows.
//
// The rewrite fires only when:
//  1. Sort fields are a prefix of stats GroupBy fields.
//  2. The next consumer after stats is NOT another sort that would override.
//  3. Only order-preserving commands appear between sort and stats.
type sortStatsReorderRule struct{}

func (r *sortStatsReorderRule) Name() string { return "SortStatsReorder" }
func (r *sortStatsReorderRule) Description() string {
	return "Moves sort after stats when sort fields are a prefix of group-by fields (sort operates on aggregated result)"
}

func (r *sortStatsReorderRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	for i, cmd := range q.Commands {
		sortCmd, ok := cmd.(*spl2.SortCommand)
		if !ok {
			continue
		}

		// Find the next non-preserving command — must be stats with group-by.
		statsIdx := -1
		for j := i + 1; j < len(q.Commands); j++ {
			behavior := orderingBehavior(q.Commands[j])
			if behavior == OrderPreserving {
				continue
			}
			if _, isStats := q.Commands[j].(*spl2.StatsCommand); isStats {
				statsIdx = j
			}

			break
		}
		if statsIdx < 0 {
			continue
		}

		statsCmd := q.Commands[statsIdx].(*spl2.StatsCommand)
		if len(statsCmd.GroupBy) == 0 {
			continue // no group-by → removeDeadSort handles this
		}

		// Check that sort fields are a prefix of GroupBy fields.
		if !sortFieldsArePrefixOfGroupBy(sortCmd.Fields, statsCmd.GroupBy) {
			continue
		}

		// Check that the consumer after stats is not another sort that would
		// override the ordering (making our rewrite pointless).
		if statsIdx+1 < len(q.Commands) {
			if _, isSort := q.Commands[statsIdx+1].(*spl2.SortCommand); isSort {
				continue
			}
		}

		// Rewrite: remove sort at i, insert it after statsIdx.
		// Since we're removing at i, the statsIdx shifts down by 1.
		newCommands := make([]spl2.Command, 0, len(q.Commands))
		newCommands = append(newCommands, q.Commands[:i]...)
		newCommands = append(newCommands, q.Commands[i+1:statsIdx+1]...)
		newCommands = append(newCommands, sortCmd)
		newCommands = append(newCommands, q.Commands[statsIdx+1:]...)
		q.Commands = newCommands

		return q, true
	}

	return q, false
}

// removeSortOnScanOrderRule eliminates a SortCommand on `_time` when the scan
// naturally produces time-ordered output, making the sort a no-op. The default
// scan direction is descending (newest-first). If the sort requests ascending
// `_time`, the rule annotates the query with `reverseScanOrder` so the scan
// iterates oldest-first instead.
//
// Safety constraints:
//   - Only applies to `_time` (the segment primary key).
//   - Only when the source is a single index (multi-source merge order is not
//     globally guaranteed).
//   - Only when no intermediate order-disrupting command exists between the
//     source and the sort.
type removeSortOnScanOrderRule struct{}

func (r *removeSortOnScanOrderRule) Name() string { return "RemoveSortOnScanOrder" }
func (r *removeSortOnScanOrderRule) Description() string {
	return "Eliminates sort on _time when scan naturally produces time-ordered output"
}

func (r *removeSortOnScanOrderRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	for i, cmd := range q.Commands {
		sortCmd, ok := cmd.(*spl2.SortCommand)
		if !ok {
			continue
		}

		// Only applies to single-field sort on _time.
		if len(sortCmd.Fields) != 1 || sortCmd.Fields[0].Name != "_time" {
			continue
		}

		// Verify the source is a single index (not multi-source or glob).
		if !isSingleSourceQuery(q) {
			continue
		}

		// Walk backward from the sort to verify no order-disrupting commands
		// exist between the source (position -1) and the sort (position i).
		orderSafe := true
		for j := 0; j < i; j++ {
			behavior := orderingBehavior(q.Commands[j])
			if behavior != OrderPreserving {
				orderSafe = false

				break
			}
		}
		if !orderSafe {
			continue
		}

		// Determine if we need to reverse the scan direction.
		// Default scan: descending _time (newest-first).
		// sort _time (ascending) → needs reverse scan.
		// sort -_time (descending) → matches default, no annotation needed.
		ascending := !sortCmd.Fields[0].Desc
		if ascending {
			q.Annotate("reverseScanOrder", true)
		}

		// Annotate diagnostic message.
		existing, _ := q.GetAnnotation("optimizerMessages")
		msgs, _ := existing.([]string)
		direction := "descending"
		if ascending {
			direction = "ascending (reverse scan)"
		}
		msgs = append(msgs, fmt.Sprintf("eliminated sort(_time) — scan produces %s time order natively", direction))
		q.Annotate("optimizerMessages", msgs)

		// Remove the sort.
		q.Commands = append(q.Commands[:i], q.Commands[i+1:]...)

		return q, true
	}

	return q, false
}

// isSingleSourceQuery returns true if the query's source clause references
// exactly one index — not a glob, not a multi-source list, not a variable.
// Queries without a source clause (pipe mode) are considered single-source
// since they read from a single stream.
func isSingleSourceQuery(q *spl2.Query) bool {
	if q.Source == nil {
		// Pipe mode or implicit source — single stream.
		return true
	}

	return q.Source.IsSingleSource()
}

// sortFieldsArePrefixOfGroupBy checks whether every sort field name matches
// the corresponding group-by field name, in order. The sort fields must be
// a prefix (can be shorter than or equal to group-by).
func sortFieldsArePrefixOfGroupBy(sortFields []spl2.SortField, groupBy []string) bool {
	if len(sortFields) == 0 || len(sortFields) > len(groupBy) {
		return false
	}
	for i, sf := range sortFields {
		if sf.Name != groupBy[i] {
			return false
		}
	}

	return true
}

// sortFieldsString returns a human-readable representation of sort fields.
func sortFieldsString(sortCmd *spl2.SortCommand) string {
	parts := make([]string, len(sortCmd.Fields))
	for i, f := range sortCmd.Fields {
		if f.Desc {
			parts[i] = fmt.Sprintf("-%s", f.Name)
		} else {
			parts[i] = f.Name
		}
	}

	return strings.Join(parts, ", ")
}

// OrderingBehavior classifies how a pipeline command affects input ordering.
type OrderingBehavior int

const (
	// OrderPreserving means output order equals input order.
	// The command neither reorders nor discards positional information.
	OrderPreserving OrderingBehavior = iota

	// OrderEstablishing means the command produces a NEW known ordering
	// that supersedes any input ordering (e.g., sort, timechart).
	OrderEstablishing

	// OrderDestroying means the output order is unspecified — any upstream
	// ordering is lost (e.g., stats uses hash aggregation, dedup uses hash set).
	OrderDestroying
)

// orderingBehavior classifies a command's effect on row ordering.
// Uses a type switch rather than a new interface method to avoid modifying
// 25+ AST types. Commands not listed default to OrderDestroying (safe default
// — unknown commands are assumed to break ordering).
func orderingBehavior(cmd spl2.Command) OrderingBehavior {
	switch cmd.(type) {
	// OrderPreserving: these commands filter, transform, or project without reordering.
	case *spl2.SearchCommand,
		*spl2.WhereCommand,
		*spl2.EvalCommand,
		*spl2.FieldsCommand,
		*spl2.TableCommand,
		*spl2.RenameCommand,
		*spl2.RexCommand,
		*spl2.BinCommand,
		*spl2.FillnullCommand,
		*spl2.HeadCommand,
		*spl2.TailCommand,
		*spl2.StreamstatsCommand,
		*spl2.EventstatsCommand,
		*spl2.FromCommand,
		*spl2.MaterializeCommand,
		*spl2.ViewsCommand,
		*spl2.DropviewCommand,
		*spl2.UnpackCommand,
		*spl2.JsonCommand,
		*spl2.PackJsonCommand:
		return OrderPreserving

	// OrderEstablishing: these commands produce output with a defined ordering.
	case *spl2.SortCommand,
		*spl2.TopNCommand,
		*spl2.TimechartCommand:
		return OrderEstablishing

	// OrderDestroying: these commands use hash-based or set-based processing;
	// output ordering is implementation-defined and not guaranteed.
	// UnrollCommand changes cardinality (one row → N rows from array explosion),
	// so the output ordering relative to the original stream is not preserved.
	case *spl2.StatsCommand,
		*spl2.DedupCommand,
		*spl2.TopCommand,
		*spl2.RareCommand,
		*spl2.JoinCommand,
		*spl2.AppendCommand,
		*spl2.MultisearchCommand,
		*spl2.TransactionCommand,
		*spl2.XYSeriesCommand,
		*spl2.UnrollCommand,
		*spl2.TraceCommand:
		return OrderDestroying

	default:
		// Unknown commands conservatively treated as order-destroying.
		return OrderDestroying
	}
}

// removeDeadSortRule eliminates a SortCommand when every downstream consumer
// either destroys or re-establishes ordering, making the sort's output
// unobservable. Special case: StreamstatsCommand depends on input ordering,
// so a sort before streamstats is always preserved.
type removeDeadSortRule struct{}

func (r *removeDeadSortRule) Name() string { return "RemoveDeadSort" }
func (r *removeDeadSortRule) Description() string {
	return "Eliminates sort when downstream commands destroy or re-establish ordering"
}

func (r *removeDeadSortRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	for i, cmd := range q.Commands {
		if _, ok := cmd.(*spl2.SortCommand); !ok {
			continue
		}

		// Walk forward from i+1 looking for the first non-preserving command.
		removable := false
		for j := i + 1; j < len(q.Commands); j++ {
			downstream := q.Commands[j]

			// StreamstatsCommand depends on input ordering — sort is needed.
			if _, isStreamstats := downstream.(*spl2.StreamstatsCommand); isStreamstats {
				break
			}

			behavior := orderingBehavior(downstream)
			switch behavior {
			case OrderDestroying:
				// Downstream destroys ordering — sort is dead.
				removable = true
			case OrderEstablishing:
				// Downstream re-establishes ordering — sort is dead.
				removable = true
			case OrderPreserving:
				// Continues propagating the sort's ordering — keep walking.
				continue
			}

			break
		}

		if removable {
			// Annotate the removal for diagnostics (explain output).
			sortCmd := q.Commands[i].(*spl2.SortCommand)
			downstreamName := "terminal"
			for j := i + 1; j < len(q.Commands); j++ {
				if orderingBehavior(q.Commands[j]) != OrderPreserving {
					downstreamName = fmt.Sprintf("%T", q.Commands[j])
					// Strip package prefix for cleaner output.
					if idx := strings.LastIndex(downstreamName, "."); idx >= 0 {
						downstreamName = downstreamName[idx+1:]
					}

					break
				}
			}
			existing, _ := q.GetAnnotation("optimizerMessages")
			msgs, _ := existing.([]string)
			msgs = append(msgs, fmt.Sprintf("removed unnecessary sort(%s) before %s (order-destroying operator)",
				sortFieldsString(sortCmd), downstreamName))
			q.Annotate("optimizerMessages", msgs)

			// Remove the sort at position i.
			q.Commands = append(q.Commands[:i], q.Commands[i+1:]...)

			return q, true
		}

		// Proactive warning: sort is kept but may be expensive if there is no
		// downstream head/tail limit to cap materialization.
		if !removable {
			sortCmd := q.Commands[i].(*spl2.SortCommand)
			if !hasDownstreamLimit(q.Commands, i) {
				existing, _ := q.GetAnnotation("optimizerWarnings")
				warns, _ := existing.([]string)
				warns = append(warns, fmt.Sprintf("sort(%s) will materialize all matching rows; "+
					"consider adding '| head N' or using timechart for time-bucketed aggregation",
					sortFieldsString(sortCmd)))
				q.Annotate("optimizerWarnings", warns)
			}
		}
	}

	return q, false
}

// hasDownstreamLimit checks whether any command after position i is a
// HeadCommand or TailCommand that limits the output of the sort.
func hasDownstreamLimit(commands []spl2.Command, sortIdx int) bool {
	for j := sortIdx + 1; j < len(commands); j++ {
		switch commands[j].(type) {
		case *spl2.HeadCommand, *spl2.TailCommand:
			return true
		}
		// Stop at the first order-destroying or order-establishing command —
		// a limit after that doesn't help the sort.
		behavior := orderingBehavior(commands[j])
		if behavior != OrderPreserving {
			return false
		}
	}

	return false
}

// removeRedundantSortRule eliminates sorts that are provably no-ops:
//
//  1. First of two sorts: when only order-preserving commands appear between
//     two sorts, the second sort overrides the first, making the first redundant.
//
//  2. Sort after matching order-establisher: when an order-establishing command
//     (timechart, topn, sort) already produces the exact ordering that a
//     downstream sort requests, the downstream sort is a no-op. Only fires when
//     the sort fields are a prefix of (or equal to) the produced ordering with
//     matching directions, and only order-preserving commands appear between.
type removeRedundantSortRule struct{}

func (r *removeRedundantSortRule) Name() string { return "RemoveRedundantSort" }
func (r *removeRedundantSortRule) Description() string {
	return "Eliminates sorts that duplicate an existing ordering (consecutive sorts or sort matching an upstream order-establishing command)"
}

func (r *removeRedundantSortRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	for i, cmd := range q.Commands {
		sortCmd, ok := cmd.(*spl2.SortCommand)
		if !ok {
			continue
		}

		// Case 1: Walk forward from i+1 looking for a second SortCommand or TopNCommand.
		for j := i + 1; j < len(q.Commands); j++ {
			downstream := q.Commands[j]

			// StreamstatsCommand depends on input ordering — first sort is needed.
			if _, isStreamstats := downstream.(*spl2.StreamstatsCommand); isStreamstats {
				break
			}

			// Found a second sort or topn — first sort is redundant.
			switch downstream.(type) {
			case *spl2.SortCommand, *spl2.TopNCommand:
				q.Commands = append(q.Commands[:i], q.Commands[i+1:]...)

				return q, true
			}

			behavior := orderingBehavior(downstream)
			if behavior != OrderPreserving {
				// Non-preserving command between sorts — stop looking.
				break
			}
		}

		// Case 2: Walk backward from sort to find the nearest non-preserving
		// predecessor. If it is OrderEstablishing and its produced ordering
		// matches the sort's requested ordering, the sort is a no-op.
		for j := i - 1; j >= 0; j-- {
			predecessor := q.Commands[j]

			// StreamstatsCommand between the establisher and the sort means
			// the sort may be intentional (streamstats depends on order).
			if _, isStreamstats := predecessor.(*spl2.StreamstatsCommand); isStreamstats {
				break
			}

			behavior := orderingBehavior(predecessor)
			if behavior == OrderPreserving {
				continue
			}

			if behavior == OrderEstablishing {
				produced := producedOrdering(predecessor)
				if produced != nil && sortFieldsMatchProduced(sortCmd.Fields, produced) {
					// The sort matches the upstream ordering — eliminate it.
					existing, _ := q.GetAnnotation("optimizerMessages")
					msgs, _ := existing.([]string)
					msgs = append(msgs, fmt.Sprintf("eliminated redundant sort(%s) — upstream %s already produces this ordering",
						sortFieldsString(sortCmd), commandName(predecessor)))
					q.Annotate("optimizerMessages", msgs)

					q.Commands = append(q.Commands[:i], q.Commands[i+1:]...)

					return q, true
				}
			}

			// Non-preserving, non-matching — stop looking.
			break
		}
	}

	return q, false
}

// producedOrdering returns the sort fields that an order-establishing command
// guarantees in its output. Returns nil for non-establishing commands or
// commands whose output ordering cannot be expressed as sort fields.
func producedOrdering(cmd spl2.Command) []spl2.SortField {
	switch c := cmd.(type) {
	case *spl2.SortCommand:
		return c.Fields
	case *spl2.TopNCommand:
		return c.Fields
	case *spl2.TimechartCommand:
		// timechart always produces ascending _time output.
		return []spl2.SortField{{Name: "_time", Desc: false}}
	default:
		return nil
	}
}

// sortFieldsMatchProduced returns true when the sort's fields are a prefix of
// (or equal to) the produced ordering, with matching directions.
func sortFieldsMatchProduced(sortFields, produced []spl2.SortField) bool {
	if len(sortFields) == 0 || len(sortFields) > len(produced) {
		return false
	}
	for i, sf := range sortFields {
		if sf.Name != produced[i].Name || sf.Desc != produced[i].Desc {
			return false
		}
	}

	return true
}

// commandName returns a human-readable name for a pipeline command.
func commandName(cmd spl2.Command) string {
	switch cmd.(type) {
	case *spl2.SortCommand:
		return "sort"
	case *spl2.TopNCommand:
		return "topn"
	case *spl2.TimechartCommand:
		return "timechart"
	default:
		name := fmt.Sprintf("%T", cmd)
		if idx := strings.LastIndex(name, "."); idx >= 0 {
			name = name[idx+1:]
		}

		return strings.ToLower(strings.TrimSuffix(name, "Command"))
	}
}
