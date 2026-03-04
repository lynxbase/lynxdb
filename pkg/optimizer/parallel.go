package optimizer

import (
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

// flattenNestedAppendRule transforms chained APPEND commands into a single
// N-way MultisearchCommand. This enables the pipeline to run all branches
// in parallel via ConcurrentUnionIterator instead of nested sequential unions.
//
// Transform: A | APPEND [B | APPEND [C]] → MULTISEARCH [A] [B] [C]
//
// The resulting MultisearchCommand is annotated with "flattenedFromAppend": true
// so that relaxAppendOrderingRule knows the default should be OrderPreserved.
type flattenNestedAppendRule struct{}

func (r *flattenNestedAppendRule) Name() string { return "FlattenNestedAppend" }
func (r *flattenNestedAppendRule) Description() string {
	return "Flattens chained APPEND commands into an N-way parallel branch for ConcurrentUnionIterator"
}

func (r *flattenNestedAppendRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	if len(q.Commands) == 0 {
		return q, false
	}

	// Find the last APPEND command — we flatten from the tail of the pipeline.
	appendIdx := -1
	for i := len(q.Commands) - 1; i >= 0; i-- {
		if _, ok := q.Commands[i].(*spl2.AppendCommand); ok {
			appendIdx = i

			break
		}
	}
	if appendIdx < 0 {
		return q, false
	}

	appendCmd := q.Commands[appendIdx].(*spl2.AppendCommand)
	if appendCmd.Subquery == nil {
		return q, false
	}

	// Only flatten when the subquery itself contains nested APPENDs.
	if !hasNestedAppend(appendCmd.Subquery) {
		return q, false
	}

	// Collect all branches by recursively flattening nested APPENDs.
	// Branch[0] = commands before the first APPEND (the "main" query).
	// Branch[1..N] = each APPEND's subquery content.
	branches := flattenAppendBranches(appendCmd.Subquery)

	// Build the "main" query: everything before the APPEND command.
	mainQuery := &spl2.Query{
		Source:   q.Source,
		Commands: make([]spl2.Command, appendIdx),
	}
	copy(mainQuery.Commands, q.Commands[:appendIdx])

	// Prepend the main query as branch[0].
	allBranches := make([]*spl2.Query, 0, len(branches)+1)
	allBranches = append(allBranches, mainQuery)
	allBranches = append(allBranches, branches...)

	// Build the replacement MultisearchCommand.
	multiCmd := &spl2.MultisearchCommand{
		Searches: allBranches,
	}

	// The new query starts with the MultisearchCommand, followed by any
	// commands that were after the APPEND in the original pipeline.
	newCommands := make([]spl2.Command, 0, 1+len(q.Commands)-appendIdx-1)
	newCommands = append(newCommands, multiCmd)
	newCommands = append(newCommands, q.Commands[appendIdx+1:]...)

	newQ := &spl2.Query{
		Source:      nil, // MultisearchCommand provides all sources
		Commands:    newCommands,
		Annotations: q.Annotations,
	}
	newQ.Annotate("flattenedFromAppend", true)

	return newQ, true
}

// hasNestedAppend returns true if the query's command list ends with an
// AppendCommand (possibly preceded by other commands).
func hasNestedAppend(q *spl2.Query) bool {
	if q == nil || len(q.Commands) == 0 {
		return false
	}
	for _, cmd := range q.Commands {
		if _, ok := cmd.(*spl2.AppendCommand); ok {
			return true
		}
	}

	return false
}

// flattenAppendBranches recursively flattens a chain of APPEND commands
// into a flat list of queries. Each branch retains its non-APPEND commands.
func flattenAppendBranches(q *spl2.Query) []*spl2.Query {
	if q == nil {
		return nil
	}

	// Find the first APPEND command in this query.
	appendIdx := -1
	for i, cmd := range q.Commands {
		if _, ok := cmd.(*spl2.AppendCommand); ok {
			appendIdx = i

			break
		}
	}

	if appendIdx < 0 {
		// No APPEND — this is a leaf branch.
		return []*spl2.Query{q}
	}

	// If there are commands after the APPEND (e.g., | APPEND [...] | head 10),
	// we cannot safely flatten because those post-APPEND commands would be lost.
	// Treat the whole query as a leaf.
	if appendIdx < len(q.Commands)-1 {
		return []*spl2.Query{q}
	}

	appendCmd := q.Commands[appendIdx].(*spl2.AppendCommand)

	// Build the "before APPEND" branch: source + commands before APPEND.
	beforeBranch := &spl2.Query{
		Source:   q.Source,
		Commands: make([]spl2.Command, appendIdx),
	}
	copy(beforeBranch.Commands, q.Commands[:appendIdx])

	// Recurse into the subquery to flatten further nested APPENDs.
	subBranches := flattenAppendBranches(appendCmd.Subquery)

	// Combine: [beforeBranch, subBranches...]
	result := make([]*spl2.Query, 0, 1+len(subBranches))
	result = append(result, beforeBranch)
	result = append(result, subBranches...)

	return result
}

// relaxAppendOrderingRule annotates APPEND and MultisearchCommand queries
// with "appendOrdering": "interleaved" when the downstream command is
// order-insensitive (commutative). This allows ConcurrentUnionIterator to
// use OrderInterleaved instead of OrderPreserved, improving throughput by
// not blocking on the first branch's drain.
//
// Detects: ... | APPEND [...] | stats/sort/dedup/top/rare/timechart
// Result: annotation appendOrdering=interleaved on the query.
type relaxAppendOrderingRule struct{}

func (r *relaxAppendOrderingRule) Name() string { return "RelaxAppendOrdering" }
func (r *relaxAppendOrderingRule) Description() string {
	return "Annotates APPEND/MULTISEARCH with interleaved ordering when downstream is order-insensitive"
}

func (r *relaxAppendOrderingRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	if q.Annotations != nil {
		if _, done := q.Annotations["appendOrdering"]; done {
			return q, false // already annotated
		}
	}

	if len(q.Commands) < 2 {
		return q, false
	}

	// Find APPEND or MultisearchCommand followed by a commutative command.
	for i := 0; i < len(q.Commands)-1; i++ {
		isAppendLike := false
		switch q.Commands[i].(type) {
		case *spl2.AppendCommand:
			isAppendLike = true
		case *spl2.MultisearchCommand:
			isAppendLike = true
		}
		if !isAppendLike {
			continue
		}

		// Relax ordering if the downstream command is commutative.
		downstream := q.Commands[i+1:]
		if canRelaxOrdering(downstream) {
			q.Annotate("appendOrdering", "interleaved")

			return q, true
		}
	}

	return q, false
}

// canRelaxOrdering returns true if the first downstream command is
// commutative/order-insensitive, meaning input order doesn't affect output.
func canRelaxOrdering(downstream []spl2.Command) bool {
	if len(downstream) == 0 {
		return false // events mode — order matters for display
	}

	switch downstream[0].(type) {
	case *spl2.StatsCommand:
		return true // aggregation is commutative
	case *spl2.TimechartCommand:
		return true // bin + stats is commutative
	case *spl2.SortCommand:
		return true // sort re-orders anyway
	case *spl2.TopCommand:
		return true // count-based, order-insensitive
	case *spl2.RareCommand:
		return true // count-based, order-insensitive
	case *spl2.TopNCommand:
		return true // heap-based selection, order-insensitive
	default:
		return false
	}
}
