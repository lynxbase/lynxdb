package optimizer

import "github.com/OrlovEvgeny/Lynxdb/pkg/spl2"

// Column Pruning Rule
// Walks the AST and computes the minimal set of columns required at each stage.
// Writes result to q.Annotations["requiredColumns"].

func (r *columnPruningRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	if q.Annotations != nil {
		if _, done := q.Annotations["requiredColumns"]; done {
			return q, false // already computed
		}
	}
	cols := computeRequiredColumns(q)
	if len(cols) == 0 {
		return q, false
	}
	q.Annotate("requiredColumns", cols)

	return q, true
}

// computeRequiredColumns analyzes commands to determine which fields are needed.
// It walks forward (like GetRequiredColumns) collecting all referenced fields.
func computeRequiredColumns(q *spl2.Query) []string {
	cols := make(map[string]bool)
	cols["_time"] = true

	for _, cmd := range q.Commands {
		commandAccessedFields(cmd, cols)
	}

	result := make([]string, 0, len(cols))
	for c := range cols {
		result = append(result, c)
	}

	return result
}

// commandAccessedFields adds fields accessed by a command to the cols map.
func commandAccessedFields(cmd spl2.Command, cols map[string]bool) {
	switch c := cmd.(type) {
	case *spl2.SearchCommand:
		cols["_raw"] = true
		if c.Expression != nil {
			collectSearchExprFieldsForOpt(c.Expression, cols)
		}
	case *spl2.WhereCommand:
		collectExprFieldsForOpt(c.Expr, cols)
	case *spl2.StatsCommand:
		for _, f := range c.GroupBy {
			cols[f] = true
		}
		for _, agg := range c.Aggregations {
			for _, arg := range agg.Args {
				collectExprFieldsForOpt(arg, cols)
			}
		}
	case *spl2.EvalCommand:
		if c.Expr != nil {
			collectExprFieldsForOpt(c.Expr, cols)
		}
		for _, a := range c.Assignments {
			collectExprFieldsForOpt(a.Expr, cols)
		}
	case *spl2.SortCommand:
		for _, sf := range c.Fields {
			cols[sf.Name] = true
		}
	case *spl2.FieldsCommand:
		for _, f := range c.Fields {
			cols[f] = true
		}
	case *spl2.TableCommand:
		for _, f := range c.Fields {
			cols[f] = true
		}
	case *spl2.DedupCommand:
		for _, f := range c.Fields {
			cols[f] = true
		}
	case *spl2.RexCommand:
		if c.Field != "" {
			cols[c.Field] = true
		} else {
			cols["_raw"] = true
		}
	case *spl2.BinCommand:
		cols[c.Field] = true
	case *spl2.RenameCommand:
		for _, r := range c.Renames {
			cols[r.Old] = true
		}
	case *spl2.StreamstatsCommand:
		for _, agg := range c.Aggregations {
			for _, arg := range agg.Args {
				collectExprFieldsForOpt(arg, cols)
			}
		}
		for _, f := range c.GroupBy {
			cols[f] = true
		}
	case *spl2.EventstatsCommand:
		for _, agg := range c.Aggregations {
			for _, arg := range agg.Args {
				collectExprFieldsForOpt(arg, cols)
			}
		}
		for _, f := range c.GroupBy {
			cols[f] = true
		}
	case *spl2.TransactionCommand:
		cols[c.Field] = true
		cols["_raw"] = true
	case *spl2.JoinCommand:
		cols[c.Field] = true
	case *spl2.TimechartCommand:
		cols["_time"] = true
		for _, agg := range c.Aggregations {
			for _, arg := range agg.Args {
				collectExprFieldsForOpt(arg, cols)
			}
		}
		for _, f := range c.GroupBy {
			cols[f] = true
		}
	case *spl2.TopCommand:
		cols[c.Field] = true
		if c.ByField != "" {
			cols[c.ByField] = true
		}
	case *spl2.RareCommand:
		cols[c.Field] = true
		if c.ByField != "" {
			cols[c.ByField] = true
		}
	case *spl2.FillnullCommand:
		for _, f := range c.Fields {
			cols[f] = true
		}
	case *spl2.AppendCommand:
		if c.Subquery != nil {
			for _, subcmd := range c.Subquery.Commands {
				commandAccessedFields(subcmd, cols)
			}
		}
	case *spl2.MultisearchCommand:
		for _, sub := range c.Searches {
			for _, subcmd := range sub.Commands {
				commandAccessedFields(subcmd, cols)
			}
		}
	}
}

// collectExprFieldsForOpt extracts field names from an expression.
func collectExprFieldsForOpt(expr spl2.Expr, cols map[string]bool) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *spl2.FieldExpr:
		cols[e.Name] = true
		// "index" and "source" are virtual aliases for "_source".
		// Ensure _source is included so column pruning doesn't drop it.
		if e.Name == "index" || e.Name == "source" {
			cols["_source"] = true
		}
	case *spl2.CompareExpr:
		collectExprFieldsForOpt(e.Left, cols)
		collectExprFieldsForOpt(e.Right, cols)
	case *spl2.BinaryExpr:
		collectExprFieldsForOpt(e.Left, cols)
		collectExprFieldsForOpt(e.Right, cols)
	case *spl2.ArithExpr:
		collectExprFieldsForOpt(e.Left, cols)
		collectExprFieldsForOpt(e.Right, cols)
	case *spl2.NotExpr:
		collectExprFieldsForOpt(e.Expr, cols)
	case *spl2.FuncCallExpr:
		for _, arg := range e.Args {
			collectExprFieldsForOpt(arg, cols)
		}
	case *spl2.InExpr:
		collectExprFieldsForOpt(e.Field, cols)
	}
}

// collectSearchExprFieldsForOpt extracts field names from a SearchExpr.
func collectSearchExprFieldsForOpt(expr spl2.SearchExpr, cols map[string]bool) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *spl2.SearchAndExpr:
		collectSearchExprFieldsForOpt(e.Left, cols)
		collectSearchExprFieldsForOpt(e.Right, cols)
	case *spl2.SearchOrExpr:
		collectSearchExprFieldsForOpt(e.Left, cols)
		collectSearchExprFieldsForOpt(e.Right, cols)
	case *spl2.SearchNotExpr:
		collectSearchExprFieldsForOpt(e.Operand, cols)
	case *spl2.SearchCompareExpr:
		cols[e.Field] = true
	case *spl2.SearchInExpr:
		cols[e.Field] = true
	}
}

// Projection Pushdown Rule
// When TABLE or FIELDS appear late in the pipeline and earlier commands
// don't need the dropped fields, insert an early FIELDS command.

func (r *projectionPushdownRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	if len(q.Commands) < 3 {
		return q, false
	}

	// Find terminal TABLE or FIELDS command.
	var terminalFields []string
	termIdx := -1
	for i := len(q.Commands) - 1; i >= 0; i-- {
		switch c := q.Commands[i].(type) {
		case *spl2.TableCommand:
			terminalFields = c.Fields
			termIdx = i
		case *spl2.FieldsCommand:
			if !c.Remove {
				terminalFields = c.Fields
				termIdx = i
			}
		}
		if termIdx >= 0 {
			break
		}
	}
	if termIdx < 0 || termIdx < 2 {
		return q, false
	}

	// Check if any preceding command is expensive (sort, join, eventstats).
	insertBefore := -1
	for i := 0; i < termIdx; i++ {
		switch q.Commands[i].(type) {
		case *spl2.SortCommand, *spl2.JoinCommand, *spl2.EventstatsCommand:
			insertBefore = i
		}
		if insertBefore >= 0 {
			break
		}
	}
	if insertBefore < 0 {
		return q, false
	}

	// Compute fields needed by commands between insertBefore and termIdx.
	needed := make(map[string]bool)
	for _, f := range terminalFields {
		needed[f] = true
	}
	needed["_time"] = true
	for i := insertBefore; i < termIdx; i++ {
		commandAccessedFields(q.Commands[i], needed)
	}

	// Build the early fields list.
	earlyFields := make([]string, 0, len(needed))
	for f := range needed {
		earlyFields = append(earlyFields, f)
	}

	// Don't insert if it would include everything anyway.
	if len(earlyFields) == 0 {
		return q, false
	}

	// Insert FIELDS command before the expensive operator.
	newCmd := &spl2.FieldsCommand{Fields: earlyFields}
	newCmds := make([]spl2.Command, 0, len(q.Commands)+1)
	newCmds = append(newCmds, q.Commands[:insertBefore]...)
	newCmds = append(newCmds, newCmd)
	newCmds = append(newCmds, q.Commands[insertBefore:]...)
	q.Commands = newCmds

	return q, true
}
