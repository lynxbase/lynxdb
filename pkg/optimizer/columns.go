package optimizer

import (
	"strings"

	"github.com/lynxbase/lynxdb/pkg/spl2"
)

func (r *columnPruningRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	if q.Annotations != nil {
		if _, done := q.Annotations["requiredColumns"]; done {
			return q, false // already computed
		}
	}
	cols := computeRequiredColumns(q)
	if cols == nil {
		// nil means glob fields present — all columns needed, skip pruning.
		return q, false
	}
	if len(cols) == 0 {
		return q, false
	}

	// Check if _raw can be skipped. When no command references _raw
	// (no search, no rex on _raw, no unpack from _raw, no transaction),
	// we can avoid decompressing it — a significant I/O saving.
	if !rawNeededByCommands(q.Commands) {
		// Remove _raw from required columns.
		filtered := make([]string, 0, len(cols))
		for _, c := range cols {
			if c != "_raw" {
				filtered = append(filtered, c)
			}
		}
		cols = filtered
		q.Annotate("skipRaw", true)
	}

	q.Annotate("requiredColumns", cols)

	return q, true
}

// rawNeededByCommands returns true if any command in the pipeline requires _raw.
// _raw is needed when:
//   - A SearchCommand exists (full-text search on _raw)
//   - A RexCommand targets _raw (default field)
//   - An UnpackCommand/JsonCommand reads from _raw (default source)
//   - A TransactionCommand exists (concatenates _raw)
//   - The query outputs raw events (no aggregating terminal command)
//
// _raw can be skipped when the query terminates with an aggregation
// (stats, timechart, top, rare) or a TABLE/FIELDS that doesn't include _raw.
func rawNeededByCommands(cmds []spl2.Command) bool {
	if len(cmds) == 0 {
		return true // no commands = raw event output
	}

	for _, cmd := range cmds {
		switch c := cmd.(type) {
		case *spl2.SearchCommand:
			return true // search operates on _raw
		case *spl2.RexCommand:
			if c.Field == "" || c.Field == "_raw" {
				return true // rex defaults to _raw
			}
		case *spl2.UnpackCommand:
			if c.SourceField == "" || c.SourceField == "_raw" {
				return true // unpack defaults to _raw
			}
		case *spl2.JsonCommand:
			if c.SourceField == "" || c.SourceField == "_raw" {
				return true // json defaults to _raw
			}
		case *spl2.TransactionCommand:
			return true // transaction concatenates _raw
		case *spl2.WhereCommand:
			if exprReferencesRaw(c.Expr) {
				return true // e.g. match(_raw, "pattern")
			}
		case *spl2.EvalCommand:
			if exprReferencesRaw(c.Expr) {
				return true
			}
			for _, a := range c.Assignments {
				if exprReferencesRaw(a.Expr) {
					return true
				}
			}
		case *spl2.AppendCommand:
			if c.Subquery != nil && rawNeededByCommands(c.Subquery.Commands) {
				return true
			}
		case *spl2.MultisearchCommand:
			for _, sub := range c.Searches {
				if rawNeededByCommands(sub.Commands) {
					return true
				}
			}
		}
	}

	// Check if the terminal command produces raw events that include _raw.
	// Aggregation commands (stats, timechart, top, rare) produce derived rows
	// without _raw. TABLE/FIELDS that omit _raw also don't need it.
	terminal := cmds[len(cmds)-1]
	switch terminal.(type) {
	case *spl2.StatsCommand, *spl2.TimechartCommand,
		*spl2.TopCommand, *spl2.RareCommand:
		return false // aggregated output, _raw not in result
	case *spl2.TableCommand:
		// TABLE lists explicit output columns. Only need _raw if it's listed.
		tc := terminal.(*spl2.TableCommand)
		for _, f := range tc.Fields {
			if f == "_raw" {
				return true
			}
		}
		return false
	case *spl2.FieldsCommand:
		fc := terminal.(*spl2.FieldsCommand)
		if fc.Remove {
			// "| fields - _raw" — explicitly removes _raw, still might need
			// it upstream, but the output doesn't include it. However,
			// upstream commands might still need it. Conservative: keep it.
			return true
		}
		// "| fields level, status" — only need _raw if it's listed.
		for _, f := range fc.Fields {
			if f == "_raw" {
				return true
			}
		}
		return false
	}

	// Default: raw event output needs _raw.
	return true
}

// exprReferencesRaw returns true if the expression tree contains a FieldExpr
// referencing "_raw" (e.g. match(_raw, "pattern"), len(_raw), _raw="...").
func exprReferencesRaw(expr spl2.Expr) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case *spl2.FieldExpr:
		return e.Name == "_raw"
	case *spl2.CompareExpr:
		return exprReferencesRaw(e.Left) || exprReferencesRaw(e.Right)
	case *spl2.BinaryExpr:
		return exprReferencesRaw(e.Left) || exprReferencesRaw(e.Right)
	case *spl2.ArithExpr:
		return exprReferencesRaw(e.Left) || exprReferencesRaw(e.Right)
	case *spl2.NotExpr:
		return exprReferencesRaw(e.Expr)
	case *spl2.FuncCallExpr:
		for _, arg := range e.Args {
			if exprReferencesRaw(arg) {
				return true
			}
		}
	case *spl2.InExpr:
		if exprReferencesRaw(e.Field) {
			return true
		}
		for _, v := range e.Values {
			if exprReferencesRaw(v) {
				return true
			}
		}
	}
	return false
}

// computeRequiredColumns analyzes commands to determine which fields are needed.
// It walks forward (like GetRequiredColumns) collecting all referenced fields.
// Returns nil when glob patterns are present (all columns needed, skip pruning).
//
// Smart unpack awareness: when a glob pattern like "pg.*" appears in FIELDS or
// TABLE, and an upstream UnpackCommand generates fields with that prefix ("pg."),
// the glob only matches unpack-generated fields — not segment columns. In this
// case, column pruning remains active and only the unpack's source field is added
// to required columns (since unpack-generated fields don't exist in segments).
func computeRequiredColumns(q *spl2.Query) []string {
	cols := make(map[string]bool)
	cols["_time"] = true
	cols["_raw"] = true // always include _raw — it's the core log line field

	// Collect prefixes from UnpackCommand nodes for glob resolution.
	// A prefix like "pg." means fields "pg.duration_ms", "pg.pid", etc.
	// are generated at runtime and don't exist in segments.
	unpackPrefixes := collectUnpackPrefixes(q.Commands)

	for _, cmd := range q.Commands {
		commandAccessedFieldsWithContext(cmd, cols, unpackPrefixes)
	}

	// "*" sentinel means a command uses glob patterns — all columns needed.
	if cols["*"] {
		return nil
	}

	result := make([]string, 0, len(cols))
	for c := range cols {
		result = append(result, c)
	}

	return result
}

// collectUnpackPrefixes returns the set of field prefixes generated by
// UnpackCommand nodes in the pipeline. These prefixes indicate fields that
// are generated at runtime (not stored in segments).
func collectUnpackPrefixes(cmds []spl2.Command) map[string]bool {
	prefixes := make(map[string]bool)
	for _, cmd := range cmds {
		if u, ok := cmd.(*spl2.UnpackCommand); ok && u.Prefix != "" {
			prefixes[u.Prefix] = true
		}
	}
	return prefixes
}

// globMatchesUnpackPrefix returns true if every glob field in the list is
// covered by an unpack prefix. For example, "pg.*" is covered by prefix "pg."
// because all fields matching "pg.*" will be generated by the unpack operator.
func globMatchesUnpackPrefix(fields []string, unpackPrefixes map[string]bool) bool {
	if len(unpackPrefixes) == 0 {
		return false
	}
	for _, f := range fields {
		if !spl2.ContainsGlobWildcard(f) {
			continue // non-glob field, skip
		}
		// Check if glob is <prefix>* where prefix matches an unpack prefix.
		// e.g., "pg.*" → trim trailing "*" → "pg." → check unpackPrefixes.
		matched := false
		for prefix := range unpackPrefixes {
			// "pg.*" should match prefix "pg." — strip the glob wildcard and
			// check if the remaining part equals the prefix.
			if strings.HasSuffix(f, "*") || strings.HasSuffix(f, "?") {
				// Handle "pg.*" → "pg." prefix match.
				globBase := strings.TrimRight(f, "*?")
				if globBase == prefix {
					matched = true
					break
				}
			}
		}
		if !matched {
			return false // this glob is NOT covered by any unpack prefix
		}
	}
	return true
}

// commandAccessedFieldsWithContext is like commandAccessedFields but aware
// of unpack-generated field prefixes for smart glob handling.
func commandAccessedFieldsWithContext(cmd spl2.Command, cols map[string]bool, unpackPrefixes map[string]bool) {
	switch c := cmd.(type) {
	case *spl2.FieldsCommand:
		if spl2.FieldListHasGlob(c.Fields) {
			if globMatchesUnpackPrefix(c.Fields, unpackPrefixes) {
				// All globs match unpack prefixes — these fields are generated
				// at runtime, not stored in segments. Don't set cols["*"].
				// Add non-glob fields normally.
				for _, f := range c.Fields {
					if !spl2.ContainsGlobWildcard(f) {
						cols[f] = true
					}
				}
				return
			}
			cols["*"] = true // unresolvable glob — all columns needed
		} else {
			for _, f := range c.Fields {
				cols[f] = true
			}
		}
	case *spl2.TableCommand:
		if spl2.FieldListHasGlob(c.Fields) {
			if globMatchesUnpackPrefix(c.Fields, unpackPrefixes) {
				// Same logic as FieldsCommand — glob matches unpack prefix.
				for _, f := range c.Fields {
					if !spl2.ContainsGlobWildcard(f) {
						cols[f] = true
					}
				}
				return
			}
			cols["*"] = true // unresolvable glob — all columns needed
		} else {
			for _, f := range c.Fields {
				cols[f] = true
			}
		}
	default:
		// Delegate to the original per-command handler for all other commands.
		commandAccessedFields(cmd, cols)
	}
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
	case *spl2.TopNCommand:
		for _, sf := range c.Fields {
			cols[sf.Name] = true
		}
	case *spl2.FieldsCommand:
		if spl2.FieldListHasGlob(c.Fields) {
			cols["*"] = true // glob patterns — all columns needed
		} else {
			for _, f := range c.Fields {
				cols[f] = true
			}
		}
	case *spl2.TableCommand:
		if spl2.FieldListHasGlob(c.Fields) {
			cols["*"] = true // glob patterns — all columns needed
		} else {
			for _, f := range c.Fields {
				cols[f] = true
			}
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
	case *spl2.UnpackCommand:
		cols[c.SourceField] = true
	case *spl2.JsonCommand:
		cols[c.SourceField] = true
	case *spl2.UnrollCommand:
		cols[c.Field] = true
		for _, f := range c.ExtraFields {
			cols[f] = true
		}
	case *spl2.PackJsonCommand:
		for _, f := range c.Fields {
			cols[f] = true
		}
	case *spl2.TraceCommand:
		cols["_time"] = true
		cols[c.TraceIDField] = true
		cols[c.SpanIDField] = true
		cols[c.ParentIDField] = true
		// Trace output needs service, operation, duration_ms for display.
		cols["service"] = true
		cols["service_name"] = true
		cols["component"] = true
		cols["operation"] = true
		cols["operation_name"] = true
		cols["name"] = true
		cols["handler"] = true
		cols["duration_ms"] = true
		cols["duration"] = true
		cols["elapsed_ms"] = true
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

	// Skip when terminal fields contain glob patterns — can't build a
	// correct early field list without knowing actual column names.
	if spl2.FieldListHasGlob(terminalFields) {
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

	// Guard against infinite re-insertion: if there is already a FIELDS
	// command immediately before the expensive operator, do not insert again.
	// Without this check, the optimizer loop (capped at 10 iterations)
	// inserts a new FIELDS on every pass, producing 10 redundant FIELDS.
	if insertBefore > 0 {
		if _, isFields := q.Commands[insertBefore-1].(*spl2.FieldsCommand); isFields {
			return q, false
		}
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

// Apply analyzes downstream commands after each UnpackCommand to determine
// which extracted fields are consumed. If the consumed set is finite (no glob
// patterns), it populates UnpackCommand.Fields so the parser can skip
// extracting unused fields.
//
// Example: | parse json(message) as j | where j.status >= 500 | table j.status, j.uri
// → UnpackCommand.Fields = ["status", "uri"] (prefix "j." stripped)
//
// This optimization does NOT fire when downstream uses a glob pattern like
// "keep pg.*" because the set of consumed fields cannot be statically determined.
func (r *unpackFieldPruningRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	changed := false

	for i, cmd := range q.Commands {
		u, ok := cmd.(*spl2.UnpackCommand)
		if !ok || len(u.Fields) > 0 {
			continue // not unpack, or already has field restriction
		}

		// Collect all fields consumed by commands downstream of this unpack.
		downstream := make(map[string]bool)
		for j := i + 1; j < len(q.Commands); j++ {
			commandAccessedFields(q.Commands[j], downstream)
		}

		// If downstream uses "*" (glob), we can't restrict fields.
		if downstream["*"] {
			continue
		}

		// If no downstream commands reference any fields, skip.
		if len(downstream) == 0 {
			continue
		}

		// Determine which unpack-generated fields are consumed downstream.
		// Unpack fields are stored as prefix+key in column names.
		// E.g., prefix "pg." generates "pg.duration_ms" → key "duration_ms".
		prefix := u.Prefix
		var neededFields []string

		for field := range downstream {
			if prefix != "" {
				if strings.HasPrefix(field, prefix) {
					key := strings.TrimPrefix(field, prefix)
					if key != "" {
						neededFields = append(neededFields, key)
					}
				}
				// Fields without the prefix are not from this unpack → ignore.
			} else {
				// No prefix: unpack fields are added with their raw key names.
				// We can't distinguish unpack-generated fields from segment fields
				// without prefix, so skip optimization for prefix-less unpacks.
				neededFields = nil
				break
			}
		}

		// Only apply if we found a non-empty, finite set of needed fields.
		if len(neededFields) == 0 {
			continue
		}

		u.Fields = neededFields
		changed = true
	}

	return q, changed
}
