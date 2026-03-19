package optimizer

import (
	"strings"

	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/storage/segment/index"
)

type predicatePushdownRule struct{}

func (r *predicatePushdownRule) Name() string { return "PredicatePushdown" }
func (r *predicatePushdownRule) Description() string {
	return "Moves WHERE filters before EVAL/RENAME to reduce rows early"
}
func (r *predicatePushdownRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	for i := 1; i < len(q.Commands); i++ {
		w, isWhere := q.Commands[i].(*spl2.WhereCommand)
		if !isWhere {
			continue
		}
		whereFields := collectFieldNames(w.Expr)
		prev := q.Commands[i-1]

		switch p := prev.(type) {
		case *spl2.EvalCommand:
			// Move WHERE before EVAL if WHERE doesn't depend on EVAL's output.
			if len(p.Assignments) > 0 {
				depends := false
				for _, a := range p.Assignments {
					if whereFields[a.Field] {
						depends = true

						break
					}
				}
				if depends {
					continue
				}
			} else if whereFields[p.Field] {
				continue
			}
			q.Commands[i-1] = w
			q.Commands[i] = p

			return q, true

		case *spl2.RenameCommand:
			// Push WHERE through RENAME by adjusting field references.
			// Build reverse rename map: new → old.
			reverseMap := make(map[string]string)
			for _, r := range p.Renames {
				reverseMap[r.New] = r.Old
			}
			// Skip if WHERE references a renamed field.
			usesRenamed := false
			for f := range whereFields {
				if _, ok := reverseMap[f]; ok {
					usesRenamed = true

					break
				}
			}
			if !usesRenamed {
				// No overlap — safe to swap directly.
				q.Commands[i-1] = w
				q.Commands[i] = p

				return q, true
			}
			// Adjust WHERE expr with reverse renames then swap.
			newExpr := substituteFieldNames(w.Expr, reverseMap)
			q.Commands[i-1] = &spl2.WhereCommand{Expr: newExpr}
			q.Commands[i] = p

			return q, true

		case *spl2.FillnullCommand:
			// Push WHERE through FILLNULL — fillnull doesn't filter or reorder.
			q.Commands[i-1] = w
			q.Commands[i] = p

			return q, true
		}
	}

	return q, false
}

// substituteFieldNames replaces field references using the given name map.
func substituteFieldNames(expr spl2.Expr, nameMap map[string]string) spl2.Expr {
	switch e := expr.(type) {
	case *spl2.FieldExpr:
		if newName, ok := nameMap[e.Name]; ok {
			return &spl2.FieldExpr{Name: newName}
		}

		return e
	case *spl2.CompareExpr:
		return &spl2.CompareExpr{
			Left:  substituteFieldNames(e.Left, nameMap),
			Op:    e.Op,
			Right: substituteFieldNames(e.Right, nameMap),
		}
	case *spl2.BinaryExpr:
		return &spl2.BinaryExpr{
			Left:  substituteFieldNames(e.Left, nameMap),
			Op:    e.Op,
			Right: substituteFieldNames(e.Right, nameMap),
		}
	case *spl2.ArithExpr:
		return &spl2.ArithExpr{
			Left:  substituteFieldNames(e.Left, nameMap),
			Op:    e.Op,
			Right: substituteFieldNames(e.Right, nameMap),
		}
	case *spl2.NotExpr:
		return &spl2.NotExpr{Expr: substituteFieldNames(e.Expr, nameMap)}
	case *spl2.FuncCallExpr:
		args := make([]spl2.Expr, len(e.Args))
		for i, arg := range e.Args {
			args[i] = substituteFieldNames(arg, nameMap)
		}

		return &spl2.FuncCallExpr{Name: e.Name, Args: args}
	default:
		return expr
	}
}

type projectionPushdownRule struct{}

func (r *projectionPushdownRule) Name() string { return "ProjectionPushdown" }
func (r *projectionPushdownRule) Description() string {
	return "Inserts early FIELDS before expensive operators to reduce column width"
}

// Apply is implemented in columns.go

type columnPruningRule struct{}

func (r *columnPruningRule) Name() string { return "ColumnPruning" }
func (r *columnPruningRule) Description() string {
	return "Computes minimal column set and skips unused columns during scan"
}

// Apply is implemented in columns.go

type unpackFieldPruningRule struct{}

func (r *unpackFieldPruningRule) Name() string { return "UnpackFieldPruning" }
func (r *unpackFieldPruningRule) Description() string {
	return "Restricts unpack field extraction to only downstream-consumed fields"
}

// Apply is implemented in columns.go

type filterPushdownIntoJoinRule struct{}

func (r *filterPushdownIntoJoinRule) Name() string { return "FilterPushdownIntoJoin" }
func (r *filterPushdownIntoJoinRule) Description() string {
	return "Pushes WHERE into JOIN subquery to reduce build-side rows"
}
func (r *filterPushdownIntoJoinRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	// Look for WHERE immediately after JOIN. If WHERE references only
	// the join field, push a copy into the subquery.
	for i := 1; i < len(q.Commands); i++ {
		w, isWhere := q.Commands[i].(*spl2.WhereCommand)
		if !isWhere {
			continue
		}
		j, isJoin := q.Commands[i-1].(*spl2.JoinCommand)
		if !isJoin || j.Subquery == nil {
			continue
		}
		// Push WHERE into subquery by appending it.
		j.Subquery.Commands = append(j.Subquery.Commands, w)
		// Keep WHERE in the outer pipeline too for correctness.
		return q, true
	}

	return q, false
}

type unionFilterPushdownRule struct{}

func (r *unionFilterPushdownRule) Name() string { return "UnionFilterPushdown" }
func (r *unionFilterPushdownRule) Description() string {
	return "Pushes WHERE into APPEND/MULTISEARCH branches"
}
func (r *unionFilterPushdownRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	// Look for WHERE after APPEND or MULTISEARCH and push into branches.
	for i := 1; i < len(q.Commands); i++ {
		w, isWhere := q.Commands[i].(*spl2.WhereCommand)
		if !isWhere {
			continue
		}
		switch c := q.Commands[i-1].(type) {
		case *spl2.AppendCommand:
			if c.Subquery != nil {
				c.Subquery.Commands = append(c.Subquery.Commands, w)

				return q, true
			}
		case *spl2.MultisearchCommand:
			for _, sub := range c.Searches {
				sub.Commands = append(sub.Commands, w)
			}

			return q, true
		}
	}

	return q, false
}

type timeRangePruningRule struct{}

func (r *timeRangePruningRule) Name() string { return "TimeRangePruning" }
func (r *timeRangePruningRule) Description() string {
	return "Extracts _time bounds for segment-level time range skip"
}
func (r *timeRangePruningRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	if q.Annotations != nil {
		if _, done := q.Annotations["timeAnnotation"]; done {
			return q, false
		}
	}
	earliest, latest := "", ""
	found := false
	for _, cmd := range q.Commands {
		w, ok := cmd.(*spl2.WhereCommand)
		if !ok {
			continue
		}
		e, l, ok2 := extractTimeBoundsFromExpr(w.Expr)
		if ok2 {
			found = true
			if e != "" {
				earliest = e
			}
			if l != "" {
				latest = l
			}
		}
	}
	if !found {
		return q, false
	}
	q.Annotate("timeAnnotation", map[string]string{
		"earliest": earliest,
		"latest":   latest,
	})

	return q, true
}

// extractTimeBoundsFromExpr returns earliest, latest, and whether any were found.
func extractTimeBoundsFromExpr(expr spl2.Expr) (earliest, latest string, found bool) {
	switch e := expr.(type) {
	case *spl2.CompareExpr:
		field, ok := e.Left.(*spl2.FieldExpr)
		if !ok || field.Name != "_time" {
			return "", "", false
		}
		lit, ok := e.Right.(*spl2.LiteralExpr)
		if !ok {
			return "", "", false
		}
		switch e.Op {
		case ">=", ">":
			return lit.Value, "", true
		case "<=", "<":
			return "", lit.Value, true
		case "=", "==":
			return lit.Value, lit.Value, true
		}
	case *spl2.BinaryExpr:
		if strings.EqualFold(e.Op, "and") {
			e1, l1, f1 := extractTimeBoundsFromExpr(e.Left)
			e2, l2, f2 := extractTimeBoundsFromExpr(e.Right)
			if e2 != "" {
				e1 = e2
			}
			if l2 != "" {
				l1 = l2
			}

			return e1, l1, f1 || f2
		}
	}

	return "", "", false
}

type bloomFilterPruningRule struct{}

func (r *bloomFilterPruningRule) Name() string { return "BloomFilterPruning" }
func (r *bloomFilterPruningRule) Description() string {
	return "Extracts bloom filter terms for segment-level skip"
}
func (r *bloomFilterPruningRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	if q.Annotations != nil {
		if _, done := q.Annotations["bloomTerms"]; done {
			return q, false
		}
	}

	// Fields whose values are bloom-indexed.
	bloomFields := map[string]bool{
		"_raw": true, "host": true, "source": true, "_source": true,
		"sourcetype": true, "_sourcetype": true,
	}

	var terms []string
	for _, cmd := range q.Commands {
		switch c := cmd.(type) {
		case *spl2.WhereCommand:
			extractBloomTermsFromExpr(c.Expr, bloomFields, &terms)
		case *spl2.SearchCommand:
			// Search terms are already extracted by hints — also capture
			// field-specific comparisons from SearchExpr.
			if c.Expression != nil {
				extractBloomTermsFromSearch(c.Expression, bloomFields, &terms)
			}
			// Also extract from SearchCommand.Term via tokenization.
			if c.Term != "" {
				tokens := index.Tokenize(c.Term)
				terms = append(terms, tokens...)
			}
		}
	}
	if len(terms) == 0 {
		return q, false
	}
	q.Annotate("bloomTerms", terms)

	return q, true
}

// extractBloomTermsFromExpr extracts bloom-eligible terms from WHERE expressions.
func extractBloomTermsFromExpr(expr spl2.Expr, bloomFields map[string]bool, terms *[]string) {
	switch e := expr.(type) {
	case *spl2.CompareExpr:
		if e.Op == "=" || e.Op == "==" {
			if field, ok := e.Left.(*spl2.FieldExpr); ok {
				if bloomFields[field.Name] {
					if lit, ok := e.Right.(*spl2.LiteralExpr); ok {
						*terms = append(*terms, strings.ToLower(lit.Value))
					}
				}
			}
		}
		// Extract leading literal from LIKE pattern for bloom pre-filter.
		// "uri LIKE '/api/%'" → bloom term "/api/" (if len >= 3).
		if e.Op == "like" {
			if field, ok := e.Left.(*spl2.FieldExpr); ok && bloomFields[field.Name] {
				if lit, ok := e.Right.(*spl2.LiteralExpr); ok {
					if prefix := extractLikePrefix(lit.Value); len(prefix) >= 3 {
						// Tokenize the same way the bloom filter writer does so
						// multi-token prefixes like "Dec 10 09:" become ["dec", "09"].
						tokens := index.Tokenize(prefix)
						for _, tok := range tokens {
							if len(tok) >= 3 {
								*terms = append(*terms, tok)
							}
						}
					}
				}
			}
		}
		// Extract leading literals from =~ regex operator for bloom pre-filter.
		// "uri =~ /\/api\/users\/\d+/" → bloom term "/api/users/" (if len >= 3).
		if e.Op == "=~" {
			if field, ok := e.Left.(*spl2.FieldExpr); ok && bloomFields[field.Name] {
				if lit, ok := e.Right.(*spl2.LiteralExpr); ok {
					extractRegexBloomTerms(lit.Value, terms)
				}
			}
		}
	case *spl2.InExpr:
		// Extract bloom terms from IN value lists on bloom-eligible fields.
		// WHERE _source IN ("nginx", "redis") → bloom terms ["nginx", "redis"].
		if !e.Negated {
			if field, ok := e.Field.(*spl2.FieldExpr); ok && bloomFields[field.Name] {
				for _, v := range e.Values {
					if lit, ok := v.(*spl2.LiteralExpr); ok {
						*terms = append(*terms, strings.ToLower(lit.Value))
					}
				}
			}
		}
	case *spl2.BinaryExpr:
		if strings.EqualFold(e.Op, "and") {
			extractBloomTermsFromExpr(e.Left, bloomFields, terms)
			extractBloomTermsFromExpr(e.Right, bloomFields, terms)
		}
		// OR branches: terms cannot be safely extracted for bloom pruning.
	case *spl2.FuncCallExpr:
		// Extract bloom terms from match(field, "regex") calls.
		if strings.EqualFold(e.Name, "match") && len(e.Args) == 2 {
			if field, ok := e.Args[0].(*spl2.FieldExpr); ok && bloomFields[field.Name] {
				if lit, ok := e.Args[1].(*spl2.LiteralExpr); ok {
					extractRegexBloomTerms(lit.Value, terms)
				}
			}
		}
	}
}

// extractRegexBloomTerms extracts leading literals from a regex pattern
// for bloom pre-filtering. Extracted literals are tokenized using the same
// tokenizer the bloom filter writer uses so that multi-token strings like
// "173.234.31.186" are split into individual tokens ["173", "234", "31", "186"].
// Only tokens >= 3 characters are useful for bloom filtering.
func extractRegexBloomTerms(pattern string, terms *[]string) {
	lits := extractRegexLiterals(pattern)
	for _, lit := range lits {
		// Must tokenize the same way the bloom filter writer does.
		// index.Tokenize already lowercases, so no need for strings.ToLower.
		tokens := index.Tokenize(lit)
		for _, tok := range tokens {
			if len(tok) >= 3 {
				*terms = append(*terms, tok)
			}
		}
	}
}

// extractLikePrefix returns the leading literal portion of a LIKE pattern
// (everything before the first % or _). Returns "" if the pattern starts
// with a wildcard or the prefix is too short to be useful.
func extractLikePrefix(pattern string) string {
	for i := 0; i < len(pattern); i++ {
		if pattern[i] == '%' || pattern[i] == '_' {
			return pattern[:i]
		}
	}

	return pattern // No wildcards — entire pattern is literal
}

// extractBloomTermsFromSearch extracts bloom-eligible terms from search expressions.
func extractBloomTermsFromSearch(expr spl2.SearchExpr, bloomFields map[string]bool, terms *[]string) {
	switch e := expr.(type) {
	case *spl2.SearchAndExpr:
		extractBloomTermsFromSearch(e.Left, bloomFields, terms)
		extractBloomTermsFromSearch(e.Right, bloomFields, terms)
	case *spl2.SearchCompareExpr:
		if e.Op == spl2.OpEq && !e.HasWildcard && bloomFields[e.Field] {
			*terms = append(*terms, strings.ToLower(e.Value))
		}
	}
}

type columnStatsPruningRule struct{}

func (r *columnStatsPruningRule) Name() string { return "ColumnStatsPruning" }
func (r *columnStatsPruningRule) Description() string {
	return "Uses column min/max stats to skip segments that cannot match"
}
func (r *columnStatsPruningRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	if q.Annotations != nil {
		if _, done := q.Annotations["fieldPredicates"]; done {
			return q, false
		}
	}

	// Track fields generated by pipeline commands (STREAMSTATS, EVENTSTATS,
	// EVAL, REX, BIN, RENAME). Predicates on these fields MUST NOT be pushed
	// to the segment reader because the fields don't exist in segment data —
	// they are created during pipeline execution. If pushed, the segment
	// reader's evalPredicatesOnEvent returns false for every event (null field
	// → no match) and the query returns 0 rows.
	generatedFields := collectGeneratedFields(q)

	var preds []FieldPredInfo
	for _, cmd := range q.Commands {
		switch c := cmd.(type) {
		case *spl2.WhereCommand:
			extractFieldPredicatesFromExpr(c.Expr, &preds)
		case *spl2.SearchCommand:
			if c.Expression != nil {
				extractFieldPredicatesFromSearch(c.Expression, &preds)
			}
		}
	}

	// Remove predicates that reference pipeline-generated fields.
	if !generatedFields.empty() {
		filtered := preds[:0]
		for _, p := range preds {
			if !generatedFields.contains(p.Field) {
				filtered = append(filtered, p)
			}
		}
		preds = filtered
	}

	if len(preds) == 0 {
		return q, false
	}
	q.Annotate("fieldPredicates", preds)

	return q, true
}

// generatedFieldsInfo holds explicitly known generated field names and
// prefixes from schema-on-read operators (UnpackCommand with no Fields list).
type generatedFieldsInfo struct {
	names    map[string]bool // explicit field names
	prefixes []string        // prefixes from unpack (e.g., "pg.", "j.")
}

// contains returns true if the field is generated — either by exact name
// match or by matching a prefix from a schema-on-read operator.
func (g *generatedFieldsInfo) contains(field string) bool {
	if g.names[field] {
		return true
	}
	for _, prefix := range g.prefixes {
		if strings.HasPrefix(field, prefix) {
			return true
		}
	}
	return false
}

// empty returns true if no generated fields were collected.
func (g *generatedFieldsInfo) empty() bool {
	return len(g.names) == 0 && len(g.prefixes) == 0
}

// collectGeneratedFields walks all commands and returns a set of field names
// and prefixes that are created by pipeline operators (not present in segment
// data). These fields must be excluded from segment-level predicate pushdown.
func collectGeneratedFields(q *spl2.Query) *generatedFieldsInfo {
	info := &generatedFieldsInfo{names: make(map[string]bool)}
	for _, cmd := range q.Commands {
		switch c := cmd.(type) {
		case *spl2.StreamstatsCommand:
			for _, agg := range c.Aggregations {
				if agg.Alias != "" {
					info.names[agg.Alias] = true
				}
			}
		case *spl2.EventstatsCommand:
			for _, agg := range c.Aggregations {
				if agg.Alias != "" {
					info.names[agg.Alias] = true
				}
			}
		case *spl2.EvalCommand:
			if c.Field != "" {
				info.names[c.Field] = true
			}
			for _, a := range c.Assignments {
				info.names[a.Field] = true
			}
		case *spl2.RexCommand:
			// REX generates fields from named capture groups in the pattern.
			// Go supports both (?P<name>...) and (?<name>...) syntax.
			for _, name := range extractNamedGroups(c.Pattern) {
				info.names[name] = true
			}
		case *spl2.BinCommand:
			if c.Alias != "" {
				info.names[c.Alias] = true
			}
			// BIN without alias overwrites the field — but it exists in
			// segments already, so we don't mark it as generated.
		case *spl2.RenameCommand:
			for _, r := range c.Renames {
				info.names[r.New] = true
			}
		case *spl2.UnpackCommand:
			// Unpack generates fields from the parsed format. When Fields is
			// specified, the exact output names are known. When Fields is empty
			// (schema-on-read — the common case), the exact field set is unknown
			// at optimization time. If the command has a Prefix, we record it so
			// any field starting with that prefix is treated as generated.
			if len(c.Fields) > 0 {
				for _, f := range c.Fields {
					name := f
					if c.Prefix != "" {
						name = c.Prefix + f
					}
					info.names[name] = true
				}
			} else if c.Prefix != "" {
				info.prefixes = append(info.prefixes, c.Prefix)
			}
		case *spl2.JsonCommand:
			// Like unpack, json generates fields dynamically. If Paths are
			// specified, mark the output names (alias or path) as generated.
			for _, p := range c.Paths {
				info.names[p.OutputName()] = true
			}
		case *spl2.UnrollCommand:
			// Unroll explodes a JSON array field into multiple rows and generates
			// dot-notation fields from object elements (e.g., items.sku, items.qty).
			// The exact generated field names are unknown at optimization time,
			// so the source field is marked as generated (it's replaced with element values).
			info.names[c.Field] = true
			for _, f := range c.ExtraFields {
				info.names[f] = true
			}
		}
	}
	return info
}

// extractNamedGroups returns the names of named capture groups from a regex
// pattern. Handles both (?P<name>...) and (?<name>...) syntax (Go supports
// both since Go 1.20).
func extractNamedGroups(pattern string) []string {
	var names []string
	for i := 0; i < len(pattern)-3; i++ {
		if pattern[i] != '(' || pattern[i+1] != '?' {
			continue
		}
		nameStart := -1
		// (?P<name>...) syntax
		if i+3 < len(pattern) && pattern[i+2] == 'P' && pattern[i+3] == '<' {
			nameStart = i + 4
		}
		// (?<name>...) syntax — exclude (?<=) lookbehind and (?<!) negative lookbehind
		if pattern[i+2] == '<' && (i+3 >= len(pattern) || (pattern[i+3] != '=' && pattern[i+3] != '!')) {
			nameStart = i + 3
		}
		if nameStart < 0 {
			continue
		}
		end := strings.IndexByte(pattern[nameStart:], '>')
		if end > 0 {
			names = append(names, pattern[nameStart:nameStart+end])
		}
	}
	return names
}

// FieldPredInfo holds a simple field op literal predicate for annotation.
type FieldPredInfo struct {
	Field string
	Op    string
	Value string
}

// extractFieldPredicatesFromSearch walks a SearchExpr and extracts field=value
// and field op value comparisons that can be used for segment column stats pruning.
// Only extracts from AND branches (conjunctive). OR and NOT are skipped because
// they cannot be safely pushed as conjunctive predicates.
func extractFieldPredicatesFromSearch(expr spl2.SearchExpr, preds *[]FieldPredInfo) {
	switch e := expr.(type) {
	case *spl2.SearchAndExpr:
		extractFieldPredicatesFromSearch(e.Left, preds)
		extractFieldPredicatesFromSearch(e.Right, preds)
	case *spl2.SearchCompareExpr:
		// Skip wildcard values — they cannot be used for min/max pruning.
		if e.HasWildcard {
			return
		}
		*preds = append(*preds, FieldPredInfo{
			Field: e.Field,
			Op:    e.Op.String(),
			Value: e.Value,
		})
	case *spl2.SearchOrExpr:
		// OR cannot be pushed as conjunctive predicates — skip.
	case *spl2.SearchNotExpr:
		// NOT complicates min/max checks — skip.
	}
}

func extractFieldPredicatesFromExpr(expr spl2.Expr, preds *[]FieldPredInfo) {
	switch e := expr.(type) {
	case *spl2.CompareExpr:
		field, ok := e.Left.(*spl2.FieldExpr)
		if !ok {
			return
		}
		lit, ok := e.Right.(*spl2.LiteralExpr)
		if !ok {
			return
		}
		if field.Name == "_time" {
			return // time handled by time range rule
		}
		// Skip wildcard patterns — they are compiled to OpStrMatch (regex) by
		// the VM but column stats pruning uses literal comparison.
		if (e.Op == "=" || e.Op == "==" || e.Op == "!=") && strings.ContainsAny(lit.Value, "*?") {
			return
		}
		*preds = append(*preds, FieldPredInfo{
			Field: field.Name, Op: e.Op, Value: lit.Value,
		})
	case *spl2.BinaryExpr:
		if strings.EqualFold(e.Op, "and") {
			extractFieldPredicatesFromExpr(e.Left, preds)
			extractFieldPredicatesFromExpr(e.Right, preds)
		}
	case *spl2.InExpr:
		// Extract min/max of IN-list literal values as synthetic range predicates.
		// This enables segment skipping when column stats show the segment's range
		// doesn't overlap the IN values. Example:
		//   status IN (400, 404, 500) → status >= 400 AND status <= 500
		if e.Negated {
			return // NOT IN doesn't define a range
		}
		field, ok := e.Field.(*spl2.FieldExpr)
		if !ok {
			return
		}
		if field.Name == "_time" {
			return
		}

		var minStr, maxStr string
		for _, v := range e.Values {
			lit, ok := v.(*spl2.LiteralExpr)
			if !ok {
				return // non-literal value, bail out
			}
			if minStr == "" || lit.Value < minStr {
				minStr = lit.Value
			}
			if maxStr == "" || lit.Value > maxStr {
				maxStr = lit.Value
			}
		}
		if minStr != "" {
			*preds = append(*preds,
				FieldPredInfo{Field: field.Name, Op: ">=", Value: minStr},
				FieldPredInfo{Field: field.Name, Op: "<=", Value: maxStr},
			)
		}
	}
}

type partitionPruningRule struct{}

func (r *partitionPruningRule) Name() string { return "PartitionPruning" }
func (r *partitionPruningRule) Description() string {
	return "Skips partitions that cannot match the query predicates"
}
func (r *partitionPruningRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	return q, false // Not yet partitioned
}
