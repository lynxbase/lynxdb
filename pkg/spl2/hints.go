package spl2

import (
	"strconv"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/storage/segment/index"
)

// SearchTermOp represents a boolean operator in a search term tree.
type SearchTermOp int

const (
	// SearchTermLeaf is a leaf node holding tokenized terms (AND'd together).
	SearchTermLeaf SearchTermOp = iota
	// SearchTermAnd combines children with intersection (all must match).
	SearchTermAnd
	// SearchTermOr combines children with union (any may match).
	SearchTermOr
)

// SearchTermTree represents a boolean tree of search terms for inverted index
// bitmap evaluation. Leaf nodes hold tokenized terms (AND'd together). Inner
// nodes combine children with AND (intersection) or OR (union).
type SearchTermTree struct {
	Op       SearchTermOp
	Terms    []string          // populated only for Leaf nodes
	Children []*SearchTermTree // populated for And/Or nodes
}

// Source scope type constants for multi-source query resolution.
const (
	SourceScopeAll    = "all"    // FROM * — scan all sources
	SourceScopeSingle = "single" // single source (FROM main, source=nginx)
	SourceScopeList   = "list"   // explicit list (FROM a, b, c or resolved glob)
	SourceScopeGlob   = "glob"   // unresolved glob pattern (FROM logs*)
)

// Field name constants for virtual-to-physical column aliasing.
const (
	fieldSource         = "source"  // virtual alias used in SPL2
	fieldSourcePhysical = "_source" // physical column name in segments
)

// QueryHints holds optimization hints extracted from a parsed query AST.
type QueryHints struct {
	SearchTerms             []string                 // tokenized search terms (lowercased)
	SearchTermTree          *SearchTermTree          // structured boolean tree for inverted index OR/AND
	IndexName               string                   // from Source.Index or SearchCommand.Index
	SourceIndices           []string                 // multiple source names from FROM a, b, c
	SourceGlob              string                   // glob pattern from FROM logs* (empty = not a glob)
	SourceScopeType         string                   // "all", "single", "list", "glob" — from optimizer
	SourceScopeSources      []string                 // resolved source names for scope (single/list)
	SourceScopePattern      string                   // glob pattern for scope
	TimeBounds              *TimeBounds              // from WHERE _time >= X AND _time <= X
	RequiredCols            []string                 // from GetRequiredColumns()
	Limit                   int                      // from terminal HeadCommand (0 = unlimited)
	TailLimit               int                      // from terminal TailCommand (0 = not a tail query)
	ReverseScan             bool                     // true if optimizer determined reverse scan is safe
	FieldPredicates         []FieldPredicate         // simple field op literal from WHERE
	InvertedIndexPredicates []InvertedIndexPredicate // field=value for inverted index
	RangePredicates         []RangePredicate         // field range for pushdown
	InPredicates            []InPredicate            // field IN (values) for segment-level pushdown
	SkipRaw                 bool                     // true when _raw is not needed — all required fields are stored columns
	searchExpr              SearchExpr               // first search expression (for pushdown analysis)
	streamable              bool                     // true if query can execute without accumulating all events
	terminalCmd             string                   // type of last command ("stats", "head", "search", etc.)
	sourceIndexSet          map[string]struct{}      // lazy cache for O(1) list lookups
}

// TimeBounds represents earliest/latest time constraints.
type TimeBounds struct {
	Earliest time.Time
	Latest   time.Time
}

// FieldPredicate represents a simple field comparison extracted from WHERE.
type FieldPredicate struct {
	Field string
	Op    string // "=", "!=", "<", "<=", ">", ">="
	Value string
}

// InvertedIndexPredicate holds a field=value pair for inverted index lookup.
type InvertedIndexPredicate struct {
	Field string
	Value string
}

// RangePredicate holds a field range for pushdown.
type RangePredicate struct {
	Field string
	Min   string
	Max   string
}

// InPredicate represents a field IN (val1, val2, ...) predicate extracted
// from WHERE for segment-level pushdown. Only non-negated predicates with
// all-literal value lists are eligible.
type InPredicate struct {
	Field  string
	Values []string
}

// ExtractQueryHints walks the AST of a Program and extracts optimization hints.
func ExtractQueryHints(prog *Program) *QueryHints {
	if prog == nil || prog.Main == nil {
		return &QueryHints{}
	}

	return extractQueryHintsFromQuery(prog.Main)
}

func extractQueryHintsFromQuery(q *Query) *QueryHints {
	h := &QueryHints{}

	if q.Source != nil && !q.Source.IsVariable {
		h.IndexName = q.Source.Index
		if len(q.Source.Indices) > 0 {
			h.SourceIndices = q.Source.Indices
		}
		if q.Source.IsGlob {
			h.SourceGlob = q.Source.Index
		}
	}

	// Collect fields generated by pipeline operators (STREAMSTATS, EVENTSTATS,
	// EVAL, REX, BIN, RENAME). Predicates on these fields must NOT be pushed
	// to the segment reader because they don't exist in stored data.
	generatedFields := collectGeneratedFieldsFromQuery(q)

	// Walk commands.
	for _, cmd := range q.Commands {
		switch c := cmd.(type) {
		case *SearchCommand:
			extractSearchHints(c, h)
		case *WhereCommand:
			extractWhereHints(c.Expr, h)
		}
	}

	// Remove predicates that reference pipeline-generated fields.
	// These fields are created during pipeline execution (by STREAMSTATS,
	// EVENTSTATS, EVAL, REX, BIN, RENAME, UNPACK) and don't exist in segment
	// data. Pushing them to the segment reader causes evalPredicatesOnEvent to
	// reject ALL events (null field → no match → 0 rows returned).
	if !generatedFields.empty() && len(h.FieldPredicates) > 0 {
		filtered := h.FieldPredicates[:0]
		for _, fp := range h.FieldPredicates {
			if !generatedFields.contains(fp.Field) {
				filtered = append(filtered, fp)
			}
		}
		h.FieldPredicates = filtered
	}

	// Same filter for IN predicates — fields created by pipeline operators
	// (EVAL, REX, STREAMSTATS, UNPACK, etc.) don't exist in segment data.
	if !generatedFields.empty() && len(h.InPredicates) > 0 {
		filtered := h.InPredicates[:0]
		for _, ip := range h.InPredicates {
			if !generatedFields.contains(ip.Field) {
				filtered = append(filtered, ip)
			}
		}
		h.InPredicates = filtered
	}

	// Terminal HeadCommand → set Limit hint only when safe to push down.
	// Limit pushdown is unsafe when intermediate commands filter rows, aggregate,
	// reorder, or deduplicate — the scan-level limit would stop reading before
	// enough rows pass through the pipeline.
	if len(q.Commands) > 0 {
		if hc, ok := q.Commands[len(q.Commands)-1].(*HeadCommand); ok {
			if isLimitPushdownSafe(q.Commands[:len(q.Commands)-1]) {
				h.Limit = hc.Count
			}
		}
	}

	// Terminal TailCommand → set TailLimit hint.
	if len(q.Commands) > 0 {
		if tc, ok := q.Commands[len(q.Commands)-1].(*TailCommand); ok {
			h.TailLimit = tc.Count
		}
	}

	// Determine terminal command and streamability.
	h.streamable = true // assume streamable unless proven otherwise
	if len(q.Commands) > 0 {
		h.terminalCmd = classifyCommand(q.Commands[len(q.Commands)-1])
	}
	for _, cmd := range q.Commands {
		if requiresFullMaterialization(cmd) {
			h.streamable = false

			break
		}
	}

	// Get required columns.
	h.RequiredCols = GetRequiredColumns(q)

	// Merge optimizer annotations into hints.
	mergeAnnotations(q, h)

	return h
}

// isLimitPushdownSafe returns true if none of the commands can reduce the
// number of rows or change their order, making it safe to stop the scan
// after Limit raw rows.
func isLimitPushdownSafe(cmds []Command) bool {
	for _, cmd := range cmds {
		switch cmd.(type) {
		case *TableCommand, *FieldsCommand, *RenameCommand,
			*EvalCommand, *FillnullCommand, *RexCommand,
			*BinCommand, *UnpackCommand:
			// Pass-through: don't filter or reorder.
			continue
		default:
			// WHERE, SEARCH, SORT, DEDUP, STATS, STREAMSTATS,
			// EVENTSTATS, TOP, RARE, HEAD, TAIL, JOIN, APPEND,
			// TRANSACTION, MULTISEARCH, XYSERIES, etc.
			return false
		}
	}
	return true
}

// mergeAnnotations reads optimizer-produced annotations and merges them into QueryHints.
func mergeAnnotations(q *Query, h *QueryHints) {
	if q.Annotations == nil {
		return
	}

	// requiredColumns — override if optimizer computed them.
	if cols, ok := q.Annotations["requiredColumns"]; ok {
		if ss, ok := cols.([]string); ok && len(ss) > 0 {
			h.RequiredCols = ss
		}
	}

	// skipRaw — optimizer determined _raw is not needed.
	if sr, ok := q.Annotations["skipRaw"]; ok {
		if b, ok := sr.(bool); ok {
			h.SkipRaw = b
		}
	}

	// bloomTerms — merge into SearchTerms.
	if bt, ok := q.Annotations["bloomTerms"]; ok {
		if terms, ok := bt.([]string); ok {
			h.SearchTerms = appendUnique(h.SearchTerms, terms)
		}
	}

	// invertedIndexPredicates — for inverted index field=value lookups.
	if iip, ok := q.Annotations["invertedIndexPredicates"]; ok {
		if preds, ok := iip.([]InvertedIndexPredicate); ok {
			h.InvertedIndexPredicates = preds
		}
	}

	// rangePredicates — for field range pushdown.
	if rp, ok := q.Annotations["rangePredicates"]; ok {
		if preds, ok := rp.([]RangePredicate); ok {
			h.RangePredicates = preds
		}
	}

	// tailScanOptimization — reverse scan is safe for this tail query.
	if tso, ok := q.Annotations["tailScanOptimization"]; ok {
		if limit, ok := tso.(int); ok && limit > 0 {
			h.ReverseScan = true
			// Reinforce TailLimit from the optimizer annotation.
			if h.TailLimit == 0 {
				h.TailLimit = limit
			}
		}
	}

	// reverseScanOrder — sort on _time was eliminated because scan produces
	// time-ordered output. When ascending sort was requested, the optimizer
	// annotates this to flip the scan direction to oldest-first.
	if rso, ok := q.Annotations["reverseScanOrder"]; ok {
		if b, ok := rso.(bool); ok && b {
			h.ReverseScan = true
		}
	}

	// sourceScope — resolved source scope from optimizer.
	// Stored as map[string]interface{} to avoid cross-package type dependency.
	if ss, ok := q.Annotations["sourceScope"]; ok {
		if m, ok := ss.(map[string]interface{}); ok {
			if t, ok := m["type"].(string); ok {
				h.SourceScopeType = t
			}
			if srcs, ok := m["sources"].([]string); ok {
				h.SourceScopeSources = srcs
			}
			if pat, ok := m["pattern"].(string); ok {
				h.SourceScopePattern = pat
			}
		}
	}

	// timeAnnotation — merge into TimeBounds.
	if ta, ok := q.Annotations["timeAnnotation"]; ok {
		if tb, ok := ta.(map[string]string); ok {
			if e := tb["earliest"]; e != "" {
				ts := parseTimeLiteral(e)
				if !ts.IsZero() {
					if h.TimeBounds == nil {
						h.TimeBounds = &TimeBounds{}
					}
					if h.TimeBounds.Earliest.IsZero() || ts.After(h.TimeBounds.Earliest) {
						h.TimeBounds.Earliest = ts
					}
				}
			}
			if l := tb["latest"]; l != "" {
				ts := parseTimeLiteral(l)
				if !ts.IsZero() {
					if h.TimeBounds == nil {
						h.TimeBounds = &TimeBounds{}
					}
					if h.TimeBounds.Latest.IsZero() || ts.Before(h.TimeBounds.Latest) {
						h.TimeBounds.Latest = ts
					}
				}
			}
		}
	}
}

// appendUnique appends items to dst that aren't already present.
func appendUnique(dst, src []string) []string {
	seen := make(map[string]bool, len(dst))
	for _, s := range dst {
		seen[s] = true
	}
	for _, s := range src {
		if !seen[s] {
			dst = append(dst, s)
			seen[s] = true
		}
	}

	return dst
}

func extractSearchHints(cmd *SearchCommand, h *QueryHints) {
	// SearchCommand.Index overrides source index.
	if cmd.Index != "" {
		h.IndexName = cmd.Index
	}

	// Full search expression: extract literal search terms for bloom filter.
	if cmd.Expression != nil {
		// Store the first search expression for pushdown analysis.
		if h.searchExpr == nil {
			h.searchExpr = cmd.Expression
		}
		// Flat terms for bloom filter (backward compat, AND-only).
		terms := ExtractSearchTermsFromExpr(cmd.Expression)
		h.SearchTerms = append(h.SearchTerms, terms...)
		// Structured tree for inverted index bitmap OR/AND.
		if h.SearchTermTree == nil {
			h.SearchTermTree = ExtractSearchTermTree(cmd.Expression)
		}

		return
	}

	// Tokenize search term to match writer's tokenizer.
	if cmd.Term != "" {
		tokens := index.Tokenize(cmd.Term)
		h.SearchTerms = append(h.SearchTerms, tokens...)
	}
}

// generatedFieldInfo holds both explicitly known generated field names and
// prefixes from schema-on-read operators (UnpackCommand with no Fields list).
// A field is considered "generated" if it's in the Names set OR if it starts
// with any of the Prefixes.
type generatedFieldInfo struct {
	Names    map[string]bool // explicit field names
	Prefixes []string        // prefixes from unpack (e.g., "pg.", "j.")
}

// contains returns true if the field is generated — either by exact name
// match or by matching a prefix from a schema-on-read operator.
func (g *generatedFieldInfo) contains(field string) bool {
	if g.Names[field] {
		return true
	}
	for _, prefix := range g.Prefixes {
		if strings.HasPrefix(field, prefix) {
			return true
		}
	}
	return false
}

// empty returns true if no generated fields were collected.
func (g *generatedFieldInfo) empty() bool {
	return len(g.Names) == 0 && len(g.Prefixes) == 0
}

// collectGeneratedFieldsFromQuery returns the set of field names and prefixes
// that are created by pipeline operators in the query. These fields don't exist
// in segment data and must not be used for segment-level predicate pushdown.
func collectGeneratedFieldsFromQuery(q *Query) *generatedFieldInfo {
	info := &generatedFieldInfo{Names: make(map[string]bool)}
	for _, cmd := range q.Commands {
		switch c := cmd.(type) {
		case *StreamstatsCommand:
			for _, agg := range c.Aggregations {
				if agg.Alias != "" {
					info.Names[agg.Alias] = true
				}
			}
		case *EventstatsCommand:
			for _, agg := range c.Aggregations {
				if agg.Alias != "" {
					info.Names[agg.Alias] = true
				}
			}
		case *EvalCommand:
			if c.Field != "" {
				info.Names[c.Field] = true
			}
			for _, a := range c.Assignments {
				info.Names[a.Field] = true
			}
		case *RexCommand:
			// REX generates fields from named capture groups.
			// Go supports both (?P<name>...) and (?<name>...) syntax.
			for _, name := range ExtractNamedGroupsFromPattern(c.Pattern) {
				info.Names[name] = true
			}
		case *BinCommand:
			if c.Alias != "" {
				info.Names[c.Alias] = true
			}
		case *RenameCommand:
			for _, r := range c.Renames {
				info.Names[r.New] = true
			}
		case *UnpackCommand:
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
					info.Names[name] = true
				}
			} else if c.Prefix != "" {
				info.Prefixes = append(info.Prefixes, c.Prefix)
			}
		}
	}
	return info
}

// ExtractNamedGroupsFromPattern returns names from named capture groups in a
// Go-style regex pattern. Handles both (?P<name>...) and (?<name>...) syntax.
func ExtractNamedGroupsFromPattern(pattern string) []string {
	var names []string
	for i := 0; i < len(pattern)-3; i++ {
		if pattern[i] != '(' || pattern[i+1] != '?' {
			continue
		}
		// (?P<name>...) syntax
		nameStart := -1
		if i+3 < len(pattern) && pattern[i+2] == 'P' && pattern[i+3] == '<' {
			nameStart = i + 4
		}
		// (?<name>...) syntax
		if pattern[i+2] == '<' && (i+3 >= len(pattern) || (pattern[i+3] != '=' && pattern[i+3] != '!')) {
			// Exclude (?<=...) lookbehind and (?<!...) negative lookbehind
			nameStart = i + 3
		}
		if nameStart < 0 {
			continue
		}
		end := -1
		for j := nameStart; j < len(pattern); j++ {
			if pattern[j] == '>' {
				end = j
				break
			}
		}
		if end > nameStart {
			names = append(names, pattern[nameStart:end])
		}
	}
	return names
}

func extractWhereHints(expr Expr, h *QueryHints) {
	switch e := expr.(type) {
	case *CompareExpr:
		extractCompareHints(e, h)
	case *BinaryExpr:
		if e.Op == "and" {
			extractWhereHints(e.Left, h)
			extractWhereHints(e.Right, h)
		}
		// OR branches cannot be safely pushed down as conjunctive predicates.
	case *InExpr:
		extractInHints(e, h)
	}
}

// extractInHints extracts field IN (values) predicates for segment-level pushdown.
// Only non-negated predicates with a field reference and all-literal values are eligible.
// NOT IN cannot safely prune segments (absence of a value doesn't mean the row group
// can be skipped — other values in the set may still match).
func extractInHints(e *InExpr, h *QueryHints) {
	if e.Negated {
		return // NOT IN cannot safely prune segments
	}
	field, ok := e.Field.(*FieldExpr)
	if !ok {
		return
	}
	var values []string
	for _, v := range e.Values {
		lit, ok := v.(*LiteralExpr)
		if !ok {
			return // Non-literal value (subquery, field ref) — bail out entirely
		}
		values = append(values, lit.Value)
	}
	h.InPredicates = append(h.InPredicates, InPredicate{
		Field:  field.Name,
		Values: values,
	})
}

func extractCompareHints(e *CompareExpr, h *QueryHints) {
	field, ok := e.Left.(*FieldExpr)
	if !ok {
		return
	}
	lit, ok := e.Right.(*LiteralExpr)
	if !ok {
		return
	}

	// Extract _time bounds.
	if field.Name == "_time" {
		ts := parseTimeLiteral(lit.Value)
		if ts.IsZero() {
			return
		}
		if h.TimeBounds == nil {
			h.TimeBounds = &TimeBounds{}
		}
		switch e.Op {
		case ">=", ">":
			if h.TimeBounds.Earliest.IsZero() || ts.After(h.TimeBounds.Earliest) {
				h.TimeBounds.Earliest = ts
			}
		case "<=", "<":
			if h.TimeBounds.Latest.IsZero() || ts.Before(h.TimeBounds.Latest) {
				h.TimeBounds.Latest = ts
			}
		case "=", "==":
			h.TimeBounds.Earliest = ts
			h.TimeBounds.Latest = ts
		}

		return
	}

	// Skip wildcard patterns — they are compiled to OpStrMatch (regex) by the
	// VM compiler, but FieldPredicates are pushed to the segment reader which
	// uses literal string equality. Pushing "*ssh*" as a literal "=" predicate
	// causes evalStringPredicate to compare val == "*ssh*" (always false),
	// incorrectly filtering out all events.
	if (e.Op == "=" || e.Op == "==" || e.Op == "!=") && containsWildcard(lit.Value) {
		return
	}

	// Extract simple field predicates (non-_time).
	h.FieldPredicates = append(h.FieldPredicates, FieldPredicate{
		Field: field.Name,
		Op:    e.Op,
		Value: lit.Value,
	})
}

// GetRequiredColumns analyzes a query's commands and returns the list of
// field names that may be accessed during execution.
func GetRequiredColumns(q *Query) []string {
	cols := make(map[string]bool)
	cols["_time"] = true
	cols["_raw"] = true // always include _raw — it's the core log line field
	for _, cmd := range q.Commands {
		switch c := cmd.(type) {
		case *SearchCommand:
			cols["_raw"] = true
			if c.Expression != nil {
				collectSearchExprFields(c.Expression, cols)
			}
		case *WhereCommand:
			collectExprFields(c.Expr, cols)
		case *StatsCommand:
			for _, f := range c.GroupBy {
				cols[normalizeFieldName(f)] = true
			}
			for _, agg := range c.Aggregations {
				for _, arg := range agg.Args {
					collectExprFields(arg, cols)
				}
			}
		case *EvalCommand:
			collectExprFields(c.Expr, cols)
		case *SortCommand:
			for _, sf := range c.Fields {
				cols[normalizeFieldName(sf.Name)] = true
			}
		case *FieldsCommand:
			if FieldListHasGlob(c.Fields) {
				return nil // glob patterns — all columns needed
			}
			for _, f := range c.Fields {
				cols[normalizeFieldName(f)] = true
			}
		case *TableCommand:
			if FieldListHasGlob(c.Fields) {
				return nil // glob patterns — all columns needed
			}
			for _, f := range c.Fields {
				cols[normalizeFieldName(f)] = true
			}
		case *DedupCommand:
			for _, f := range c.Fields {
				cols[normalizeFieldName(f)] = true
			}
		case *RexCommand:
			if c.Field != "" {
				cols[c.Field] = true
			} else {
				cols["_raw"] = true
			}
		case *BinCommand:
			cols[c.Field] = true
		case *RenameCommand:
			for _, r := range c.Renames {
				cols[r.Old] = true
			}
		case *StreamstatsCommand:
			for _, agg := range c.Aggregations {
				for _, arg := range agg.Args {
					collectExprFields(arg, cols)
				}
			}
			for _, f := range c.GroupBy {
				cols[normalizeFieldName(f)] = true
			}
		case *EventstatsCommand:
			for _, agg := range c.Aggregations {
				for _, arg := range agg.Args {
					collectExprFields(arg, cols)
				}
			}
			for _, f := range c.GroupBy {
				cols[normalizeFieldName(f)] = true
			}
		case *TransactionCommand:
			cols[c.Field] = true
			cols["_raw"] = true
		case *TopCommand:
			cols[normalizeFieldName(c.Field)] = true
			if c.ByField != "" {
				cols[normalizeFieldName(c.ByField)] = true
			}
		case *RareCommand:
			cols[normalizeFieldName(c.Field)] = true
			if c.ByField != "" {
				cols[normalizeFieldName(c.ByField)] = true
			}
		case *TimechartCommand:
			for _, agg := range c.Aggregations {
				for _, arg := range agg.Args {
					collectExprFields(arg, cols)
				}
			}
			for _, f := range c.GroupBy {
				cols[normalizeFieldName(f)] = true
			}
		case *UnpackCommand:
			if c.SourceField != "" {
				cols[c.SourceField] = true
			} else {
				cols["_raw"] = true
			}
		case *JsonCommand:
			if c.SourceField != "" {
				cols[c.SourceField] = true
			} else {
				cols["_raw"] = true
			}
		case *TraceCommand:
			// Trace reads arbitrary span fields (service, operation, duration_ms, etc.)
			// so all columns are needed — force full JSON parsing during ingest.
			return nil
		}
	}
	result := make([]string, 0, len(cols))
	for c := range cols {
		result = append(result, c)
	}

	return result
}

// normalizeFieldName maps virtual field aliases to their physical column names.
// "source" → "_source" (physical column). "index" is a real column, no aliasing.
func normalizeFieldName(name string) string {
	if name == fieldSource {
		return fieldSourcePhysical
	}

	return name
}

func collectSearchExprFields(expr SearchExpr, cols map[string]bool) {
	switch e := expr.(type) {
	case *SearchAndExpr:
		collectSearchExprFields(e.Left, cols)
		collectSearchExprFields(e.Right, cols)
	case *SearchOrExpr:
		collectSearchExprFields(e.Left, cols)
		collectSearchExprFields(e.Right, cols)
	case *SearchNotExpr:
		collectSearchExprFields(e.Operand, cols)
	case *SearchCompareExpr:
		// Normalize "source" → "_source": the physical column name is "_source",
		// but search expressions use the alias "source". Without normalization,
		// column projection requests "source" which doesn't exist in segments.
		cols[normalizeFieldName(e.Field)] = true
	case *SearchInExpr:
		cols[normalizeFieldName(e.Field)] = true
	}
}

func collectExprFields(expr Expr, cols map[string]bool) {
	switch e := expr.(type) {
	case *FieldExpr:
		cols[normalizeFieldName(e.Name)] = true
	case *CompareExpr:
		collectExprFields(e.Left, cols)
		collectExprFields(e.Right, cols)
	case *BinaryExpr:
		collectExprFields(e.Left, cols)
		collectExprFields(e.Right, cols)
	case *ArithExpr:
		collectExprFields(e.Left, cols)
		collectExprFields(e.Right, cols)
	case *NotExpr:
		collectExprFields(e.Expr, cols)
	case *FuncCallExpr:
		for _, arg := range e.Args {
			collectExprFields(arg, cols)
		}
	case *InExpr:
		collectExprFields(e.Field, cols)
	}
}

// IsStreamable returns true if the query can be executed without accumulating
// all events first. True for queries ending with stats, head, or pure filters.
// False for queries requiring global ordering (sort, tail, eventstats, join).
func (h *QueryHints) IsStreamable() bool {
	if h == nil {
		return false
	}

	return h.streamable
}

// TerminalCommand returns the type name of the last command in the pipeline.
func (h *QueryHints) TerminalCommand() string {
	if h == nil {
		return ""
	}

	return h.terminalCmd
}

// IsMultiSource returns true if the query targets multiple sources (list, glob, or all).
// Single-source queries (FROM main, source=nginx) and no-source queries return false.
func (h *QueryHints) IsMultiSource() bool {
	if h == nil {
		return false
	}

	return len(h.SourceIndices) > 0 || h.SourceGlob != "" ||
		h.SourceScopeType == "list" || h.SourceScopeType == "glob" || h.SourceScopeType == "all"
}

// SourceIndexSet returns a set of source scope sources for O(1) membership testing.
// Only builds the set when the list exceeds 16 entries; returns nil for short lists
// (linear scan is faster due to no allocation). The set is lazily cached.
func (h *QueryHints) SourceIndexSet() map[string]struct{} {
	if h.sourceIndexSet != nil {
		return h.sourceIndexSet
	}
	if len(h.SourceScopeSources) > 16 {
		h.sourceIndexSet = make(map[string]struct{}, len(h.SourceScopeSources))
		for _, s := range h.SourceScopeSources {
			h.sourceIndexSet[s] = struct{}{}
		}

		return h.sourceIndexSet
	}

	return nil
}

// RequiredFieldsMap returns the set of fields the query accesses as a map.
// Returns nil if no columns were analyzed (treat as "all fields needed").
func (h *QueryHints) RequiredFieldsMap() map[string]bool {
	if h == nil || len(h.RequiredCols) == 0 {
		return nil
	}
	m := make(map[string]bool, len(h.RequiredCols))
	for _, col := range h.RequiredCols {
		m[col] = true
	}

	return m
}

// CanPushdownToReader returns true if the query's filter can be partially
// evaluated on raw text lines before parsing.
// True when:
//   - Query starts with a search command (keyword search on _raw)
//   - Search expression is a simple term or AND of simple keyword terms
//   - No OR branches (either side could match)
//   - No NOT branches (pre-filter could give false negatives)
//   - No field-specific predicates that require parsed fields
//
// False when:
//   - Search has OR branches
//   - Search has NOT branches
//   - First command is where/stats/eval (no search to pushdown)
//   - Query uses field:value syntax (requires parsed fields)
func (h *QueryHints) CanPushdownToReader() bool {
	if h == nil || h.searchExpr == nil {
		return false
	}

	return isSimpleKeywordExpr(h.searchExpr)
}

// isSimpleKeywordExpr returns true if the expression is purely keyword/AND
// based (no OR, NOT, or field comparisons).
func isSimpleKeywordExpr(expr SearchExpr) bool {
	switch ex := expr.(type) {
	case *SearchAndExpr:
		return isSimpleKeywordExpr(ex.Left) && isSimpleKeywordExpr(ex.Right)
	case *SearchKeywordExpr:
		return true
	case *SearchOrExpr:
		return false
	case *SearchNotExpr:
		return false
	case *SearchCompareExpr:
		return false // field predicates require parsed fields
	case *SearchInExpr:
		return false // field predicates require parsed fields
	default:
		return false
	}
}

// CollectPreFilterBytes extracts literal substrings from the search expression
// that can be used for bytes.Contains pre-filtering at the reader level.
// Returns lowercased byte slices (search keywords are case-insensitive by default).
func (h *QueryHints) CollectPreFilterBytes() [][]byte {
	if h == nil || h.searchExpr == nil {
		return nil
	}
	var lits []string
	collectKeywordLiterals(h.searchExpr, &lits)
	if len(lits) == 0 {
		return nil
	}
	result := make([][]byte, len(lits))
	for i, s := range lits {
		result[i] = []byte(strings.ToLower(s))
	}

	return result
}

// collectKeywordLiterals walks a SearchExpr and extracts literal substrings
// from keyword nodes. Only collects from AND branches.
func collectKeywordLiterals(expr SearchExpr, lits *[]string) {
	switch ex := expr.(type) {
	case *SearchAndExpr:
		collectKeywordLiterals(ex.Left, lits)
		collectKeywordLiterals(ex.Right, lits)
	case *SearchKeywordExpr:
		if ex.Value == "*" {
			return // match-all, no useful literal
		}
		extracted := ExtractLiterals(ex.Value)
		if !ex.CaseSensitive {
			for i, l := range extracted {
				extracted[i] = strings.ToLower(l)
			}
		}
		*lits = append(*lits, extracted...)
	}
}

// CollectAllIndexNames walks the entire Program AST and returns all
// distinct index names referenced in FROM clauses, SEARCH index= syntax,
// APPEND/JOIN/MULTISEARCH subqueries, and CTE datasets.
// Used to determine if a query is multi-index (APPEND, JOIN, MULTISEARCH).
func CollectAllIndexNames(prog *Program) []string {
	if prog == nil {
		return nil
	}
	names := make(map[string]bool)
	if prog.Main != nil {
		collectIndexNamesFromQuery(prog.Main, names)
	}
	for _, ds := range prog.Datasets {
		collectIndexNamesFromQuery(ds.Query, names)
	}
	result := make([]string, 0, len(names))
	for name := range names {
		result = append(result, name)
	}

	return result
}

// collectIndexNamesFromQuery recursively walks a Query AST, collecting
// index names from the source clause, SEARCH commands, and subqueries
// inside APPEND, JOIN, and MULTISEARCH commands.
func collectIndexNamesFromQuery(q *Query, names map[string]bool) {
	if q == nil {
		return
	}
	if q.Source != nil && !q.Source.IsVariable {
		if len(q.Source.Indices) > 0 {
			for _, idx := range q.Source.Indices {
				names[idx] = true
			}
		} else if q.Source.Index != "" {
			names[q.Source.Index] = true
		}
	}
	for _, cmd := range q.Commands {
		switch c := cmd.(type) {
		case *AppendCommand:
			collectIndexNamesFromQuery(c.Subquery, names)
		case *JoinCommand:
			collectIndexNamesFromQuery(c.Subquery, names)
		case *MultisearchCommand:
			for _, sub := range c.Searches {
				collectIndexNamesFromQuery(sub, names)
			}
		case *SearchCommand:
			if c.Index != "" {
				names[c.Index] = true
			}
		}
	}
}

// classifyCommand returns a short type name for a command.
func classifyCommand(cmd Command) string {
	switch cmd.(type) {
	case *SearchCommand:
		return "search"
	case *WhereCommand:
		return "where"
	case *StatsCommand:
		return "stats"
	case *EvalCommand:
		return "eval"
	case *HeadCommand:
		return "head"
	case *TailCommand:
		return "tail"
	case *FieldsCommand:
		return "fields"
	case *TableCommand:
		return "table"
	case *SortCommand:
		return "sort"
	case *DedupCommand:
		return "dedup"
	case *RexCommand:
		return "rex"
	case *BinCommand:
		return "bin"
	case *StreamstatsCommand:
		return "streamstats"
	case *EventstatsCommand:
		return "eventstats"
	case *JoinCommand:
		return "join"
	case *RenameCommand:
		return "rename"
	default:
		return "unknown"
	}
}

// requiresFullMaterialization returns true for commands that need all input
// data before they can produce output.
//
// DedupCommand is NOT included: it produces output row-by-row (streaming) and
// its seen set can spill to disk when memory is exceeded.
//
// EventstatsCommand is NOT included: although it is two-pass internally, with
// spill support it handles its own row buffering. Removing it from this list
// allows the streaming scan path to feed it row groups on-demand, rather than
// pre-materializing all events in the server.
func requiresFullMaterialization(cmd Command) bool {
	switch cmd.(type) {
	case *SortCommand:
		return true // must see all rows to sort
	case *TailCommand:
		return true // must see all rows to get last N
	case *JoinCommand:
		return true // must materialize at least one side
	default:
		return false
	}
}

// parseTimeLiteral tries to parse a time value from a string.
// Supports RFC3339, Unix epoch seconds, and Unix nanoseconds.
func parseTimeLiteral(s string) time.Time {
	// Try RFC3339.
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t
	}

	// Try as numeric (Unix seconds or nanoseconds).
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		if f > 1e15 {
			return time.Unix(0, int64(f))
		}

		return time.Unix(int64(f), 0)
	}
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		if i > 1e15 {
			return time.Unix(0, i)
		}

		return time.Unix(i, 0)
	}

	return time.Time{}
}
