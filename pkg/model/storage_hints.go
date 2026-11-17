package model

// QueryHints carries storage-level hints extracted from a parsed query.
// These hints enable segment/row-group pruning during query execution.
type QueryHints struct {
	SearchTerms             []string                 // tokenized search terms (lowercased)
	TokenGlobs              []string                 // lowercased glob patterns matched against whole _raw tokens (FST expansion; candidate filter only — must never feed bloom MayContainAll)
	SearchTermTree          *SearchTermTree          // structured boolean tree for inverted index OR/AND
	IndexName               string                   // from Source.Index or SearchCommand.Index
	SourceIndices           []string                 // multiple source names from FROM a, b, c
	SourceGlob              string                   // glob pattern from FROM logs* (empty = not a glob)
	SourceIncludeGlobs      []string                 // include globs from source lists
	SourceExcludeGlobs      []string                 // exclude globs from source lists
	SourceScopeType         string                   // "all", "single", "list", "glob"
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
	PrewherePlan            *PrewherePlan            // internal staged-read plan for physical field predicates
	RexPreFilters           []RexPreFilter           // safe literal prefilters for rex-generated field predicates
	SkipRaw                 bool                     // true when _raw is not needed
	Warnings                []string                 // user-facing warnings about the query

	sourceIndexSet map[string]struct{} // lazy cache for O(1) list lookups
}

// SourceIndexSet returns a lazily-built set for O(1) SourceIndices lookups.
func (h *QueryHints) SourceIndexSet() map[string]struct{} {
	if h.sourceIndexSet != nil {
		return h.sourceIndexSet
	}
	if len(h.SourceIndices) == 0 {
		return nil
	}
	h.sourceIndexSet = make(map[string]struct{}, len(h.SourceIndices))
	for _, idx := range h.SourceIndices {
		h.sourceIndexSet[idx] = struct{}{}
	}
	return h.sourceIndexSet
}

// SearchTermTree is a structured boolean tree for inverted index OR/AND queries.
type SearchTermTree struct {
	Op       string            // "AND", "OR", "NOT", "TERM"
	Term     string            // non-empty when Op == "TERM"
	Children []*SearchTermTree // sub-trees
}

// FieldPredicate represents a simple field op literal predicate from WHERE.
type FieldPredicate struct {
	Field    string
	Op       string // "=", "!=", ">", ">=", "<", "<="
	Value    string
	Wildcard bool // true when Value contains * or ?
}

// InvertedIndexPredicate represents a field=value for inverted index lookup.
type InvertedIndexPredicate struct {
	Field  string
	Value  string
	Negate bool // NOT field=value
}

// RangePredicate represents a field range for pushdown.
type RangePredicate struct {
	Field        string
	Min          string // empty = no lower bound
	Max          string // empty = no upper bound
	MinInclusive bool
	MaxInclusive bool
	LoweredToBSI bool // true if this predicate was lowered to BSI evaluation
}

// InPredicate represents a field IN (values) for segment-level pushdown.
type InPredicate struct {
	Field  string
	Values []string
	Negate bool // NOT IN
}

// PrewherePlan is an internal staged-read plan for physical field predicates.
type PrewherePlan struct {
	Stages []PrewhereStage
}

// PrewhereStage represents one stage in a prewhere evaluation plan.
type PrewhereStage struct {
	Predicates  []FieldPredicate
	Selectivity float64 // estimated fraction of rows passing (0..1)
}

// RexPreFilter is a safe literal prefilter for rex-generated field predicates.
type RexPreFilter struct {
	Field   string
	Pattern string
	Literal string // the literal substring extracted from the regex
}
