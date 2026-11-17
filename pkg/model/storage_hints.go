package model

// QueryHints carries storage-level hints extracted from a parsed query.
type QueryHints struct {
	SearchTerms        []string         // tokenized search terms (lowercased)
	TokenGlobs         []string         // lowercased glob patterns matched against whole _raw tokens
	SearchTermTree     *SearchTermTree  // structured boolean tree for inverted index OR/AND
	IndexName          string           // from Source.Index or SearchCommand.Index
	SourceIndices      []string         // multiple source names from source=a, b, c
	SourceGlob         string           // glob pattern from source=logs* (empty = not a glob)
	SourceIncludeGlobs []string         // include globs from source lists
	SourceScopeType    string           // "all", "single", "list", "glob"
	SourceScopeSources []string         // resolved source names for scope (single/list)
	SourceScopePattern string           // glob pattern for scope
	TimeBounds         *TimeBounds      // from WHERE _time >= X AND _time <= X
	RequiredCols       []string         // from GetRequiredColumns()
	Limit              int              // from terminal HeadCommand (0 = unlimited)
	ReverseScan        bool             // true if optimizer determined reverse scan is safe
	FieldPredicates    []FieldPredicate // simple field op literal from WHERE
}

// SearchTermTree is a structured boolean tree for inverted index OR/AND queries.
type SearchTermTree struct {
	Op       string            // "AND", "OR", "NOT", "TERM"
	Term     string            // non-empty when Op == "TERM"
	Children []*SearchTermTree // sub-trees
}

// FieldPredicate represents a simple field op literal predicate from WHERE.
type FieldPredicate struct {
	Field string
	Op    string // "=", "!=", ">", ">=", "<", "<="
	Value string
}
