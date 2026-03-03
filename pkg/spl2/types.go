package spl2

// ResultRow represents a single row in query results.
type ResultRow struct {
	Fields map[string]interface{}
}

// IndexStore holds events per index for multi-index queries.
type IndexStore struct {
	Indexes map[string][]ResultRow
}
