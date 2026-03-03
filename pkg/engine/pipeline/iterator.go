package pipeline

import "context"

// FieldInfo describes a column in the output schema.
type FieldInfo struct {
	Name string
	Type string // "string", "int", "float", "bool", "timestamp", "any"
}

// Iterator is the universal operator interface for the Volcano/pull model.
// Every pipeline stage implements this.
type Iterator interface {
	// Init prepares the operator. Called once before first Next().
	Init(ctx context.Context) error

	// Next returns the next batch of results.
	// Returns (nil, nil) when exhausted.
	Next(ctx context.Context) (*Batch, error)

	// Close releases resources. MUST be called even on error path.
	Close() error

	// Schema returns output column names and types.
	Schema() []FieldInfo
}
