package pipeline

import (
	"context"
	"fmt"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// SystemTableResolver resolves virtual system tables (system.parts,
// system.merges, system.columns). Implemented by the server package
// which has access to part registry, compactor, and field catalog.
type SystemTableResolver interface {
	// ResolveSystemTable returns rows for the given system table name
	// (e.g., "parts", "merges", "columns" — without the "system." prefix).
	// Returns an error if the table name is unknown.
	ResolveSystemTable(ctx context.Context, table string) ([]map[string]event.Value, error)
}

// SystemTableIterator reads data from a virtual system table.
type SystemTableIterator struct {
	table     string
	resolver  SystemTableResolver
	batchSize int
	rows      []map[string]event.Value
	offset    int
	inited    bool
}

// NewSystemTableIterator creates a new iterator for reading a system table.
func NewSystemTableIterator(table string, resolver SystemTableResolver, batchSize int) *SystemTableIterator {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	return &SystemTableIterator{
		table:     table,
		resolver:  resolver,
		batchSize: batchSize,
	}
}

func (s *SystemTableIterator) Init(ctx context.Context) error {
	if s.resolver == nil {
		return fmt.Errorf("system table: no resolver configured")
	}

	rows, err := s.resolver.ResolveSystemTable(ctx, s.table)
	if err != nil {
		return fmt.Errorf("system table %q: %w", s.table, err)
	}

	s.rows = rows
	s.inited = true

	return nil
}

func (s *SystemTableIterator) Next(ctx context.Context) (*Batch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if !s.inited {
		return nil, fmt.Errorf("system table: not initialized")
	}

	if s.offset >= len(s.rows) {
		return nil, nil
	}

	end := s.offset + s.batchSize
	if end > len(s.rows) {
		end = len(s.rows)
	}

	batch := BatchFromRows(s.rows[s.offset:end])
	s.offset = end

	return batch, nil
}

func (s *SystemTableIterator) Close() error        { return nil }
func (s *SystemTableIterator) Schema() []FieldInfo { return nil }
