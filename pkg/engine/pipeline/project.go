package pipeline

import (
	"context"
	"path"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

// ProjectIterator selects/removes columns from passing batches.
type ProjectIterator struct {
	child    Iterator
	fields   []string
	remove   bool // true = remove listed fields, false = keep only listed fields
	hasGlobs bool // true if any field contains glob wildcard characters
}

// NewProjectIterator creates a column selector (FIELDS/TABLE).
func NewProjectIterator(child Iterator, fields []string, remove bool) *ProjectIterator {
	return &ProjectIterator{
		child:    child,
		fields:   fields,
		remove:   remove,
		hasGlobs: spl2.FieldListHasGlob(fields),
	}
}

func (p *ProjectIterator) Init(ctx context.Context) error {
	return p.child.Init(ctx)
}

func (p *ProjectIterator) Next(ctx context.Context) (*Batch, error) {
	batch, err := p.child.Next(ctx)
	if batch == nil || err != nil {
		return nil, err
	}

	result := &Batch{
		Columns: make(map[string][]event.Value, len(p.fields)),
		Len:     batch.Len,
	}

	if !p.hasGlobs {
		// Fast path: no globs — exact field name matching.
		if p.remove {
			removeSet := make(map[string]bool, len(p.fields))
			for _, f := range p.fields {
				removeSet[f] = true
			}
			for k, v := range batch.Columns {
				if !removeSet[k] {
					result.Columns[k] = v
				}
			}
		} else {
			for _, f := range p.fields {
				if col, ok := batch.Columns[f]; ok {
					result.Columns[f] = col
				}
			}
		}
	} else {
		// Glob path: pattern matching against column names.
		if p.remove {
			// Split fields into exact set and glob patterns for efficiency.
			exactRemove := make(map[string]bool)
			var globPatterns []string
			for _, f := range p.fields {
				if spl2.ContainsGlobWildcard(f) {
					globPatterns = append(globPatterns, f)
				} else {
					exactRemove[f] = true
				}
			}
			for colName, colVal := range batch.Columns {
				if exactRemove[colName] {
					continue
				}
				matched := false
				for _, pat := range globPatterns {
					if ok, _ := path.Match(pat, colName); ok {
						matched = true
						break
					}
				}
				if !matched {
					result.Columns[colName] = colVal
				}
			}
		} else {
			// Keep mode: for each field pattern, collect matching columns.
			for _, f := range p.fields {
				if spl2.ContainsGlobWildcard(f) {
					for colName, colVal := range batch.Columns {
						if ok, _ := path.Match(f, colName); ok {
							result.Columns[colName] = colVal
						}
					}
				} else {
					if col, ok := batch.Columns[f]; ok {
						result.Columns[f] = col
					}
				}
			}
		}
	}

	return result, nil
}

func (p *ProjectIterator) Close() error {
	return p.child.Close()
}

func (p *ProjectIterator) Schema() []FieldInfo {
	// When globs are present, the schema is runtime-dependent — return nil.
	if p.hasGlobs {
		return nil
	}
	var schema []FieldInfo
	for _, f := range p.fields {
		schema = append(schema, FieldInfo{Name: f, Type: "any"})
	}

	return schema
}
