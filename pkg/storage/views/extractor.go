package views

import (
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
)

// Extractor creates new events with only the specified columns populated.
type Extractor struct {
	columns []ColumnDef
}

// NewExtractor creates an extractor from a list of column definitions.
func NewExtractor(columns []ColumnDef) *Extractor {
	return &Extractor{columns: columns}
}

// Extract creates a new event with only the specified columns from the source event.
func (x *Extractor) Extract(e *event.Event) *event.Event {
	out := &event.Event{
		Fields: make(map[string]event.Value, len(x.columns)),
	}

	for _, col := range x.columns {
		switch col.Name {
		case "_time":
			out.Time = e.Time
			if out.Time.IsZero() {
				out.Time = time.Now()
			}
		case "_raw":
			out.Raw = e.Raw
		case "_source":
			out.Source = e.Source
		case "_sourcetype":
			out.SourceType = e.SourceType
		case "host":
			out.Host = e.Host
		case "index":
			out.Index = e.Index
		default:
			val := e.GetField(col.Name)
			out.Fields[col.Name] = val
		}
	}

	// Always copy time if not explicitly requested.
	if out.Time.IsZero() {
		out.Time = e.Time
	}

	return out
}
