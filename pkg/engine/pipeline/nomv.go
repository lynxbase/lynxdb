package pipeline

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// NomvIterator converts a multivalue field into a single newline-delimited value.
type NomvIterator struct {
	child Iterator
	field string
}

// NewNomvIterator creates a streaming nomv operator.
func NewNomvIterator(child Iterator, field string) *NomvIterator {
	return &NomvIterator{child: child, field: field}
}

func (n *NomvIterator) Init(ctx context.Context) error {
	return n.child.Init(ctx)
}

func (n *NomvIterator) Next(ctx context.Context) (*Batch, error) {
	batch, err := n.child.Next(ctx)
	if err != nil || batch == nil {
		return batch, err
	}

	col, ok := batch.Columns[n.field]
	if !ok {
		return batch, nil
	}
	for i := range col {
		col[i] = nomvValue(col[i])
	}
	batch.Columns[n.field] = col

	return batch, nil
}

func (n *NomvIterator) Close() error {
	return n.child.Close()
}

func (n *NomvIterator) Schema() []FieldInfo {
	return n.child.Schema()
}

func nomvValue(v event.Value) event.Value {
	if v.IsNull() {
		return v
	}

	raw := strings.TrimSpace(v.String())
	if strings.Contains(raw, "|||") {
		return event.StringValue(strings.Join(strings.Split(raw, "|||"), "\n"))
	}
	if raw == "" || raw[0] != '[' {
		return v
	}

	var arr []json.RawMessage
	if err := json.Unmarshal([]byte(raw), &arr); err != nil {
		return v
	}
	values := make([]string, 0, len(arr))
	for _, elem := range arr {
		values = append(values, jsonRawToValue(elem).String())
	}

	return event.StringValue(strings.Join(values, "\n"))
}
