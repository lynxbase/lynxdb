package pipeline

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// PackJsonIterator implements the `| pack_json` pipe command.
// It assembles event fields into a JSON string stored in a target field.
// When Fields is nil, all non-internal fields are packed.
// When Fields lists specific field names, only those are packed.
type PackJsonIterator struct {
	child  Iterator
	fields []string // nil = all non-internal fields
	target string   // output field name
}

// internalFields are field names that are excluded from pack_json when
// no explicit field list is provided (Fields == nil).
var internalFields = map[string]struct{}{
	"_time":   {},
	"_raw":    {},
	"_source": {},
}

// NewPackJsonIterator creates a pack_json operator.
func NewPackJsonIterator(child Iterator, fields []string, target string) *PackJsonIterator {
	return &PackJsonIterator{
		child:  child,
		fields: fields,
		target: target,
	}
}

func (p *PackJsonIterator) Init(ctx context.Context) error {
	return p.child.Init(ctx)
}

func (p *PackJsonIterator) Next(ctx context.Context) (*Batch, error) {
	batch, err := p.child.Next(ctx)
	if batch == nil || err != nil {
		return nil, err
	}

	targetCol, exists := batch.Columns[p.target]
	if !exists {
		targetCol = make([]event.Value, batch.Len)
		batch.Columns[p.target] = targetCol
	} else if len(targetCol) < batch.Len {
		extended := make([]event.Value, batch.Len)
		copy(extended, targetCol)
		targetCol = extended
		batch.Columns[p.target] = targetCol
	}

	if p.fields != nil {
		p.packSpecificFields(batch, targetCol)
	} else {
		p.packAllFields(batch, targetCol)
	}

	return batch, nil
}

func (p *PackJsonIterator) packSpecificFields(batch *Batch, targetCol []event.Value) {
	for i := 0; i < batch.Len; i++ {
		obj := make(map[string]interface{}, len(p.fields))
		for _, f := range p.fields {
			col := batch.Columns[f]
			if col == nil || i >= len(col) {
				continue
			}
			v := col[i]
			if v.IsNull() {
				continue
			}
			obj[f] = packValueToInterface(v)
		}

		data, err := json.Marshal(obj)
		if err != nil {
			targetCol[i] = event.NullValue()
		} else {
			targetCol[i] = event.StringValue(string(data))
		}
	}
}

func (p *PackJsonIterator) packAllFields(batch *Batch, targetCol []event.Value) {
	// Collect non-internal column names (exclude target too).
	colNames := make([]string, 0, len(batch.Columns))
	for name := range batch.Columns {
		if _, internal := internalFields[name]; internal {
			continue
		}
		if name == p.target {
			continue
		}
		if strings.HasPrefix(name, "_") {
			continue
		}
		colNames = append(colNames, name)
	}

	for i := 0; i < batch.Len; i++ {
		obj := make(map[string]interface{}, len(colNames))
		for _, name := range colNames {
			col := batch.Columns[name]
			if col == nil || i >= len(col) {
				continue
			}
			v := col[i]
			if v.IsNull() {
				continue
			}
			obj[name] = packValueToInterface(v)
		}

		data, err := json.Marshal(obj)
		if err != nil {
			targetCol[i] = event.NullValue()
		} else {
			targetCol[i] = event.StringValue(string(data))
		}
	}
}

// packValueToInterface converts an event.Value to a Go interface{} for JSON
// marshaling. Preserves native types: int→int64, float→float64, bool→bool,
// null→nil, string→string (without numeric coercion, unlike the VM's
// eventValueToInterface which promotes string-typed numbers).
func packValueToInterface(v event.Value) interface{} {
	switch v.Type() {
	case event.FieldTypeNull:
		return nil
	case event.FieldTypeBool:
		return v.AsBool()
	case event.FieldTypeInt:
		return v.AsInt()
	case event.FieldTypeFloat:
		return v.AsFloat()
	case event.FieldTypeString:
		return v.AsString()
	default:
		return v.String()
	}
}

func (p *PackJsonIterator) Close() error { return p.child.Close() }

func (p *PackJsonIterator) Schema() []FieldInfo { return p.child.Schema() }
