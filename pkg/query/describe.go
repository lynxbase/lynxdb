package query

import (
	"sort"

	"github.com/lynxbase/lynxdb/pkg/engine/physical"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ir"
)

// TypeName is the PPL type name LynxDB reports for a value type.
func TypeName(t event.FieldType) string {
	switch t {
	case event.FieldTypeString:
		return "string"
	case event.FieldTypeInt:
		return "bigint"
	case event.FieldTypeFloat:
		return "double"
	case event.FieldTypeBool:
		return "boolean"
	case event.FieldTypeTimestamp:
		return "timestamp"
	case event.FieldTypeDuration:
		return "interval"
	case event.FieldTypeArray:
		return "array"
	case event.FieldTypeObject:
		return "struct"
	}
	return "undefined"
}

// DescribeRows builds describe output from observed events: one row per
// column, typed by the first non-NULL value seen.
func DescribeRows(source string, events []*event.Event) []map[string]event.Value {
	types := map[string]event.FieldType{}
	for _, row := range physical.EventsToRows(events) {
		for k, v := range row {
			if _, ok := types[k]; !ok || types[k] == event.FieldTypeNull {
				types[k] = v.Type()
			}
		}
	}
	names := make([]string, 0, len(types))
	for n := range types {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]map[string]event.Value, len(names))
	for i, n := range names {
		out[i] = map[string]event.Value{
			"TABLE_NAME":       event.StringValue(source),
			"COLUMN_NAME":      event.StringValue(n),
			"TYPE_NAME":        event.StringValue(TypeName(types[n])),
			"ORDINAL_POSITION": event.IntValue(int64(i + 1)),
		}
	}
	return out
}

// DescribeStore answers describe from an in-memory store.
func DescribeStore(store physical.EventStore) func(ir.Source) ([]map[string]event.Value, error) {
	return func(src ir.Source) ([]map[string]event.Value, error) {
		return DescribeRows(src.Name, store.MaterializeEvents(src.Name)), nil
	}
}
