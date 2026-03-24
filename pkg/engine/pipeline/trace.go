package pipeline

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// TraceIterator is a blocking operator that groups events by trace_id,
// builds parent-child span trees via span_id/parent_span_id DFS,
// and emits rows enriched with _span_depth (int) and _span_tree (string).
type TraceIterator struct {
	child       Iterator
	traceField  string
	spanField   string
	parentField string

	// Accumulation.
	done bool

	// Emission.
	output *Batch
	offset int
}

// NewTraceIterator creates a new trace iterator.
func NewTraceIterator(child Iterator, traceField, spanField, parentField string) *TraceIterator {
	if traceField == "" {
		traceField = "trace_id"
	}
	if spanField == "" {
		spanField = "span_id"
	}
	if parentField == "" {
		parentField = "parent_span_id"
	}

	return &TraceIterator{
		child:       child,
		traceField:  traceField,
		spanField:   spanField,
		parentField: parentField,
	}
}

func (t *TraceIterator) Init(ctx context.Context) error {
	return t.child.Init(ctx)
}

func (t *TraceIterator) Next(ctx context.Context) (*Batch, error) {
	if !t.done {
		t.done = true

		var rows []map[string]event.Value
		for {
			batch, err := t.child.Next(ctx)
			if err != nil {
				return nil, err
			}
			if batch == nil {
				break
			}
			for i := 0; i < batch.Len; i++ {
				rows = append(rows, batch.Row(i))
			}
		}

		t.output = t.buildSpanTree(rows)
	}

	if t.output == nil || t.offset >= t.output.Len {
		return nil, nil
	}

	end := t.offset + defaultDrainBatchSize
	if end > t.output.Len {
		end = t.output.Len
	}

	result := t.output.Slice(t.offset, end)
	t.offset = end

	return result, nil
}

func (t *TraceIterator) Close() error {
	return t.child.Close()
}

func (t *TraceIterator) Schema() []FieldInfo {
	return []FieldInfo{
		{Name: "_time", Type: "timestamp"},
		{Name: t.traceField, Type: "string"},
		{Name: "service", Type: "string"},
		{Name: "operation", Type: "string"},
		{Name: "duration_ms", Type: "int"},
		{Name: "_span_depth", Type: "int"},
		{Name: "_span_tree", Type: "string"},
	}
}

// spanNode represents a single span in the tree.
type spanNode struct {
	row     map[string]event.Value
	spanID  string
	depth   int
	treeStr string
}

// buildSpanTree groups rows by trace_id, builds DFS tree, emits in tree order.
func (t *TraceIterator) buildSpanTree(rows []map[string]event.Value) *Batch {
	if len(rows) == 0 {
		return NewBatch(0)
	}

	// Sort rows by _time ascending for deterministic output.
	sort.Slice(rows, func(i, j int) bool {
		ti := rows[i]["_time"]
		tj := rows[j]["_time"]
		if ti.IsNull() || tj.IsNull() {
			return i < j
		}
		return ti.String() < tj.String()
	})

	// Group by trace_id.
	type traceGroup struct {
		rows []map[string]event.Value
	}
	traces := make(map[string]*traceGroup)
	var traceOrder []string

	for _, row := range rows {
		traceIDVal := row[t.traceField]
		traceID := traceIDVal.AsString()
		if traceID == "" {
			continue // skip rows without trace_id
		}
		g, ok := traces[traceID]
		if !ok {
			g = &traceGroup{}
			traces[traceID] = g
			traceOrder = append(traceOrder, traceID)
		}
		g.rows = append(g.rows, row)
	}

	// Build output — compact 7-column format.
	b := NewBatch(len(rows))
	for _, traceID := range traceOrder {
		g := traces[traceID]
		nodes := t.buildTreeForTrace(g.rows)
		for _, node := range nodes {
			service := fieldString(node.row, "service", "service_name", "component")
			operation := fieldString(node.row, "operation", "operation_name", "name", "handler")
			duration := fieldInt(node.row, "duration_ms", "duration", "elapsed_ms")

			b.AddRow(map[string]event.Value{
				"_time":       node.row["_time"],
				t.traceField:  event.StringValue(traceID),
				"service":     event.StringValue(service),
				"operation":   event.StringValue(operation),
				"duration_ms": duration,
				"_span_depth": event.IntValue(int64(node.depth)),
				"_span_tree":  event.StringValue(node.treeStr),
			})
		}
	}

	return b
}

// buildTreeForTrace builds the DFS tree for a single trace's spans.
func (t *TraceIterator) buildTreeForTrace(rows []map[string]event.Value) []*spanNode {
	// Index spans by span_id.
	spanIndex := make(map[string]int) // span_id → row index
	for i, row := range rows {
		spanID := row[t.spanField].AsString()
		if spanID != "" {
			spanIndex[spanID] = i
		}
	}

	// Build parent → children adjacency.
	children := make(map[string][]int) // parent_span_id → []row index
	var roots []int

	for i, row := range rows {
		parentID := row[t.parentField].AsString()
		if parentID == "" {
			// No parent — root span.
			roots = append(roots, i)
			continue
		}
		if _, parentExists := spanIndex[parentID]; parentExists {
			children[parentID] = append(children[parentID], i)
		} else {
			// Parent not found — treat as root.
			roots = append(roots, i)
		}
	}

	// Build display name for a row.
	displayName := func(row map[string]event.Value) string {
		// Try common span fields: service, operation, name.
		service := ""
		operation := ""
		for _, svcField := range []string{"service", "service_name", "component"} {
			if v, ok := row[svcField]; ok && !v.IsNull() && v.AsString() != "" {
				service = v.AsString()
				break
			}
		}
		for _, opField := range []string{"operation", "operation_name", "name", "handler"} {
			if v, ok := row[opField]; ok && !v.IsNull() && v.AsString() != "" {
				operation = v.AsString()
				break
			}
		}
		if service != "" && operation != "" {
			return service + "." + operation
		}
		if service != "" {
			return service
		}
		if operation != "" {
			return operation
		}
		// Fallback to span_id.
		spanID := row[t.spanField].AsString()
		if len(spanID) > 8 {
			spanID = spanID[:8]
		}
		return "span:" + spanID
	}

	// Duration display.
	durationStr := func(row map[string]event.Value) string {
		for _, field := range []string{"duration_ms", "duration", "elapsed_ms"} {
			if v, ok := row[field]; ok && !v.IsNull() {
				switch v.Type() {
				case event.FieldTypeInt:
					return fmt.Sprintf("%dms", v.AsInt())
				case event.FieldTypeFloat:
					return fmt.Sprintf("%.0fms", v.AsFloat())
				}
			}
		}
		return ""
	}

	// DFS traversal.
	var result []*spanNode
	spanIDForRow := func(row map[string]event.Value) string {
		return row[t.spanField].AsString()
	}

	var dfs func(rowIndex int, depth int, prefix string, isLast bool)
	dfs = func(rowIndex int, depth int, prefix string, isLast bool) {
		row := rows[rowIndex]

		// Build tree display string.
		connector := "├── "
		if isLast {
			connector = "└── "
		}
		name := displayName(row)
		dur := durationStr(row)
		var treeStr string
		if depth == 0 {
			if dur != "" {
				treeStr = fmt.Sprintf("%s [%s]", name, dur)
			} else {
				treeStr = name
			}
		} else {
			if dur != "" {
				treeStr = fmt.Sprintf("%s%s [%s]", prefix+connector, name, dur)
			} else {
				treeStr = prefix + connector + name
			}
		}

		result = append(result, &spanNode{
			row:     row,
			spanID:  spanIDForRow(row),
			depth:   depth,
			treeStr: treeStr,
		})

		// Visit children.
		childIndices := children[spanIDForRow(row)]
		for ci, childIdx := range childIndices {
			childIsLast := ci == len(childIndices)-1
			childPrefix := prefix
			if depth > 0 {
				if isLast {
					childPrefix += "    "
				} else {
					childPrefix += "│   "
				}
			}
			dfs(childIdx, depth+1, childPrefix, childIsLast)
		}
	}

	// Sort roots by _time for deterministic order.
	sort.Slice(roots, func(i, j int) bool {
		return rows[roots[i]]["_time"].String() < rows[roots[j]]["_time"].String()
	})

	for _, rootIdx := range roots {
		dfs(rootIdx, 0, "", true)
	}

	return result
}

// isTraceCompatibleField checks if a field name looks like a trace ID field.
func isTraceCompatibleField(name string) bool {
	lower := strings.ToLower(name)
	return strings.Contains(lower, "trace") || strings.Contains(lower, "span")
}

// fieldString returns the first non-empty string value from the given field names.
func fieldString(row map[string]event.Value, fields ...string) string {
	for _, f := range fields {
		if v, ok := row[f]; ok && !v.IsNull() && v.AsString() != "" {
			return v.AsString()
		}
	}
	return ""
}

// fieldInt returns the first non-null int value from the given field names.
func fieldInt(row map[string]event.Value, fields ...string) event.Value {
	for _, f := range fields {
		if v, ok := row[f]; ok && !v.IsNull() {
			return v
		}
	}
	return event.NullValue()
}
