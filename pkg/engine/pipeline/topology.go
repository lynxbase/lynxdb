package pipeline

import (
	"context"
	"sort"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// TopologyIterator is a blocking operator that builds a connection graph
// from log events. It groups events by (source, destination) pairs,
// counts connections, computes in/out degree, and emits one row per edge.
type TopologyIterator struct {
	child       Iterator
	sourceField string
	destField   string
	weightField string
	maxNodes    int
	batchSize   int

	done   bool
	output *Batch
	offset int
}

type topoEdge struct {
	Src, Dst string
}

type edgeStats struct {
	Count  int64
	Weight float64
}

type nodeStats struct {
	OutDegree int
	InDegree  int
	OutCount  int64
	InCount   int64
}

// NewTopologyIterator creates a new topology iterator.
func NewTopologyIterator(child Iterator, src, dst, weight string, maxNodes, batchSize int) *TopologyIterator {
	return &TopologyIterator{
		child:       child,
		sourceField: src,
		destField:   dst,
		weightField: weight,
		maxNodes:    maxNodes,
		batchSize:   batchSize,
	}
}

func (t *TopologyIterator) Init(ctx context.Context) error {
	return t.child.Init(ctx)
}

func (t *TopologyIterator) Next(ctx context.Context) (*Batch, error) {
	if !t.done {
		if err := t.materialize(ctx); err != nil {
			return nil, err
		}
		t.done = true
	}
	if t.output == nil || t.offset >= t.output.Len {
		return nil, nil
	}
	end := t.offset + t.batchSize
	if end > t.output.Len {
		end = t.output.Len
	}
	batch := t.output.Slice(t.offset, end)
	t.offset = end

	return batch, nil
}

func (t *TopologyIterator) materialize(ctx context.Context) error {
	edges := make(map[topoEdge]*edgeStats)
	nodes := make(map[string]*nodeStats)

	for {
		batch, err := t.child.Next(ctx)
		if batch == nil {
			break
		}
		if err != nil {
			return err
		}

		for i := 0; i < batch.Len; i++ {
			srcVal := batch.Value(t.sourceField, i)
			dstVal := batch.Value(t.destField, i)
			src, ok1 := topoGetString(srcVal)
			dst, ok2 := topoGetString(dstVal)
			if !ok1 || !ok2 || src == "" || dst == "" {
				continue
			}

			key := topoEdge{Src: src, Dst: dst}
			es, ok := edges[key]
			if !ok {
				es = &edgeStats{}
				edges[key] = es
			}
			es.Count++

			if t.weightField != "" {
				wv := batch.Value(t.weightField, i)
				if w, ok := topoGetFloat(wv); ok {
					es.Weight += w
				}
			}

			if _, ok := nodes[src]; !ok {
				nodes[src] = &nodeStats{}
			}
			nodes[src].OutDegree++
			nodes[src].OutCount++

			if _, ok := nodes[dst]; !ok {
				nodes[dst] = &nodeStats{}
			}
			nodes[dst].InDegree++
			nodes[dst].InCount++
		}
	}

	if len(edges) == 0 {
		return nil
	}

	var rows []map[string]event.Value
	for e, es := range edges {
		row := map[string]event.Value{
			"_source":            event.StringValue(e.Src),
			"_destination":       event.StringValue(e.Dst),
			"_edge_count":        event.IntValue(es.Count),
			"_source_out_degree": event.IntValue(int64(nodes[e.Src].OutDegree)),
			"_dest_in_degree":    event.IntValue(int64(nodes[e.Dst].InDegree)),
		}
		if t.weightField != "" {
			row["_edge_weight"] = event.FloatValue(es.Weight)
		}
		rows = append(rows, row)
	}

	sort.Slice(rows, func(i, j int) bool {
		return rows[i]["_edge_count"].AsInt() > rows[j]["_edge_count"].AsInt()
	})

	if t.maxNodes > 0 && len(rows) > 0 {
		seen := make(map[string]bool)
		var limited []map[string]event.Value
		for _, row := range rows {
			src := row["_source"].AsString()
			dst := row["_destination"].AsString()
			if len(seen) >= t.maxNodes && !seen[src] && !seen[dst] {
				continue
			}
			seen[src] = true
			seen[dst] = true
			limited = append(limited, row)
			if len(seen) >= t.maxNodes+50 {
				break
			}
		}
		rows = limited
	}

	t.output = BatchFromRows(rows)

	return nil
}

func topoGetString(v event.Value) (string, bool) {
	if v.IsNull() {
		return "", false
	}
	if v.Type() == event.FieldTypeString {
		s := v.AsString()
		return s, s != ""
	}

	return "", false
}

func topoGetFloat(v event.Value) (float64, bool) {
	if v.IsNull() {
		return 0, false
	}
	switch v.Type() {
	case event.FieldTypeFloat:
		return v.AsFloat(), true
	case event.FieldTypeInt:
		return float64(v.AsInt()), true
	}

	return 0, false
}

func (t *TopologyIterator) Close() error {
	return t.child.Close()
}

func (t *TopologyIterator) Schema() []FieldInfo {
	fields := []FieldInfo{
		{Name: "_source", Type: "string"},
		{Name: "_destination", Type: "string"},
		{Name: "_edge_count", Type: "int"},
		{Name: "_source_out_degree", Type: "int"},
		{Name: "_dest_in_degree", Type: "int"},
	}
	if t.weightField != "" {
		fields = append(fields, FieldInfo{Name: "_edge_weight", Type: "float"})
	}

	return fields
}
