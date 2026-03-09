package meta

import (
	"fmt"
	"sort"

	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

// GlobalFieldInfo holds cluster-wide field metadata, merged from per-node deltas.
type GlobalFieldInfo struct {
	Name       string                       `msgpack:"name"`
	Type       string                       `msgpack:"type"`        // dominant type across all nodes
	TotalCount int64                        `msgpack:"total_count"` // sum of counts across all nodes
	NodeCounts map[sharding.NodeID]int64    `msgpack:"node_counts"` // per-node event count for this field
	TopValues  []FieldValueEntry            `msgpack:"top_values"`  // top 10 values by count
}

// FieldValueEntry holds a field value and its occurrence count.
type FieldValueEntry struct {
	Value string `msgpack:"value"`
	Count int64  `msgpack:"count"`
}

// UpdateFieldCatalogPayload is the payload for CmdUpdateFieldCatalog.
type UpdateFieldCatalogPayload struct {
	NodeID sharding.NodeID `msgpack:"node_id"`
	Fields []FieldDelta    `msgpack:"fields"`
}

// FieldDelta describes a field's local stats from a single ingest node.
type FieldDelta struct {
	Name      string            `msgpack:"name"`
	Type      string            `msgpack:"type"`
	Count     int64             `msgpack:"count"`
	TopValues []FieldValueEntry `msgpack:"top_values"`
}

// applyUpdateFieldCatalog merges a field catalog delta from an ingest node.
// For each field: upsert NodeCounts, recompute TotalCount, merge top values
// (keep top 10), and set Type to most-reported across all nodes.
func (s *MetaState) applyUpdateFieldCatalog(payload []byte) error {
	var p UpdateFieldCatalogPayload
	if err := UnmarshalPayload(payload, &p); err != nil {
		return fmt.Errorf("meta.applyUpdateFieldCatalog: %w", err)
	}

	for _, fd := range p.Fields {
		gfi, ok := s.FieldCatalog[fd.Name]
		if !ok {
			gfi = &GlobalFieldInfo{
				Name:       fd.Name,
				NodeCounts: make(map[sharding.NodeID]int64),
			}
			s.FieldCatalog[fd.Name] = gfi
		}

		// Update per-node count.
		gfi.NodeCounts[p.NodeID] = fd.Count

		// Recompute total across all nodes.
		var total int64
		for _, c := range gfi.NodeCounts {
			total += c
		}
		gfi.TotalCount = total

		// Set type from this delta (last writer wins for now;
		// a more sophisticated approach would use type voting).
		if fd.Type != "" {
			gfi.Type = fd.Type
		}

		// Merge top values: sum matching value counts, keep top 10.
		gfi.TopValues = mergeTopValues(gfi.TopValues, fd.TopValues, 10)
	}

	s.Version++

	return nil
}

// mergeTopValues merges two top-value lists by summing counts for matching
// values, then returns the top N entries sorted by count descending.
func mergeTopValues(existing, incoming []FieldValueEntry, topN int) []FieldValueEntry {
	counts := make(map[string]int64, len(existing)+len(incoming))
	for _, e := range existing {
		counts[e.Value] += e.Count
	}
	for _, e := range incoming {
		counts[e.Value] += e.Count
	}

	merged := make([]FieldValueEntry, 0, len(counts))
	for v, c := range counts {
		merged = append(merged, FieldValueEntry{Value: v, Count: c})
	}

	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Count > merged[j].Count
	})

	if len(merged) > topN {
		merged = merged[:topN]
	}

	return merged
}
