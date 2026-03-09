package meta

import (
	"fmt"
	"time"

	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

// GlobalSourceInfo holds cluster-wide source metadata, merged from per-node deltas.
type GlobalSourceInfo struct {
	Name       string                    `msgpack:"name"`
	FirstSeen  time.Time                 `msgpack:"first_seen"`
	LastSeen   time.Time                 `msgpack:"last_seen"`
	EventCount int64                     `msgpack:"event_count"` // sum across all nodes
	NodeSet    map[sharding.NodeID]int64 `msgpack:"node_set"`    // per-node event count
}

// UpdateSourceRegistryPayload is the payload for CmdUpdateSourceRegistry.
type UpdateSourceRegistryPayload struct {
	NodeID  sharding.NodeID `msgpack:"node_id"`
	Sources []SourceDelta   `msgpack:"sources"`
}

// SourceDelta describes a source's local stats from a single ingest node.
type SourceDelta struct {
	Name       string    `msgpack:"name"`
	EventCount int64     `msgpack:"event_count"`
	LastSeen   time.Time `msgpack:"last_seen"`
}

// applyUpdateSourceRegistry merges a source set delta from an ingest node.
// For each source: upsert NodeSet, recompute EventCount, track min FirstSeen
// and max LastSeen.
func (s *MetaState) applyUpdateSourceRegistry(payload []byte) error {
	var p UpdateSourceRegistryPayload
	if err := UnmarshalPayload(payload, &p); err != nil {
		return fmt.Errorf("meta.applyUpdateSourceRegistry: %w", err)
	}

	for _, sd := range p.Sources {
		gsi, ok := s.Sources[sd.Name]
		if !ok {
			gsi = &GlobalSourceInfo{
				Name:      sd.Name,
				FirstSeen: sd.LastSeen, // first report becomes first seen
				NodeSet:   make(map[sharding.NodeID]int64),
			}
			s.Sources[sd.Name] = gsi
		}

		// Update per-node count.
		gsi.NodeSet[p.NodeID] = sd.EventCount

		// Recompute total across all nodes.
		var total int64
		for _, c := range gsi.NodeSet {
			total += c
		}
		gsi.EventCount = total

		// Track min FirstSeen.
		if !sd.LastSeen.IsZero() && (gsi.FirstSeen.IsZero() || sd.LastSeen.Before(gsi.FirstSeen)) {
			gsi.FirstSeen = sd.LastSeen
		}

		// Track max LastSeen.
		if sd.LastSeen.After(gsi.LastSeen) {
			gsi.LastSeen = sd.LastSeen
		}
	}

	s.Version++

	return nil
}
