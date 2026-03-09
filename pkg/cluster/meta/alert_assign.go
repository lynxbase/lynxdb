package meta

import (
	"fmt"
	"sort"
	"time"

	"github.com/cespare/xxhash/v2"

	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

// AlertAssignment tracks which query node is responsible for evaluating an alert.
type AlertAssignment struct {
	AlertID      string          `msgpack:"alert_id"`
	AssignedNode sharding.NodeID `msgpack:"assigned_node"`
	LastFiredAt  *time.Time      `msgpack:"last_fired_at,omitempty"`
	Version      uint64          `msgpack:"version"`
}

// AssignAlertPayload is the payload for CmdAssignAlert.
type AssignAlertPayload struct {
	AlertID    string            `msgpack:"alert_id"`
	QueryNodes []sharding.NodeID `msgpack:"query_nodes"`
}

// UpdateAlertFiredPayload is the payload for CmdUpdateAlertFired.
type UpdateAlertFiredPayload struct {
	AlertID string    `msgpack:"alert_id"`
	FiredAt time.Time `msgpack:"fired_at"`
}

// RendezvousAssign picks the node with the highest rendezvous hash score
// for the given alert ID. This provides consistent hashing with minimal
// disruption when nodes join/leave: only ~1/N alerts move per node change.
func RendezvousAssign(alertID string, nodes []sharding.NodeID) sharding.NodeID {
	if len(nodes) == 0 {
		return ""
	}
	if len(nodes) == 1 {
		return nodes[0]
	}

	var bestNode sharding.NodeID
	var bestScore uint64

	for _, nodeID := range nodes {
		score := rendezvousScore(alertID, nodeID)
		if score > bestScore || bestNode == "" {
			bestScore = score
			bestNode = nodeID
		}
	}

	return bestNode
}

// rendezvousScore computes a hash score for an (alertID, nodeID) pair.
// Uses xxhash64 for fast, well-distributed hashing with excellent avalanche
// properties. The null separator prevents collisions between alertID/nodeID
// boundaries (e.g., "alert\x001" vs "alert1\x00").
func rendezvousScore(alertID string, nodeID sharding.NodeID) uint64 {
	return xxhash.Sum64String(alertID + "\x00" + string(nodeID))
}

// applyAssignAlert computes the alert assignment via rendezvous hashing
// and stores it in AlertAssign. If QueryNodes is empty, the alert is unassigned.
func (s *MetaState) applyAssignAlert(payload []byte) error {
	var p AssignAlertPayload
	if err := UnmarshalPayload(payload, &p); err != nil {
		return fmt.Errorf("meta.applyAssignAlert: %w", err)
	}

	if len(p.QueryNodes) == 0 {
		// Unassign.
		delete(s.AlertAssign, p.AlertID)
		s.Version++

		return nil
	}

	assigned := RendezvousAssign(p.AlertID, p.QueryNodes)

	existing, ok := s.AlertAssign[p.AlertID]
	if ok {
		existing.AssignedNode = assigned
		existing.Version++
	} else {
		s.AlertAssign[p.AlertID] = &AlertAssignment{
			AlertID:      p.AlertID,
			AssignedNode: assigned,
			Version:      1,
		}
	}

	s.Version++

	return nil
}

// applyUpdateAlertFired records the time an alert was fired.
func (s *MetaState) applyUpdateAlertFired(payload []byte) error {
	var p UpdateAlertFiredPayload
	if err := UnmarshalPayload(payload, &p); err != nil {
		return fmt.Errorf("meta.applyUpdateAlertFired: %w", err)
	}

	aa, ok := s.AlertAssign[p.AlertID]
	if !ok {
		return fmt.Errorf("meta.applyUpdateAlertFired: alert %q not assigned", p.AlertID)
	}

	firedAt := p.FiredAt
	aa.LastFiredAt = &firedAt
	s.Version++

	return nil
}

// ReassignAlertsFromDeadNode reassigns all alerts currently assigned to a dead
// node. Uses rendezvous hashing with the dead node excluded, ensuring that only
// ~1/N alerts move (rendezvous property). Returns the list of reassigned alert IDs.
func (s *MetaState) ReassignAlertsFromDeadNode(deadNode sharding.NodeID) []string {
	// Collect live query nodes.
	var liveQueryNodes []sharding.NodeID
	for id, entry := range s.Nodes {
		if id == deadNode || entry.State == NodeDead {
			continue
		}
		for _, role := range entry.Info.Roles {
			if role == "query" {
				liveQueryNodes = append(liveQueryNodes, id)

				break
			}
		}
	}

	// Sort for deterministic assignment.
	sort.Slice(liveQueryNodes, func(i, j int) bool {
		return liveQueryNodes[i] < liveQueryNodes[j]
	})

	if len(liveQueryNodes) == 0 {
		return nil
	}

	var reassigned []string
	for alertID, aa := range s.AlertAssign {
		if aa.AssignedNode != deadNode {
			continue
		}

		newNode := RendezvousAssign(alertID, liveQueryNodes)
		aa.AssignedNode = newNode
		aa.Version++
		reassigned = append(reassigned, alertID)
	}

	if len(reassigned) > 0 {
		s.Version++
	}

	return reassigned
}
