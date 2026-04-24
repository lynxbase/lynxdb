package meta

import (
	"time"

	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

// ClockProvider abstracts time for testability.
// Mirrors ClockProvider to avoid import cycles.
type ClockProvider interface {
	Now() time.Time
}

// NodeState represents the health state of a registered cluster node.
type NodeState int

const (
	// NodeAlive means the node is sending heartbeats normally.
	NodeAlive NodeState = iota
	// NodeSuspect means the node has missed recent heartbeats.
	NodeSuspect
	// NodeDead means the node has been unresponsive beyond the dead timeout.
	NodeDead
)

// String returns the human-readable name of the node state.
func (s NodeState) String() string {
	switch s {
	case NodeAlive:
		return "alive"
	case NodeSuspect:
		return "suspect"
	case NodeDead:
		return "dead"
	default:
		return "unknown"
	}
}

// NodeInfo describes a cluster node's identity and metadata.
// Defined locally in meta to avoid import cycles with the cluster package.
type NodeInfo struct {
	ID        sharding.NodeID `json:"id" msgpack:"id"`
	Addr      string          `json:"addr" msgpack:"addr"`
	GRPCAddr  string          `json:"grpc_addr" msgpack:"grpc_addr"`
	Roles     []string        `json:"roles" msgpack:"roles"`
	StartedAt time.Time       `json:"started_at" msgpack:"started_at"`
	Version   string          `json:"version" msgpack:"version"`
}

// NodeEntry tracks a cluster node's info and health state.
type NodeEntry struct {
	Info          NodeInfo           `msgpack:"info"`
	State         NodeState          `msgpack:"state"`
	LastHeartbeat time.Time          `msgpack:"last_heartbeat"`
	Resources     NodeResourceReport `msgpack:"resources"`
}

// NodeResourceReport carries resource utilization data from heartbeats.
type NodeResourceReport struct {
	CPUPercent    float64 `msgpack:"cpu_percent"`
	MemoryUsed    int64   `msgpack:"memory_used"`
	MemoryTotal   int64   `msgpack:"memory_total"`
	DiskUsed      int64   `msgpack:"disk_used"`
	DiskTotal     int64   `msgpack:"disk_total"`
	ActiveQueries int64   `msgpack:"active_queries"`
	IngestRateEPS int64   `msgpack:"ingest_rate_eps"`
}

// ProcessHeartbeat updates the last heartbeat time for a node and stores the
// latest resource report. If the node is in Suspect state, it transitions back to Alive.
func (s *MetaState) ProcessHeartbeat(nodeID sharding.NodeID, resources NodeResourceReport) {
	entry, ok := s.Nodes[nodeID]
	if !ok {
		return
	}

	entry.LastHeartbeat = time.Now()
	entry.Resources = resources
	if entry.State == NodeSuspect {
		entry.State = NodeAlive
	}
}

// DetectFailures scans all nodes and transitions their states based on
// the time since their last heartbeat.
//   - Alive -> Suspect after suspectTimeout without a heartbeat
//   - Suspect -> Dead after deadTimeout without a heartbeat
//
// Returns the list of nodes that transitioned to Dead.
func (s *MetaState) DetectFailures(clock ClockProvider, suspectTimeout, deadTimeout time.Duration) []sharding.NodeID {
	now := clock.Now()
	var dead []sharding.NodeID

	for id, entry := range s.Nodes {
		elapsed := now.Sub(entry.LastHeartbeat)

		switch entry.State {
		case NodeAlive:
			if elapsed > suspectTimeout {
				entry.State = NodeSuspect
			}
		case NodeSuspect:
			if elapsed > deadTimeout {
				entry.State = NodeDead
				dead = append(dead, id)
			}
		}
	}

	return dead
}
