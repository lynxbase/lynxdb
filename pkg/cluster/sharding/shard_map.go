package sharding

// NodeID is a unique identifier for a cluster node.
// Defined in the sharding package to avoid import cycles between
// cluster and cluster/sharding.
type NodeID string

// ShardState represents the lifecycle state of a shard assignment.
type ShardState int

const (
	// ShardActive means the shard is actively receiving writes and serving queries.
	ShardActive ShardState = iota
	// ShardDraining means the shard is no longer accepting new writes but still serves queries.
	ShardDraining
	// ShardMigrating means the shard is being moved to a new node.
	ShardMigrating
	// ShardSplitting means the shard is being split into smaller shards.
	ShardSplitting
)

// String returns the human-readable name of the shard state.
func (s ShardState) String() string {
	switch s {
	case ShardActive:
		return "active"
	case ShardDraining:
		return "draining"
	case ShardMigrating:
		return "migrating"
	case ShardSplitting:
		return "splitting"
	default:
		return "unknown"
	}
}

// ShardAssignment describes which nodes own a shard and its current state.
type ShardAssignment struct {
	ShardID  ShardID    `json:"shard_id" msgpack:"shard_id"`
	Primary  NodeID     `json:"primary" msgpack:"primary"`
	Replicas []NodeID   `json:"replicas" msgpack:"replicas"`
	State    ShardState `json:"state" msgpack:"state"`
	Epoch    uint64     `json:"epoch" msgpack:"epoch"`

	// PendingPrimary is the target primary during a ShardMigrating transition.
	// Set by CmdApplyRebalance, cleared when the drain completes.
	PendingPrimary NodeID `json:"pending_primary,omitempty" msgpack:"pending_primary,omitempty"`
	// PendingReplicas is the target replica set during a ShardMigrating transition.
	PendingReplicas []NodeID `json:"pending_replicas,omitempty" msgpack:"pending_replicas,omitempty"`
}

// ShardMap is a point-in-time snapshot of all shard assignments in the cluster.
type ShardMap struct {
	Assignments map[string]*ShardAssignment `json:"assignments" msgpack:"assignments"`
	Epoch       uint64                      `json:"epoch" msgpack:"epoch"`
}

// NewShardMap creates an empty shard map.
func NewShardMap() *ShardMap {
	return &ShardMap{
		Assignments: make(map[string]*ShardAssignment),
	}
}

// Assignment looks up the assignment for a given shard ID.
// Returns nil if the shard is not assigned.
func (m *ShardMap) Assignment(id ShardID) *ShardAssignment {
	if m == nil || m.Assignments == nil {
		return nil
	}

	return m.Assignments[id.String()]
}

// SetAssignment stores a shard assignment in the map.
func (m *ShardMap) SetAssignment(a *ShardAssignment) {
	if m.Assignments == nil {
		m.Assignments = make(map[string]*ShardAssignment)
	}

	m.Assignments[a.ShardID.String()] = a
}

// Len returns the number of shard assignments.
func (m *ShardMap) Len() int {
	if m == nil {
		return 0
	}

	return len(m.Assignments)
}
