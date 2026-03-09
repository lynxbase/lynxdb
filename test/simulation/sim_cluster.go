package simulation

import (
	"sync"
	"time"

	"github.com/lynxbase/lynxdb/pkg/cluster/meta"
	"github.com/lynxbase/lynxdb/pkg/cluster/rebalance"
	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

// SimNode represents a simulated cluster node with its own FSM state.
type SimNode struct {
	ID    sharding.NodeID
	Roles []string
	State *meta.MetaState
	Alive bool
}

// SimCluster orchestrates N simulated nodes for deterministic testing.
// It provides a shared FSM state (simulating Raft consensus) and supports
// node add/remove/kill operations for property-based tests.
//
// Unlike a real cluster, all nodes share a single MetaState — Raft
// consensus is simulated by having all mutations apply to this shared
// state directly. This enables fast, deterministic property testing
// without real network or Raft overhead.
type SimCluster struct {
	mu    sync.Mutex
	nodes map[sharding.NodeID]*SimNode
	state *meta.MetaState
	clock *SimClock
	net   *SimNetwork
	s3    *SimS3

	vPartCount uint32
	rf         int
}

// SimClusterConfig configures a simulated cluster.
type SimClusterConfig struct {
	VirtualPartitionCount uint32
	ReplicationFactor     int
	NetworkSeed           int64
}

// NewSimCluster creates a new SimCluster with the given configuration.
func NewSimCluster(cfg SimClusterConfig) *SimCluster {
	if cfg.VirtualPartitionCount == 0 {
		cfg.VirtualPartitionCount = 32
	}
	if cfg.ReplicationFactor == 0 {
		cfg.ReplicationFactor = 1
	}

	return &SimCluster{
		nodes:      make(map[sharding.NodeID]*SimNode),
		state:      meta.NewMetaState(),
		clock:      NewSimClock(baseTime()),
		net:        NewSimNetwork(cfg.NetworkSeed),
		s3:         NewSimS3(),
		vPartCount: cfg.VirtualPartitionCount,
		rf:         cfg.ReplicationFactor,
	}
}

// AddNode adds a node to the cluster and updates the ring.
func (c *SimCluster) AddNode(id sharding.NodeID, roles []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.nodes[id] = &SimNode{
		ID:    id,
		Roles: roles,
		State: c.state,
		Alive: true,
	}

	c.state.Ring.AddNode(id)
	c.state.Nodes[id] = &meta.NodeEntry{
		Info: meta.NodeInfo{
			ID:    id,
			Roles: roles,
		},
		State:         meta.NodeAlive,
		LastHeartbeat: c.clock.Now(),
	}
	c.state.Version++
}

// RemoveNode removes a node from the cluster and updates the ring.
func (c *SimCluster) RemoveNode(id sharding.NodeID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.nodes, id)
	c.state.Ring.RemoveNode(id)
	delete(c.state.Nodes, id)
	c.state.Version++
}

// KillNode marks a node as dead without removing it from the ring.
func (c *SimCluster) KillNode(id sharding.NodeID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if node, ok := c.nodes[id]; ok {
		node.Alive = false
	}
	if entry, ok := c.state.Nodes[id]; ok {
		entry.State = meta.NodeDead
	}
}

// ReviveNode marks a previously killed node as alive.
func (c *SimCluster) ReviveNode(id sharding.NodeID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if node, ok := c.nodes[id]; ok {
		node.Alive = true
	}
	if entry, ok := c.state.Nodes[id]; ok {
		entry.State = meta.NodeAlive
		entry.LastHeartbeat = c.clock.Now()
	}
}

// RecomputeShardMap rebuilds the shard map from scratch.
func (c *SimCluster) RecomputeShardMap() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.state.RecomputeShardMap(c.vPartCount, c.rf)
}

// IncrementalRebalance computes and applies an incremental rebalance plan.
// Returns the number of moves applied.
//
// Since we don't have real Raft in simulation, we apply the plan's effects
// directly to the shared MetaState, mirroring the FSM's applyRebalance logic.
func (c *SimCluster) IncrementalRebalance() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	plan := c.state.IncrementalRebalance(c.vPartCount, c.rf)
	if plan.IsEmpty() {
		return 0
	}

	c.applyPlanToState(plan)

	return len(plan.Moves)
}

// applyPlanToState applies a rebalance plan directly to the MetaState,
// simulating what the FSM's applyRebalance method does.
func (c *SimCluster) applyPlanToState(plan *rebalance.RebalancePlan) {
	c.state.ShardMap.Epoch = plan.Epoch

	for _, move := range plan.Moves {
		existing, ok := c.state.ShardMap.Assignments[move.ShardKey]

		if !ok {
			// New partition assignment.
			c.state.ShardMap.Assignments[move.ShardKey] = &sharding.ShardAssignment{
				ShardID: sharding.ShardID{
					Partition: move.Partition,
				},
				Primary:  move.NewPrimary,
				Replicas: move.NewReplicas,
				State:    sharding.ShardActive,
				Epoch:    plan.Epoch,
			}

			continue
		}

		if existing.Primary != move.NewPrimary {
			existing.State = sharding.ShardMigrating
			existing.PendingPrimary = move.NewPrimary
			existing.PendingReplicas = move.NewReplicas
			existing.Epoch++
		} else {
			existing.Replicas = move.NewReplicas
			existing.Epoch++
		}
	}

	c.state.Version++
}

// State returns the shared MetaState. The caller must not modify it
// without holding the cluster lock.
func (c *SimCluster) State() *meta.MetaState {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.state
}

// NodeIDs returns the IDs of all nodes in the cluster.
func (c *SimCluster) NodeIDs() []sharding.NodeID {
	c.mu.Lock()
	defer c.mu.Unlock()

	ids := make([]sharding.NodeID, 0, len(c.nodes))
	for id := range c.nodes {
		ids = append(ids, id)
	}

	return ids
}

// AliveNodeIDs returns IDs of alive nodes only.
func (c *SimCluster) AliveNodeIDs() []sharding.NodeID {
	c.mu.Lock()
	defer c.mu.Unlock()

	var ids []sharding.NodeID
	for id, node := range c.nodes {
		if node.Alive {
			ids = append(ids, id)
		}
	}

	return ids
}

// baseTime returns a deterministic base time for simulation.
func baseTime() time.Time {
	return time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC)
}

// Clock returns the simulated clock.
func (c *SimCluster) Clock() *SimClock { return c.clock }

// Network returns the simulated network.
func (c *SimCluster) Network() *SimNetwork { return c.net }

// S3 returns the simulated S3 store.
func (c *SimCluster) S3() *SimS3 { return c.s3 }

// VPartCount returns the virtual partition count.
func (c *SimCluster) VPartCount() uint32 { return c.vPartCount }

// RF returns the replication factor.
func (c *SimCluster) RF() int { return c.rf }
