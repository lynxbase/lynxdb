// Package rebalance implements incremental shard rebalancing and splitting
// for LynxDB cluster topology changes. It computes minimal-move rebalance
// plans when nodes join or leave, and supports hash-bit subdivision for
// hot-partition splitting.
package rebalance

import (
	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

// ShardMove describes a single partition reassignment within a rebalance plan.
// It captures the before/after state so the FSM can apply the change atomically
// and old primaries know to initiate drain.
type ShardMove struct {
	ShardKey    string            `msgpack:"shard_key"`
	Partition   uint32            `msgpack:"partition"`
	OldPrimary  sharding.NodeID   `msgpack:"old_primary"`
	NewPrimary  sharding.NodeID   `msgpack:"new_primary"`
	OldReplicas []sharding.NodeID `msgpack:"old_replicas"`
	NewReplicas []sharding.NodeID `msgpack:"new_replicas"`
}

// RebalancePlan is a pre-computed, deterministic set of shard moves
// committed as a single Raft log entry. Only partitions whose primary
// or replicas differ from the current assignment are included.
type RebalancePlan struct {
	Moves       []ShardMove     `msgpack:"moves"`
	Epoch       uint64          `msgpack:"epoch"`
	AddedNode   sharding.NodeID `msgpack:"added_node,omitempty"`
	RemovedNode sharding.NodeID `msgpack:"removed_node,omitempty"`
}

// IsEmpty reports whether the plan contains no moves.
func (p *RebalancePlan) IsEmpty() bool {
	return p == nil || len(p.Moves) == 0
}
