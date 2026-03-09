package meta

import (
	"fmt"

	"github.com/lynxbase/lynxdb/pkg/cluster/rebalance"
	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

// RecomputeShardMap rebuilds the shard map from the hash ring.
// Called after node addition/removal to rebalance partition assignments.
// This computes assignments for all VirtualPartitionCount partitions
// against the current hash ring.
//
// WARNING: This replaces the entire shard map, losing per-shard state
// (epochs, drain status, ISR membership). Prefer IncrementalRebalance
// for topology changes in production clusters.
func (s *MetaState) RecomputeShardMap(vPartCount uint32, rf int) {
	sm := sharding.NewShardMap()
	sm.Epoch = s.ShardMap.Epoch + 1

	for p := uint32(0); p < vPartCount; p++ {
		nodes := s.Ring.AssignPartitionReplicas(p, rf)
		if len(nodes) == 0 {
			continue
		}

		key := fmt.Sprintf("p%d", p)
		sm.Assignments[key] = &sharding.ShardAssignment{
			ShardID: sharding.ShardID{
				Partition: p,
			},
			Primary:  nodes[0],
			Replicas: nodes[1:],
			State:    sharding.ShardActive,
			Epoch:    sm.Epoch,
		}
	}

	s.ShardMap = sm
	s.Version++
}

// IncrementalRebalance computes a minimal-move rebalance plan by comparing
// the current shard map against the hash ring's desired assignments. Only
// partitions whose primary or replicas differ are included in the plan.
// Unchanged partitions keep their existing state, epoch, and ISR intact.
//
// The returned plan is deterministic for a given ring state and shard map,
// ensuring all Raft followers apply the same moves.
func (s *MetaState) IncrementalRebalance(vPartCount uint32, rf int) *rebalance.RebalancePlan {
	plan := &rebalance.RebalancePlan{
		Epoch: s.ShardMap.Epoch + 1,
	}

	for p := uint32(0); p < vPartCount; p++ {
		desired := s.Ring.AssignPartitionReplicas(p, rf)
		if len(desired) == 0 {
			continue
		}

		key := fmt.Sprintf("p%d", p)
		existing, ok := s.ShardMap.Assignments[key]

		if !ok {
			// New partition — add as a move from empty to desired.
			plan.Moves = append(plan.Moves, rebalance.ShardMove{
				ShardKey:    key,
				Partition:   p,
				NewPrimary:  desired[0],
				NewReplicas: desired[1:],
			})

			continue
		}

		// Skip partitions in transient states — they are already being
		// migrated, drained, or split by another operation.
		if existing.State != sharding.ShardActive {
			continue
		}

		// Compare primary and replicas.
		if existing.Primary == desired[0] && nodeSliceEqual(existing.Replicas, desired[1:]) {
			continue // No change needed.
		}

		plan.Moves = append(plan.Moves, rebalance.ShardMove{
			ShardKey:    key,
			Partition:   p,
			OldPrimary:  existing.Primary,
			NewPrimary:  desired[0],
			OldReplicas: existing.Replicas,
			NewReplicas: desired[1:],
		})
	}

	return plan
}

// ApplyRebalancePayload is the payload for CmdApplyRebalance.
type ApplyRebalancePayload struct {
	Plan rebalance.RebalancePlan `msgpack:"plan"`
}

// applyRebalance applies a pre-computed rebalance plan to the shard map.
// For each move:
//   - If the primary changed, the shard transitions to ShardMigrating with
//     PendingPrimary set, signaling the old primary to drain.
//   - If only replicas changed, the shard stays ShardActive with updated replicas.
//   - New partitions (no existing assignment) are created in ShardActive state.
func (s *MetaState) applyRebalance(payload []byte) error {
	var p ApplyRebalancePayload
	if err := UnmarshalPayload(payload, &p); err != nil {
		return fmt.Errorf("meta.applyRebalance: %w", err)
	}

	s.ShardMap.Epoch = p.Plan.Epoch

	for _, move := range p.Plan.Moves {
		existing, ok := s.ShardMap.Assignments[move.ShardKey]

		if !ok {
			// New partition assignment.
			s.ShardMap.Assignments[move.ShardKey] = &sharding.ShardAssignment{
				ShardID: sharding.ShardID{
					Partition: move.Partition,
				},
				Primary:  move.NewPrimary,
				Replicas: move.NewReplicas,
				State:    sharding.ShardActive,
				Epoch:    p.Plan.Epoch,
			}

			continue
		}

		if existing.Primary != move.NewPrimary {
			// Primary changed — mark as migrating so old primary drains.
			existing.State = sharding.ShardMigrating
			existing.PendingPrimary = move.NewPrimary
			existing.PendingReplicas = move.NewReplicas
			existing.Epoch++
		} else {
			// Only replicas changed — update in place, stay active.
			existing.Replicas = move.NewReplicas
			existing.Epoch++
		}
	}

	s.Version++

	return nil
}

// nodeSliceEqual reports whether two NodeID slices contain the same elements
// in the same order.
func nodeSliceEqual(a, b []sharding.NodeID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

// TransitionShard changes the state of a shard assignment.
// Returns an error if the shard is not found.
func (s *MetaState) TransitionShard(shardKey string, state sharding.ShardState) error {
	a, ok := s.ShardMap.Assignments[shardKey]
	if !ok {
		return fmt.Errorf("meta.TransitionShard: shard %q not found", shardKey)
	}

	a.State = state
	s.Version++

	return nil
}
