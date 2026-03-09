package meta

import (
	"fmt"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/cluster/rebalance"
	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

func TestRecomputeShardMap_BasicAssignment(t *testing.T) {
	state := NewMetaState()

	// Add 3 nodes to the ring.
	state.Ring.AddNode("node-1")
	state.Ring.AddNode("node-2")
	state.Ring.AddNode("node-3")

	state.RecomputeShardMap(16, 2) // 16 partitions, RF=2

	if len(state.ShardMap.Assignments) != 16 {
		t.Fatalf("expected 16 assignments, got %d", len(state.ShardMap.Assignments))
	}

	// Every assignment should have a primary and at least 1 replica.
	for key, a := range state.ShardMap.Assignments {
		if a.Primary == "" {
			t.Errorf("partition %s has no primary", key)
		}
		if len(a.Replicas) < 1 {
			t.Errorf("partition %s has %d replicas, want >=1", key, len(a.Replicas))
		}
		if a.State != sharding.ShardActive {
			t.Errorf("partition %s state = %d, want ShardActive", key, a.State)
		}
	}

	// Epoch should increment.
	if state.ShardMap.Epoch != 1 {
		t.Errorf("expected epoch 1, got %d", state.ShardMap.Epoch)
	}
}

func TestRecomputeShardMap_EmptyRing(t *testing.T) {
	state := NewMetaState()

	state.RecomputeShardMap(8, 1)

	if len(state.ShardMap.Assignments) != 0 {
		t.Errorf("expected 0 assignments with empty ring, got %d", len(state.ShardMap.Assignments))
	}
}

func TestRecomputeShardMap_SingleNode(t *testing.T) {
	state := NewMetaState()
	state.Ring.AddNode("node-1")

	state.RecomputeShardMap(4, 3) // RF=3 but only 1 node

	for key, a := range state.ShardMap.Assignments {
		if a.Primary != "node-1" {
			t.Errorf("partition %s primary = %s, want node-1", key, a.Primary)
		}
		// Replicas will be empty since there's only 1 node.
	}
}

func TestRecomputeShardMap_EpochIncrement(t *testing.T) {
	state := NewMetaState()
	state.Ring.AddNode("node-1")

	state.ShardMap.Epoch = 5
	state.RecomputeShardMap(2, 1)

	if state.ShardMap.Epoch != 6 {
		t.Errorf("expected epoch 6, got %d", state.ShardMap.Epoch)
	}
}

func TestTransitionShard(t *testing.T) {
	state := NewMetaState()
	state.Ring.AddNode("node-1")
	state.RecomputeShardMap(4, 1)

	// Transition a shard.
	err := state.TransitionShard("p0", sharding.ShardDraining)
	if err != nil {
		t.Fatalf("TransitionShard: %v", err)
	}

	a := state.ShardMap.Assignments["p0"]
	if a.State != sharding.ShardDraining {
		t.Errorf("expected ShardDraining, got %d", a.State)
	}

	// Transition nonexistent shard.
	err = state.TransitionShard("p999", sharding.ShardDraining)
	if err == nil {
		t.Error("expected error for nonexistent shard")
	}
}

// --- Incremental rebalance tests ---

func TestIncrementalRebalance_EmptyShardMap(t *testing.T) {
	state := NewMetaState()
	state.Ring.AddNode("node-1")
	state.Ring.AddNode("node-2")

	plan := state.IncrementalRebalance(8, 1)

	// All 8 partitions should be new moves.
	if len(plan.Moves) != 8 {
		t.Fatalf("expected 8 moves for empty shard map, got %d", len(plan.Moves))
	}

	// Every move should have empty OldPrimary (new partition).
	for _, m := range plan.Moves {
		if m.OldPrimary != "" {
			t.Errorf("partition %s: expected empty OldPrimary, got %q", m.ShardKey, m.OldPrimary)
		}
		if m.NewPrimary == "" {
			t.Errorf("partition %s: expected non-empty NewPrimary", m.ShardKey)
		}
	}
}

func TestIncrementalRebalance_NoChangeNoPlan(t *testing.T) {
	state := NewMetaState()
	state.Ring.AddNode("node-1")
	state.Ring.AddNode("node-2")
	state.Ring.AddNode("node-3")

	// Build initial shard map.
	state.RecomputeShardMap(16, 2)

	// With the same ring, incremental rebalance should produce no moves.
	plan := state.IncrementalRebalance(16, 2)
	if len(plan.Moves) != 0 {
		t.Errorf("expected 0 moves with unchanged topology, got %d", len(plan.Moves))
	}
}

func TestIncrementalRebalance_NodeAddMovesPartialPartitions(t *testing.T) {
	state := NewMetaState()
	state.Ring.AddNode("node-1")
	state.Ring.AddNode("node-2")

	// Build initial shard map with 2 nodes.
	state.RecomputeShardMap(64, 1)
	initialMap := make(map[string]sharding.NodeID)
	for key, a := range state.ShardMap.Assignments {
		initialMap[key] = a.Primary
	}

	// Add a third node.
	state.Ring.AddNode("node-3")

	plan := state.IncrementalRebalance(64, 1)

	// Should move ~1/3 of partitions (approximately), not all.
	if len(plan.Moves) == 0 {
		t.Fatal("expected some moves after adding node")
	}
	if len(plan.Moves) >= 64 {
		t.Errorf("expected fewer than 64 moves, got %d (should be ~1/N)", len(plan.Moves))
	}

	// Moved partitions should have the new node as primary.
	hasNewNode := false
	for _, m := range plan.Moves {
		if m.NewPrimary == "node-3" {
			hasNewNode = true
		}
	}
	if !hasNewNode {
		t.Error("expected at least one partition moved to node-3")
	}
}

func TestIncrementalRebalance_SkipsTransientStates(t *testing.T) {
	state := NewMetaState()
	state.Ring.AddNode("node-1")
	state.Ring.AddNode("node-2")

	state.RecomputeShardMap(8, 1)

	// Mark p0 as draining — should be skipped.
	state.ShardMap.Assignments["p0"].State = sharding.ShardDraining
	// Mark p1 as migrating — should be skipped.
	state.ShardMap.Assignments["p1"].State = sharding.ShardMigrating

	// Add a node to change topology.
	state.Ring.AddNode("node-3")

	plan := state.IncrementalRebalance(8, 1)

	for _, m := range plan.Moves {
		if m.ShardKey == "p0" {
			t.Error("expected p0 (draining) to be skipped")
		}
		if m.ShardKey == "p1" {
			t.Error("expected p1 (migrating) to be skipped")
		}
	}
}

func TestIncrementalRebalance_PreservesExistingState(t *testing.T) {
	state := NewMetaState()
	state.Ring.AddNode("node-1")
	state.Ring.AddNode("node-2")

	state.RecomputeShardMap(16, 1)

	// Bump epoch on a specific partition.
	state.ShardMap.Assignments["p5"].Epoch = 42

	// Add a node — some partitions will move, p5 should be preserved if unchanged.
	state.Ring.AddNode("node-3")

	plan := state.IncrementalRebalance(16, 1)

	// Verify that unmoved partitions retain their state.
	movedKeys := make(map[string]bool)
	for _, m := range plan.Moves {
		movedKeys[m.ShardKey] = true
	}

	if !movedKeys["p5"] {
		// p5 was not moved — its epoch should still be 42.
		if state.ShardMap.Assignments["p5"].Epoch != 42 {
			t.Errorf("expected p5 epoch=42 (preserved), got %d", state.ShardMap.Assignments["p5"].Epoch)
		}
	}
}

func TestApplyRebalance_PrimaryChange_MarksMigrating(t *testing.T) {
	state := NewMetaState()
	state.Ring.AddNode("node-1")
	state.Ring.AddNode("node-2")
	state.RecomputeShardMap(4, 1)

	// Find a partition owned by node-1.
	var targetKey string
	for key, a := range state.ShardMap.Assignments {
		if a.Primary == "node-1" {
			targetKey = key
			break
		}
	}
	if targetKey == "" {
		t.Fatal("no partition owned by node-1")
	}

	oldEpoch := state.ShardMap.Assignments[targetKey].Epoch

	payload, _ := MarshalPayload(ApplyRebalancePayload{
		Plan: rebalancePlanForTest(targetKey, "node-1", "node-2"),
	})

	if err := state.applyRebalance(payload); err != nil {
		t.Fatalf("applyRebalance: %v", err)
	}

	a := state.ShardMap.Assignments[targetKey]
	if a.State != sharding.ShardMigrating {
		t.Errorf("expected ShardMigrating, got %s", a.State)
	}
	if a.PendingPrimary != "node-2" {
		t.Errorf("expected PendingPrimary=node-2, got %q", a.PendingPrimary)
	}
	if a.Epoch <= oldEpoch {
		t.Errorf("expected epoch increment, got %d (was %d)", a.Epoch, oldEpoch)
	}
}

func TestApplyRebalance_ReplicaOnlyChange_StaysActive(t *testing.T) {
	state := NewMetaState()
	state.Ring.AddNode("node-1")
	state.Ring.AddNode("node-2")
	state.Ring.AddNode("node-3")
	state.RecomputeShardMap(4, 2)

	// Find a partition and change only its replicas.
	var targetKey string
	var primary sharding.NodeID
	for key, a := range state.ShardMap.Assignments {
		targetKey = key
		primary = a.Primary
		break
	}

	// Pick a replica that's different from current.
	newReplica := sharding.NodeID("node-4")

	payload, _ := MarshalPayload(ApplyRebalancePayload{
		Plan: rebalancePlanForReplicaChange(targetKey, primary, newReplica),
	})

	if err := state.applyRebalance(payload); err != nil {
		t.Fatalf("applyRebalance: %v", err)
	}

	a := state.ShardMap.Assignments[targetKey]
	if a.State != sharding.ShardActive {
		t.Errorf("expected ShardActive for replica-only change, got %s", a.State)
	}
	if len(a.Replicas) != 1 || a.Replicas[0] != newReplica {
		t.Errorf("expected replicas=[node-4], got %v", a.Replicas)
	}
}

func TestApplyRebalance_NewPartition(t *testing.T) {
	state := NewMetaState()

	payload, _ := MarshalPayload(ApplyRebalancePayload{
		Plan: rebalancePlanForNewPartition("p99", 99, "node-1"),
	})

	if err := state.applyRebalance(payload); err != nil {
		t.Fatalf("applyRebalance: %v", err)
	}

	a := state.ShardMap.Assignments["p99"]
	if a == nil {
		t.Fatal("expected assignment for p99")
	}
	if a.Primary != "node-1" {
		t.Errorf("expected primary=node-1, got %q", a.Primary)
	}
	if a.State != sharding.ShardActive {
		t.Errorf("expected ShardActive, got %s", a.State)
	}
}

// --- Split tests ---

func TestApplyProposeSplit(t *testing.T) {
	state := NewMetaState()
	state.Ring.AddNode("node-1")
	state.RecomputeShardMap(16, 1)

	payload, _ := MarshalPayload(ProposeSplitPayload{
		ParentPartition: 0,
		ChildA:          1024,
		ChildB:          1025,
		SplitBit:        0,
	})

	if err := state.applyProposeSplit(payload); err != nil {
		t.Fatalf("applyProposeSplit: %v", err)
	}

	// Parent should be splitting.
	parent := state.ShardMap.Assignments["p0"]
	if parent.State != sharding.ShardSplitting {
		t.Errorf("expected ShardSplitting, got %s", parent.State)
	}

	// Children should exist in migrating state.
	childA := state.ShardMap.Assignments["p1024"]
	if childA == nil {
		t.Fatal("expected child_a assignment")
	}
	if childA.State != sharding.ShardMigrating {
		t.Errorf("child_a: expected ShardMigrating, got %s", childA.State)
	}
	if childA.Primary != parent.Primary {
		t.Errorf("child_a should inherit parent's primary")
	}

	childB := state.ShardMap.Assignments["p1025"]
	if childB == nil {
		t.Fatal("expected child_b assignment")
	}

	// Split info should be recorded.
	split := state.Splits[0]
	if split == nil {
		t.Fatal("expected split info for partition 0")
	}
	if split.ChildA != 1024 || split.ChildB != 1025 {
		t.Errorf("wrong children: %d, %d", split.ChildA, split.ChildB)
	}
}

func TestApplyCompleteSplit(t *testing.T) {
	state := NewMetaState()
	state.Ring.AddNode("node-1")
	state.RecomputeShardMap(16, 1)

	// Propose split.
	propPayload, _ := MarshalPayload(ProposeSplitPayload{
		ParentPartition: 0,
		ChildA:          1024,
		ChildB:          1025,
		SplitBit:        0,
	})
	if err := state.applyProposeSplit(propPayload); err != nil {
		t.Fatalf("propose: %v", err)
	}

	// Complete split.
	compPayload, _ := MarshalPayload(CompleteSplitPayload{
		ParentPartition: 0,
	})
	if err := state.applyCompleteSplit(compPayload); err != nil {
		t.Fatalf("complete: %v", err)
	}

	// Parent should be removed.
	if state.ShardMap.Assignments["p0"] != nil {
		t.Error("expected parent p0 to be removed after split")
	}

	// Children should be active.
	childA := state.ShardMap.Assignments["p1024"]
	if childA == nil || childA.State != sharding.ShardActive {
		t.Error("expected child_a to be active")
	}

	childB := state.ShardMap.Assignments["p1025"]
	if childB == nil || childB.State != sharding.ShardActive {
		t.Error("expected child_b to be active")
	}

	// Split info should be removed.
	if state.Splits[0] != nil {
		t.Error("expected split info to be removed")
	}
}

func TestApplyProposeSplit_NonActivePartitionFails(t *testing.T) {
	state := NewMetaState()
	state.Ring.AddNode("node-1")
	state.RecomputeShardMap(4, 1)

	// Mark partition as draining.
	state.ShardMap.Assignments["p0"].State = sharding.ShardDraining

	payload, _ := MarshalPayload(ProposeSplitPayload{
		ParentPartition: 0,
		ChildA:          1024,
		ChildB:          1025,
		SplitBit:        0,
	})

	if err := state.applyProposeSplit(payload); err == nil {
		t.Error("expected error for non-active partition")
	}
}

func TestApplyProposeSplit_DuplicateFails(t *testing.T) {
	state := NewMetaState()
	state.Ring.AddNode("node-1")
	state.RecomputeShardMap(4, 1)

	payload, _ := MarshalPayload(ProposeSplitPayload{
		ParentPartition: 0,
		ChildA:          1024,
		ChildB:          1025,
		SplitBit:        0,
	})

	if err := state.applyProposeSplit(payload); err != nil {
		t.Fatalf("first propose: %v", err)
	}

	// Second propose for same partition should fail.
	if err := state.applyProposeSplit(payload); err == nil {
		t.Error("expected error for duplicate split")
	}
}

// --- Helpers ---

func rebalancePlanForTest(key string, oldPrimary, newPrimary sharding.NodeID) rebalance.RebalancePlan {
	return rebalance.RebalancePlan{
		Epoch: 10,
		Moves: []rebalance.ShardMove{
			{
				ShardKey:   key,
				Partition:  0,
				OldPrimary: oldPrimary,
				NewPrimary: newPrimary,
			},
		},
	}
}

func rebalancePlanForReplicaChange(key string, primary, newReplica sharding.NodeID) rebalance.RebalancePlan {
	return rebalance.RebalancePlan{
		Epoch: 10,
		Moves: []rebalance.ShardMove{
			{
				ShardKey:    key,
				Partition:   0,
				OldPrimary:  primary,
				NewPrimary:  primary,
				NewReplicas: []sharding.NodeID{newReplica},
			},
		},
	}
}

func rebalancePlanForNewPartition(key string, partition uint32, primary sharding.NodeID) rebalance.RebalancePlan {
	return rebalance.RebalancePlan{
		Epoch: 10,
		Moves: []rebalance.ShardMove{
			{
				ShardKey:   key,
				Partition:  partition,
				NewPrimary: primary,
			},
		},
	}
}

// TestIncrementalRebalance_PartitionCoverage verifies every partition has
// exactly one primary after applying a rebalance plan.
func TestIncrementalRebalance_PartitionCoverage(t *testing.T) {
	vPartCount := uint32(32)
	state := NewMetaState()
	state.Ring.AddNode("node-1")
	state.Ring.AddNode("node-2")
	state.Ring.AddNode("node-3")
	state.RecomputeShardMap(vPartCount, 2)

	// Add a 4th node.
	state.Ring.AddNode("node-4")
	plan := state.IncrementalRebalance(vPartCount, 2)

	// Apply the plan in-memory to verify coverage.
	payload, _ := MarshalPayload(ApplyRebalancePayload{Plan: *plan})
	if err := state.applyRebalance(payload); err != nil {
		t.Fatalf("applyRebalance: %v", err)
	}

	// Every partition 0..31 should have an assignment.
	for p := uint32(0); p < vPartCount; p++ {
		key := fmt.Sprintf("p%d", p)
		a := state.ShardMap.Assignments[key]
		if a == nil {
			t.Errorf("partition %d has no assignment after rebalance", p)

			continue
		}
		if a.Primary == "" {
			t.Errorf("partition %d has empty primary", p)
		}
	}
}

// TestIncrementalRebalance_ReplicaSetsNoDuplicates verifies no duplicate
// nodes in the replica set (including primary).
func TestIncrementalRebalance_ReplicaSetsNoDuplicates(t *testing.T) {
	state := NewMetaState()
	state.Ring.AddNode("node-1")
	state.Ring.AddNode("node-2")
	state.Ring.AddNode("node-3")
	state.Ring.AddNode("node-4")
	state.RecomputeShardMap(32, 3)

	for key, a := range state.ShardMap.Assignments {
		seen := map[sharding.NodeID]bool{a.Primary: true}
		for _, r := range a.Replicas {
			if seen[r] {
				t.Errorf("partition %s: duplicate node %s in primary+replicas", key, r)
			}
			seen[r] = true
		}
	}
}
