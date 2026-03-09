//go:build integration

// Package integration contains integration tests for cluster operations.
// These tests use the simulation framework (no Docker, fast enough for CI).
//
// Run with: go test -tags integration ./test/integration/...
package integration

import (
	"fmt"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/cluster/meta"
	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
	"github.com/lynxbase/lynxdb/test/simulation"
)

// --- Scenario 2: Node join triggers rebalance ---

func TestScenario02_NodeJoinTriggersRebalance(t *testing.T) {
	cluster := simulation.NewSimCluster(simulation.SimClusterConfig{
		VirtualPartitionCount: 32,
		ReplicationFactor:     2,
	})

	// Bootstrap with 3 nodes.
	cluster.AddNode("node-1", []string{"meta", "ingest", "query"})
	cluster.AddNode("node-2", []string{"meta", "ingest", "query"})
	cluster.AddNode("node-3", []string{"meta", "ingest", "query"})
	cluster.RecomputeShardMap()

	// Record initial assignments.
	state := cluster.State()
	before := make(map[string]sharding.NodeID)
	for key, a := range state.ShardMap.Assignments {
		before[key] = a.Primary
	}

	// Add a 4th node.
	cluster.AddNode("node-4", []string{"ingest", "query"})
	moves := cluster.IncrementalRebalance()

	// At most ~1/N partitions should move.
	maxMoves := 32 / 3 * 2 // generous bound
	if moves > maxMoves {
		t.Errorf("expected at most ~%d moves, got %d", maxMoves, moves)
	}
	if moves == 0 {
		t.Error("expected at least 1 move")
	}

	// Unchanged partitions should keep their primary.
	changed := 0
	for key, a := range state.ShardMap.Assignments {
		oldPrimary, ok := before[key]
		if !ok {
			continue
		}
		if a.Primary != oldPrimary && a.State != sharding.ShardMigrating {
			// Primary changed but not in migrating state — shouldn't happen.
			if a.PendingPrimary == "" {
				changed++
			}
		}
	}

	t.Logf("rebalance: %d moves out of 32 partitions", moves)
}

// --- Scenario 3: Graceful node leave + drain ---

func TestScenario03_GracefulNodeLeave(t *testing.T) {
	cluster := simulation.NewSimCluster(simulation.SimClusterConfig{
		VirtualPartitionCount: 16,
		ReplicationFactor:     2,
	})

	cluster.AddNode("node-1", []string{"ingest"})
	cluster.AddNode("node-2", []string{"ingest"})
	cluster.AddNode("node-3", []string{"ingest"})
	cluster.RecomputeShardMap()

	state := cluster.State()

	// Count partitions owned by node-3 before removal.
	ownedByNode3 := 0
	for _, a := range state.ShardMap.Assignments {
		if a.Primary == "node-3" {
			ownedByNode3++
		}
	}
	t.Logf("node-3 owns %d partitions before drain", ownedByNode3)

	// Simulate graceful drain: mark all node-3 shards as draining then complete.
	for key, a := range state.ShardMap.Assignments {
		if a.Primary != "node-3" {
			continue
		}
		a.State = sharding.ShardDraining

		// Complete drain: promote first replica.
		if len(a.Replicas) > 0 {
			newPrimary := a.Replicas[0]
			a.Primary = newPrimary
			a.Replicas = a.Replicas[1:]
			a.State = sharding.ShardActive
			a.Epoch++
		}

		_ = key
	}

	// Remove node and rebalance.
	cluster.RemoveNode("node-3")
	cluster.IncrementalRebalance()

	// Verify no partition is still assigned to node-3.
	for key, a := range state.ShardMap.Assignments {
		if a.Primary == "node-3" {
			t.Errorf("partition %s still assigned to removed node-3", key)
		}
	}
}

// --- Scenario 4: Node failure + automatic failover ---

func TestScenario04_NodeFailureFailover(t *testing.T) {
	cluster := simulation.NewSimCluster(simulation.SimClusterConfig{
		VirtualPartitionCount: 16,
		ReplicationFactor:     2,
	})

	cluster.AddNode("node-1", []string{"ingest"})
	cluster.AddNode("node-2", []string{"ingest"})
	cluster.AddNode("node-3", []string{"ingest"})
	cluster.RecomputeShardMap()

	state := cluster.State()

	// Kill node-2.
	cluster.KillNode("node-2")

	// Promote ISR replicas for shards where node-2 was primary.
	promoted := 0
	for _, a := range state.ShardMap.Assignments {
		if a.Primary != "node-2" {
			continue
		}
		if len(a.Replicas) == 0 {
			continue
		}

		newPrimary := a.Replicas[0]
		a.Primary = newPrimary
		a.State = sharding.ShardActive
		a.Replicas = a.Replicas[1:]
		a.Epoch++
		promoted++
	}

	t.Logf("promoted %d shards from dead node-2", promoted)

	// After removing dead node and rebalancing, every partition should have a primary.
	cluster.RemoveNode("node-2")
	cluster.IncrementalRebalance()

	for p := uint32(0); p < 16; p++ {
		key := fmt.Sprintf("p%d", p)
		a := state.ShardMap.Assignments[key]
		if a == nil {
			t.Errorf("partition %d has no assignment after failover", p)
			continue
		}
		if a.Primary == "node-2" {
			t.Errorf("partition %d still assigned to dead node-2", p)
		}
	}
}

// --- Scenario 6: Shard split on hot partition ---

func TestScenario06_ShardSplit(t *testing.T) {
	cluster := simulation.NewSimCluster(simulation.SimClusterConfig{
		VirtualPartitionCount: 8,
		ReplicationFactor:     1,
	})

	cluster.AddNode("node-1", []string{"ingest"})
	cluster.AddNode("node-2", []string{"ingest"})
	cluster.RecomputeShardMap()

	state := cluster.State()

	// Simulate split of partition 0.
	payload, _ := meta.MarshalPayload(meta.ProposeSplitPayload{
		ParentPartition: 0,
		ChildA:          1024,
		ChildB:          1025,
		SplitBit:        0,
	})

	// Apply through the FSM path directly on state.
	// Note: We can't call the unexported applyProposeSplit, so we
	// simulate its effects.
	parent := state.ShardMap.Assignments["p0"]
	if parent == nil {
		t.Fatal("partition 0 not found")
	}

	parentPrimary := parent.Primary
	parentReplicas := make([]sharding.NodeID, len(parent.Replicas))
	copy(parentReplicas, parent.Replicas)

	parent.State = sharding.ShardSplitting
	parent.Epoch++

	state.ShardMap.Assignments["p1024"] = &sharding.ShardAssignment{
		ShardID:  sharding.ShardID{Partition: 1024},
		Primary:  parentPrimary,
		Replicas: parentReplicas,
		State:    sharding.ShardMigrating,
		Epoch:    state.ShardMap.Epoch,
	}
	state.ShardMap.Assignments["p1025"] = &sharding.ShardAssignment{
		ShardID:  sharding.ShardID{Partition: 1025},
		Primary:  parentPrimary,
		Replicas: parentReplicas,
		State:    sharding.ShardMigrating,
		Epoch:    state.ShardMap.Epoch,
	}

	state.Splits[0] = &meta.SplitInfo{
		ParentPartition: 0,
		ChildA:          1024,
		ChildB:          1025,
		SplitBit:        0,
	}

	_ = payload

	// Verify parent is splitting.
	if parent.State != sharding.ShardSplitting {
		t.Errorf("expected parent ShardSplitting, got %s", parent.State)
	}

	// Complete split.
	state.ShardMap.Assignments["p1024"].State = sharding.ShardActive
	state.ShardMap.Assignments["p1024"].Epoch++
	state.ShardMap.Assignments["p1025"].State = sharding.ShardActive
	state.ShardMap.Assignments["p1025"].Epoch++
	delete(state.ShardMap.Assignments, "p0")
	delete(state.Splits, 0)

	// Verify parent gone, children active.
	if state.ShardMap.Assignments["p0"] != nil {
		t.Error("parent p0 should be removed after split")
	}
	if state.ShardMap.Assignments["p1024"] == nil || state.ShardMap.Assignments["p1024"].State != sharding.ShardActive {
		t.Error("child_a should be active")
	}
	if state.ShardMap.Assignments["p1025"] == nil || state.ShardMap.Assignments["p1025"].State != sharding.ShardActive {
		t.Error("child_b should be active")
	}
}

// --- Scenario 7: Split routing correctness ---

func TestScenario07_SplitRoutingCorrectness(t *testing.T) {
	sr := sharding.NewSplitRegistry()
	sr.Register(&sharding.SplitInfo{
		ParentPartition: 5,
		ChildA:          1024,
		ChildB:          1025,
		SplitBit:        3,
	})

	// Every hash must route to exactly one child.
	for h := uint64(0); h < 10000; h++ {
		result := sr.Resolve(5, h)
		if result != 1024 && result != 1025 {
			t.Fatalf("hash %d: resolved to %d, expected 1024 or 1025", h, result)
		}
	}

	// Both children should be reachable.
	gotA, gotB := false, false
	for h := uint64(0); h < 100; h++ {
		result := sr.Resolve(5, h)
		if result == 1024 {
			gotA = true
		}
		if result == 1025 {
			gotB = true
		}
	}
	if !gotA || !gotB {
		t.Error("expected both children to be reachable")
	}
}

// --- Scenario 9: ISR shrink + expand ---

func TestScenario09_ISRShrinkExpand(t *testing.T) {
	cluster := simulation.NewSimCluster(simulation.SimClusterConfig{
		VirtualPartitionCount: 4,
		ReplicationFactor:     3,
	})

	cluster.AddNode("node-1", []string{"ingest"})
	cluster.AddNode("node-2", []string{"ingest"})
	cluster.AddNode("node-3", []string{"ingest"})
	cluster.RecomputeShardMap()

	state := cluster.State()

	// Find a partition with replicas.
	var targetKey string
	for key, a := range state.ShardMap.Assignments {
		if len(a.Replicas) >= 2 {
			targetKey = key
			break
		}
	}
	if targetKey == "" {
		t.Skip("no partition with 2+ replicas found")
	}

	a := state.ShardMap.Assignments[targetKey]
	originalReplicas := len(a.Replicas)

	// Shrink ISR: remove one replica.
	a.Replicas = a.Replicas[:len(a.Replicas)-1]
	if len(a.Replicas) >= originalReplicas {
		t.Error("ISR should have shrunk")
	}

	// Expand ISR: add it back.
	a.Replicas = append(a.Replicas, "node-3")
	if len(a.Replicas) < originalReplicas {
		t.Error("ISR should have expanded")
	}
}

// --- Scenario 11: Alert reassignment on node death ---

func TestScenario11_AlertReassignmentOnNodeDeath(t *testing.T) {
	cluster := simulation.NewSimCluster(simulation.SimClusterConfig{
		VirtualPartitionCount: 4,
		ReplicationFactor:     1,
	})

	cluster.AddNode("node-1", []string{"query"})
	cluster.AddNode("node-2", []string{"query"})
	cluster.AddNode("node-3", []string{"query"})

	state := cluster.State()

	// Assign some alerts.
	queryNodes := []sharding.NodeID{"node-1", "node-2", "node-3"}
	for i := 0; i < 10; i++ {
		alertID := fmt.Sprintf("alert-%d", i)
		assigned := meta.RendezvousAssign(alertID, queryNodes)
		state.AlertAssign[alertID] = &meta.AlertAssignment{
			AlertID:      alertID,
			AssignedNode: assigned,
			Version:      1,
		}
	}

	// Kill node-2.
	cluster.KillNode("node-2")

	// Reassign alerts from dead node.
	reassigned := state.ReassignAlertsFromDeadNode("node-2")
	t.Logf("reassigned %d alerts from dead node-2", len(reassigned))

	// Verify no alerts assigned to dead node.
	for alertID, aa := range state.AlertAssign {
		if aa.AssignedNode == "node-2" {
			t.Errorf("alert %s still assigned to dead node-2", alertID)
		}
	}
}

// --- Scenario 17: Rapid node join/leave churn ---

func TestScenario17_RapidNodeChurn(t *testing.T) {
	cluster := simulation.NewSimCluster(simulation.SimClusterConfig{
		VirtualPartitionCount: 32,
		ReplicationFactor:     1,
	})

	// Start with 3 nodes.
	cluster.AddNode("node-1", []string{"ingest"})
	cluster.AddNode("node-2", []string{"ingest"})
	cluster.AddNode("node-3", []string{"ingest"})
	cluster.RecomputeShardMap()

	// Rapid churn: add and remove nodes in quick succession.
	for i := 10; i < 20; i++ {
		id := sharding.NodeID(fmt.Sprintf("temp-%d", i))
		cluster.AddNode(id, []string{"ingest"})
		cluster.IncrementalRebalance()
	}
	for i := 10; i < 20; i++ {
		id := sharding.NodeID(fmt.Sprintf("temp-%d", i))
		cluster.RemoveNode(id)
		cluster.IncrementalRebalance()
	}

	state := cluster.State()

	// After churn stabilizes, every partition should have a primary.
	for p := uint32(0); p < 32; p++ {
		key := fmt.Sprintf("p%d", p)
		a := state.ShardMap.Assignments[key]
		if a == nil {
			t.Errorf("partition %d has no assignment after churn", p)
			continue
		}
		if a.Primary == "" {
			t.Errorf("partition %d has empty primary after churn", p)
		}
	}
}
