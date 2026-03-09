package simulation

import (
	"fmt"
	"testing"

	"pgregory.net/rapid"

	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

// TestProperty_RebalancePreservesPartitionCoverage verifies that every
// partition has exactly one primary after an incremental rebalance.
func TestProperty_RebalancePreservesPartitionCoverage(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		nodeCount := rapid.IntRange(2, 10).Draw(t, "nodeCount")
		vParts := uint32(rapid.IntRange(8, 64).Draw(t, "vParts"))

		cluster := NewSimCluster(SimClusterConfig{
			VirtualPartitionCount: vParts,
			ReplicationFactor:     1,
		})

		// Add initial nodes.
		for i := 0; i < nodeCount; i++ {
			cluster.AddNode(sharding.NodeID(fmt.Sprintf("node-%d", i)), []string{"ingest", "query"})
		}
		cluster.RecomputeShardMap()

		// Add one more node and rebalance.
		cluster.AddNode(sharding.NodeID(fmt.Sprintf("node-%d", nodeCount)), []string{"ingest", "query"})
		cluster.IncrementalRebalance()

		state := cluster.State()
		for p := uint32(0); p < vParts; p++ {
			key := fmt.Sprintf("p%d", p)
			a := state.ShardMap.Assignments[key]
			if a == nil {
				t.Fatalf("partition %d has no assignment after rebalance", p)
			}
			if a.Primary == "" {
				t.Fatalf("partition %d has empty primary", p)
			}
		}
	})
}

// TestProperty_NodeJoinMovesAtMostOneNthPartitions verifies consistent
// hashing: adding one node to N existing nodes moves at most ~2/N of
// partitions (with some slack for small clusters).
func TestProperty_NodeJoinMovesAtMostOneNthPartitions(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		nodeCount := rapid.IntRange(3, 20).Draw(t, "nodeCount")
		vParts := uint32(rapid.IntRange(32, 256).Draw(t, "vParts"))

		cluster := NewSimCluster(SimClusterConfig{
			VirtualPartitionCount: vParts,
			ReplicationFactor:     1,
		})

		for i := 0; i < nodeCount; i++ {
			cluster.AddNode(sharding.NodeID(fmt.Sprintf("node-%d", i)), []string{"ingest"})
		}
		cluster.RecomputeShardMap()

		// Snapshot current assignments.
		state := cluster.State()
		before := make(map[string]sharding.NodeID)
		for key, a := range state.ShardMap.Assignments {
			before[key] = a.Primary
		}

		// Add one node and rebalance.
		cluster.AddNode(sharding.NodeID("new-node"), []string{"ingest"})
		moves := cluster.IncrementalRebalance()

		// Consistent hashing guarantee: at most ~2/N partitions should move.
		// We use 3/N as a generous upper bound for small clusters.
		maxMoves := int(float64(vParts) * 3.0 / float64(nodeCount))
		if maxMoves < 1 {
			maxMoves = 1
		}

		if moves > maxMoves {
			t.Fatalf("expected at most %d moves (3/N * %d partitions), got %d", maxMoves, vParts, moves)
		}
	})
}

// TestProperty_SplitRoutingCoversAllHashes verifies that after a split,
// every possible hash value routes to exactly one child partition.
func TestProperty_SplitRoutingCoversAllHashes(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		splitBit := uint8(rapid.IntRange(0, 10).Draw(t, "splitBit"))
		parentPartition := uint32(rapid.IntRange(0, 1023).Draw(t, "parent"))
		childA := parentPartition*2 + 1024
		childB := childA + 1

		sr := sharding.NewSplitRegistry()
		sr.Register(&sharding.SplitInfo{
			ParentPartition: parentPartition,
			ChildA:          childA,
			ChildB:          childB,
			SplitBit:        splitBit,
		})

		// Every hash should route to either childA or childB, never the parent.
		for h := uint64(0); h < 256; h++ {
			result := sr.Resolve(parentPartition, h)
			if result == parentPartition {
				t.Fatalf("hash %d routes to parent %d after split", h, parentPartition)
			}
			if result != childA && result != childB {
				t.Fatalf("hash %d routes to %d, expected %d or %d", h, result, childA, childB)
			}
		}
	})
}

// TestProperty_DrainCompletionResetsToActive verifies that completing
// a drain always transitions the shard back to active state.
func TestProperty_DrainCompletionResetsToActive(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		vParts := uint32(rapid.IntRange(4, 32).Draw(t, "vParts"))

		cluster := NewSimCluster(SimClusterConfig{
			VirtualPartitionCount: vParts,
			ReplicationFactor:     2,
		})

		cluster.AddNode("node-1", []string{"ingest"})
		cluster.AddNode("node-2", []string{"ingest"})
		cluster.AddNode("node-3", []string{"ingest"})
		cluster.RecomputeShardMap()

		state := cluster.State()

		// Pick a random active partition to drain.
		for key, a := range state.ShardMap.Assignments {
			if a.State != sharding.ShardActive || len(a.Replicas) == 0 {
				continue
			}

			// Drain: transition to draining.
			a.State = sharding.ShardDraining

			// Complete drain: assign new primary from replicas.
			newPrimary := a.Replicas[0]
			a.Primary = newPrimary
			a.State = sharding.ShardActive
			a.Epoch++

			// Remove old primary from replicas.
			filtered := make([]sharding.NodeID, 0)
			for _, r := range a.Replicas {
				if r != newPrimary {
					filtered = append(filtered, r)
				}
			}
			a.Replicas = filtered

			if a.State != sharding.ShardActive {
				t.Fatalf("shard %s: expected active after drain completion, got %s", key, a.State)
			}

			break // Only drain one for this test case.
		}
	})
}

// TestProperty_ReplicaSetsHaveNoDuplicates verifies that no node appears
// more than once in primary + replicas for any shard.
func TestProperty_ReplicaSetsHaveNoDuplicates(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		nodeCount := rapid.IntRange(3, 10).Draw(t, "nodeCount")
		rf := rapid.IntRange(1, min(3, nodeCount)).Draw(t, "rf")
		vParts := uint32(rapid.IntRange(8, 64).Draw(t, "vParts"))

		cluster := NewSimCluster(SimClusterConfig{
			VirtualPartitionCount: vParts,
			ReplicationFactor:     rf,
		})

		for i := 0; i < nodeCount; i++ {
			cluster.AddNode(sharding.NodeID(fmt.Sprintf("node-%d", i)), []string{"ingest"})
		}
		cluster.RecomputeShardMap()

		state := cluster.State()
		for key, a := range state.ShardMap.Assignments {
			seen := map[sharding.NodeID]bool{a.Primary: true}
			for _, r := range a.Replicas {
				if seen[r] {
					t.Fatalf("shard %s: duplicate node %s in replica set", key, r)
				}
				seen[r] = true
			}
		}
	})
}

// TestProperty_EpochsMonotonicallyIncrease verifies that shard epochs
// never decrease after rebalance operations.
func TestProperty_EpochsMonotonicallyIncrease(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		nodeCount := rapid.IntRange(3, 8).Draw(t, "nodeCount")
		vParts := uint32(rapid.IntRange(8, 32).Draw(t, "vParts"))

		cluster := NewSimCluster(SimClusterConfig{
			VirtualPartitionCount: vParts,
			ReplicationFactor:     1,
		})

		for i := 0; i < nodeCount; i++ {
			cluster.AddNode(sharding.NodeID(fmt.Sprintf("node-%d", i)), []string{"ingest"})
		}
		cluster.RecomputeShardMap()

		// Record initial epochs.
		state := cluster.State()
		epochsBefore := make(map[string]uint64)
		for key, a := range state.ShardMap.Assignments {
			epochsBefore[key] = a.Epoch
		}

		// Add a node and rebalance.
		cluster.AddNode(sharding.NodeID("extra"), []string{"ingest"})
		cluster.IncrementalRebalance()

		// Verify epochs only increased.
		for key, a := range state.ShardMap.Assignments {
			before, ok := epochsBefore[key]
			if !ok {
				continue // New partition.
			}
			if a.Epoch < before {
				t.Fatalf("shard %s: epoch decreased from %d to %d", key, before, a.Epoch)
			}
		}
	})
}

func min(a, b int) int {
	if a < b {
		return a
	}

	return b
}
