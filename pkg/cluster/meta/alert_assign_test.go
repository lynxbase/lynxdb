package meta

import (
	"sort"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

func TestRendezvousAssign_Empty(t *testing.T) {
	result := RendezvousAssign("alert-1", nil)
	if result != "" {
		t.Errorf("expected empty, got %q", result)
	}
}

func TestRendezvousAssign_Single(t *testing.T) {
	result := RendezvousAssign("alert-1", []sharding.NodeID{"node-1"})
	if result != "node-1" {
		t.Errorf("expected node-1, got %q", result)
	}
}

func TestRendezvousAssign_Deterministic(t *testing.T) {
	nodes := []sharding.NodeID{"node-1", "node-2", "node-3"}

	// Same input should always produce same output.
	first := RendezvousAssign("alert-42", nodes)
	for i := 0; i < 100; i++ {
		result := RendezvousAssign("alert-42", nodes)
		if result != first {
			t.Fatalf("non-deterministic: got %q then %q", first, result)
		}
	}
}

func TestRendezvousAssign_Distribution(t *testing.T) {
	nodes := []sharding.NodeID{"node-1", "node-2", "node-3"}
	counts := make(map[sharding.NodeID]int)

	// Distribute 300 alerts across 3 nodes — should be roughly even.
	for i := 0; i < 300; i++ {
		alertID := "alert-" + string(rune(i))
		assigned := RendezvousAssign(alertID, nodes)
		counts[assigned]++
	}

	for _, node := range nodes {
		count := counts[node]
		// Each node should get at least 50 alerts (expected ~100).
		if count < 50 {
			t.Errorf("node %q got only %d alerts (expected ~100)", node, count)
		}
	}
}

func TestRendezvousAssign_MinimalDisruption(t *testing.T) {
	nodes3 := []sharding.NodeID{"node-1", "node-2", "node-3"}
	nodes2 := []sharding.NodeID{"node-1", "node-2"} // node-3 removed

	moved := 0
	total := 300
	for i := 0; i < total; i++ {
		alertID := "alert-" + string(rune(i))
		before := RendezvousAssign(alertID, nodes3)
		after := RendezvousAssign(alertID, nodes2)
		if before != after {
			moved++
		}
	}

	// Only alerts previously on node-3 should move (~100 of 300 = ~33%).
	// Allow generous bounds: 10%-60%.
	pct := float64(moved) / float64(total) * 100
	if pct < 10 || pct > 60 {
		t.Errorf("moved %d/%d alerts (%.1f%%), expected ~33%%", moved, total, pct)
	}
}

func TestApplyAssignAlert(t *testing.T) {
	fsm := NewMetaFSM(testLogger())

	resp := applyCommand(t, fsm, CmdAssignAlert, AssignAlertPayload{
		AlertID:    "alert-1",
		QueryNodes: []sharding.NodeID{"node-1", "node-2", "node-3"},
	}, 1)

	if resp != nil {
		t.Fatalf("expected nil response, got: %v", resp)
	}

	state := fsm.State()
	aa := state.AlertAssign["alert-1"]
	if aa == nil {
		t.Fatal("expected alert assignment")
	}
	if aa.AlertID != "alert-1" {
		t.Errorf("expected alert-1, got %q", aa.AlertID)
	}
	if aa.AssignedNode == "" {
		t.Error("expected non-empty assigned node")
	}
	if aa.Version != 1 {
		t.Errorf("expected version 1, got %d", aa.Version)
	}
}

func TestApplyAssignAlert_Unassign(t *testing.T) {
	fsm := NewMetaFSM(testLogger())

	// Assign first.
	applyCommand(t, fsm, CmdAssignAlert, AssignAlertPayload{
		AlertID:    "alert-1",
		QueryNodes: []sharding.NodeID{"node-1"},
	}, 1)

	// Unassign.
	applyCommand(t, fsm, CmdAssignAlert, AssignAlertPayload{
		AlertID:    "alert-1",
		QueryNodes: nil,
	}, 2)

	state := fsm.State()
	if _, ok := state.AlertAssign["alert-1"]; ok {
		t.Error("expected alert to be unassigned")
	}
}

func TestApplyUpdateAlertFired(t *testing.T) {
	fsm := NewMetaFSM(testLogger())

	// Assign first.
	applyCommand(t, fsm, CmdAssignAlert, AssignAlertPayload{
		AlertID:    "alert-1",
		QueryNodes: []sharding.NodeID{"node-1"},
	}, 1)

	now := time.Now()
	resp := applyCommand(t, fsm, CmdUpdateAlertFired, UpdateAlertFiredPayload{
		AlertID: "alert-1",
		FiredAt: now,
	}, 2)

	if resp != nil {
		t.Fatalf("expected nil response, got: %v", resp)
	}

	state := fsm.State()
	aa := state.AlertAssign["alert-1"]
	if aa.LastFiredAt == nil {
		t.Fatal("expected LastFiredAt to be set")
	}
	if !aa.LastFiredAt.Equal(now) {
		t.Errorf("expected fired_at %v, got %v", now, *aa.LastFiredAt)
	}
}

func TestApplyUpdateAlertFired_NotAssigned(t *testing.T) {
	fsm := NewMetaFSM(testLogger())

	resp := applyCommand(t, fsm, CmdUpdateAlertFired, UpdateAlertFiredPayload{
		AlertID: "nonexistent",
		FiredAt: time.Now(),
	}, 1)

	if resp == nil {
		t.Fatal("expected error for unassigned alert")
	}
}

func TestReassignAlertsFromDeadNode(t *testing.T) {
	state := NewMetaState()

	// Register 3 query nodes.
	for _, id := range []sharding.NodeID{"q-1", "q-2", "q-3"} {
		state.Nodes[id] = &NodeEntry{
			Info:          NodeInfo{ID: id, Roles: []string{"query"}},
			State:         NodeAlive,
			LastHeartbeat: time.Now(),
		}
	}

	// Assign 9 alerts distributed across nodes.
	for i := 0; i < 9; i++ {
		alertID := "alert-" + string(rune('a'+i))
		nodes := []sharding.NodeID{"q-1", "q-2", "q-3"}
		assigned := RendezvousAssign(alertID, nodes)
		state.AlertAssign[alertID] = &AlertAssignment{
			AlertID:      alertID,
			AssignedNode: assigned,
			Version:      1,
		}
	}

	// Count assignments before.
	beforeCounts := make(map[sharding.NodeID]int)
	for _, aa := range state.AlertAssign {
		beforeCounts[aa.AssignedNode]++
	}

	// Kill q-3.
	state.Nodes["q-3"].State = NodeDead
	reassigned := state.ReassignAlertsFromDeadNode("q-3")

	// All alerts on q-3 should have moved.
	if len(reassigned) != beforeCounts["q-3"] {
		t.Errorf("expected %d reassigned, got %d", beforeCounts["q-3"], len(reassigned))
	}

	// No alert should be assigned to q-3 anymore.
	for alertID, aa := range state.AlertAssign {
		if aa.AssignedNode == "q-3" {
			t.Errorf("alert %q still assigned to dead node q-3", alertID)
		}
	}

	// All alerts should still be assigned to a live node.
	liveNodes := map[sharding.NodeID]bool{"q-1": true, "q-2": true}
	for alertID, aa := range state.AlertAssign {
		if !liveNodes[aa.AssignedNode] {
			t.Errorf("alert %q assigned to non-live node %q", alertID, aa.AssignedNode)
		}
	}

	// Verify reassigned alerts have incremented version.
	sort.Strings(reassigned)
	for _, alertID := range reassigned {
		aa := state.AlertAssign[alertID]
		if aa.Version < 2 {
			t.Errorf("alert %q version should be >= 2 after reassign, got %d", alertID, aa.Version)
		}
	}
}
