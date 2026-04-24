package meta

import (
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// applyCommand is a test helper that encodes and applies a command to the FSM.
func applyCommand(t *testing.T, fsm *MetaFSM, cmdType CommandType, payload interface{}, index uint64) interface{} {
	t.Helper()

	data, err := MarshalPayload(payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}

	cmdData, err := EncodeCommand(&Command{Type: cmdType, Data: data})
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}

	return fsm.Apply(&raft.Log{Index: index, Data: cmdData})
}

func TestFSM_ApplyRegisterNode(t *testing.T) {
	fsm := NewMetaFSM(testLogger())

	resp := applyCommand(t, fsm, CmdRegisterNode, RegisterNodePayload{
		Info: NodeInfo{
			ID:        "node-1",
			Addr:      "10.0.0.1:3100",
			GRPCAddr:  "10.0.0.1:9400",
			Roles:     []string{"meta", "ingest", "query"},
			StartedAt: time.Now(),
			Version:   "1.0.0",
		},
	}, 1)

	if resp != nil {
		t.Fatalf("expected nil response, got: %v", resp)
	}

	state := fsm.State()
	if len(state.Nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(state.Nodes))
	}

	entry := state.Nodes["node-1"]
	if entry == nil {
		t.Fatal("expected node-1 entry")
	}
	if entry.Info.Addr != "10.0.0.1:3100" {
		t.Errorf("expected addr 10.0.0.1:3100, got %s", entry.Info.Addr)
	}
	if entry.State != NodeAlive {
		t.Errorf("expected NodeAlive, got %s", entry.State)
	}
	if state.Version != 1 {
		t.Errorf("expected version 1, got %d", state.Version)
	}
}

func TestFSM_ApplyDeregisterNode(t *testing.T) {
	fsm := NewMetaFSM(testLogger())

	// Register first.
	applyCommand(t, fsm, CmdRegisterNode, RegisterNodePayload{
		Info: NodeInfo{ID: "node-1", Addr: "10.0.0.1:3100"},
	}, 1)

	// Deregister.
	resp := applyCommand(t, fsm, CmdDeregisterNode, DeregisterNodePayload{
		NodeID: "node-1",
	}, 2)

	if resp != nil {
		t.Fatalf("expected nil response, got: %v", resp)
	}

	state := fsm.State()
	if len(state.Nodes) != 0 {
		t.Fatalf("expected 0 nodes, got %d", len(state.Nodes))
	}
	if state.Version != 2 {
		t.Errorf("expected version 2, got %d", state.Version)
	}
}

func TestFSM_ApplyUpdateShardMap(t *testing.T) {
	fsm := NewMetaFSM(testLogger())

	sm := sharding.NewShardMap()
	sm.Epoch = 5
	sm.Assignments["p0"] = &sharding.ShardAssignment{
		ShardID: sharding.ShardID{Partition: 0},
		Primary: "node-1",
		State:   sharding.ShardActive,
		Epoch:   5,
	}

	resp := applyCommand(t, fsm, CmdUpdateShardMap, sm, 1)
	if resp != nil {
		t.Fatalf("expected nil response, got: %v", resp)
	}

	state := fsm.State()
	if state.ShardMap.Epoch != 5 {
		t.Errorf("expected epoch 5, got %d", state.ShardMap.Epoch)
	}
	if len(state.ShardMap.Assignments) != 1 {
		t.Errorf("expected 1 assignment, got %d", len(state.ShardMap.Assignments))
	}
}

func TestFSM_ApplyGrantAndRenewLease(t *testing.T) {
	fsm := NewMetaFSM(testLogger())

	// Grant lease.
	resp := applyCommand(t, fsm, CmdGrantLease, GrantLeasePayload{
		ShardID:       "shard-1",
		HolderID:      "node-1",
		LeaseDuration: 10 * time.Second,
	}, 1)

	if resp != nil {
		t.Fatalf("expected nil response, got: %v", resp)
	}

	state := fsm.State()
	lease := state.Leases["shard-1"]
	if lease == nil {
		t.Fatal("expected lease for shard-1")
	}
	if lease.HolderID != "node-1" {
		t.Errorf("expected holder node-1, got %s", lease.HolderID)
	}
	if lease.Epoch != 1 {
		t.Errorf("expected epoch 1, got %d", lease.Epoch)
	}

	// Renew with correct epoch.
	resp = applyCommand(t, fsm, CmdRenewLease, RenewLeasePayload{
		ShardID:       "shard-1",
		HolderID:      "node-1",
		Epoch:         1,
		LeaseDuration: 20 * time.Second,
	}, 2)

	if resp != nil {
		t.Fatalf("expected nil response, got: %v", resp)
	}

	// Renew with wrong epoch should fail.
	resp = applyCommand(t, fsm, CmdRenewLease, RenewLeasePayload{
		ShardID:       "shard-1",
		HolderID:      "node-1",
		Epoch:         99,
		LeaseDuration: 20 * time.Second,
	}, 3)

	if resp == nil {
		t.Fatal("expected error for epoch mismatch")
	}
}

func TestFSM_ApplyGrantAndReleaseCompaction(t *testing.T) {
	fsm := NewMetaFSM(testLogger())

	// Grant compaction.
	resp := applyCommand(t, fsm, CmdGrantCompaction, GrantCompactionPayload{
		ShardID:        "shard-1",
		AssignedNode:   "node-1",
		CatalogVersion: 10,
		InputParts:     []string{"part-a", "part-b"},
		TargetLevel:    1,
		TTL:            5 * time.Minute,
	}, 1)

	if resp != nil {
		t.Fatalf("expected nil response, got: %v", resp)
	}

	state := fsm.State()
	if len(state.Compaction) != 1 {
		t.Fatalf("expected 1 compaction task, got %d", len(state.Compaction))
	}

	// Granting again for the same shard should fail.
	resp = applyCommand(t, fsm, CmdGrantCompaction, GrantCompactionPayload{
		ShardID:      "shard-1",
		AssignedNode: "node-2",
		TTL:          5 * time.Minute,
	}, 2)

	if resp == nil {
		t.Fatal("expected error for duplicate compaction")
	}

	// Release.
	resp = applyCommand(t, fsm, CmdReleaseCompaction, ReleaseCompactionPayload{
		ShardID: "shard-1",
		Success: true,
	}, 3)

	if resp != nil {
		t.Fatalf("expected nil response, got: %v", resp)
	}

	state = fsm.State()
	if len(state.Compaction) != 0 {
		t.Fatalf("expected 0 compaction tasks, got %d", len(state.Compaction))
	}
}

func TestFSM_SnapshotRestore(t *testing.T) {
	fsm1 := NewMetaFSM(testLogger())

	// Populate state.
	applyCommand(t, fsm1, CmdRegisterNode, RegisterNodePayload{
		Info: NodeInfo{ID: "node-1", Addr: "10.0.0.1:3100", Roles: []string{"meta"}},
	}, 1)
	applyCommand(t, fsm1, CmdRegisterNode, RegisterNodePayload{
		Info: NodeInfo{ID: "node-2", Addr: "10.0.0.2:3100", Roles: []string{"ingest"}},
	}, 2)
	applyCommand(t, fsm1, CmdGrantLease, GrantLeasePayload{
		ShardID:       "shard-1",
		HolderID:      "node-1",
		LeaseDuration: 10 * time.Second,
	}, 3)

	// Take snapshot.
	snap, err := fsm1.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Capture snapshot data via a mock sink.
	sink := &mockSnapshotSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	// Restore into a new FSM.
	fsm2 := NewMetaFSM(testLogger())
	rc := &readCloserFromBytes{data: sink.data}
	if err := fsm2.Restore(rc); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	state := fsm2.State()
	if len(state.Nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(state.Nodes))
	}
	if state.Nodes["node-1"] == nil {
		t.Error("expected node-1")
	}
	if state.Nodes["node-2"] == nil {
		t.Error("expected node-2")
	}
	if state.Leases["shard-1"] == nil {
		t.Error("expected lease for shard-1")
	}
	// Ring should be rebuilt from nodes.
	if state.Ring == nil {
		t.Error("expected ring to be rebuilt")
	}
	if state.Version != fsm1.State().Version {
		t.Errorf("version mismatch: got %d, want %d", state.Version, fsm1.State().Version)
	}
}

func TestFSM_ApplyProposeDrain(t *testing.T) {
	fsm := NewMetaFSM(testLogger())

	// Set up a shard map with an active shard.
	sm := sharding.NewShardMap()
	sm.Epoch = 1
	sm.Assignments["test-shard"] = &sharding.ShardAssignment{
		ShardID:  sharding.ShardID{Index: "logs", Partition: 0},
		Primary:  "node-1",
		Replicas: []sharding.NodeID{"node-2"},
		State:    sharding.ShardActive,
		Epoch:    1,
	}
	applyCommand(t, fsm, CmdUpdateShardMap, sm, 1)

	// Propose drain from the primary.
	resp := applyCommand(t, fsm, CmdProposeDrain, ProposeDrainPayload{
		ShardID: "test-shard",
		NodeID:  "node-1",
	}, 2)

	if resp != nil {
		t.Fatalf("expected nil response, got: %v", resp)
	}

	state := fsm.State()
	a := state.ShardMap.Assignments["test-shard"]
	if a.State != sharding.ShardDraining {
		t.Errorf("expected ShardDraining, got %s", a.State)
	}
}

func TestFSM_ApplyProposeDrain_NotPrimary(t *testing.T) {
	fsm := NewMetaFSM(testLogger())

	sm := sharding.NewShardMap()
	sm.Epoch = 1
	sm.Assignments["test-shard"] = &sharding.ShardAssignment{
		Primary: "node-1",
		State:   sharding.ShardActive,
	}
	applyCommand(t, fsm, CmdUpdateShardMap, sm, 1)

	// Propose drain from a non-primary node should fail.
	resp := applyCommand(t, fsm, CmdProposeDrain, ProposeDrainPayload{
		ShardID: "test-shard",
		NodeID:  "node-2",
	}, 2)

	if resp == nil {
		t.Fatal("expected error for non-primary drain proposal")
	}
}

func TestFSM_ApplyCompleteDrain(t *testing.T) {
	fsm := NewMetaFSM(testLogger())

	sm := sharding.NewShardMap()
	sm.Epoch = 1
	sm.Assignments["test-shard"] = &sharding.ShardAssignment{
		Primary:  "node-1",
		Replicas: []sharding.NodeID{"node-2", "node-3"},
		State:    sharding.ShardDraining,
		Epoch:    1,
	}
	applyCommand(t, fsm, CmdUpdateShardMap, sm, 1)

	// Complete drain with node-2 as new primary.
	resp := applyCommand(t, fsm, CmdCompleteDrain, CompleteDrainPayload{
		ShardID:      "test-shard",
		NodeID:       "node-1",
		NewPrimaryID: "node-2",
	}, 2)

	if resp != nil {
		t.Fatalf("expected nil response, got: %v", resp)
	}

	state := fsm.State()
	a := state.ShardMap.Assignments["test-shard"]
	if a.State != sharding.ShardActive {
		t.Errorf("expected ShardActive, got %s", a.State)
	}
	if a.Primary != "node-2" {
		t.Errorf("expected node-2 as primary, got %s", a.Primary)
	}
	if a.Epoch != 2 {
		t.Errorf("expected epoch 2, got %d", a.Epoch)
	}
	// node-1 (old primary) and node-2 (new primary) should be removed from replicas.
	for _, r := range a.Replicas {
		if r == "node-1" || r == "node-2" {
			t.Errorf("old primary or new primary should not be in replicas: %v", a.Replicas)
		}
	}
	if len(a.Replicas) != 1 || a.Replicas[0] != "node-3" {
		t.Errorf("expected replicas [node-3], got %v", a.Replicas)
	}
}

func TestFSM_ApplyCompleteDrain_NotDraining(t *testing.T) {
	fsm := NewMetaFSM(testLogger())

	sm := sharding.NewShardMap()
	sm.Epoch = 1
	sm.Assignments["test-shard"] = &sharding.ShardAssignment{
		Primary: "node-1",
		State:   sharding.ShardActive, // Not draining
	}
	applyCommand(t, fsm, CmdUpdateShardMap, sm, 1)

	resp := applyCommand(t, fsm, CmdCompleteDrain, CompleteDrainPayload{
		ShardID:      "test-shard",
		NodeID:       "node-1",
		NewPrimaryID: "node-2",
	}, 2)

	if resp == nil {
		t.Fatal("expected error for completing drain on non-draining shard")
	}
}

func TestFSM_ApplyUpdateISR(t *testing.T) {
	fsm := NewMetaFSM(testLogger())

	sm := sharding.NewShardMap()
	sm.Epoch = 1
	sm.Assignments["test-shard"] = &sharding.ShardAssignment{
		Primary:  "node-1",
		Replicas: []sharding.NodeID{"node-2", "node-3"},
		State:    sharding.ShardActive,
	}
	applyCommand(t, fsm, CmdUpdateShardMap, sm, 1)

	resp := applyCommand(t, fsm, CmdUpdateISR, UpdateISRPayload{
		ShardID: "test-shard",
		Members: []sharding.NodeID{"node-2"},
	}, 2)

	if resp != nil {
		t.Fatalf("expected nil response, got: %v", resp)
	}

	state := fsm.State()
	a := state.ShardMap.Assignments["test-shard"]
	if len(a.Replicas) != 1 || a.Replicas[0] != "node-2" {
		t.Errorf("expected ISR [node-2], got %v", a.Replicas)
	}
}

func TestFSM_ApplyUpdateISR_ShardNotFound(t *testing.T) {
	fsm := NewMetaFSM(testLogger())

	resp := applyCommand(t, fsm, CmdUpdateISR, UpdateISRPayload{
		ShardID: "nonexistent",
		Members: []sharding.NodeID{"node-1"},
	}, 1)

	if resp == nil {
		t.Fatal("expected error for unknown shard")
	}
}

func TestFSM_UnknownCommand(t *testing.T) {
	fsm := NewMetaFSM(testLogger())

	cmdData, err := EncodeCommand(&Command{Type: 255, Data: nil})
	if err != nil {
		t.Fatalf("EncodeCommand: %v", err)
	}

	resp := fsm.Apply(&raft.Log{Index: 1, Data: cmdData})
	if resp == nil {
		t.Fatal("expected error for unknown command type")
	}
}

func TestFSM_Version(t *testing.T) {
	fsm := NewMetaFSM(testLogger())

	if fsm.Version() != 0 {
		t.Errorf("expected initial version 0, got %d", fsm.Version())
	}

	applyCommand(t, fsm, CmdRegisterNode, RegisterNodePayload{
		Info: NodeInfo{ID: "node-1"},
	}, 1)

	if fsm.Version() != 1 {
		t.Errorf("expected version 1 after register, got %d", fsm.Version())
	}
}

// mockSnapshotSink captures written data for testing.
type mockSnapshotSink struct {
	data []byte
}

func (s *mockSnapshotSink) Write(p []byte) (int, error) {
	s.data = append(s.data, p...)
	return len(p), nil
}

func (s *mockSnapshotSink) Close() error  { return nil }
func (s *mockSnapshotSink) Cancel() error { return nil }
func (s *mockSnapshotSink) ID() string    { return "mock" }

// readCloserFromBytes wraps a byte slice as an io.ReadCloser.
type readCloserFromBytes struct {
	data []byte
	pos  int
}

func (r *readCloserFromBytes) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func (r *readCloserFromBytes) Close() error { return nil }
