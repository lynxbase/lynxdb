package meta

import (
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
	"github.com/vmihailenco/msgpack/v5"
)

// MetaFSM implements hashicorp/raft.FSM for the cluster metadata store.
// All state mutations flow through Apply(), ensuring they are linearizable
// via Raft consensus.
type MetaFSM struct {
	mu     sync.RWMutex
	state  *MetaState
	logger *slog.Logger
}

// NewMetaFSM creates a new FSM with empty state.
func NewMetaFSM(logger *slog.Logger) *MetaFSM {
	return &MetaFSM{
		state:  NewMetaState(),
		logger: logger,
	}
}

// Apply implements raft.FSM. It decodes the log entry as a Command and
// dispatches to the appropriate state mutation method.
func (f *MetaFSM) Apply(log *raft.Log) interface{} {
	cmd, err := DecodeCommand(log.Data)
	if err != nil {
		f.logger.Error("FSM apply: decode command failed",
			"index", log.Index,
			"error", err)

		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch cmd.Type {
	case CmdRegisterNode:
		err = f.state.applyRegisterNode(cmd.Data)
	case CmdDeregisterNode:
		err = f.state.applyDeregisterNode(cmd.Data)
	case CmdUpdateShardMap:
		err = f.state.applyUpdateShardMap(cmd.Data)
	case CmdGrantLease:
		err = f.state.applyGrantLease(cmd.Data)
	case CmdRenewLease:
		err = f.state.applyRenewLease(cmd.Data)
	case CmdGrantCompaction:
		err = f.state.applyGrantCompaction(cmd.Data)
	case CmdReleaseCompaction:
		err = f.state.applyReleaseCompaction(cmd.Data)
	case CmdProposeDrain:
		err = f.state.applyProposeDrain(cmd.Data)
	case CmdCompleteDrain:
		err = f.state.applyCompleteDrain(cmd.Data)
	case CmdUpdateISR:
		err = f.state.applyUpdateISR(cmd.Data)
	case CmdUpdateFieldCatalog:
		err = f.state.applyUpdateFieldCatalog(cmd.Data)
	case CmdUpdateSourceRegistry:
		err = f.state.applyUpdateSourceRegistry(cmd.Data)
	case CmdRegisterView:
		err = f.state.applyRegisterView(cmd.Data)
	case CmdUnregisterView:
		err = f.state.applyUnregisterView(cmd.Data)
	case CmdApplyRebalance:
		err = f.state.applyRebalance(cmd.Data)
	case CmdProposeSplit:
		err = f.state.applyProposeSplit(cmd.Data)
	case CmdCompleteSplit:
		err = f.state.applyCompleteSplit(cmd.Data)
	default:
		err = fmt.Errorf("meta.FSM.Apply: unknown command type %d", cmd.Type)
	}

	if err != nil {
		f.logger.Error("FSM apply failed",
			"command", cmd.Type.String(),
			"index", log.Index,
			"error", err)
	}

	return err
}

// Snapshot implements raft.FSM. It creates a serializable snapshot of the
// current state for Raft snapshotting.
func (f *MetaFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Serialize the entire state to msgpack.
	data, err := msgpack.Marshal(f.state)
	if err != nil {
		return nil, fmt.Errorf("meta.FSM.Snapshot: %w", err)
	}

	return &fsmSnapshot{data: data}, nil
}

// Restore implements raft.FSM. It replaces the current state with a
// previously snapshotted version.
func (f *MetaFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	// Limit snapshot reads to 256MB as defense against corrupted snapshots.
	const maxSnapshotSize = 256 << 20
	data, err := io.ReadAll(io.LimitReader(rc, maxSnapshotSize+1))
	if err != nil {
		return fmt.Errorf("meta.FSM.Restore: read: %w", err)
	}
	if len(data) > maxSnapshotSize {
		return fmt.Errorf("meta.FSM.Restore: snapshot exceeds %dMB limit", maxSnapshotSize>>20)
	}

	var state MetaState
	if err := msgpack.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("meta.FSM.Restore: unmarshal: %w", err)
	}

	// Rebuild the hash ring from the node list (not serialized).
	state.Ring = rebuildRing(&state)

	// Ensure new maps are initialized when restoring from older snapshots
	// that predate Phase 5 distributed subsystems.
	if state.FieldCatalog == nil {
		state.FieldCatalog = make(map[string]*GlobalFieldInfo)
	}
	if state.Sources == nil {
		state.Sources = make(map[string]*GlobalSourceInfo)
	}
	if state.Views == nil {
		state.Views = make(map[string]*GlobalViewInfo)
	}
	if state.Splits == nil {
		state.Splits = make(map[uint32]*SplitInfo)
	}

	f.mu.Lock()
	f.state = &state
	f.mu.Unlock()

	f.logger.Info("FSM restored from snapshot",
		"version", state.Version,
		"nodes", len(state.Nodes),
		"leases", len(state.Leases))

	return nil
}

// State returns a read-locked reference to the current state.
// The caller must NOT modify the returned state.
func (f *MetaFSM) State() *MetaState {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.state
}

// Version returns the current state version (monotonically increasing).
func (f *MetaFSM) Version() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.state.Version
}

// rebuildRing reconstructs the hash ring from the registered nodes.
func rebuildRing(state *MetaState) *sharding.HashRing {
	ring := sharding.NewHashRing(128)
	for id := range state.Nodes {
		ring.AddNode(id)
	}

	return ring
}

// fsmSnapshot holds a serialized copy of the FSM state.
type fsmSnapshot struct {
	data []byte
}

// Persist implements raft.FSMSnapshot.
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		sink.Cancel()

		return fmt.Errorf("meta.fsmSnapshot.Persist: %w", err)
	}

	return sink.Close()
}

// Release implements raft.FSMSnapshot.
func (s *fsmSnapshot) Release() {}
