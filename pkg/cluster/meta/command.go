// Package meta implements the Raft-based metadata store for LynxDB clustering.
// It manages node registration, shard map, leases, and compaction coordination.
package meta

import (
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

// CommandType identifies the type of a Raft FSM command.
type CommandType uint8

const (
	// CmdRegisterNode registers a new node in the cluster.
	CmdRegisterNode CommandType = iota + 1
	// CmdDeregisterNode removes a node from the cluster.
	CmdDeregisterNode
	// CmdUpdateShardMap replaces the shard map with a new version.
	CmdUpdateShardMap
	// CmdGrantLease grants a shard lease to a node.
	CmdGrantLease
	// CmdRenewLease extends an existing shard lease.
	CmdRenewLease
	// CmdGrantCompaction assigns a compaction task to a node.
	CmdGrantCompaction
	// CmdReleaseCompaction releases a compaction lock.
	CmdReleaseCompaction
	// CmdProposeDrain transitions a shard to draining state.
	CmdProposeDrain
	// CmdCompleteDrain completes a shard drain and assigns a new primary.
	CmdCompleteDrain
	// CmdUpdateISR updates the ISR membership for a shard.
	CmdUpdateISR
	// CmdUpdateFieldCatalog merges a field catalog delta from an ingest node.
	CmdUpdateFieldCatalog
	// CmdUpdateSourceRegistry merges a source set delta from an ingest node.
	CmdUpdateSourceRegistry
	// CmdRegisterView registers or updates a materialized view definition in the cluster.
	CmdRegisterView
	// CmdUnregisterView removes a materialized view definition from the cluster.
	CmdUnregisterView
	// CmdApplyRebalance applies a pre-computed incremental rebalance plan.
	CmdApplyRebalance
	// CmdProposeSplit proposes splitting a hot partition into two children.
	CmdProposeSplit
	// CmdCompleteSplit completes a split and activates child partitions.
	CmdCompleteSplit
)

// String returns the human-readable name of the command type.
func (ct CommandType) String() string {
	switch ct {
	case CmdRegisterNode:
		return "RegisterNode"
	case CmdDeregisterNode:
		return "DeregisterNode"
	case CmdUpdateShardMap:
		return "UpdateShardMap"
	case CmdGrantLease:
		return "GrantLease"
	case CmdRenewLease:
		return "RenewLease"
	case CmdGrantCompaction:
		return "GrantCompaction"
	case CmdReleaseCompaction:
		return "ReleaseCompaction"
	case CmdProposeDrain:
		return "ProposeDrain"
	case CmdCompleteDrain:
		return "CompleteDrain"
	case CmdUpdateISR:
		return "UpdateISR"
	case CmdUpdateFieldCatalog:
		return "UpdateFieldCatalog"
	case CmdUpdateSourceRegistry:
		return "UpdateSourceRegistry"
	case CmdRegisterView:
		return "RegisterView"
	case CmdUnregisterView:
		return "UnregisterView"
	case CmdApplyRebalance:
		return "ApplyRebalance"
	case CmdProposeSplit:
		return "ProposeSplit"
	case CmdCompleteSplit:
		return "CompleteSplit"
	default:
		return fmt.Sprintf("unknown(%d)", int(ct))
	}
}

// Command is a Raft FSM command, serialized as msgpack.
type Command struct {
	Type CommandType `msgpack:"type"`
	Data []byte      `msgpack:"data"`
}

// EncodeCommand serializes a Command to bytes.
func EncodeCommand(cmd *Command) ([]byte, error) {
	data, err := msgpack.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("meta.EncodeCommand: %w", err)
	}

	return data, nil
}

// DecodeCommand deserializes a Command from bytes.
func DecodeCommand(data []byte) (*Command, error) {
	var cmd Command
	if err := msgpack.Unmarshal(data, &cmd); err != nil {
		return nil, fmt.Errorf("meta.DecodeCommand: %w", err)
	}

	return &cmd, nil
}

// MarshalPayload serializes an arbitrary payload for embedding in a Command.
func MarshalPayload(v interface{}) ([]byte, error) {
	return msgpack.Marshal(v)
}

// UnmarshalPayload deserializes a payload from Command.Data.
func UnmarshalPayload(data []byte, v interface{}) error {
	return msgpack.Unmarshal(data, v)
}
