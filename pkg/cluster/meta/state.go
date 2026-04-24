package meta

import (
	"fmt"
	"time"

	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

// MetaState holds all cluster metadata managed by the Raft FSM.
// All mutations are applied through typed apply methods, ensuring
// that state changes are deterministic and replayable.
type MetaState struct {
	Nodes      map[sharding.NodeID]*NodeEntry `msgpack:"nodes"`
	ShardMap   *sharding.ShardMap             `msgpack:"shard_map"`
	Leases     map[string]*ShardLease         `msgpack:"leases"`     // key: ShardID.String()
	Compaction map[string]*CompactionTask     `msgpack:"compaction"` // key: ShardID.String()
	Ring       *sharding.HashRing             `msgpack:"-"`          // rebuilt from Nodes, not serialized
	Version    uint64                         `msgpack:"version"`

	// Distributed subsystem state (Phase 5).
	FieldCatalog map[string]*GlobalFieldInfo  `msgpack:"field_catalog"` // key: field name
	Sources      map[string]*GlobalSourceInfo `msgpack:"sources"`       // key: source name
	AlertAssign  map[string]*AlertAssignment  `msgpack:"alert_assign"`  // key: alert ID
	Views        map[string]*GlobalViewInfo   `msgpack:"views"`         // key: view name

	// Rebalancing and splitting state (Phase 6).
	Splits map[uint32]*SplitInfo `msgpack:"splits"` // key: parent partition
}

// SplitInfo records a partition split: parent -> two children via hash-bit subdivision.
type SplitInfo struct {
	ParentPartition uint32 `msgpack:"parent"`
	ChildA          uint32 `msgpack:"child_a"`
	ChildB          uint32 `msgpack:"child_b"`
	SplitBit        uint8  `msgpack:"split_bit"`
}

// NewMetaState creates an empty MetaState.
func NewMetaState() *MetaState {
	return &MetaState{
		Nodes:        make(map[sharding.NodeID]*NodeEntry),
		ShardMap:     sharding.NewShardMap(),
		Leases:       make(map[string]*ShardLease),
		Compaction:   make(map[string]*CompactionTask),
		Ring:         sharding.NewHashRing(128),
		FieldCatalog: make(map[string]*GlobalFieldInfo),
		Sources:      make(map[string]*GlobalSourceInfo),
		AlertAssign:  make(map[string]*AlertAssignment),
		Views:        make(map[string]*GlobalViewInfo),
		Splits:       make(map[uint32]*SplitInfo),
	}
}

// RegisterNodePayload is the payload for CmdRegisterNode.
type RegisterNodePayload struct {
	Info NodeInfo `msgpack:"info"`
}

// DeregisterNodePayload is the payload for CmdDeregisterNode.
type DeregisterNodePayload struct {
	NodeID sharding.NodeID `msgpack:"node_id"`
}

// applyRegisterNode adds or updates a node in the cluster.
func (s *MetaState) applyRegisterNode(payload []byte) error {
	var p RegisterNodePayload
	if err := UnmarshalPayload(payload, &p); err != nil {
		return fmt.Errorf("meta.applyRegisterNode: %w", err)
	}

	entry, exists := s.Nodes[p.Info.ID]
	if exists {
		entry.Info = p.Info
		entry.State = NodeAlive
		entry.LastHeartbeat = time.Now()
	} else {
		s.Nodes[p.Info.ID] = &NodeEntry{
			Info:          p.Info,
			State:         NodeAlive,
			LastHeartbeat: time.Now(),
		}
		s.Ring.AddNode(p.Info.ID)
	}

	s.Version++

	return nil
}

// applyDeregisterNode removes a node from the cluster.
func (s *MetaState) applyDeregisterNode(payload []byte) error {
	var p DeregisterNodePayload
	if err := UnmarshalPayload(payload, &p); err != nil {
		return fmt.Errorf("meta.applyDeregisterNode: %w", err)
	}

	delete(s.Nodes, p.NodeID)
	s.Ring.RemoveNode(p.NodeID)
	s.Version++

	return nil
}

// applyUpdateShardMap replaces the shard map.
func (s *MetaState) applyUpdateShardMap(payload []byte) error {
	var sm sharding.ShardMap
	if err := UnmarshalPayload(payload, &sm); err != nil {
		return fmt.Errorf("meta.applyUpdateShardMap: %w", err)
	}

	s.ShardMap = &sm
	s.Version++

	return nil
}

// ProposeDrainPayload is the payload for CmdProposeDrain.
type ProposeDrainPayload struct {
	ShardID string          `msgpack:"shard_id"`
	NodeID  sharding.NodeID `msgpack:"node_id"`
}

// applyProposeDrain transitions a shard to ShardDraining state.
// The shard must exist and be in ShardActive state, and the requesting
// node must be the current primary.
func (s *MetaState) applyProposeDrain(payload []byte) error {
	var p ProposeDrainPayload
	if err := UnmarshalPayload(payload, &p); err != nil {
		return fmt.Errorf("meta.applyProposeDrain: %w", err)
	}

	a, ok := s.ShardMap.Assignments[p.ShardID]
	if !ok {
		return fmt.Errorf("meta.applyProposeDrain: shard %q not found", p.ShardID)
	}
	if a.State != sharding.ShardActive {
		return fmt.Errorf("meta.applyProposeDrain: shard %q is %s, expected active", p.ShardID, a.State)
	}
	if a.Primary != p.NodeID {
		return fmt.Errorf("meta.applyProposeDrain: node %q is not primary of shard %q (primary is %q)", p.NodeID, p.ShardID, a.Primary)
	}

	a.State = sharding.ShardDraining
	s.Version++

	return nil
}

// CompleteDrainPayload is the payload for CmdCompleteDrain.
type CompleteDrainPayload struct {
	ShardID      string          `msgpack:"shard_id"`
	NodeID       sharding.NodeID `msgpack:"node_id"`
	NewPrimaryID sharding.NodeID `msgpack:"new_primary_id"`
}

// applyCompleteDrain completes a drain by assigning a new primary and
// transitioning back to ShardActive. The old primary is removed from
// the replica set.
func (s *MetaState) applyCompleteDrain(payload []byte) error {
	var p CompleteDrainPayload
	if err := UnmarshalPayload(payload, &p); err != nil {
		return fmt.Errorf("meta.applyCompleteDrain: %w", err)
	}

	a, ok := s.ShardMap.Assignments[p.ShardID]
	if !ok {
		return fmt.Errorf("meta.applyCompleteDrain: shard %q not found", p.ShardID)
	}
	if a.State != sharding.ShardDraining {
		return fmt.Errorf("meta.applyCompleteDrain: shard %q is %s, expected draining", p.ShardID, a.State)
	}

	a.Primary = p.NewPrimaryID
	a.State = sharding.ShardActive
	a.Epoch++

	// Remove old node from replicas if present.
	filtered := make([]sharding.NodeID, 0, len(a.Replicas))
	for _, r := range a.Replicas {
		if r != p.NodeID && r != p.NewPrimaryID {
			filtered = append(filtered, r)
		}
	}
	a.Replicas = filtered

	s.Version++

	return nil
}

// UpdateISRPayload is the payload for CmdUpdateISR.
type UpdateISRPayload struct {
	ShardID string            `msgpack:"shard_id"`
	Members []sharding.NodeID `msgpack:"members"`
}

// ProposeSplitPayload is the payload for CmdProposeSplit.
type ProposeSplitPayload struct {
	ParentPartition uint32 `msgpack:"parent"`
	ChildA          uint32 `msgpack:"child_a"`
	ChildB          uint32 `msgpack:"child_b"`
	SplitBit        uint8  `msgpack:"split_bit"`
}

// CompleteSplitPayload is the payload for CmdCompleteSplit.
type CompleteSplitPayload struct {
	ParentPartition uint32 `msgpack:"parent"`
}

// applyProposeSplit transitions the parent partition to ShardSplitting and
// records the split info. Child partitions are created in ShardMigrating
// state (they become active after CmdCompleteSplit).
func (s *MetaState) applyProposeSplit(payload []byte) error {
	var p ProposeSplitPayload
	if err := UnmarshalPayload(payload, &p); err != nil {
		return fmt.Errorf("meta.applyProposeSplit: %w", err)
	}

	parentKey := fmt.Sprintf("p%d", p.ParentPartition)
	parent, ok := s.ShardMap.Assignments[parentKey]
	if !ok {
		return fmt.Errorf("meta.applyProposeSplit: parent partition %q not found", parentKey)
	}
	if parent.State != sharding.ShardActive {
		return fmt.Errorf("meta.applyProposeSplit: parent %q is %s, expected active", parentKey, parent.State)
	}

	if _, exists := s.Splits[p.ParentPartition]; exists {
		return fmt.Errorf("meta.applyProposeSplit: partition %d already splitting", p.ParentPartition)
	}

	// Record split info.
	s.Splits[p.ParentPartition] = &SplitInfo{
		ParentPartition: p.ParentPartition,
		ChildA:          p.ChildA,
		ChildB:          p.ChildB,
		SplitBit:        p.SplitBit,
	}

	// Mark parent as splitting.
	parent.State = sharding.ShardSplitting
	parent.Epoch++

	// Create child assignments in migrating state with same primary/replicas.
	childAKey := fmt.Sprintf("p%d", p.ChildA)
	childBKey := fmt.Sprintf("p%d", p.ChildB)

	replicasCopy := func() []sharding.NodeID {
		c := make([]sharding.NodeID, len(parent.Replicas))
		copy(c, parent.Replicas)
		return c
	}

	s.ShardMap.Assignments[childAKey] = &sharding.ShardAssignment{
		ShardID:  sharding.ShardID{Partition: p.ChildA},
		Primary:  parent.Primary,
		Replicas: replicasCopy(),
		State:    sharding.ShardMigrating,
		Epoch:    s.ShardMap.Epoch,
	}
	s.ShardMap.Assignments[childBKey] = &sharding.ShardAssignment{
		ShardID:  sharding.ShardID{Partition: p.ChildB},
		Primary:  parent.Primary,
		Replicas: replicasCopy(),
		State:    sharding.ShardMigrating,
		Epoch:    s.ShardMap.Epoch,
	}

	s.ShardMap.Epoch++
	s.Version++

	return nil
}

// applyCompleteSplit activates child partitions and removes the parent.
func (s *MetaState) applyCompleteSplit(payload []byte) error {
	var p CompleteSplitPayload
	if err := UnmarshalPayload(payload, &p); err != nil {
		return fmt.Errorf("meta.applyCompleteSplit: %w", err)
	}

	split, ok := s.Splits[p.ParentPartition]
	if !ok {
		return fmt.Errorf("meta.applyCompleteSplit: no split info for partition %d", p.ParentPartition)
	}

	// Activate children.
	childAKey := fmt.Sprintf("p%d", split.ChildA)
	childBKey := fmt.Sprintf("p%d", split.ChildB)

	if a, ok := s.ShardMap.Assignments[childAKey]; ok {
		a.State = sharding.ShardActive
		a.Epoch++
	}
	if b, ok := s.ShardMap.Assignments[childBKey]; ok {
		b.State = sharding.ShardActive
		b.Epoch++
	}

	// Remove parent assignment and split record.
	parentKey := fmt.Sprintf("p%d", p.ParentPartition)
	delete(s.ShardMap.Assignments, parentKey)
	delete(s.Splits, p.ParentPartition)

	s.ShardMap.Epoch++
	s.Version++

	return nil
}

// applyUpdateISR updates the ISR (in-sync replica) membership for a shard.
// The ISR is stored in the Replicas field of the ShardAssignment — only
// replicas that are confirmed in-sync are listed.
func (s *MetaState) applyUpdateISR(payload []byte) error {
	var p UpdateISRPayload
	if err := UnmarshalPayload(payload, &p); err != nil {
		return fmt.Errorf("meta.applyUpdateISR: %w", err)
	}

	a, ok := s.ShardMap.Assignments[p.ShardID]
	if !ok {
		return fmt.Errorf("meta.applyUpdateISR: shard %q not found", p.ShardID)
	}

	a.Replicas = make([]sharding.NodeID, len(p.Members))
	copy(a.Replicas, p.Members)
	s.Version++

	return nil
}
