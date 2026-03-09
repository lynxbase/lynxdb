package sharding

import "sync"

// SplitInfo records a partition split: parent -> two children via hash-bit
// subdivision. Examining one additional bit of the hash value routes events
// to either child_a (bit=0) or child_b (bit=1). No rehashing of existing
// data is required — old segments remain readable under the parent prefix.
type SplitInfo struct {
	ParentPartition uint32 `msgpack:"parent"`
	ChildA          uint32 `msgpack:"child_a"`
	ChildB          uint32 `msgpack:"child_b"`
	SplitBit        uint8  `msgpack:"split_bit"`
}

// SplitRegistry tracks active partition splits and provides routing
// resolution. When a partition has been split, Resolve() returns the
// correct child partition for a given hash value.
//
// Thread-safe: all methods acquire appropriate locks.
type SplitRegistry struct {
	splits map[uint32]*SplitInfo
	mu     sync.RWMutex
}

// NewSplitRegistry creates an empty split registry.
func NewSplitRegistry() *SplitRegistry {
	return &SplitRegistry{
		splits: make(map[uint32]*SplitInfo),
	}
}

// Register records a partition split. Overwrites any existing split
// for the same parent partition.
func (sr *SplitRegistry) Register(info *SplitInfo) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	sr.splits[info.ParentPartition] = info
}

// Unregister removes a split record for the given parent partition.
func (sr *SplitRegistry) Unregister(parentPartition uint32) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	delete(sr.splits, parentPartition)
}

// Resolve maps a partition and hash value to the effective partition,
// traversing the split tree. If the partition has been split, the
// split bit of the hash value determines which child receives the event.
//
// This is O(depth) where depth is the number of chained splits
// (typically 1-2, never more than ~20 for 1M partitions).
func (sr *SplitRegistry) Resolve(partition uint32, hashValue uint64) uint32 {
	sr.mu.RLock()
	defer sr.mu.RUnlock()

	// Walk the split chain until we reach a leaf partition.
	for {
		info, ok := sr.splits[partition]
		if !ok {
			return partition
		}

		if (hashValue>>info.SplitBit)&1 == 0 {
			partition = info.ChildA
		} else {
			partition = info.ChildB
		}
	}
}

// Get returns the split info for a parent partition, or nil if not split.
func (sr *SplitRegistry) Get(parentPartition uint32) *SplitInfo {
	sr.mu.RLock()
	defer sr.mu.RUnlock()

	return sr.splits[parentPartition]
}

// All returns a copy of all split records.
func (sr *SplitRegistry) All() map[uint32]*SplitInfo {
	sr.mu.RLock()
	defer sr.mu.RUnlock()

	result := make(map[uint32]*SplitInfo, len(sr.splits))
	for k, v := range sr.splits {
		result[k] = v
	}

	return result
}

// Len returns the number of active splits.
func (sr *SplitRegistry) Len() int {
	sr.mu.RLock()
	defer sr.mu.RUnlock()

	return len(sr.splits)
}
