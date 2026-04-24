// Package sharding implements the two-level sharding scheme for LynxDB:
// Level 1 is time-based bucketing, Level 2 is hash-based partitioning.
package sharding

import (
	"fmt"
	"time"

	"github.com/cespare/xxhash/v2"
)

// DefaultVirtualPartitionCount is the default number of virtual partitions.
const DefaultVirtualPartitionCount = 1024

// ShardID uniquely identifies a shard within the cluster.
// A shard is the intersection of an index, a time bucket, and a hash partition.
type ShardID struct {
	Index      string    `json:"index" msgpack:"index"`
	TimeBucket time.Time `json:"time_bucket" msgpack:"time_bucket"`
	Partition  uint32    `json:"partition" msgpack:"partition"`
}

// String returns a human-readable shard identifier.
// Format: "<index>/t<bucket>/p<partition>"
func (s ShardID) String() string {
	return fmt.Sprintf("%s/t%s/p%d", s.Index, s.TimeBucket.UTC().Format("2006-01-02"), s.Partition)
}

// ShardConfig controls the sharding parameters.
type ShardConfig struct {
	TimeBucketSize        time.Duration
	VirtualPartitionCount uint32
}

// AssignShard computes the ShardID for an event based on its metadata.
//
// Level 1 (time): ts is truncated to the nearest TimeBucketSize boundary.
// Level 2 (hash): xxhash64(source + "\x00" + host) mod VirtualPartitionCount.
//
// Using xxhash64 instead of fnv32a provides better distribution and is
// already a dependency (used in bloom filters and dedup).
func AssignShard(source, host, index string, ts time.Time, cfg ShardConfig) ShardID {
	bucket := ts.Truncate(cfg.TimeBucketSize).UTC()

	// The null byte separator prevents collisions like ("ab","c") vs ("a","bc").
	key := source + "\x00" + host
	h := xxhash.Sum64String(key)
	partition := uint32(h % uint64(cfg.VirtualPartitionCount))

	return ShardID{
		Index:      index,
		TimeBucket: bucket,
		Partition:  partition,
	}
}

// AssignShardWithSplits computes the ShardID like AssignShard, but also
// consults a SplitRegistry to resolve the final partition when splits
// are active. The full hash value is passed to the registry for bit
// subdivision routing.
func AssignShardWithSplits(source, host, index string, ts time.Time, cfg ShardConfig, splits *SplitRegistry) ShardID {
	bucket := ts.Truncate(cfg.TimeBucketSize).UTC()

	key := source + "\x00" + host
	h := xxhash.Sum64String(key)
	partition := uint32(h % uint64(cfg.VirtualPartitionCount))

	// Resolve through split registry if present.
	if splits != nil {
		partition = splits.Resolve(partition, h)
	}

	return ShardID{
		Index:      index,
		TimeBucket: bucket,
		Partition:  partition,
	}
}
