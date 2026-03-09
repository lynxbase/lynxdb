package cluster

import "sync/atomic"

// Metrics holds observable counters and gauges for cluster operations.
// All fields are safe for concurrent access via atomic operations.
// The metrics are exposed via the /api/v1/stats endpoint and can be
// scraped by Prometheus (when the exporter is enabled).
type Metrics struct {
	// Rebalance metrics.
	RebalanceTotal           atomic.Int64 // Total rebalances applied.
	RebalanceMoveTotal       atomic.Int64 // Total shard moves across all rebalances.
	RebalanceDurationNs      atomic.Int64 // Duration of last rebalance in nanoseconds.

	// Shard state gauges (updated after each shard map change).
	ShardActive    atomic.Int64
	ShardDraining  atomic.Int64
	ShardMigrating atomic.Int64
	ShardSplitting atomic.Int64
	ShardMapEpoch  atomic.Int64

	// Split metrics.
	SplitTotal      atomic.Int64 // Total splits proposed.
	SplitDurationNs atomic.Int64 // Duration of last split in nanoseconds.

	// Node state gauges (updated after each heartbeat/failure detection cycle).
	NodesAlive   atomic.Int64
	NodesSuspect atomic.Int64
	NodesDead    atomic.Int64

	// Leader metrics.
	LeaderChangesTotal atomic.Int64 // Total Raft leader transitions observed.

	// Meta loss metrics.
	MetaLossDurationNs   atomic.Int64 // Duration of current or last meta-loss episode.
	MetaLossDuplicateParts atomic.Int64 // Duplicate partitions detected during meta loss.
}

// NewMetrics creates a zero-initialized Metrics.
func NewMetrics() *Metrics {
	return &Metrics{}
}

// ShardStateCounts holds a point-in-time count of shards per state.
type ShardStateCounts struct {
	Active    int64
	Draining  int64
	Migrating int64
	Splitting int64
}

// UpdateShardCounts updates all shard state gauges from a snapshot.
func (m *Metrics) UpdateShardCounts(counts ShardStateCounts) {
	m.ShardActive.Store(counts.Active)
	m.ShardDraining.Store(counts.Draining)
	m.ShardMigrating.Store(counts.Migrating)
	m.ShardSplitting.Store(counts.Splitting)
}

// NodeStateCounts holds a point-in-time count of nodes per state.
type NodeStateCounts struct {
	Alive   int64
	Suspect int64
	Dead    int64
}

// UpdateNodeCounts updates all node state gauges from a snapshot.
func (m *Metrics) UpdateNodeCounts(counts NodeStateCounts) {
	m.NodesAlive.Store(counts.Alive)
	m.NodesSuspect.Store(counts.Suspect)
	m.NodesDead.Store(counts.Dead)
}
