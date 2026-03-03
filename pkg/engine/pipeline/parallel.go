package pipeline

import "runtime"

// ParallelConfig controls branch-level parallelism for APPEND, MULTISEARCH,
// multi-source FROM, and CTE materialization.
//
// When Enabled is true, multi-branch operators (APPEND, MULTISEARCH, multi-source
// FROM) use ConcurrentUnionIterator instead of the sequential UnionIterator.
// Independent CTEs at the same dependency level are materialized in parallel.
//
// MaxBranchParallelism is a soft goroutine limit — each ConcurrentUnionIterator
// spawns at most this many goroutines. The real I/O bottleneck should be
// controlled by a future I/O semaphore at the SegmentReader.ReadRowGroup level.
type ParallelConfig struct {
	// MaxBranchParallelism limits the number of concurrent branch goroutines.
	// 0 means auto (GOMAXPROCS). This is a soft limit on goroutines, not I/O.
	MaxBranchParallelism int

	// ChannelBufferSize is the per-child channel buffer for ConcurrentUnionIterator.
	// Higher values reduce producer blocking at the cost of more buffered batches.
	// Default: 2.
	ChannelBufferSize int

	// Enabled is the global kill switch for branch parallelism.
	// When false, all operators use the sequential UnionIterator path.
	Enabled bool
}

// effectiveMaxParallel returns the resolved max parallelism value.
// Returns GOMAXPROCS when MaxBranchParallelism is 0 (auto).
func (c *ParallelConfig) effectiveMaxParallel() int {
	if c.MaxBranchParallelism > 0 {
		return c.MaxBranchParallelism
	}

	return runtime.GOMAXPROCS(0)
}

// effectiveBufferSize returns the resolved channel buffer size.
// Returns 2 when ChannelBufferSize is 0 (default).
func (c *ParallelConfig) effectiveBufferSize() int {
	if c.ChannelBufferSize > 0 {
		return c.ChannelBufferSize
	}

	return 2
}
