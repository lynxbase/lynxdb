package memgov

// MemoryClass identifies the type of memory allocation.
// The governor uses classes to determine revocation priority under pressure.
type MemoryClass int

const (
	// ClassNonRevocable holds pinned frames, active decode buffers.
	// Cannot be reclaimed without aborting the operation.
	ClassNonRevocable MemoryClass = iota

	// ClassRevocable holds prefetch, lookahead, speculative buffers.
	// Cheapest to drop — no writeback, no spill, just discard.
	ClassRevocable

	// ClassSpillable holds operator sort/agg/join working memory.
	// Reclamation triggers operator spill-to-disk.
	ClassSpillable

	// ClassPageCache holds segment column block cache.
	// Clean pages can be evicted for free; dirty pages need writeback first.
	ClassPageCache

	// ClassMetadata holds bloom filters, FST indexes, footers.
	// Small, rarely evicted, but can be re-read from disk.
	ClassMetadata

	// ClassTempIO holds spill staging, compaction scratch.
	// Transient allocations freed after I/O completes.
	ClassTempIO

	numClasses // sentinel — must be last
)

// String returns a human-readable name for the memory class.
func (c MemoryClass) String() string {
	switch c {
	case ClassNonRevocable:
		return "non-revocable"
	case ClassRevocable:
		return "revocable"
	case ClassSpillable:
		return "spillable"
	case ClassPageCache:
		return "page-cache"
	case ClassMetadata:
		return "metadata"
	case ClassTempIO:
		return "temp-io"
	default:
		return "unknown"
	}
}

// ClassStats reports current/peak/limit for a single memory class.
type ClassStats struct {
	Allocated int64 `json:"allocated"`
	Peak      int64 `json:"peak"`
	Limit     int64 `json:"limit"` // 0 = no per-class limit, governed by total
}

// TotalStats reports aggregate stats across all memory classes.
type TotalStats struct {
	Allocated int64                   `json:"allocated"`
	Peak      int64                   `json:"peak"`
	Limit     int64                   `json:"limit"`
	ByClass   [numClasses]ClassStats  `json:"by_class"`
}
