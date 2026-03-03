// Package buffer implements the Unified Buffer Manager for LynxDB.
//
// The buffer manager provides a single memory pool that serves all major memory
// consumers: segment cache, query operators (hash tables, sort buffers, dedup
// sets), and memtable ingestion buffers. By managing memory through fixed-size
// pages with a unified eviction policy, it eliminates the problem of independent
// memory pools competing for RAM without coordination.
//
// The design is inspired by DuckDB's Unified Buffer Manager. All data lives in
// fixed-size pages allocated off the Go heap via mmap (on Unix) or standard Go
// allocation (fallback). This minimizes GC pressure because the Go garbage
// collector only sees page descriptor structs (~128 bytes each), not the
// multi-gigabyte data payloads.
//
// # Core Concepts
//
//   - Page: A fixed-size memory block (default 64KB). Pages are allocated off-heap
//     and managed by the BufferPool. Each page has an owner (cache, query, or memtable),
//     a pin count, and a dirty flag.
//
//   - Pin/Unpin: Any consumer that actively reads or writes a page must pin it first.
//     Pinned pages cannot be evicted. Every Pin() must have a matching Unpin() —
//     use defer page.Unpin() in all code paths.
//
//   - BufferPool: The central memory manager. Allocates pages from a fixed pool,
//     evicts unpinned pages using the Clock algorithm when the pool is full, and
//     writes back dirty pages before eviction.
//
//   - Clock Evictor: O(1) amortized eviction using the clock (second-chance)
//     algorithm. Scans pages circularly: pinned pages are skipped, pages with
//     the reference bit set get a second chance, pages without the reference bit
//     are evicted.
//
// # Consumer Integration
//
//   - SegmentCacheConsumer: Wraps the buffer pool for caching segment column data.
//     Pages are loaded from .lsg files and cached for query performance.
//
//   - OperatorPageAllocator: Provides page-based memory for query operators.
//     When the pool is full, evicting an operator's page triggers
//     spill-to-disk logic.
//
//   - MemtablePageWriter: Writes incoming events into buffer pool pages. Memtable
//     pages are always dirty and have elevated eviction priority (evicted last).
//
// # Feature Flag
//
// The buffer manager is opt-in via the buffer_manager.enabled configuration flag.
// When disabled (default), the system uses streaming scan with per-operator spill
// at fixed thresholds.
package buffer
