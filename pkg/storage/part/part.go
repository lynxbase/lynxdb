// Package part implements the direct-to-disk Part model for LynxDB.
//
// Instead of the traditional WAL -> memtable -> flush pipeline, the Part model
// writes events directly to immutable .lsg segment files ("parts") using atomic
// rename (write to tmp_* -> os.Rename to final path). This eliminates WAL encoding
// overhead, memtable lock contention, and flush blocking.
//
// Each part is a standard .lsg V4 segment file readable by the existing
// segment.Reader. Parts are organized in time-partitioned directories and tracked
// by a filesystem-scanned registry (no separate metadata files).
//
// Design precedent: ClickHouse writes each INSERT as an immutable "part" directory;
// VictoriaLogs uses the same pattern. Both achieve millions of events/sec by trading
// WAL overhead for atomic rename + background merge.
package part

import (
	"time"
)

// DefaultRowGroupSize is the row group size for parts.
// Matches segment.DefaultRowGroupSize (8192) for fine-grained bloom filter
// pruning and effective ConstColumn detection.
const DefaultRowGroupSize = 8192

// Meta describes an immutable part on disk.
// Parts are the fundamental unit of storage in the direct-to-disk model.
// Each part is a single .lsg V4 segment file with one or more row groups.
type Meta struct {
	// ID uniquely identifies this part.
	// Format: "part-<index>-L<level>-<tsNano>"
	ID string

	// Index is the logical index name (physical partition key).
	Index string

	// MinTime is the earliest event timestamp in this part.
	MinTime time.Time

	// MaxTime is the latest event timestamp in this part.
	MaxTime time.Time

	// EventCount is the number of events stored in this part.
	EventCount int64

	// SizeBytes is the on-disk size of the .lsg file.
	SizeBytes int64

	// Level is the compaction level.
	// 0 = freshly flushed, 1+ = merged by compaction.
	Level int

	// Path is the absolute filesystem path to the .lsg file.
	Path string

	// CreatedAt is when this part was written.
	CreatedAt time.Time

	// Columns lists all column names present in this part.
	Columns []string

	// Tier is the storage tier: "hot", "warm", or "cold".
	Tier string

	// Partition is the time partition key (e.g., "2026-03-02").
	Partition string
}
