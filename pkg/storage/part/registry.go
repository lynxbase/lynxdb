package part

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/lynxbase/lynxdb/pkg/storage/segment"
)

// Registry tracks all known parts in memory.
// It is rebuilt from the filesystem on startup via ScanDir.
// No separate metadata files are needed — the filesystem is the source of truth.
//
// Registry is safe for concurrent use.
type Registry struct {
	mu      sync.RWMutex
	parts   map[string]*Meta   // id -> meta
	byIndex map[string][]*Meta // index -> parts (sorted by MinTime)
	byLevel map[int][]*Meta    // level -> parts
	logger  *slog.Logger
}

// NewRegistry creates an empty Registry.
func NewRegistry(logger *slog.Logger) *Registry {
	return &Registry{
		parts:   make(map[string]*Meta),
		byIndex: make(map[string][]*Meta),
		byLevel: make(map[int][]*Meta),
		logger:  logger,
	}
}

// Add registers a part in the registry.
func (r *Registry) Add(meta *Meta) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.parts[meta.ID] = meta
	r.logger.Debug("part registry add",
		"id", meta.ID,
		"index", meta.Index,
		"level", meta.Level,
		"size", meta.SizeBytes,
		"events", meta.EventCount,
		"partition", meta.Partition,
	)
	r.byIndex[meta.Index] = insertSorted(r.byIndex[meta.Index], meta)
	r.byLevel[meta.Level] = append(r.byLevel[meta.Level], meta)
}

// Remove unregisters a part from the registry.
func (r *Registry) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	meta, ok := r.parts[id]
	if !ok {
		return
	}

	r.logger.Debug("part registry remove",
		"id", id,
		"index", meta.Index,
		"level", meta.Level,
	)
	delete(r.parts, id)
	r.byIndex[meta.Index] = removeByID(r.byIndex[meta.Index], id)
	r.byLevel[meta.Level] = removeByID(r.byLevel[meta.Level], id)
}

// Get returns the metadata for a part by ID, or nil if not found.
func (r *Registry) Get(id string) *Meta {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.parts[id]
}

// Has returns true if a part with the given ID exists in the registry.
func (r *Registry) Has(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.parts[id]
	return ok
}

// All returns all registered parts. The returned slice is a copy.
func (r *Registry) All() []*Meta {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*Meta, 0, len(r.parts))
	for _, meta := range r.parts {
		result = append(result, meta)
	}

	return result
}

// ByIndex returns all parts for the given index, sorted by MinTime ascending.
// The returned slice is a copy.
func (r *Registry) ByIndex(index string) []*Meta {
	r.mu.RLock()
	defer r.mu.RUnlock()

	src := r.byIndex[index]
	result := make([]*Meta, len(src))
	copy(result, src)

	return result
}

// ByLevel returns all parts at the given compaction level.
// The returned slice is a copy.
func (r *Registry) ByLevel(level int) []*Meta {
	r.mu.RLock()
	defer r.mu.RUnlock()

	src := r.byLevel[level]
	result := make([]*Meta, len(src))
	copy(result, src)

	return result
}

// Count returns the total number of registered parts.
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.parts)
}

// CountByIndex returns the number of parts for a given index.
func (r *Registry) CountByIndex(index string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.byIndex[index])
}

// Indexes returns all index names that have parts.
func (r *Registry) Indexes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]string, 0, len(r.byIndex))
	for idx := range r.byIndex {
		if len(r.byIndex[idx]) > 0 {
			result = append(result, idx)
		}
	}

	sort.Strings(result)

	return result
}

// ScanDir rebuilds the registry from the filesystem.
// It walks all partition directories, deletes tmp_* files (incomplete writes
// from a crash), and reads footers of valid .lsg files to populate Meta.
func (r *Registry) ScanDir(layout *Layout) error {
	indexes, err := layout.ListIndexes()
	if err != nil {
		return fmt.Errorf("part.Registry.ScanDir: %w", err)
	}

	var tmpCleaned int

	for _, index := range indexes {
		partitions, err := layout.ListPartitions(index)
		if err != nil {
			return fmt.Errorf("part.Registry.ScanDir: list partitions for %s: %w", index, err)
		}

		for _, partition := range partitions {
			dir := layout.PartitionDirByKey(index, partition)

			entries, err := os.ReadDir(dir)
			if err != nil {
				r.logger.Warn("part.Registry.ScanDir: read partition dir failed",
					"dir", dir, "error", err)

				continue
			}

			for _, entry := range entries {
				if entry.IsDir() {
					continue
				}

				name := entry.Name()

				// Clean up incomplete writes from crash and
				// .deleted files from prior compaction.
				if IsTempFile(name) || IsDeletedFile(name) {
					cleanPath := filepath.Join(dir, name)
					if removeErr := os.Remove(cleanPath); removeErr != nil {
						r.logger.Warn("part.Registry.ScanDir: failed to remove stale file",
							"path", cleanPath, "error", removeErr)
					} else {
						tmpCleaned++
					}

					continue
				}

				// Skip non-.lsg files.
				if filepath.Ext(name) != ".lsg" {
					continue
				}

				path := filepath.Join(dir, name)
				meta, err := readPartMeta(path, index, partition)
				if err != nil {
					r.logger.Warn("part.Registry.ScanDir: failed to read part",
						"path", path, "error", err)

					continue
				}

				r.Add(meta)
			}
		}

		r.logger.Debug("part registry scan index",
			"index", index,
			"partitions", len(partitions),
		)
	}

	if tmpCleaned > 0 {
		r.logger.Info("part.Registry.ScanDir: cleaned up incomplete writes",
			"tmp_files_removed", tmpCleaned)
	}

	r.logger.Info("part.Registry.ScanDir: complete",
		"parts", r.Count(),
		"indexes", len(r.Indexes()))

	return nil
}

// tailReadSize is the number of bytes to read from the end of a part file.
// 64KB is sufficient for the footer + bloom filter summary in typical parts
// (8-50 columns). If the footer exceeds this, we fall back to a full read.
const tailReadSize = 64 * 1024

// readPartMeta reads a .lsg file's footer to extract part metadata.
// Uses a tail-read optimization: only the last 64KB of the file are read,
// since segment.DecodeFooter reads from the end of the byte slice. This
// avoids reading the entire file for large parts (100MB+), reducing startup
// time from O(total_data_size) to O(num_parts * 64KB).
func readPartMeta(path, index, partition string) (*Meta, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat: %w", err)
	}

	size := fi.Size()

	tail, err := readTail(path, size)
	if err != nil {
		return nil, err
	}

	footer, err := segment.DecodeFooter(tail)
	if err != nil {
		// Footer may exceed tailReadSize (e.g., 200+ columns).
		// Fall back to full file read.
		if size > tailReadSize {
			fullData, readErr := os.ReadFile(path)
			if readErr != nil {
				return nil, fmt.Errorf("full read fallback: %w", readErr)
			}

			footer, err = segment.DecodeFooter(fullData)
		}

		if err != nil {
			return nil, fmt.Errorf("decode footer: %w", err)
		}
	}

	// Extract column names from footer.
	var columns []string
	if len(footer.RowGroups) > 0 {
		columns = make([]string, len(footer.RowGroups[0].Columns))
		for i, c := range footer.RowGroups[0].Columns {
			columns[i] = c.Name
		}
	}

	// Extract time range from _time column stats in the footer directly,
	// avoiding the overhead of creating a full segment.Reader.
	minTime, maxTime := extractTimeRange(footer)

	// Parse level from filename: part-<index>-L<level>-<tsNano>.lsg.
	base := filepath.Base(path)
	id := base[:len(base)-len(".lsg")]
	level := parseLevelFromFilename(base)

	return &Meta{
		ID:         id,
		Index:      index,
		MinTime:    minTime,
		MaxTime:    maxTime,
		EventCount: footer.EventCount,
		SizeBytes:  size,
		Level:      level,
		Path:       path,
		CreatedAt:  fi.ModTime(),
		Columns:    columns,
		Tier:       "hot",
		Partition:  partition,
	}, nil
}

// readTail reads the last tailReadSize bytes from a file (or the entire file
// if smaller than tailReadSize). This is the hot path for startup performance.
func readTail(path string, size int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	readSize := size
	if readSize > tailReadSize {
		readSize = tailReadSize
	}

	tail := make([]byte, readSize)

	if size > tailReadSize {
		// Seek to the tail position and read.
		if _, err := f.ReadAt(tail, size-tailReadSize); err != nil {
			return nil, fmt.Errorf("read tail: %w", err)
		}
	} else {
		// Small file — read the whole thing.
		if _, err := io.ReadFull(f, tail); err != nil {
			return nil, fmt.Errorf("read: %w", err)
		}
	}

	return tail, nil
}

// extractTimeRange extracts min/max timestamps from the _time column stats
// in the footer. This avoids creating a full segment.Reader just for time bounds.
func extractTimeRange(footer *segment.Footer) (time.Time, time.Time) {
	var minTime, maxTime time.Time

	for _, stat := range footer.Stats() {
		if stat.Name != "_time" {
			continue
		}

		if stat.MinValue != "" {
			if ns, err := strconv.ParseInt(stat.MinValue, 10, 64); err == nil {
				minTime = time.Unix(0, ns)
			}
		}

		if stat.MaxValue != "" {
			if ns, err := strconv.ParseInt(stat.MaxValue, 10, 64); err == nil {
				maxTime = time.Unix(0, ns)
			}
		}

		break
	}

	return minTime, maxTime
}

// parseLevelFromFilename extracts the compaction level from a part filename.
// Expected format: part-<index>-L<level>-<tsNano>.lsg.
// Returns 0 if parsing fails.
func parseLevelFromFilename(name string) int {
	// Find "L" followed by a digit.
	for i := 0; i < len(name)-1; i++ {
		if name[i] == 'L' && name[i+1] >= '0' && name[i+1] <= '9' {
			if i > 0 && name[i-1] == '-' {
				level := 0
				for j := i + 1; j < len(name) && name[j] >= '0' && name[j] <= '9'; j++ {
					level = level*10 + int(name[j]-'0')
				}

				return level
			}
		}
	}

	return 0
}

// insertSorted inserts meta into the slice maintaining MinTime ascending order.
func insertSorted(slice []*Meta, meta *Meta) []*Meta {
	i := sort.Search(len(slice), func(i int) bool {
		return slice[i].MinTime.After(meta.MinTime)
	})

	slice = append(slice, nil)
	copy(slice[i+1:], slice[i:])
	slice[i] = meta

	return slice
}

// removeByID removes the element with the given ID from the slice.
func removeByID(slice []*Meta, id string) []*Meta {
	for i, m := range slice {
		if m.ID == id {
			return append(slice[:i], slice[i+1:]...)
		}
	}

	return slice
}
