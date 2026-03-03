package pipeline

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

// ErrTempSpaceFull is returned when a spill file cannot be created because the
// total spill bytes on disk would exceed the configured quota (MaxTotalBytes).
// The error message includes an actionable suggestion for the operator.
var ErrTempSpaceFull = errors.New("spill temporary space quota exceeded")

// SpillManager manages the lifecycle of spill files created during query execution.
// It ensures all spill files are tracked, cleaned up on query completion, and
// orphaned files from previous crashes are removed on startup.
//
// SpillManager is thread-safe and designed to be shared across queries within a
// single server engine. Each query creates spill files through the manager and
// releases them when the query completes. CleanupAll is called during server
// shutdown as a safety net.
//
// Not intended to be created via init() or global state — pass as a dependency.
type SpillManager struct {
	mu            sync.Mutex
	dir           string              // base directory for spill files
	files         map[string]struct{} // tracked file paths
	totalBytes    atomic.Int64        // total spill bytes currently on disk
	maxTotalBytes int64               // quota: max total spill bytes (0 = unlimited)
	logger        *slog.Logger
}

// NewSpillManager creates a new spill file lifecycle manager.
// If dir is empty, os.TempDir() is used. A subdirectory "lynxdb-spill-<pid>"
// is created inside the base dir for isolation from other processes.
//
// The logger is optional — pass nil for no logging.
func NewSpillManager(dir string, logger *slog.Logger) (*SpillManager, error) {
	return NewSpillManagerWithQuota(dir, 0, logger)
}

// NewSpillManagerWithQuota creates a spill manager with a maximum total bytes
// quota. When the quota would be exceeded, NewSpillFile returns ErrTempSpaceFull.
// A maxBytes of 0 means unlimited (no quota enforcement).
func NewSpillManagerWithQuota(dir string, maxBytes int64, logger *slog.Logger) (*SpillManager, error) {
	if dir == "" {
		dir = os.TempDir()
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	}

	spillDir := filepath.Join(dir, fmt.Sprintf("lynxdb-spill-%d", os.Getpid()))
	if err := os.MkdirAll(spillDir, 0o700); err != nil {
		return nil, fmt.Errorf("spill_manager: create spill dir %s: %w", spillDir, err)
	}

	return &SpillManager{
		dir:           spillDir,
		files:         make(map[string]struct{}),
		maxTotalBytes: maxBytes,
		logger:        logger,
	}, nil
}

// NewSpillFile creates a new temporary spill file with the given prefix.
// The file is registered with the manager for lifecycle tracking.
// The caller is responsible for writing to and closing the *os.File handle,
// then calling Release(path) when the spill data is no longer needed.
//
// Returns ErrTempSpaceFull if the total spill bytes on disk would exceed the
// configured quota (maxTotalBytes). The error wraps ErrTempSpaceFull for
// errors.Is() matching.
func (m *SpillManager) NewSpillFile(prefix string) (*os.File, error) {
	if m == nil {
		// Fallback: create an unmanaged temp file in os.TempDir().
		return os.CreateTemp("", "lynxdb-spill-"+prefix+"-*.tmp")
	}

	m.mu.Lock()
	// Enforce quota under lock to prevent concurrent callers from racing past the check.
	if m.maxTotalBytes > 0 && m.totalBytes.Load() >= m.maxTotalBytes {
		m.mu.Unlock()

		return nil, fmt.Errorf(
			"spill_manager: %w: current=%d bytes, limit=%d bytes; "+
				"increase query.max_temp_dir_size_bytes or add filters to reduce data volume",
			ErrTempSpaceFull, m.totalBytes.Load(), m.maxTotalBytes,
		)
	}

	f, err := os.CreateTemp(m.dir, "lynxdb-spill-"+prefix+"-*.tmp")
	if err != nil {
		m.mu.Unlock()

		return nil, fmt.Errorf("spill_manager: create temp file: %w", err)
	}

	m.files[f.Name()] = struct{}{}
	m.mu.Unlock()

	return f, nil
}

// Release removes a spill file from tracking and deletes it from disk.
// Safe to call with a path that is not tracked (no-op in that case).
// Nil-safe: no-op on nil receiver.
func (m *SpillManager) Release(path string) {
	if m == nil {
		// Unmanaged: best-effort remove.
		os.Remove(path)

		return
	}

	m.mu.Lock()
	_, tracked := m.files[path]
	if tracked {
		delete(m.files, path)
	}
	m.mu.Unlock()

	if !tracked {
		return // not our file, don't touch it
	}
	if info, err := os.Stat(path); err == nil {
		m.totalBytes.Add(-info.Size())
	}
	os.Remove(path)
}

// TrackBytes adjusts the total spill bytes counter for observability and quota
// enforcement. Callers should invoke this with positive deltas after writing
// data to spill files, so that NewSpillFile can enforce the quota accurately.
// Nil-safe: no-op on nil receiver.
func (m *SpillManager) TrackBytes(delta int64) {
	if m == nil {
		return
	}
	m.totalBytes.Add(delta)
}

// CleanupAll removes all tracked spill files and clears the tracking map.
// Called during server shutdown as a safety net. Also removes the spill
// directory itself if it is empty after cleanup.
// Nil-safe: no-op on nil receiver.
func (m *SpillManager) CleanupAll() {
	if m == nil {
		return
	}

	m.mu.Lock()
	files := make([]string, 0, len(m.files))
	for path := range m.files {
		files = append(files, path)
	}
	m.files = make(map[string]struct{})
	m.mu.Unlock()

	for _, path := range files {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			m.logger.Warn("spill cleanup failed", "path", path, "error", err)
		}
	}
	m.totalBytes.Store(0)

	// Best-effort remove the spill directory.
	os.Remove(m.dir)
}

// Stats returns a snapshot of the current spill file count and total bytes on disk.
// Nil-safe: returns (0, 0) on nil receiver.
func (m *SpillManager) Stats() (fileCount int, totalBytes int64) {
	if m == nil {
		return 0, 0
	}

	m.mu.Lock()
	fileCount = len(m.files)
	m.mu.Unlock()
	totalBytes = m.totalBytes.Load()

	return fileCount, totalBytes
}

// Dir returns the spill directory path.
// Nil-safe: returns empty string on nil receiver.
func (m *SpillManager) Dir() string {
	if m == nil {
		return ""
	}

	return m.dir
}

// CleanupOrphans walks the given directory and removes any files matching the
// "lynxdb-spill-" prefix. These are orphans from previous crashes. This function
// is intended to be called once during server startup.
//
// Returns the number of orphan files removed.
func CleanupOrphans(dir string, logger *slog.Logger) int {
	if dir == "" {
		return 0
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	}

	removed := 0
	currentDir := fmt.Sprintf("lynxdb-spill-%d", os.Getpid())
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			// Cannot access this entry — skip it and continue walking.
			if d != nil && d.IsDir() {
				return fs.SkipDir
			}

			return walkErr
		}
		if d.IsDir() {
			// Skip the current PID's spill directory to avoid deleting our own.
			if d.Name() == currentDir {
				return fs.SkipDir
			}
			// Remove empty lynxdb-spill-* directories from previous PIDs.
			if strings.HasPrefix(d.Name(), "lynxdb-spill-") && path != dir {
				// Try removing; will fail if non-empty which is fine.
				entries, readErr := os.ReadDir(path)
				if readErr == nil && len(entries) == 0 {
					os.Remove(path)
				} else if readErr == nil {
					// Directory has files — remove them.
					for _, entry := range entries {
						entryPath := filepath.Join(path, entry.Name())
						if !entry.IsDir() && strings.HasPrefix(entry.Name(), "lynxdb-spill-") {
							os.Remove(entryPath)
							removed++
						}
					}
					// Try removing now-empty directory.
					os.Remove(path)
				}

				return fs.SkipDir
			}

			return nil
		}
		if strings.HasPrefix(d.Name(), "lynxdb-spill-") {
			os.Remove(path)
			removed++
		}

		return nil
	})
	if err != nil {
		logger.Warn("spill cleanup walk error", "dir", dir, "error", err)
	}
	if removed > 0 {
		logger.Info("cleaned up orphan spill files", "count", removed, "dir", dir)
	}

	return removed
}
