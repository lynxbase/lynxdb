package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Layout manages the data directory structure for LynxDB.
// Segments are organized by index name and level:
//
//	<dataDir>/segments/hot/<index_name>/seg-<index>-L<level>-<timestamp_ns>.lsg
//	<dataDir>/segment-cache/
//	<dataDir>/query-cache/
//	<dataDir>/views/<mv_name>/
type Layout struct {
	dataDir string
}

// NewLayout creates a layout manager for the given data directory.
func NewLayout(dataDir string) *Layout {
	return &Layout{dataDir: dataDir}
}

// DataDir returns the root data directory.
func (l *Layout) DataDir() string {
	return l.dataDir
}

// SegmentCacheDir returns the directory for cached remote segment chunks.
func (l *Layout) SegmentCacheDir() string {
	return filepath.Join(l.dataDir, "segment-cache")
}

// QueryCacheDir returns the directory for query result cache.
func (l *Layout) QueryCacheDir() string {
	return filepath.Join(l.dataDir, "query-cache")
}

// SegmentDir returns the directory for hot segments of the given index.
func (l *Layout) SegmentDir(indexName string) string {
	return filepath.Join(l.dataDir, "segments", "hot", indexName)
}

// SegmentPath generates the full path for a new segment file.
func (l *Layout) SegmentPath(indexName string, level int, ts time.Time) string {
	name := SegmentName(indexName, level, ts)

	return filepath.Join(l.SegmentDir(indexName), name)
}

// SegmentName generates a segment filename: seg-<index>-L<level>-<timestamp_ns>.lsg.
func SegmentName(indexName string, level int, ts time.Time) string {
	return fmt.Sprintf("seg-%s-L%d-%d.lsg", indexName, level, ts.UnixNano())
}

// ViewsDir returns the directory for materialized view definitions.
func (l *Layout) ViewsDir() string {
	return filepath.Join(l.dataDir, "views")
}

// ViewDir returns the directory for a specific materialized view.
func (l *Layout) ViewDir(name string) string {
	return filepath.Join(l.dataDir, "views", name)
}

// ViewSegmentDir returns the segment directory for a materialized view.
func (l *Layout) ViewSegmentDir(name string) string {
	return filepath.Join(l.dataDir, "views", name, "segments")
}

// EnsureViewDirs creates all directories for a materialized view.
func (l *Layout) EnsureViewDirs(name string) error {
	dirs := []string{
		l.ViewDir(name),
		l.ViewSegmentDir(name),
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("layout: create view dir %s: %w", dir, err)
		}
	}

	return nil
}

// EnsureDirs creates all necessary directories for the layout.
func (l *Layout) EnsureDirs(indexNames ...string) error {
	dirs := []string{
		l.SegmentCacheDir(),
		l.QueryCacheDir(),
	}
	for _, idx := range indexNames {
		dirs = append(dirs, l.SegmentDir(idx))
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("layout: create dir %s: %w", dir, err)
		}
	}

	return nil
}
