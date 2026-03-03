package part

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// PartitionGranularity controls the time bucketing for partition directories.
type PartitionGranularity int

const (
	// GranularityDaily creates YYYY-MM-DD partition directories.
	GranularityDaily PartitionGranularity = iota
	// GranularityHourly creates YYYY-MM-DD-HH partition directories.
	GranularityHourly
	// GranularityWeekly creates YYYY-Www partition directories (ISO 8601 week).
	GranularityWeekly
	// GranularityMonthly creates YYYY-MM partition directories.
	GranularityMonthly
	// GranularityNone disables time partitioning — all parts live in a flat directory.
	GranularityNone
)

// ParseGranularity converts a config string to a PartitionGranularity enum.
// Valid values: "daily", "hourly", "weekly", "monthly", "none".
// Returns GranularityDaily for unrecognized values.
func ParseGranularity(s string) PartitionGranularity {
	switch s {
	case "hourly":
		return GranularityHourly
	case "weekly":
		return GranularityWeekly
	case "monthly":
		return GranularityMonthly
	case "none":
		return GranularityNone
	default:
		return GranularityDaily
	}
}

// Layout manages the time-partitioned directory structure for parts.
//
// Directory hierarchy:
//
//	<dataDir>/segments/hot/<index>/<YYYY-MM-DD>/part-*.lsg
//
// This is compatible with the existing storage.Layout which already uses
// <dataDir>/segments/hot/<index>/ for segment files.
type Layout struct {
	dataDir     string
	granularity PartitionGranularity
}

// NewLayout creates a Layout with daily partition granularity.
func NewLayout(dataDir string) *Layout {
	return &Layout{
		dataDir:     dataDir,
		granularity: GranularityDaily,
	}
}

// NewLayoutWithGranularity creates a Layout with the specified partition granularity.
func NewLayoutWithGranularity(dataDir string, g PartitionGranularity) *Layout {
	return &Layout{
		dataDir:     dataDir,
		granularity: g,
	}
}

// DataDir returns the root data directory.
func (l *Layout) DataDir() string {
	return l.dataDir
}

// Granularity returns the partition granularity.
func (l *Layout) Granularity() PartitionGranularity {
	return l.granularity
}

// PartitionKey returns the partition key string for the given time.
func (l *Layout) PartitionKey(t time.Time) string {
	t = t.UTC()

	switch l.granularity {
	case GranularityHourly:
		return t.Format("2006-01-02-15")
	case GranularityWeekly:
		year, week := t.ISOWeek()

		return fmt.Sprintf("%04d-W%02d", year, week)
	case GranularityMonthly:
		return t.Format("2006-01")
	case GranularityNone:
		return "all"
	default:
		return t.Format("2006-01-02")
	}
}

// PartitionDir returns the directory path for a given index and time partition.
func (l *Layout) PartitionDir(index string, t time.Time) string {
	return filepath.Join(l.dataDir, "segments", "hot", index, l.PartitionKey(t))
}

// PartitionDirByKey returns the directory path for a given index and partition key string.
func (l *Layout) PartitionDirByKey(index, partition string) string {
	return filepath.Join(l.dataDir, "segments", "hot", index, partition)
}

// IndexDir returns the base directory for a given index.
func (l *Layout) IndexDir(index string) string {
	return filepath.Join(l.dataDir, "segments", "hot", index)
}

// EnsurePartitionDir creates the partition directory (and parents) if it does not exist.
func (l *Layout) EnsurePartitionDir(index string, t time.Time) error {
	dir := l.PartitionDir(index, t)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("part.Layout.EnsurePartitionDir: %w", err)
	}

	return nil
}

// ListIndexes returns all index names that have partition directories.
func (l *Layout) ListIndexes() ([]string, error) {
	hotDir := filepath.Join(l.dataDir, "segments", "hot")

	entries, err := os.ReadDir(hotDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("part.Layout.ListIndexes: %w", err)
	}

	var indexes []string
	for _, e := range entries {
		if e.IsDir() {
			indexes = append(indexes, e.Name())
		}
	}

	return indexes, nil
}

// ListPartitions returns all partition directory names for a given index.
func (l *Layout) ListPartitions(index string) ([]string, error) {
	indexDir := l.IndexDir(index)

	entries, err := os.ReadDir(indexDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("part.Layout.ListPartitions: %w", err)
	}

	var partitions []string
	for _, e := range entries {
		if e.IsDir() {
			partitions = append(partitions, e.Name())
		}
	}

	return partitions, nil
}

// ListParts returns all .lsg file names within a specific partition directory.
func (l *Layout) ListParts(index, partition string) ([]string, error) {
	dir := l.PartitionDirByKey(index, partition)

	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("part.Layout.ListParts: %w", err)
	}

	var parts []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".lsg" {
			parts = append(parts, e.Name())
		}
	}

	return parts, nil
}

// Filename generates a part filename.
// Format: part-<index>-L<level>-<tsNano>.lsg.
func Filename(index string, level int, ts time.Time) string {
	return fmt.Sprintf("part-%s-L%d-%d.lsg", index, level, ts.UnixNano())
}

// ID returns the ID for a part (filename without .lsg extension).
func ID(index string, level int, ts time.Time) string {
	return fmt.Sprintf("part-%s-L%d-%d", index, level, ts.UnixNano())
}
