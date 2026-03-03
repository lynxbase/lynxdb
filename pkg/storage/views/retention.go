package views

import (
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/storage"
	"github.com/OrlovEvgeny/Lynxdb/pkg/storage/segment"
)

// EnforceRetention deletes view segments that are older than the view's retention period.
// Retention is segment-level: delete entire segment file if all events are expired.
func EnforceRetention(def ViewDefinition, layout *storage.Layout, logger *slog.Logger) error {
	if def.Retention <= 0 {
		return nil // No retention limit.
	}

	segDir := layout.ViewSegmentDir(def.Name)
	entries, err := os.ReadDir(segDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return err
	}

	cutoff := time.Now().Add(-def.Retention)
	var removed int

	// Sort to process oldest first.
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".lsg" {
			continue
		}

		path := filepath.Join(segDir, entry.Name())

		// Try to read segment time range from footer.
		expired, err := isSegmentExpired(path, cutoff)
		if err != nil {
			logger.Warn("views retention: check segment", "path", path, "err", err)

			continue
		}

		if expired {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				logger.Warn("views retention: remove expired segment", "path", path, "err", err)
			}
			removed++
		}
	}

	if removed > 0 {
		logger.Info("views retention: removed expired segments",
			"view", def.Name,
			"removed", removed,
		)
	}

	return nil
}

// isSegmentExpired checks if all events in a segment are older than cutoff.
func isSegmentExpired(path string, cutoff time.Time) (bool, error) {
	ms, err := segment.OpenSegmentFile(path)
	if err != nil {
		return false, err
	}
	defer ms.Close()

	reader := ms.Reader()
	maxTime := reader.MaxTimestamp()

	return maxTime.Before(cutoff), nil
}
