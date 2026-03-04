package views

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/storage"
	"github.com/lynxbase/lynxdb/pkg/storage/segment"
)

const (
	mergeMinSegments     = 4    // minimum segments to trigger merge
	maxValuesCardinality = 1000 // cap on distinct values kept during merge
)

// MergeView merges segments for a view, combining partial aggregates for
// aggregation views and concatenating rows for projection views.
// It picks up to mergeMinSegments oldest segments, merges them into one,
// and removes the originals.
func MergeView(def ViewDefinition, layout *storage.Layout, logger *slog.Logger) error {
	segDir := layout.ViewSegmentDir(def.Name)
	entries, err := os.ReadDir(segDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return fmt.Errorf("views merge: readdir: %w", err)
	}

	// Collect .lsg files.
	var segFiles []string
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".lsg" {
			continue
		}
		segFiles = append(segFiles, filepath.Join(segDir, entry.Name()))
	}

	if len(segFiles) < mergeMinSegments {
		return nil
	}

	// Sort by name (timestamp-ordered) and pick oldest batch.
	sort.Strings(segFiles)
	batch := segFiles
	if len(batch) > mergeMinSegments {
		batch = batch[:mergeMinSegments]
	}

	// Read all events from the batch.
	var allEvents []*event.Event
	for _, path := range batch {
		ms, err := segment.OpenSegmentFile(path)
		if err != nil {
			logger.Warn("views merge: open segment", "path", path, "err", err)

			continue
		}
		reader := ms.Reader()
		events, err := reader.ReadEvents()
		ms.Close()
		if err != nil {
			logger.Warn("views merge: read events", "path", path, "err", err)

			continue
		}
		allEvents = append(allEvents, events...)
	}

	if len(allEvents) == 0 {
		return nil
	}

	// For aggregation views, merge partial aggregates by group key.
	if def.Type == ViewTypeAggregation {
		allEvents = mergeViewAgg(allEvents, def, logger)
	}

	// Sort events by sort key (falls back to time order if no sort key).
	if len(def.SortKey) > 0 {
		sort.SliceStable(allEvents, func(i, j int) bool {
			for _, field := range def.SortKey {
				vi := allEvents[i].GetField(field).String()
				vj := allEvents[j].GetField(field).String()
				if vi != vj {
					return vi < vj
				}
			}

			return false
		})
	} else {
		sort.Slice(allEvents, func(i, j int) bool {
			return allEvents[i].Time.Before(allEvents[j].Time)
		})
	}

	// Write merged segment atomically: write to .tmp, then rename.
	ts := time.Now()
	segName := fmt.Sprintf("seg-%s-L1-%d.lsg", def.Name, ts.UnixNano())
	outPath := filepath.Join(segDir, segName)
	tmpPath := outPath + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("views merge: create segment: %w", err)
	}

	sw := segment.NewWriter(f)
	if len(def.SortKey) > 0 {
		sw.SetSortKey(def.SortKey)
	}
	if _, err := sw.Write(allEvents); err != nil {
		f.Close()
		os.Remove(tmpPath)

		return fmt.Errorf("views merge: write segment: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)

		return fmt.Errorf("views merge: sync segment: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpPath)

		return fmt.Errorf("views merge: close segment: %w", err)
	}
	if err := os.Rename(tmpPath, outPath); err != nil {
		os.Remove(tmpPath)

		return fmt.Errorf("views merge: rename segment: %w", err)
	}
	if err := syncDir(filepath.Dir(outPath)); err != nil {
		return fmt.Errorf("views merge: sync dir: %w", err)
	}

	// Remove old segments.
	for _, path := range batch {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			logger.Warn("views merge: remove old segment", "path", path, "err", err)
		}
	}

	logger.Info("views merge: merged segments",
		"view", def.Name,
		"input", len(batch),
		"events_in", len(allEvents),
		"output", outPath,
	)

	return nil
}

// mergeViewAgg merges aggregation view events using the appropriate strategy.
// If AggSpec is present (new-style partial state), deserializes events to
// PartialAggGroup, merges intermediate state without finalizing, and
// serializes back. Falls back to legacy mergeAggregates for older views
// that use AggregationDef-based state columns.
func mergeViewAgg(allEvents []*event.Event, def ViewDefinition, logger *slog.Logger) []*event.Event {
	if def.AggSpec != nil {
		// New path: partial-state-aware merge via PartialAggGroup.
		groups := EventsToPartialGroups(allEvents, def.AggSpec)
		merged := pipeline.MergePartialGroupsNoFinalize(groups, def.AggSpec)

		return PartialGroupsToEvents(merged, def.AggSpec, def.Name)
	}

	// Legacy path: merge using AggregationDef state columns.
	if len(def.GroupBy) > 0 {
		for _, agg := range def.Aggregations {
			if agg.Type == "dc" {
				logger.Warn("views merge: dc (distinct count) merge produces upper-bound estimate; "+
					"sum(dc) >= dc(union). Consider HLL sketches for exact merge.",
					"view", def.Name, "agg", agg.Name)
			}
		}

		return mergeAggregates(allEvents, def)
	}

	return allEvents
}

// valFloat extracts a float64 from an event field, handling all value types safely.
// Returns (value, true) if the field is present and numeric, or (0, false) if null.
func valFloat(e *event.Event, field string) (float64, bool) {
	v := e.GetField(field)
	if v.IsNull() {
		return 0, false
	}
	switch v.Type() {
	case event.FieldTypeFloat:
		return v.AsFloat(), true
	case event.FieldTypeInt:
		return float64(v.AsInt()), true
	default:
		f, err := strconv.ParseFloat(v.String(), 64)
		if err != nil {
			return 0, false
		}

		return f, true
	}
}

// mergeAggregates combines events with the same group-by key, merging partial
// aggregation state columns according to the MV's aggregation definitions.
func mergeAggregates(events []*event.Event, def ViewDefinition) []*event.Event {
	type groupKey string

	buildKey := func(e *event.Event) groupKey {
		var parts []string
		for _, field := range def.GroupBy {
			val := e.GetField(field)
			parts = append(parts, val.String())
		}

		return groupKey(strings.Join(parts, "\x00"))
	}

	groups := make(map[groupKey]*event.Event)
	order := make([]groupKey, 0)

	for _, e := range events {
		key := buildKey(e)
		existing, ok := groups[key]
		if !ok {
			// Clone the event.
			clone := &event.Event{
				Time:       e.Time,
				Raw:        e.Raw,
				Source:     e.Source,
				SourceType: e.SourceType,
				Host:       e.Host,
				Index:      e.Index,
				Fields:     make(map[string]event.Value, len(e.Fields)),
			}
			for k, v := range e.Fields {
				clone.Fields[k] = v
			}
			groups[key] = clone
			order = append(order, key)

			continue
		}

		// Merge aggregation state columns.
		for _, agg := range def.Aggregations {
			mergeAggState(existing, e, agg)
		}

		// Keep the latest timestamp.
		if e.Time.After(existing.Time) {
			existing.Time = e.Time
		}
	}

	result := make([]*event.Event, 0, len(groups))
	for _, key := range order {
		result = append(result, groups[key])
	}

	return result
}

// mergeAggState merges a single aggregation's state columns from src into dst.
func mergeAggState(dst, src *event.Event, agg AggregationDef) {
	switch agg.Type {
	case aggFnCount, aggFnSum:
		// Additive: null treated as 0 (additive identity).
		if len(agg.StateColumns) >= 1 {
			col := agg.StateColumns[0]
			dstVal, _ := valFloat(dst, col)
			srcVal, _ := valFloat(src, col)
			dst.SetField(col, event.FloatValue(dstVal+srcVal))
		}
	case aggFnAvg:
		// Sum and count are both additive; null treated as 0.
		if len(agg.StateColumns) >= 2 {
			sumCol := agg.StateColumns[0]
			countCol := agg.StateColumns[1]
			dstSum, _ := valFloat(dst, sumCol)
			srcSum, _ := valFloat(src, sumCol)
			dst.SetField(sumCol, event.FloatValue(dstSum+srcSum))
			dstCnt, _ := valFloat(dst, countCol)
			srcCnt, _ := valFloat(src, countCol)
			dst.SetField(countCol, event.FloatValue(dstCnt+srcCnt))
		}
	case aggFnMin:
		if len(agg.StateColumns) >= 1 {
			col := agg.StateColumns[0]
			srcVal, srcOk := valFloat(src, col)
			if !srcOk {
				break
			}
			dstVal, dstOk := valFloat(dst, col)
			if !dstOk || srcVal < dstVal {
				dst.SetField(col, event.FloatValue(srcVal))
			}
		}
	case aggFnMax:
		if len(agg.StateColumns) >= 1 {
			col := agg.StateColumns[0]
			srcVal, srcOk := valFloat(src, col)
			if !srcOk {
				break
			}
			dstVal, dstOk := valFloat(dst, col)
			if !dstOk || srcVal > dstVal {
				dst.SetField(col, event.FloatValue(srcVal))
			}
		}
	case aggFnDC:
		// Sum is an upper bound — exact dc requires HLL sketches.
		if len(agg.StateColumns) >= 1 {
			col := agg.StateColumns[0]
			srcVal, srcOk := valFloat(src, col)
			if !srcOk {
				break
			}
			dstVal, dstOk := valFloat(dst, col)
			if dstOk {
				dst.SetField(col, event.FloatValue(dstVal+srcVal))
			} else {
				dst.SetField(col, event.FloatValue(srcVal))
			}
		}
	case "values":
		if len(agg.StateColumns) >= 1 {
			col := agg.StateColumns[0]
			dstStr := dst.GetField(col).String()
			srcStr := src.GetField(col).String()
			if srcStr != "" && srcStr != "<null>" {
				var combined string
				if dstStr != "" && dstStr != "<null>" {
					combined = dstStr + "," + srcStr
				} else {
					combined = srcStr
				}
				// Deduplicate and cap cardinality to prevent unbounded growth.
				parts := strings.Split(combined, ",")
				seen := make(map[string]struct{}, len(parts))
				deduped := make([]string, 0, len(parts))
				for _, p := range parts {
					if p == "" || p == "<null>" {
						continue
					}
					if _, ok := seen[p]; !ok {
						seen[p] = struct{}{}
						deduped = append(deduped, p)
					}
				}
				if len(deduped) > maxValuesCardinality {
					deduped = deduped[:maxValuesCardinality]
				}
				dst.SetField(col, event.StringValue(strings.Join(deduped, ",")))
			}
		}
	case "first", "earliest":
		if len(agg.StateColumns) >= 1 {
			col := agg.StateColumns[0]
			if dst.GetField(col).IsNull() {
				dst.SetField(col, src.GetField(col))
			}
		}
	case "last", "latest":
		if len(agg.StateColumns) >= 1 {
			col := agg.StateColumns[0]
			srcVal := src.GetField(col)
			if !srcVal.IsNull() {
				dst.SetField(col, srcVal)
			}
		}
	}
}
