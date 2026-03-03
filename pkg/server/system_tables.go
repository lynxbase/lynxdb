package server

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
	"github.com/OrlovEvgeny/Lynxdb/pkg/storage/part"
)

// systemTableResolver implements pipeline.SystemTableResolver using the
// Engine's part registry and compactor to populate virtual system tables.
type systemTableResolver struct {
	engine *Engine
}

// ResolveSystemTable returns rows for the given system table.
// Supported tables: "parts", "columns".
func (r *systemTableResolver) ResolveSystemTable(ctx context.Context, table string) ([]map[string]event.Value, error) {
	switch table {
	case "parts":
		return r.resolveParts(ctx)
	case "columns":
		return r.resolveColumns(ctx)
	default:
		return nil, fmt.Errorf("unknown system table %q (available: system.parts, system.columns)", table)
	}
}

// resolveParts returns one row per part from the part registry.
// Columns: id, index, partition, level, event_count, size_bytes, min_time, max_time, columns, tier, created_at.
func (r *systemTableResolver) resolveParts(_ context.Context) ([]map[string]event.Value, error) {
	registry := r.engine.partRegistry
	if registry == nil {
		// In-memory mode: build rows from segmentHandle metadata.
		return r.resolvePartsInMemory(), nil
	}

	parts := registry.All()
	rows := make([]map[string]event.Value, 0, len(parts))

	for _, p := range parts {
		rows = append(rows, partToRow(p))
	}

	return rows, nil
}

// resolvePartsInMemory builds system.parts rows from in-memory segments
// when no part registry exists (in-memory mode without a data directory).
func (r *systemTableResolver) resolvePartsInMemory() []map[string]event.Value {
	r.engine.mu.RLock()
	segs := r.engine.currentEpoch.segments
	r.engine.mu.RUnlock()

	rows := make([]map[string]event.Value, 0, len(segs))

	for _, sh := range segs {
		row := map[string]event.Value{
			"id":          event.StringValue(sh.meta.ID),
			"index":       event.StringValue(sh.index),
			"level":       event.IntValue(int64(sh.meta.Level)),
			"event_count": event.IntValue(sh.meta.EventCount),
			"size_bytes":  event.IntValue(sh.meta.SizeBytes),
			"min_time":    event.StringValue(sh.meta.MinTime.Format("2006-01-02T15:04:05Z")),
			"max_time":    event.StringValue(sh.meta.MaxTime.Format("2006-01-02T15:04:05Z")),
			"tier":        event.StringValue("hot"),
		}
		rows = append(rows, row)
	}

	return rows
}

func partToRow(p *part.Meta) map[string]event.Value {
	tier := p.Tier
	if tier == "" {
		tier = "hot"
	}

	cols := strings.Join(p.Columns, ",")

	return map[string]event.Value{
		"id":           event.StringValue(p.ID),
		"index":        event.StringValue(p.Index),
		"partition":    event.StringValue(p.Partition),
		"level":        event.IntValue(int64(p.Level)),
		"event_count":  event.IntValue(p.EventCount),
		"size_bytes":   event.IntValue(p.SizeBytes),
		"min_time":     event.StringValue(p.MinTime.Format("2006-01-02T15:04:05Z")),
		"max_time":     event.StringValue(p.MaxTime.Format("2006-01-02T15:04:05Z")),
		"columns":      event.StringValue(cols),
		"column_count": event.IntValue(int64(len(p.Columns))),
		"tier":         event.StringValue(tier),
		"created_at":   event.StringValue(p.CreatedAt.Format("2006-01-02T15:04:05Z")),
	}
}

// resolveColumns returns one row per distinct column name found across all parts.
// Columns: name, type, part_count, total_events, coverage_pct.
func (r *systemTableResolver) resolveColumns(_ context.Context) ([]map[string]event.Value, error) {
	// Collect column statistics from part registry or in-memory segments.
	type colStats struct {
		partCount   int
		totalEvents int64
	}

	stats := make(map[string]*colStats)
	var totalParts int
	var totalEvents int64

	if r.engine.partRegistry != nil {
		parts := r.engine.partRegistry.All()
		totalParts = len(parts)

		for _, p := range parts {
			totalEvents += p.EventCount
			for _, col := range p.Columns {
				cs, ok := stats[col]
				if !ok {
					cs = &colStats{}
					stats[col] = cs
				}

				cs.partCount++
				cs.totalEvents += p.EventCount
			}
		}
	} else {
		r.engine.mu.RLock()
		segs := r.engine.currentEpoch.segments
		r.engine.mu.RUnlock()

		totalParts = len(segs)

		for _, sh := range segs {
			totalEvents += sh.meta.EventCount
			if sh.reader != nil {
				for _, col := range sh.reader.ColumnNames() {
					cs, ok := stats[col]
					if !ok {
						cs = &colStats{}
						stats[col] = cs
					}

					cs.partCount++
					cs.totalEvents += sh.meta.EventCount
				}
			}
		}
	}

	rows := make([]map[string]event.Value, 0, len(stats))

	for name, cs := range stats {
		coveragePct := float64(0)
		if totalParts > 0 {
			coveragePct = float64(cs.partCount) / float64(totalParts) * 100
		}

		rows = append(rows, map[string]event.Value{
			"name":         event.StringValue(name),
			"part_count":   event.IntValue(int64(cs.partCount)),
			"total_events": event.IntValue(cs.totalEvents),
			"coverage_pct": event.StringValue(strconv.FormatFloat(coveragePct, 'f', 1, 64)),
		})
	}

	return rows, nil
}
