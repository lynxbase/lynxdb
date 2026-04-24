package views

import (
	"fmt"
	"time"
)

// RebuildRequired returns true if the mutation requires a full rebuild of the view.
// Safe mutations (no rebuild): retention change, aggregate removal.
// Unsafe mutations (rebuild required): filter change, GROUP BY change, new aggregate, partition change.
func RebuildRequired(old, updated ViewDefinition) bool {
	// Filter change requires rebuild.
	if old.Filter != updated.Filter {
		return true
	}

	// GROUP BY change requires rebuild.
	if !stringSliceEqual(old.GroupBy, updated.GroupBy) {
		return true
	}

	// New columns require rebuild.
	oldCols := make(map[string]struct{}, len(old.Columns))
	for _, c := range old.Columns {
		oldCols[c.Name] = struct{}{}
	}
	for _, c := range updated.Columns {
		if _, exists := oldCols[c.Name]; !exists {
			return true
		}
	}

	// New aggregations require rebuild.
	oldAggs := make(map[string]struct{}, len(old.Aggregations))
	for _, a := range old.Aggregations {
		oldAggs[a.Name] = struct{}{}
	}
	for _, a := range updated.Aggregations {
		if _, exists := oldAggs[a.Name]; !exists {
			return true
		}
	}

	// Query text change (covers time bucket, etc.).
	if old.Query != updated.Query {
		// Check if it's only a retention change — the query text shouldn't differ.
		return true
	}

	return false
}

// SafeUpdate applies safe mutations (retention, aggregate removal) without rebuild.
// Returns the updated definition.
func SafeUpdate(old ViewDefinition, retention time.Duration, removedAggs []string) ViewDefinition {
	updated := old
	updated.UpdatedAt = time.Now()

	if retention > 0 {
		updated.Retention = retention
	}

	if len(removedAggs) > 0 {
		removed := make(map[string]struct{}, len(removedAggs))
		for _, name := range removedAggs {
			removed[name] = struct{}{}
		}
		kept := make([]AggregationDef, 0, len(updated.Aggregations))
		for _, agg := range updated.Aggregations {
			if _, ok := removed[agg.Name]; !ok {
				kept = append(kept, agg)
			}
		}
		updated.Aggregations = kept
	}

	return updated
}

// StartRebuild initiates a versioned rebuild of a view.
// Creates v2 definition and marks the old one as rebuilding.
// Returns the new definition.
func StartRebuild(registry *ViewRegistry, newDef ViewDefinition) (ViewDefinition, error) {
	old, err := registry.Get(newDef.Name)
	if err != nil {
		return ViewDefinition{}, err
	}

	newDef.Version = old.Version + 1
	newDef.Status = ViewStatusBackfill
	newDef.CreatedAt = old.CreatedAt
	newDef.UpdatedAt = time.Now()

	// Mark old version as rebuilding.
	old.Status = ViewStatusRebuilding
	old.UpdatedAt = time.Now()
	if err := registry.Update(old); err != nil {
		return ViewDefinition{}, fmt.Errorf("views rebuild: update old: %w", err)
	}

	// Replace with new version.
	if err := registry.Update(newDef); err != nil {
		return ViewDefinition{}, fmt.Errorf("views rebuild: update new: %w", err)
	}

	return newDef, nil
}

// CompleteRebuild finalizes a rebuild by activating the new version.
func CompleteRebuild(registry *ViewRegistry, name string) error {
	def, err := registry.Get(name)
	if err != nil {
		return err
	}

	def.Status = ViewStatusActive
	def.UpdatedAt = time.Now()

	return registry.Update(def)
}

func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
