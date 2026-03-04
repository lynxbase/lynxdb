package views

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestRebuildRequired_FilterChange(t *testing.T) {
	old := ViewDefinition{Filter: "source=nginx"}
	updated := ViewDefinition{Filter: "source=api"}
	if !RebuildRequired(old, updated) {
		t.Error("expected rebuild required for filter change")
	}
}

func TestRebuildRequired_GroupByChange(t *testing.T) {
	old := ViewDefinition{GroupBy: []string{"source"}}
	updated := ViewDefinition{GroupBy: []string{"source", "host"}}
	if !RebuildRequired(old, updated) {
		t.Error("expected rebuild required for GROUP BY change")
	}
}

func TestRebuildRequired_NewAggregate(t *testing.T) {
	old := ViewDefinition{
		Aggregations: []AggregationDef{{Name: "count", Type: "count"}},
	}
	updated := ViewDefinition{
		Aggregations: []AggregationDef{
			{Name: "count", Type: "count"},
			{Name: "avg(duration)", Type: "avg"},
		},
	}
	if !RebuildRequired(old, updated) {
		t.Error("expected rebuild required for new aggregate")
	}
}

func TestRebuildRequired_RetentionOnlyChange(t *testing.T) {
	old := ViewDefinition{
		Filter:    "source=nginx",
		GroupBy:   []string{"source"},
		Columns:   []ColumnDef{{Name: "source"}},
		Retention: 30 * 24 * time.Hour,
	}
	updated := old
	updated.Retention = 90 * 24 * time.Hour
	// Same filter, GROUP BY, columns, aggregations — no rebuild.
	if RebuildRequired(old, updated) {
		t.Error("expected no rebuild for retention-only change")
	}
}

func TestRebuildRequired_AggRemoval(t *testing.T) {
	old := ViewDefinition{
		Filter:  "source=nginx",
		Columns: []ColumnDef{{Name: "source"}},
		Aggregations: []AggregationDef{
			{Name: "count", Type: "count"},
			{Name: "avg(duration)", Type: "avg"},
		},
	}
	updated := ViewDefinition{
		Filter:  "source=nginx",
		Columns: []ColumnDef{{Name: "source"}},
		Aggregations: []AggregationDef{
			{Name: "count", Type: "count"},
		},
	}
	// Removing an aggregate doesn't require rebuild.
	if RebuildRequired(old, updated) {
		t.Error("expected no rebuild for aggregate removal")
	}
}

func TestSafeUpdate_Retention(t *testing.T) {
	def := ViewDefinition{
		Name:      "mv_test",
		Retention: 30 * 24 * time.Hour,
	}

	updated := SafeUpdate(def, 90*24*time.Hour, nil)
	if updated.Retention != 90*24*time.Hour {
		t.Errorf("retention: got %v, want 90d", updated.Retention)
	}
}

func TestSafeUpdate_RemoveAgg(t *testing.T) {
	def := ViewDefinition{
		Name: "mv_test",
		Aggregations: []AggregationDef{
			{Name: "count", Type: "count"},
			{Name: "avg(x)", Type: "avg"},
			{Name: "max(x)", Type: "max"},
		},
	}

	updated := SafeUpdate(def, 0, []string{"avg(x)"})
	if len(updated.Aggregations) != 2 {
		t.Errorf("aggregations: got %d, want 2", len(updated.Aggregations))
	}
	for _, agg := range updated.Aggregations {
		if agg.Name == "avg(x)" {
			t.Error("avg(x) should have been removed")
		}
	}
}

func TestStartRebuild(t *testing.T) {
	dir := t.TempDir()
	viewsDir := filepath.Join(dir, "views")
	os.MkdirAll(viewsDir, 0o755)

	reg, err := Open(viewsDir)
	if err != nil {
		t.Fatal(err)
	}

	old := ViewDefinition{
		Name:    "mv_rebuild",
		Version: 1,
		Type:    ViewTypeAggregation,
		Filter:  "source=nginx",
		Columns: []ColumnDef{{Name: "_time", Type: event.FieldTypeTimestamp}},
		Status:  ViewStatusActive,
	}
	reg.Create(old)

	newDef := old
	newDef.Filter = "source=api"

	rebuilt, err := StartRebuild(reg, newDef)
	if err != nil {
		t.Fatalf("StartRebuild: %v", err)
	}

	if rebuilt.Version != 2 {
		t.Errorf("version: got %d, want 2", rebuilt.Version)
	}
	if rebuilt.Status != ViewStatusBackfill {
		t.Errorf("status: got %v, want backfill", rebuilt.Status)
	}

	// Verify in registry.
	stored, _ := reg.Get("mv_rebuild")
	if stored.Status != ViewStatusBackfill {
		t.Errorf("stored status: got %v, want backfill", stored.Status)
	}
}

func TestCompleteRebuild(t *testing.T) {
	dir := t.TempDir()
	viewsDir := filepath.Join(dir, "views")
	os.MkdirAll(viewsDir, 0o755)

	reg, _ := Open(viewsDir)

	def := ViewDefinition{
		Name:    "mv_complete",
		Version: 2,
		Columns: []ColumnDef{{Name: "_time", Type: event.FieldTypeTimestamp}},
		Status:  ViewStatusBackfill,
	}
	reg.Create(def)

	if err := CompleteRebuild(reg, "mv_complete"); err != nil {
		t.Fatalf("CompleteRebuild: %v", err)
	}

	stored, _ := reg.Get("mv_complete")
	if stored.Status != ViewStatusActive {
		t.Errorf("status after complete: got %v, want active", stored.Status)
	}
}
