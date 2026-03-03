package part

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/storage/segment"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

func TestRegistry_AddAndGet(t *testing.T) {
	r := NewRegistry(testLogger())

	meta := &Meta{
		ID:         "part-main-L0-1",
		Index:      "main",
		MinTime:    time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC),
		MaxTime:    time.Date(2026, 3, 2, 1, 0, 0, 0, time.UTC),
		EventCount: 100,
		Level:      0,
	}

	r.Add(meta)

	got := r.Get("part-main-L0-1")
	if got == nil {
		t.Fatal("expected meta, got nil")
	}
	if got.EventCount != 100 {
		t.Errorf("EventCount: got %d, want 100", got.EventCount)
	}
}

func TestRegistry_GetNonExistent(t *testing.T) {
	r := NewRegistry(testLogger())

	if got := r.Get("nonexistent"); got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

func TestRegistry_Remove(t *testing.T) {
	r := NewRegistry(testLogger())

	meta := &Meta{
		ID:    "part-main-L0-1",
		Index: "main",
		Level: 0,
	}

	r.Add(meta)
	if r.Count() != 1 {
		t.Fatalf("Count after Add: got %d, want 1", r.Count())
	}

	r.Remove("part-main-L0-1")
	if r.Count() != 0 {
		t.Fatalf("Count after Remove: got %d, want 0", r.Count())
	}
	if r.Get("part-main-L0-1") != nil {
		t.Error("Get after Remove should return nil")
	}
}

func TestRegistry_RemoveNonExistent(t *testing.T) {
	r := NewRegistry(testLogger())
	// Should not panic.
	r.Remove("nonexistent")
}

func TestRegistry_All(t *testing.T) {
	r := NewRegistry(testLogger())

	for i := 0; i < 5; i++ {
		r.Add(&Meta{
			ID:    Filename("main", 0, time.Now().Add(time.Duration(i)*time.Second)),
			Index: "main",
			Level: 0,
		})
	}

	all := r.All()
	if len(all) != 5 {
		t.Errorf("All: got %d, want 5", len(all))
	}
}

func TestRegistry_ByIndex(t *testing.T) {
	r := NewRegistry(testLogger())

	// Add parts for different indexes.
	r.Add(&Meta{
		ID:      "p1",
		Index:   "main",
		Level:   0,
		MinTime: time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC),
	})
	r.Add(&Meta{
		ID:      "p2",
		Index:   "nginx",
		Level:   0,
		MinTime: time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC),
	})
	r.Add(&Meta{
		ID:      "p3",
		Index:   "main",
		Level:   0,
		MinTime: time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
	})

	mainParts := r.ByIndex("main")
	if len(mainParts) != 2 {
		t.Fatalf("ByIndex(main): got %d, want 2", len(mainParts))
	}

	// Verify sorted by MinTime ascending.
	if mainParts[0].MinTime.After(mainParts[1].MinTime) {
		t.Error("ByIndex should return parts sorted by MinTime ascending")
	}

	nginxParts := r.ByIndex("nginx")
	if len(nginxParts) != 1 {
		t.Errorf("ByIndex(nginx): got %d, want 1", len(nginxParts))
	}

	emptyParts := r.ByIndex("nonexistent")
	if len(emptyParts) != 0 {
		t.Errorf("ByIndex(nonexistent): got %d, want 0", len(emptyParts))
	}
}

func TestRegistry_ByLevel(t *testing.T) {
	r := NewRegistry(testLogger())

	r.Add(&Meta{ID: "p1", Index: "main", Level: 0})
	r.Add(&Meta{ID: "p2", Index: "main", Level: 0})
	r.Add(&Meta{ID: "p3", Index: "main", Level: 1})

	l0 := r.ByLevel(0)
	if len(l0) != 2 {
		t.Errorf("ByLevel(0): got %d, want 2", len(l0))
	}

	l1 := r.ByLevel(1)
	if len(l1) != 1 {
		t.Errorf("ByLevel(1): got %d, want 1", len(l1))
	}

	l2 := r.ByLevel(2)
	if len(l2) != 0 {
		t.Errorf("ByLevel(2): got %d, want 0", len(l2))
	}
}

func TestRegistry_Count(t *testing.T) {
	r := NewRegistry(testLogger())

	if r.Count() != 0 {
		t.Errorf("initial Count: got %d, want 0", r.Count())
	}

	r.Add(&Meta{ID: "p1", Index: "main", Level: 0})
	r.Add(&Meta{ID: "p2", Index: "main", Level: 0})

	if r.Count() != 2 {
		t.Errorf("Count: got %d, want 2", r.Count())
	}
}

func TestRegistry_CountByIndex(t *testing.T) {
	r := NewRegistry(testLogger())

	r.Add(&Meta{ID: "p1", Index: "main", Level: 0})
	r.Add(&Meta{ID: "p2", Index: "main", Level: 0})
	r.Add(&Meta{ID: "p3", Index: "nginx", Level: 0})

	if r.CountByIndex("main") != 2 {
		t.Errorf("CountByIndex(main): got %d, want 2", r.CountByIndex("main"))
	}
	if r.CountByIndex("nginx") != 1 {
		t.Errorf("CountByIndex(nginx): got %d, want 1", r.CountByIndex("nginx"))
	}
	if r.CountByIndex("nonexistent") != 0 {
		t.Errorf("CountByIndex(nonexistent): got %d, want 0", r.CountByIndex("nonexistent"))
	}
}

func TestRegistry_Indexes(t *testing.T) {
	r := NewRegistry(testLogger())

	r.Add(&Meta{ID: "p1", Index: "main", Level: 0})
	r.Add(&Meta{ID: "p2", Index: "nginx", Level: 0})
	r.Add(&Meta{ID: "p3", Index: "api", Level: 0})

	indexes := r.Indexes()
	if len(indexes) != 3 {
		t.Fatalf("Indexes: got %d, want 3", len(indexes))
	}

	// Should be sorted.
	if indexes[0] != "api" || indexes[1] != "main" || indexes[2] != "nginx" {
		t.Errorf("Indexes: got %v, want [api main nginx]", indexes)
	}
}

func TestRegistry_ScanDir(t *testing.T) {
	dir := t.TempDir()
	layout := NewLayout(dir)
	w := NewWriter(layout, segment.CompressionLZ4, DefaultRowGroupSize)
	ctx := context.Background()

	// Write several parts across two indexes.
	for i := 0; i < 3; i++ {
		events := generateTestEvents(50)
		if _, err := w.Write(ctx, "main", events, 0); err != nil {
			t.Fatalf("Write main[%d]: %v", i, err)
		}
	}
	for i := 0; i < 2; i++ {
		events := generateTestEvents(30)
		for _, ev := range events {
			ev.Index = "nginx"
		}
		if _, err := w.Write(ctx, "nginx", events, 0); err != nil {
			t.Fatalf("Write nginx[%d]: %v", i, err)
		}
	}

	// Create a fresh registry and scan the directory.
	reg := NewRegistry(testLogger())
	if err := reg.ScanDir(layout); err != nil {
		t.Fatalf("ScanDir: %v", err)
	}

	if reg.Count() != 5 {
		t.Errorf("Count: got %d, want 5", reg.Count())
	}
	if reg.CountByIndex("main") != 3 {
		t.Errorf("CountByIndex(main): got %d, want 3", reg.CountByIndex("main"))
	}
	if reg.CountByIndex("nginx") != 2 {
		t.Errorf("CountByIndex(nginx): got %d, want 2", reg.CountByIndex("nginx"))
	}

	// Verify all parts have valid metadata.
	for _, meta := range reg.All() {
		if meta.EventCount <= 0 {
			t.Errorf("part %s: EventCount=%d, want > 0", meta.ID, meta.EventCount)
		}
		if meta.SizeBytes <= 0 {
			t.Errorf("part %s: SizeBytes=%d, want > 0", meta.ID, meta.SizeBytes)
		}
		if meta.MinTime.IsZero() {
			t.Errorf("part %s: MinTime is zero", meta.ID)
		}
		if meta.Path == "" {
			t.Errorf("part %s: Path is empty", meta.ID)
		}
		if meta.Tier != "hot" {
			t.Errorf("part %s: Tier=%q, want %q", meta.ID, meta.Tier, "hot")
		}
		if meta.Partition == "" {
			t.Errorf("part %s: Partition is empty", meta.ID)
		}
	}
}

func TestRegistry_ScanDir_CleansTmpFiles(t *testing.T) {
	dir := t.TempDir()
	layout := NewLayout(dir)
	w := NewWriter(layout, segment.CompressionLZ4, DefaultRowGroupSize)
	ctx := context.Background()

	// Write a valid part.
	events := generateTestEvents(20)
	meta, err := w.Write(ctx, "main", events, 0)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Manually create tmp_* files (simulating a crash mid-write).
	partDir := filepath.Dir(meta.Path)
	tmpPaths := []string{
		filepath.Join(partDir, "tmp_crash1.lsg"),
		filepath.Join(partDir, "tmp_crash2.lsg"),
	}
	for _, p := range tmpPaths {
		if err := os.WriteFile(p, []byte("incomplete"), 0o644); err != nil {
			t.Fatalf("create tmp file: %v", err)
		}
	}

	// Scan — should clean up tmp files and load the valid part.
	reg := NewRegistry(testLogger())
	if err := reg.ScanDir(layout); err != nil {
		t.Fatalf("ScanDir: %v", err)
	}

	// Only the valid part should be registered.
	if reg.Count() != 1 {
		t.Errorf("Count: got %d, want 1", reg.Count())
	}

	// Tmp files should be gone.
	for _, p := range tmpPaths {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("tmp file should have been removed: %s", p)
		}
	}
}

func TestRegistry_ScanDir_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	layout := NewLayout(dir)

	reg := NewRegistry(testLogger())
	if err := reg.ScanDir(layout); err != nil {
		t.Fatalf("ScanDir: %v", err)
	}

	if reg.Count() != 0 {
		t.Errorf("Count: got %d, want 0", reg.Count())
	}
}

func TestRegistry_RemoveUpdatesAllMaps(t *testing.T) {
	r := NewRegistry(testLogger())

	meta := &Meta{
		ID:      "p1",
		Index:   "main",
		Level:   0,
		MinTime: time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC),
	}
	r.Add(meta)

	if r.CountByIndex("main") != 1 {
		t.Fatalf("CountByIndex before remove: got %d", r.CountByIndex("main"))
	}
	if len(r.ByLevel(0)) != 1 {
		t.Fatalf("ByLevel(0) before remove: got %d", len(r.ByLevel(0)))
	}

	r.Remove("p1")

	if r.CountByIndex("main") != 0 {
		t.Errorf("CountByIndex after remove: got %d, want 0", r.CountByIndex("main"))
	}
	if len(r.ByLevel(0)) != 0 {
		t.Errorf("ByLevel(0) after remove: got %d, want 0", len(r.ByLevel(0)))
	}
}

func TestParseLevelFromFilename(t *testing.T) {
	tests := []struct {
		name  string
		level int
	}{
		{"part-main-L0-12345.lsg", 0},
		{"part-main-L1-12345.lsg", 1},
		{"part-main-L2-12345.lsg", 2},
		{"part-nginx-L10-12345.lsg", 10},
		{"unknown-format.lsg", 0},
		{"noL.lsg", 0},
	}

	for _, tc := range tests {
		got := parseLevelFromFilename(tc.name)
		if got != tc.level {
			t.Errorf("parseLevelFromFilename(%q): got %d, want %d", tc.name, got, tc.level)
		}
	}
}
