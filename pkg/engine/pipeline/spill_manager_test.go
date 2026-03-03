package pipeline

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSpillManagerCreateAndRelease(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	f, err := mgr.NewSpillFile("test")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	f.Close()

	// File should exist.
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("spill file should exist: %v", err)
	}

	count, _ := mgr.Stats()
	if count != 1 {
		t.Fatalf("expected 1 tracked file, got %d", count)
	}

	// Release should delete it.
	mgr.Release(path)

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("spill file should be deleted after Release")
	}

	count, _ = mgr.Stats()
	if count != 0 {
		t.Fatalf("expected 0 tracked files after release, got %d", count)
	}
}

func TestSpillManagerCleanupAll(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}

	var paths []string
	for i := 0; i < 5; i++ {
		f, err := mgr.NewSpillFile("test")
		if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, f.Name())
		f.Close()
	}

	count, _ := mgr.Stats()
	if count != 5 {
		t.Fatalf("expected 5 tracked files, got %d", count)
	}

	mgr.CleanupAll()

	for _, path := range paths {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("spill file should be deleted after CleanupAll: %s", path)
		}
	}

	count, _ = mgr.Stats()
	if count != 0 {
		t.Fatalf("expected 0 tracked files after CleanupAll, got %d", count)
	}
}

func TestSpillManagerStats(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	count, bytes := mgr.Stats()
	if count != 0 || bytes != 0 {
		t.Fatalf("expected (0, 0) initially, got (%d, %d)", count, bytes)
	}

	f1, _ := mgr.NewSpillFile("a")
	f1.Close()
	f2, _ := mgr.NewSpillFile("b")
	f2.Close()

	count, _ = mgr.Stats()
	if count != 2 {
		t.Fatalf("expected 2, got %d", count)
	}

	mgr.TrackBytes(1024)
	_, bytes = mgr.Stats()
	if bytes != 1024 {
		t.Fatalf("expected 1024 bytes, got %d", bytes)
	}
}

func TestSpillManagerStartupCleanup(t *testing.T) {
	dir := t.TempDir()

	// Create orphan spill files as if from a previous crash.
	orphanDir := filepath.Join(dir, "lynxdb-spill-99999")
	if err := os.MkdirAll(orphanDir, 0o700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		f, err := os.CreateTemp(orphanDir, "lynxdb-spill-orphan-*.tmp")
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
	}

	// Verify orphans exist.
	entries, _ := os.ReadDir(orphanDir)
	if len(entries) != 3 {
		t.Fatalf("expected 3 orphan files, got %d", len(entries))
	}

	removed := CleanupOrphans(dir, nil)
	if removed != 3 {
		t.Fatalf("expected 3 removed, got %d", removed)
	}

	// Directory should also be removed.
	if _, err := os.Stat(orphanDir); !os.IsNotExist(err) {
		t.Fatalf("orphan directory should be removed")
	}
}

func TestSpillManagerNilSafe(t *testing.T) {
	var mgr *SpillManager

	// All methods should be no-ops on nil receiver.
	mgr.Release("/nonexistent")
	mgr.TrackBytes(100)
	mgr.CleanupAll()

	count, bytes := mgr.Stats()
	if count != 0 || bytes != 0 {
		t.Fatalf("nil Stats should return (0, 0)")
	}
	if mgr.Dir() != "" {
		t.Fatalf("nil Dir should return empty string")
	}
}

func TestSpillManagerFilePrefix(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	f, err := mgr.NewSpillFile("sort")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	base := filepath.Base(f.Name())
	if !strings.HasPrefix(base, "lynxdb-spill-sort-") {
		t.Fatalf("expected prefix 'lynxdb-spill-sort-', got %s", base)
	}
}

func TestCleanupOrphansEmptyDir(t *testing.T) {
	removed := CleanupOrphans("", nil)
	if removed != 0 {
		t.Fatalf("expected 0 for empty dir, got %d", removed)
	}
}

func TestCleanupOrphansNonexistentDir(t *testing.T) {
	removed := CleanupOrphans("/nonexistent/path/that/does/not/exist", nil)
	if removed != 0 {
		t.Fatalf("expected 0 for nonexistent dir, got %d", removed)
	}
}

func TestSpillManagerQuotaEnforcement(t *testing.T) {
	// Create a manager with a very small quota (1KB).
	mgr, err := NewSpillManagerWithQuota(t.TempDir(), 1024, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// First file should succeed.
	f1, err := mgr.NewSpillFile("test")
	if err != nil {
		t.Fatal(err)
	}
	// Write 1KB of data so os.Stat in Release() reports the correct size.
	data := make([]byte, 1024)
	if _, writeErr := f1.Write(data); writeErr != nil {
		t.Fatal(writeErr)
	}
	f1.Close()

	// Track the written bytes so the quota counter is accurate.
	mgr.TrackBytes(1024)

	// Second file should fail with ErrTempSpaceFull.
	_, err = mgr.NewSpillFile("test")
	if err == nil {
		t.Fatal("expected ErrTempSpaceFull, got nil")
	}
	if !errors.Is(err, ErrTempSpaceFull) {
		t.Fatalf("expected ErrTempSpaceFull, got: %v", err)
	}

	// After releasing the first file (which decrements totalBytes via os.Stat),
	// creating a new file should succeed again.
	mgr.Release(f1.Name())

	f2, err := mgr.NewSpillFile("test")
	if err != nil {
		t.Fatalf("expected success after release, got: %v", err)
	}
	f2.Close()
	mgr.Release(f2.Name())
}

func TestSpillManagerQuotaZeroUnlimited(t *testing.T) {
	// maxBytes=0 means unlimited — should never fail.
	mgr, err := NewSpillManagerWithQuota(t.TempDir(), 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	mgr.TrackBytes(1 << 40) // pretend 1TB tracked

	f, err := mgr.NewSpillFile("test")
	if err != nil {
		t.Fatalf("unlimited quota should not fail: %v", err)
	}
	f.Close()
}
