package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLayout_Dirs(t *testing.T) {
	dir := t.TempDir()
	layout := NewLayout(dir)

	if layout.DataDir() != dir {
		t.Errorf("DataDir: got %q", layout.DataDir())
	}
	if !strings.HasSuffix(layout.SegmentCacheDir(), "segment-cache") {
		t.Errorf("SegmentCacheDir: got %q", layout.SegmentCacheDir())
	}
	if !strings.HasSuffix(layout.QueryCacheDir(), "query-cache") {
		t.Errorf("QueryCacheDir: got %q", layout.QueryCacheDir())
	}
}

func TestLayout_SegmentPath(t *testing.T) {
	dir := t.TempDir()
	layout := NewLayout(dir)

	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	path := layout.SegmentPath("main", 0, ts)

	expected := filepath.Join(dir, "segments", "hot", "main", "seg-main-L0-1704067200000000000.lsg")
	if path != expected {
		t.Errorf("SegmentPath:\n  got  %q\n  want %q", path, expected)
	}
}

func TestSegmentName(t *testing.T) {
	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	name := SegmentName("security", 2, ts)
	if !strings.HasPrefix(name, "seg-security-L2-") {
		t.Errorf("SegmentName: got %q", name)
	}
	if !strings.HasSuffix(name, ".lsg") {
		t.Errorf("SegmentName should end with .lsg: got %q", name)
	}
}

func TestLayout_EnsureDirs(t *testing.T) {
	dir := t.TempDir()
	layout := NewLayout(dir)

	if err := layout.EnsureDirs("main", "security"); err != nil {
		t.Fatalf("EnsureDirs: %v", err)
	}

	// Verify directories exist.
	for _, subdir := range []string{
		"segment-cache",
		"query-cache",
		filepath.Join("segments", "hot", "main"),
		filepath.Join("segments", "hot", "security"),
	} {
		full := filepath.Join(dir, subdir)
		info, err := os.Stat(full)
		if err != nil {
			t.Errorf("missing dir: %s: %v", subdir, err)

			continue
		}
		if !info.IsDir() {
			t.Errorf("%s is not a directory", subdir)
		}
	}
}

func TestLayout_EnsureDirs_Idempotent(t *testing.T) {
	dir := t.TempDir()
	layout := NewLayout(dir)

	if err := layout.EnsureDirs("main"); err != nil {
		t.Fatal(err)
	}
	// Calling again should not fail.
	if err := layout.EnsureDirs("main"); err != nil {
		t.Fatalf("second EnsureDirs failed: %v", err)
	}
}

func TestLayout_SegmentDir(t *testing.T) {
	dir := t.TempDir()
	layout := NewLayout(dir)

	segDir := layout.SegmentDir("main")
	expected := filepath.Join(dir, "segments", "hot", "main")
	if segDir != expected {
		t.Errorf("SegmentDir: got %q, want %q", segDir, expected)
	}
}

func TestLayout_ViewDirs(t *testing.T) {
	dir := t.TempDir()
	layout := NewLayout(dir)

	if got, want := layout.ViewsDir(), filepath.Join(dir, "views"); got != want {
		t.Errorf("ViewsDir: got %q, want %q", got, want)
	}
	if got, want := layout.ViewDir("mv_test"), filepath.Join(dir, "views", "mv_test"); got != want {
		t.Errorf("ViewDir: got %q, want %q", got, want)
	}
	if got, want := layout.ViewSegmentDir("mv_test"), filepath.Join(dir, "views", "mv_test", "segments"); got != want {
		t.Errorf("ViewSegmentDir: got %q, want %q", got, want)
	}
}

func TestLayout_EnsureViewDirs(t *testing.T) {
	dir := t.TempDir()
	layout := NewLayout(dir)

	if err := layout.EnsureViewDirs("mv_test"); err != nil {
		t.Fatalf("EnsureViewDirs: %v", err)
	}

	for _, subdir := range []string{
		filepath.Join("views", "mv_test"),
		filepath.Join("views", "mv_test", "segments"),
	} {
		full := filepath.Join(dir, subdir)
		info, err := os.Stat(full)
		if err != nil {
			t.Errorf("missing dir: %s: %v", subdir, err)

			continue
		}
		if !info.IsDir() {
			t.Errorf("%s is not a directory", subdir)
		}
	}
}
