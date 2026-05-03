package server

import (
	"bytes"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	storageformat "github.com/lynxbase/lynxdb/pkg/storage/format"
	"github.com/lynxbase/lynxdb/pkg/storage/segment"
)

func TestValidateStorageFormatGreenfieldWritesMarker(t *testing.T) {
	dir := t.TempDir()
	e := &Engine{dataDir: dir, logger: slog.Default()}

	major, err := e.validateStorageFormat()
	if err != nil {
		t.Fatalf("validateStorageFormat: %v", err)
	}
	if major != segment.LSG_BINARY_MAX_MAJOR {
		t.Fatalf("major = %d, want %d", major, segment.LSG_BINARY_MAX_MAJOR)
	}
	got, err := storageformat.ReadMarker([]string{dir})
	if err != nil {
		t.Fatal(err)
	}
	if got != segment.LSG_BINARY_MAX_MAJOR {
		t.Fatalf("marker = %d, want %d", got, segment.LSG_BINARY_MAX_MAJOR)
	}
}

func TestValidateStorageFormatRefusesMissingMarkerWithSegments(t *testing.T) {
	dir := t.TempDir()
	writeBootTestSegment(t, dir)
	e := &Engine{dataDir: dir, logger: slog.Default()}

	_, err := e.validateStorageFormat()
	if !errors.Is(err, storageformat.ErrMissingMarker) {
		t.Fatalf("validateStorageFormat error = %v, want ErrMissingMarker", err)
	}
}

func TestValidateStorageFormatRefusesFutureMarker(t *testing.T) {
	dir := t.TempDir()
	if err := storageformat.WriteMarker([]string{dir}, segment.LSG_BINARY_MAX_MAJOR+1); err != nil {
		t.Fatal(err)
	}
	e := &Engine{dataDir: dir, logger: slog.Default()}

	_, err := e.validateStorageFormat()
	if !errors.Is(err, storageformat.ErrFutureFormat) {
		t.Fatalf("validateStorageFormat error = %v, want ErrFutureFormat", err)
	}
}

func writeBootTestSegment(t *testing.T, dir string) {
	t.Helper()
	segDir := filepath.Join(dir, "segments", "hot", "main", "2030-01-02")
	if err := os.MkdirAll(segDir, 0o755); err != nil {
		t.Fatal(err)
	}
	e := event.NewEvent(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC), "boot validation")
	e.Source = "boot.log"
	e.SourceType = "test"
	e.Host = "host"
	e.Index = "main"

	var buf bytes.Buffer
	w := segment.NewWriter(&buf)
	if _, err := w.Write([]*event.Event{e}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(segDir, "part-main-L0-1.lsg"), buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}
