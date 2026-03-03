package views

import (
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
	"github.com/OrlovEvgeny/Lynxdb/pkg/storage"
)

func TestEnforceRetention_RemovesExpired(t *testing.T) {
	dir := t.TempDir()
	layout := storage.NewLayout(dir)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	def := ViewDefinition{
		Name:      "mv_ret",
		Retention: 1 * time.Hour,
	}

	segDir := layout.ViewSegmentDir("mv_ret")
	os.MkdirAll(segDir, 0o755)

	// Create an old segment (2 hours ago).
	oldEvent := event.NewEvent(time.Now().Add(-2*time.Hour), "old event")
	oldEvent.Index = "mv_ret"
	writeTestSegment(t, segDir, []*event.Event{oldEvent})

	// Create a recent segment.
	newEvent := event.NewEvent(time.Now(), "new event")
	newEvent.Index = "mv_ret"
	time.Sleep(time.Millisecond) // ensure different filename
	writeTestSegment(t, segDir, []*event.Event{newEvent})

	entries, _ := os.ReadDir(segDir)
	if len(entries) != 2 {
		t.Fatalf("expected 2 segments, got %d", len(entries))
	}

	if err := EnforceRetention(def, layout, logger); err != nil {
		t.Fatalf("EnforceRetention: %v", err)
	}

	entries, _ = os.ReadDir(segDir)
	if len(entries) != 1 {
		t.Errorf("expected 1 segment after retention, got %d", len(entries))
	}
}

func TestEnforceRetention_NoRetention(t *testing.T) {
	dir := t.TempDir()
	layout := storage.NewLayout(dir)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	def := ViewDefinition{
		Name:      "mv_noret",
		Retention: 0, // No limit.
	}

	segDir := layout.ViewSegmentDir("mv_noret")
	os.MkdirAll(segDir, 0o755)

	oldEvent := event.NewEvent(time.Now().Add(-24*365*time.Hour), "ancient event")
	oldEvent.Index = "mv_noret"
	writeTestSegment(t, segDir, []*event.Event{oldEvent})

	if err := EnforceRetention(def, layout, logger); err != nil {
		t.Fatalf("EnforceRetention: %v", err)
	}

	entries, _ := os.ReadDir(segDir)
	if len(entries) != 1 {
		t.Errorf("expected 1 segment (no retention), got %d", len(entries))
	}
}

func TestEnforceRetention_AllExpired(t *testing.T) {
	dir := t.TempDir()
	layout := storage.NewLayout(dir)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	def := ViewDefinition{
		Name:      "mv_allold",
		Retention: 1 * time.Hour,
	}

	segDir := layout.ViewSegmentDir("mv_allold")
	os.MkdirAll(segDir, 0o755)

	for i := 0; i < 3; i++ {
		e := event.NewEvent(time.Now().Add(-3*time.Hour), fmt.Sprintf("old %d", i))
		e.Index = "mv_allold"
		writeTestSegment(t, segDir, []*event.Event{e})
		time.Sleep(time.Millisecond)
	}

	if err := EnforceRetention(def, layout, logger); err != nil {
		t.Fatalf("EnforceRetention: %v", err)
	}

	entries, _ := os.ReadDir(segDir)
	if len(entries) != 0 {
		t.Errorf("expected 0 segments after retention, got %d", len(entries))
	}
}
