package part

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/storage/segment"
)

func generateTestEvents(n int) []*event.Event {
	rng := rand.New(rand.NewSource(42))
	base := time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)
	hosts := []string{"web-01", "web-02", "web-03", "db-01"}
	levels := []string{"INFO", "WARN", "ERROR", "DEBUG"}

	events := make([]*event.Event, n)
	ts := base
	for i := 0; i < n; i++ {
		ts = ts.Add(time.Duration(90+rng.Intn(20)) * time.Millisecond)
		host := hosts[rng.Intn(len(hosts))]
		level := levels[rng.Intn(len(levels))]
		status := int64(200 + rng.Intn(4)*100)

		raw := fmt.Sprintf("%s host=%s level=%s status=%d msg=\"request\"",
			ts.Format(time.RFC3339Nano), host, level, status)

		e := event.NewEvent(ts, raw)
		e.Host = host
		e.Source = "/var/log/app.log"
		e.SourceType = "json"
		e.Index = "main"
		e.SetField("level", event.StringValue(level))
		e.SetField("status", event.IntValue(status))
		events[i] = e
	}

	return events
}

func TestWriter_Basic(t *testing.T) {
	dir := t.TempDir()
	layout := NewLayout(dir)
	w := NewWriter(layout, segment.CompressionLZ4, DefaultRowGroupSize)

	events := generateTestEvents(100)
	ctx := context.Background()

	meta, err := w.Write(ctx, "main", events, 0)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Verify meta fields.
	if meta.Index != "main" {
		t.Errorf("Index: got %q, want %q", meta.Index, "main")
	}
	if meta.EventCount != 100 {
		t.Errorf("EventCount: got %d, want 100", meta.EventCount)
	}
	if meta.Level != 0 {
		t.Errorf("Level: got %d, want 0", meta.Level)
	}
	if meta.SizeBytes <= 0 {
		t.Errorf("SizeBytes: got %d, want > 0", meta.SizeBytes)
	}
	if meta.Tier != "hot" {
		t.Errorf("Tier: got %q, want %q", meta.Tier, "hot")
	}
	if meta.MinTime.IsZero() || meta.MaxTime.IsZero() {
		t.Error("MinTime/MaxTime should not be zero")
	}
	if meta.MinTime.After(meta.MaxTime) {
		t.Error("MinTime should be <= MaxTime")
	}
	if !strings.HasPrefix(meta.ID, "part-main-L0-") {
		t.Errorf("ID: got %q, expected prefix 'part-main-L0-'", meta.ID)
	}
	if meta.Partition == "" {
		t.Error("Partition should not be empty")
	}

	// Verify columns contain expected names.
	colSet := make(map[string]bool, len(meta.Columns))
	for _, c := range meta.Columns {
		colSet[c] = true
	}
	for _, expected := range []string{"_time", "_raw", "host", "level", "status"} {
		if !colSet[expected] {
			t.Errorf("missing column %q in meta.Columns", expected)
		}
	}
}

func TestWriter_FileIsValidSegment(t *testing.T) {
	dir := t.TempDir()
	layout := NewLayout(dir)
	w := NewWriter(layout, segment.CompressionLZ4, DefaultRowGroupSize)

	events := generateTestEvents(200)
	ctx := context.Background()

	meta, err := w.Write(ctx, "main", events, 0)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Verify the file is a valid .lsg segment readable by segment.OpenSegmentFile.
	ms, err := segment.OpenSegmentFile(meta.Path)
	if err != nil {
		t.Fatalf("OpenSegmentFile: %v", err)
	}
	defer ms.Close()

	if ms.Reader().EventCount() != 200 {
		t.Errorf("EventCount: got %d, want 200", ms.Reader().EventCount())
	}

	// Verify we can read events back.
	readEvents, err := ms.Reader().ReadEvents()
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(readEvents) != 200 {
		t.Fatalf("ReadEvents: got %d, want 200", len(readEvents))
	}

	// Spot-check first and last event.
	if !readEvents[0].Time.Equal(events[0].Time) {
		t.Errorf("event[0].Time: got %v, want %v", readEvents[0].Time, events[0].Time)
	}
	if readEvents[0].Host != events[0].Host {
		t.Errorf("event[0].Host: got %q, want %q", readEvents[0].Host, events[0].Host)
	}
}

func TestWriter_AtomicRename_NoTmpFiles(t *testing.T) {
	dir := t.TempDir()
	layout := NewLayout(dir)
	w := NewWriter(layout, segment.CompressionLZ4, DefaultRowGroupSize)

	events := generateTestEvents(50)
	ctx := context.Background()

	meta, err := w.Write(ctx, "main", events, 0)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Walk the partition directory and verify no tmp_* files remain.
	partDir := filepath.Dir(meta.Path)
	entries, err := os.ReadDir(partDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}

	for _, e := range entries {
		if IsTempFile(e.Name()) {
			t.Errorf("found leftover tmp file: %s", e.Name())
		}
	}

	// Verify the final file exists.
	if _, err := os.Stat(meta.Path); err != nil {
		t.Errorf("final file does not exist: %v", err)
	}
}

func TestWriter_MinMaxTime(t *testing.T) {
	dir := t.TempDir()
	layout := NewLayout(dir)
	w := NewWriter(layout, segment.CompressionLZ4, DefaultRowGroupSize)

	// Create events with known times, not in order.
	times := []time.Time{
		time.Date(2026, 3, 2, 10, 0, 0, 0, time.UTC),
		time.Date(2026, 3, 2, 5, 0, 0, 0, time.UTC),  // min
		time.Date(2026, 3, 2, 15, 0, 0, 0, time.UTC), // max
		time.Date(2026, 3, 2, 8, 0, 0, 0, time.UTC),
	}

	events := make([]*event.Event, len(times))
	for i, ts := range times {
		events[i] = event.NewEvent(ts, fmt.Sprintf("event %d", i))
		events[i].Index = "main"
	}

	ctx := context.Background()
	meta, err := w.Write(ctx, "main", events, 0)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	expectedMin := time.Date(2026, 3, 2, 5, 0, 0, 0, time.UTC)
	expectedMax := time.Date(2026, 3, 2, 15, 0, 0, 0, time.UTC)

	if !meta.MinTime.Equal(expectedMin) {
		t.Errorf("MinTime: got %v, want %v", meta.MinTime, expectedMin)
	}
	if !meta.MaxTime.Equal(expectedMax) {
		t.Errorf("MaxTime: got %v, want %v", meta.MaxTime, expectedMax)
	}
}

func TestWriter_Level(t *testing.T) {
	dir := t.TempDir()
	layout := NewLayout(dir)
	w := NewWriter(layout, segment.CompressionLZ4, DefaultRowGroupSize)

	events := generateTestEvents(10)
	ctx := context.Background()

	meta, err := w.Write(ctx, "main", events, 2)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	if meta.Level != 2 {
		t.Errorf("Level: got %d, want 2", meta.Level)
	}
	if !strings.Contains(meta.ID, "-L2-") {
		t.Errorf("ID should contain '-L2-': %q", meta.ID)
	}
}

func TestWriter_EmptyEvents(t *testing.T) {
	dir := t.TempDir()
	layout := NewLayout(dir)
	w := NewWriter(layout, segment.CompressionLZ4, DefaultRowGroupSize)

	ctx := context.Background()
	_, err := w.Write(ctx, "main", nil, 0)
	if err == nil {
		t.Fatal("expected error for empty events")
	}
}

func TestWriter_CanceledContext(t *testing.T) {
	dir := t.TempDir()
	layout := NewLayout(dir)
	w := NewWriter(layout, segment.CompressionLZ4, DefaultRowGroupSize)

	events := generateTestEvents(10)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	_, err := w.Write(ctx, "main", events, 0)
	if err == nil {
		t.Fatal("expected error for canceled context")
	}
}

func TestWriter_BloomAndInvertedIndex(t *testing.T) {
	dir := t.TempDir()
	layout := NewLayout(dir)
	w := NewWriter(layout, segment.CompressionLZ4, DefaultRowGroupSize)

	events := generateTestEvents(100)
	ctx := context.Background()

	meta, err := w.Write(ctx, "main", events, 0)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Open and check bloom filter and inverted index are present.
	ms, err := segment.OpenSegmentFile(meta.Path)
	if err != nil {
		t.Fatalf("OpenSegmentFile: %v", err)
	}
	defer ms.Close()

	bf, err := ms.Reader().BloomFilter()
	if err != nil {
		t.Fatalf("BloomFilter: %v", err)
	}
	if bf == nil {
		t.Error("expected bloom filter, got nil")
	}

	ii, err := ms.Reader().InvertedIndex()
	if err != nil {
		t.Fatalf("InvertedIndex: %v", err)
	}
	if ii == nil {
		t.Error("expected inverted index, got nil")
	}
}

func TestWriter_ZSTDCompression(t *testing.T) {
	dir := t.TempDir()
	layout := NewLayout(dir)
	w := NewWriter(layout, segment.CompressionZSTD, DefaultRowGroupSize)

	events := generateTestEvents(100)
	ctx := context.Background()

	meta, err := w.Write(ctx, "main", events, 0)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Verify file is readable.
	ms, err := segment.OpenSegmentFile(meta.Path)
	if err != nil {
		t.Fatalf("OpenSegmentFile: %v", err)
	}
	defer ms.Close()

	if ms.Reader().EventCount() != 100 {
		t.Errorf("EventCount: got %d, want 100", ms.Reader().EventCount())
	}
}

func TestIsTempFile(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{"tmp_abc123.lsg", true},
		{"tmp_.lsg", true},
		{"part-main-L0-12345.lsg", false},
		{"tmp_abc123.txt", false},
		{"something.lsg", false},
		{"tmp_", false},
	}

	for _, tc := range tests {
		if got := IsTempFile(tc.name); got != tc.want {
			t.Errorf("IsTempFile(%q): got %v, want %v", tc.name, got, tc.want)
		}
	}
}
