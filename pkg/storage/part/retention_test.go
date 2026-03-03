package part

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
	"github.com/OrlovEvgeny/Lynxdb/pkg/storage/segment"
)

func writeTestPart(t *testing.T, layout *Layout, index string, ts time.Time) *Meta {
	t.Helper()

	writer := NewWriter(layout, segment.CompressionLZ4, DefaultRowGroupSize)

	ev := &event.Event{
		Time:   ts,
		Raw:    "test retention event",
		Source: "test",
		Index:  index,
		Fields: map[string]event.Value{
			"level": event.StringValue("info"),
		},
	}

	meta, err := writer.Write(context.Background(), index, []*event.Event{ev}, 0)
	if err != nil {
		t.Fatalf("write test part: %v", err)
	}

	return meta
}

func TestRetentionManager_DeletesOldPartitions(t *testing.T) {
	dir := t.TempDir()
	layout := NewLayout(dir)
	registry := NewRegistry(testLogger())

	// Create parts in different date partitions.
	now := time.Now().UTC()
	old := now.Add(-100 * 24 * time.Hour)
	recent := now.Add(-1 * time.Hour)

	oldMeta := writeTestPart(t, layout, "main", old)
	registry.Add(oldMeta)

	recentMeta := writeTestPart(t, layout, "main", recent)
	registry.Add(recentMeta)

	if registry.Count() != 2 {
		t.Fatalf("expected 2 parts, got %d", registry.Count())
	}

	// Run retention with 90-day max age.
	rm := NewRetentionManager(layout, registry, RetentionConfig{
		MaxAge:   90 * 24 * time.Hour,
		Interval: 1 * time.Hour,
	}, testLogger())

	deleted, err := rm.RunOnce()
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if deleted != 1 {
		t.Errorf("expected 1 partition deleted, got %d", deleted)
	}

	// Old part should be removed from registry.
	if registry.Get(oldMeta.ID) != nil {
		t.Error("expected old part to be removed from registry")
	}

	// Recent part should still be in registry.
	if registry.Get(recentMeta.ID) == nil {
		t.Error("expected recent part to still be in registry")
	}

	// Old partition directory should be deleted.
	oldDir := layout.PartitionDir("main", old)
	if _, err := os.Stat(oldDir); !os.IsNotExist(err) {
		t.Errorf("expected old partition dir to be deleted: %s", oldDir)
	}

	// Recent partition directory should still exist.
	recentDir := layout.PartitionDir("main", recent)
	if _, err := os.Stat(recentDir); err != nil {
		t.Errorf("expected recent partition dir to still exist: %s", recentDir)
	}
}

func TestRetentionManager_MultipleIndexes(t *testing.T) {
	dir := t.TempDir()
	layout := NewLayout(dir)
	registry := NewRegistry(testLogger())

	now := time.Now().UTC()
	old := now.Add(-100 * 24 * time.Hour)

	// Create old parts in different indexes.
	meta1 := writeTestPart(t, layout, "nginx", old)
	registry.Add(meta1)

	meta2 := writeTestPart(t, layout, "redis", old)
	registry.Add(meta2)

	rm := NewRetentionManager(layout, registry, RetentionConfig{
		MaxAge: 90 * 24 * time.Hour,
	}, testLogger())

	deleted, err := rm.RunOnce()
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if deleted != 2 {
		t.Errorf("expected 2 partitions deleted, got %d", deleted)
	}

	if registry.Count() != 0 {
		t.Errorf("expected 0 parts remaining, got %d", registry.Count())
	}
}

func TestRetentionManager_OnDeleteCallback(t *testing.T) {
	dir := t.TempDir()
	layout := NewLayout(dir)
	registry := NewRegistry(testLogger())

	now := time.Now().UTC()
	old := now.Add(-100 * 24 * time.Hour)

	meta := writeTestPart(t, layout, "main", old)
	registry.Add(meta)

	rm := NewRetentionManager(layout, registry, RetentionConfig{
		MaxAge: 90 * 24 * time.Hour,
	}, testLogger())

	var callbackIndex, callbackPartition string
	var callbackIDs []string
	var mu sync.Mutex

	rm.SetOnDelete(func(index, partition string, removedIDs []string) {
		mu.Lock()
		callbackIndex = index
		callbackPartition = partition
		callbackIDs = removedIDs
		mu.Unlock()
	})

	deleted, err := rm.RunOnce()
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if deleted != 1 {
		t.Fatalf("expected 1 deleted, got %d", deleted)
	}

	mu.Lock()
	defer mu.Unlock()

	if callbackIndex != "main" {
		t.Errorf("expected callback index 'main', got %q", callbackIndex)
	}

	if callbackPartition == "" {
		t.Error("expected non-empty callback partition")
	}

	if len(callbackIDs) != 1 || callbackIDs[0] != meta.ID {
		t.Errorf("expected callback IDs [%s], got %v", meta.ID, callbackIDs)
	}
}

func TestRetentionManager_NothingToDelete(t *testing.T) {
	dir := t.TempDir()
	layout := NewLayout(dir)
	registry := NewRegistry(testLogger())

	now := time.Now().UTC()
	meta := writeTestPart(t, layout, "main", now)
	registry.Add(meta)

	rm := NewRetentionManager(layout, registry, RetentionConfig{
		MaxAge: 90 * 24 * time.Hour,
	}, testLogger())

	deleted, err := rm.RunOnce()
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if deleted != 0 {
		t.Errorf("expected 0 deleted, got %d", deleted)
	}

	if registry.Count() != 1 {
		t.Errorf("expected 1 part remaining, got %d", registry.Count())
	}
}

func TestRetentionManager_EmptyDataDir(t *testing.T) {
	dir := t.TempDir()
	layout := NewLayout(dir)
	registry := NewRegistry(testLogger())

	rm := NewRetentionManager(layout, registry, RetentionConfig{
		MaxAge: 90 * 24 * time.Hour,
	}, testLogger())

	deleted, err := rm.RunOnce()
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if deleted != 0 {
		t.Errorf("expected 0 deleted on empty data dir, got %d", deleted)
	}
}

func TestParsePartitionTime_Daily(t *testing.T) {
	ts, err := parsePartitionTime("2026-03-02", GranularityDaily)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	expected := time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)
	if !ts.Equal(expected) {
		t.Errorf("expected %v, got %v", expected, ts)
	}
}

func TestParsePartitionTime_Hourly(t *testing.T) {
	ts, err := parsePartitionTime("2026-03-02-14", GranularityHourly)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	expected := time.Date(2026, 3, 2, 14, 0, 0, 0, time.UTC)
	if !ts.Equal(expected) {
		t.Errorf("expected %v, got %v", expected, ts)
	}
}

func TestParsePartitionTime_Invalid(t *testing.T) {
	_, err := parsePartitionTime("not-a-date", GranularityDaily)
	if err == nil {
		t.Error("expected error for invalid partition key")
	}
}

func TestRetentionManager_HourlyGranularity(t *testing.T) {
	dir := t.TempDir()
	layout := NewLayoutWithGranularity(dir, GranularityHourly)
	registry := NewRegistry(testLogger())

	now := time.Now().UTC()
	old := now.Add(-100 * 24 * time.Hour) // 100 days ago

	// Create partition dir and a dummy part file manually.
	partDir := layout.PartitionDir("main", old)
	if err := os.MkdirAll(partDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	dummyPath := filepath.Join(partDir, "dummy.txt")
	if err := os.WriteFile(dummyPath, []byte("x"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	rm := NewRetentionManager(layout, registry, RetentionConfig{
		MaxAge: 90 * 24 * time.Hour,
	}, testLogger())

	deleted, err := rm.RunOnce()
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if deleted != 1 {
		t.Errorf("expected 1 partition deleted, got %d", deleted)
	}

	if _, err := os.Stat(partDir); !os.IsNotExist(err) {
		t.Error("expected partition dir to be deleted")
	}
}
