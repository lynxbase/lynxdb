package tiering

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/internal/objstore"
	"github.com/OrlovEvgeny/Lynxdb/pkg/model"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestClassifyTier(t *testing.T) {
	cfg := model.DefaultIndexConfig("main") // hot=7d, warm=30d, retention=90d
	now := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name    string
		created time.Time
		want    Tier
	}{
		{"just created", now, TierHot},
		{"1 day old", now.Add(-24 * time.Hour), TierHot},
		{"6 days old", now.Add(-6 * 24 * time.Hour), TierHot},
		{"8 days old", now.Add(-8 * 24 * time.Hour), TierWarm},
		{"20 days old", now.Add(-20 * 24 * time.Hour), TierWarm},
		{"31 days old", now.Add(-31 * 24 * time.Hour), TierCold},
		{"60 days old", now.Add(-60 * 24 * time.Hour), TierCold},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ClassifyTier(tt.created, now, cfg)
			if got != tt.want {
				t.Errorf("ClassifyTier(%v): got %q, want %q", tt.created, got, tt.want)
			}
		})
	}
}

func TestManager_AddAndGetSegment(t *testing.T) {
	store := objstore.NewMemStore()
	mgr := NewManager(store, testLogger())

	meta := model.SegmentMeta{
		ID:        "seg-001",
		Index:     "main",
		CreatedAt: time.Now(),
	}
	mgr.AddSegment(meta)

	seg, ok := mgr.GetSegment("seg-001")
	if !ok {
		t.Fatal("segment not found")
	}
	if seg.Tier != TierHot {
		t.Errorf("tier: got %q, want %q", seg.Tier, TierHot)
	}
}

func TestManager_MoveToWarm(t *testing.T) {
	store := objstore.NewMemStore()
	mgr := NewManager(store, testLogger())
	ctx := context.Background()

	meta := model.SegmentMeta{
		ID:    "seg-001",
		Index: "main",
	}
	mgr.AddSegment(meta)

	data := []byte("segment-data-here")
	if err := mgr.MoveToWarm(ctx, "seg-001", data); err != nil {
		t.Fatalf("MoveToWarm: %v", err)
	}

	seg, _ := mgr.GetSegment("seg-001")
	if seg.Tier != TierWarm {
		t.Errorf("tier: got %q, want %q", seg.Tier, TierWarm)
	}
	if seg.ObjectKey == "" {
		t.Error("object key should be set")
	}

	// Verify data is in store.
	got, err := store.Get(ctx, seg.ObjectKey)
	if err != nil {
		t.Fatalf("store get: %v", err)
	}
	if string(got) != "segment-data-here" {
		t.Errorf("stored data: %q", got)
	}
}

func TestManager_MoveToCold(t *testing.T) {
	store := objstore.NewMemStore()
	mgr := NewManager(store, testLogger())
	ctx := context.Background()

	meta := model.SegmentMeta{
		ID:    "seg-002",
		Index: "main",
	}
	mgr.AddSegment(meta)
	mgr.MoveToWarm(ctx, "seg-002", []byte("warm-data"))

	seg, _ := mgr.GetSegment("seg-002")
	warmKey := seg.ObjectKey

	if err := mgr.MoveToCold(ctx, "seg-002"); err != nil {
		t.Fatalf("MoveToCold: %v", err)
	}

	seg, _ = mgr.GetSegment("seg-002")
	if seg.Tier != TierCold {
		t.Errorf("tier: got %q, want %q", seg.Tier, TierCold)
	}

	// Warm key should be deleted.
	exists, _ := store.Exists(ctx, warmKey)
	if exists {
		t.Error("warm key should be deleted after move to cold")
	}

	// Cold key should exist.
	exists, _ = store.Exists(ctx, seg.ObjectKey)
	if !exists {
		t.Error("cold key should exist")
	}
}

func TestManager_ReadFromStore(t *testing.T) {
	store := objstore.NewMemStore()
	mgr := NewManager(store, testLogger())
	ctx := context.Background()

	meta := model.SegmentMeta{ID: "seg-003", Index: "main"}
	mgr.AddSegment(meta)
	mgr.MoveToWarm(ctx, "seg-003", []byte("read-me-back"))

	data, err := mgr.ReadFromStore(ctx, "seg-003")
	if err != nil {
		t.Fatalf("ReadFromStore: %v", err)
	}
	if string(data) != "read-me-back" {
		t.Errorf("got %q", data)
	}
}

func TestManager_ReadRangeFromStore(t *testing.T) {
	store := objstore.NewMemStore()
	mgr := NewManager(store, testLogger())
	ctx := context.Background()

	meta := model.SegmentMeta{ID: "seg-004", Index: "main"}
	mgr.AddSegment(meta)
	mgr.MoveToWarm(ctx, "seg-004", []byte("0123456789ABCDEF"))

	data, err := mgr.ReadRangeFromStore(ctx, "seg-004", 4, 6)
	if err != nil {
		t.Fatalf("ReadRangeFromStore: %v", err)
	}
	if string(data) != "456789" {
		t.Errorf("got %q, want %q", data, "456789")
	}
}

func TestManager_Evaluate(t *testing.T) {
	store := objstore.NewMemStore()
	mgr := NewManager(store, testLogger())

	now := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	mgr.SetClock(func() time.Time { return now })

	cfg := model.DefaultIndexConfig("main")
	configs := map[string]model.IndexConfig{"main": cfg}

	// Hot segment, 1 day old → should stay hot.
	mgr.AddSegment(model.SegmentMeta{
		ID: "hot-seg", Index: "main",
		CreatedAt: now.Add(-24 * time.Hour),
	})

	// Hot segment, 10 days old → should move to warm.
	mgr.AddSegment(model.SegmentMeta{
		ID: "warm-candidate", Index: "main",
		CreatedAt: now.Add(-10 * 24 * time.Hour),
	})

	// Warm segment, 35 days old → should move to cold.
	mgr.AddSegment(model.SegmentMeta{
		ID: "cold-candidate", Index: "main",
		CreatedAt: now.Add(-35 * 24 * time.Hour),
	})
	// Manually set tier to warm (simulating it was already moved).
	mgr.mu.Lock()
	mgr.segments["cold-candidate"].Tier = TierWarm
	mgr.segments["cold-candidate"].ObjectKey = "warm/main/cold-candidate.lsg"
	mgr.mu.Unlock()

	// Expired segment, 100 days old.
	mgr.AddSegment(model.SegmentMeta{
		ID: "expired-seg", Index: "main",
		CreatedAt: now.Add(-100 * 24 * time.Hour),
	})

	result := mgr.Evaluate(configs)

	if len(result.MovedToWarm) != 1 || result.MovedToWarm[0] != "warm-candidate" {
		t.Errorf("MovedToWarm: %v", result.MovedToWarm)
	}
	if len(result.MovedToCold) != 1 || result.MovedToCold[0] != "cold-candidate" {
		t.Errorf("MovedToCold: %v", result.MovedToCold)
	}
	if len(result.Expired) != 1 || result.Expired[0] != "expired-seg" {
		t.Errorf("Expired: %v", result.Expired)
	}
}

func TestManager_DeleteExpired(t *testing.T) {
	store := objstore.NewMemStore()
	mgr := NewManager(store, testLogger())
	ctx := context.Background()

	meta := model.SegmentMeta{ID: "exp-seg", Index: "main"}
	mgr.AddSegment(meta)
	mgr.MoveToWarm(ctx, "exp-seg", []byte("expired-data"))

	seg, _ := mgr.GetSegment("exp-seg")
	key := seg.ObjectKey

	if err := mgr.DeleteExpired(ctx, "exp-seg"); err != nil {
		t.Fatalf("DeleteExpired: %v", err)
	}

	// Segment should be removed from manager.
	_, ok := mgr.GetSegment("exp-seg")
	if ok {
		t.Error("segment should be removed after expiration")
	}

	// Data should be removed from store.
	exists, _ := store.Exists(ctx, key)
	if exists {
		t.Error("object should be deleted from store")
	}
}

func TestManager_SegmentsByTier(t *testing.T) {
	store := objstore.NewMemStore()
	mgr := NewManager(store, testLogger())
	ctx := context.Background()

	now := time.Now()
	mgr.AddSegment(model.SegmentMeta{ID: "h1", Index: "main", CreatedAt: now})
	mgr.AddSegment(model.SegmentMeta{ID: "h2", Index: "main", CreatedAt: now})
	mgr.AddSegment(model.SegmentMeta{ID: "w1", Index: "main", CreatedAt: now})
	mgr.MoveToWarm(ctx, "w1", []byte("data"))

	hot := mgr.SegmentsByTier(TierHot)
	warm := mgr.SegmentsByTier(TierWarm)
	cold := mgr.SegmentsByTier(TierCold)

	if len(hot) != 2 {
		t.Errorf("hot: got %d, want 2", len(hot))
	}
	if len(warm) != 1 {
		t.Errorf("warm: got %d, want 1", len(warm))
	}
	if len(cold) != 0 {
		t.Errorf("cold: got %d, want 0", len(cold))
	}
}

func TestManager_MoveToWarm_SafetyVerification(t *testing.T) {
	store := objstore.NewMemStore()
	mgr := NewManager(store, testLogger())
	ctx := context.Background()

	now := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	mgr.SetClock(func() time.Time { return now })

	meta := model.SegmentMeta{ID: "seg-safe", Index: "main"}
	mgr.AddSegment(meta)

	if err := mgr.MoveToWarm(ctx, "seg-safe", []byte("safe-data")); err != nil {
		t.Fatalf("MoveToWarm: %v", err)
	}

	seg, _ := mgr.GetSegment("seg-safe")
	if !seg.SafeToDeleteLocal {
		t.Error("SafeToDeleteLocal should be true after verified upload")
	}
	if seg.DeleteLocalAfter.IsZero() {
		t.Error("DeleteLocalAfter should be set")
	}

	// Should NOT be ready for deletion yet (within the 1-minute safety window).
	ready := mgr.ReadyForLocalDelete()
	if len(ready) != 0 {
		t.Errorf("should not be ready for deletion yet, got %v", ready)
	}

	// Advance clock past the safety window.
	mgr.SetClock(func() time.Time { return now.Add(2 * time.Minute) })
	ready = mgr.ReadyForLocalDelete()
	if len(ready) != 1 || ready[0] != "seg-safe" {
		t.Errorf("expected seg-safe ready for deletion, got %v", ready)
	}

	// Mark as deleted.
	mgr.MarkLocalDeleted("seg-safe")
	seg, _ = mgr.GetSegment("seg-safe")
	if seg.SafeToDeleteLocal {
		t.Error("SafeToDeleteLocal should be false after MarkLocalDeleted")
	}

	ready = mgr.ReadyForLocalDelete()
	if len(ready) != 0 {
		t.Errorf("should not be ready after marking deleted, got %v", ready)
	}
}

func TestManager_MoveToWarm_MigratingState(t *testing.T) {
	store := objstore.NewMemStore()
	mgr := NewManager(store, testLogger())

	meta := model.SegmentMeta{ID: "seg-mig", Index: "main"}
	mgr.AddSegment(meta)

	// Verify segment is initially hot.
	seg, _ := mgr.GetSegment("seg-mig")
	if seg.Tier != TierHot {
		t.Errorf("initial tier: got %q, want %q", seg.Tier, TierHot)
	}

	// After successful upload, should be warm.
	ctx := context.Background()
	if err := mgr.MoveToWarm(ctx, "seg-mig", []byte("data")); err != nil {
		t.Fatalf("MoveToWarm: %v", err)
	}
	seg, _ = mgr.GetSegment("seg-mig")
	if seg.Tier != TierWarm {
		t.Errorf("after upload tier: got %q, want %q", seg.Tier, TierWarm)
	}
}

func TestManager_ReadFromStore_HotError(t *testing.T) {
	store := objstore.NewMemStore()
	mgr := NewManager(store, testLogger())
	ctx := context.Background()

	mgr.AddSegment(model.SegmentMeta{ID: "hot-seg", Index: "main"})

	_, err := mgr.ReadFromStore(ctx, "hot-seg")
	if err == nil {
		t.Error("expected error reading hot segment from store")
	}
}
