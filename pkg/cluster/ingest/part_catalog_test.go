package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/internal/objstore"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestPartCatalog_LoadEmpty(t *testing.T) {
	store := objstore.NewMemStore()
	cat := NewPartCatalog(store, testLogger())
	ctx := context.Background()

	c, err := cat.Load(ctx, "logs", 0)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Version != 0 {
		t.Errorf("expected version 0, got %d", c.Version)
	}
	if len(c.Parts) != 0 {
		t.Errorf("expected 0 parts, got %d", len(c.Parts))
	}
}

func TestPartCatalog_Load_PropagatesNonNotFound(t *testing.T) {
	store := &failingStore{err: errors.New("boom")}
	cat := NewPartCatalog(store, testLogger())

	_, err := cat.Load(context.Background(), "logs", 0)
	if err == nil {
		t.Fatal("expected load error")
	}
	if !errors.Is(err, store.err) {
		t.Fatalf("expected wrapped error %v, got %v", store.err, err)
	}
}

func TestPartCatalog_AddPart(t *testing.T) {
	store := objstore.NewMemStore()
	cat := NewPartCatalog(store, testLogger())
	ctx := context.Background()

	entry := PartEntry{
		PartID:      "part-001",
		Index:       "logs",
		MinTimeNs:   time.Now().UnixNano(),
		MaxTimeNs:   time.Now().Add(time.Hour).UnixNano(),
		EventCount:  1000,
		SizeBytes:   65536,
		Level:       0,
		S3Key:       "parts/logs/t20260309/p0/part-001.lsg",
		Columns:     []string{"level", "host", "message"},
		BatchSeq:    1,
		WriterEpoch: 1,
		CreatedAtNs: time.Now().UnixNano(),
	}

	if err := cat.AddPart(ctx, "logs", 0, entry); err != nil {
		t.Fatalf("AddPart: %v", err)
	}

	c, err := cat.Load(ctx, "logs", 0)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Version != 1 {
		t.Errorf("expected version 1, got %d", c.Version)
	}
	if len(c.Parts) != 1 {
		t.Errorf("expected 1 part, got %d", len(c.Parts))
	}
	if c.Parts[0].PartID != "part-001" {
		t.Errorf("expected part-001, got %s", c.Parts[0].PartID)
	}
}

func TestPartCatalog_AddMultipleParts(t *testing.T) {
	store := objstore.NewMemStore()
	cat := NewPartCatalog(store, testLogger())
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		entry := PartEntry{
			PartID:   fmt.Sprintf("part-%03d", i),
			Index:    "logs",
			BatchSeq: uint64(i + 1),
		}
		if err := cat.AddPart(ctx, "logs", 0, entry); err != nil {
			t.Fatalf("AddPart %d: %v", i, err)
		}
	}

	c, err := cat.Load(ctx, "logs", 0)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Version != 5 {
		t.Errorf("expected version 5, got %d", c.Version)
	}
	if len(c.Parts) != 5 {
		t.Errorf("expected 5 parts, got %d", len(c.Parts))
	}
}

func TestPartCatalog_RemoveParts(t *testing.T) {
	store := objstore.NewMemStore()
	cat := NewPartCatalog(store, testLogger())
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		entry := PartEntry{
			PartID: fmt.Sprintf("part-%03d", i),
			Index:  "logs",
		}
		if err := cat.AddPart(ctx, "logs", 0, entry); err != nil {
			t.Fatalf("AddPart %d: %v", i, err)
		}
	}

	if err := cat.RemoveParts(ctx, "logs", 0, []string{"part-001"}); err != nil {
		t.Fatalf("RemoveParts: %v", err)
	}

	c, err := cat.Load(ctx, "logs", 0)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(c.Parts) != 2 {
		t.Errorf("expected 2 parts, got %d", len(c.Parts))
	}

	ids := make(map[string]bool)
	for _, p := range c.Parts {
		ids[p.PartID] = true
	}
	if ids["part-001"] {
		t.Error("part-001 should have been removed")
	}
	if !ids["part-000"] || !ids["part-002"] {
		t.Error("part-000 and part-002 should remain")
	}
}

func TestPartCatalog_RemoveEmpty(t *testing.T) {
	store := objstore.NewMemStore()
	cat := NewPartCatalog(store, testLogger())
	ctx := context.Background()

	if err := cat.RemoveParts(ctx, "logs", 0, nil); err != nil {
		t.Fatalf("RemoveParts: %v", err)
	}
}

func TestPartCatalog_LastBatchSeq(t *testing.T) {
	store := objstore.NewMemStore()
	cat := NewPartCatalog(store, testLogger())
	ctx := context.Background()

	seq, err := cat.LastBatchSeq(ctx, "logs", 0)
	if err != nil {
		t.Fatalf("LastBatchSeq: %v", err)
	}
	if seq != 0 {
		t.Errorf("expected 0, got %d", seq)
	}

	for _, s := range []uint64{5, 3, 10, 7} {
		entry := PartEntry{
			PartID:   fmt.Sprintf("part-%d", s),
			Index:    "logs",
			BatchSeq: s,
		}
		if err := cat.AddPart(ctx, "logs", 0, entry); err != nil {
			t.Fatalf("AddPart: %v", err)
		}
	}

	seq, err = cat.LastBatchSeq(ctx, "logs", 0)
	if err != nil {
		t.Fatalf("LastBatchSeq: %v", err)
	}
	if seq != 10 {
		t.Errorf("expected 10, got %d", seq)
	}
}

func TestPartCatalog_VersionMonotonicity(t *testing.T) {
	store := objstore.NewMemStore()
	cat := NewPartCatalog(store, testLogger())
	ctx := context.Background()

	const n = 10
	for i := 0; i < n; i++ {
		entry := PartEntry{PartID: fmt.Sprintf("part-%d", i), Index: "logs"}
		if err := cat.AddPart(ctx, "logs", 0, entry); err != nil {
			t.Fatalf("AddPart %d: %v", i, err)
		}
	}

	c, err := cat.Load(ctx, "logs", 0)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Version != uint64(n) {
		t.Errorf("expected version %d, got %d", n, c.Version)
	}
}

func TestPartCatalog_ConcurrentAddPart(t *testing.T) {
	store := objstore.NewMemStore()
	cat := NewPartCatalog(store, testLogger())
	ctx := context.Background()

	const goroutines = 5
	const perGoroutine = 10

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				entry := PartEntry{
					PartID: fmt.Sprintf("part-g%d-i%d", g, i),
					Index:  "logs",
				}
				_ = cat.AddPart(ctx, "logs", 0, entry)
			}
		}()
	}

	wg.Wait()

	c, err := cat.Load(ctx, "logs", 0)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if int(c.Version) != len(c.Parts) {
		t.Errorf("version (%d) should match parts count (%d)", c.Version, len(c.Parts))
	}
}

func TestPartCatalog_IndependentPartitions(t *testing.T) {
	store := objstore.NewMemStore()
	cat := NewPartCatalog(store, testLogger())
	ctx := context.Background()

	if err := cat.AddPart(ctx, "logs", 0, PartEntry{PartID: "p0-part", Index: "logs"}); err != nil {
		t.Fatalf("AddPart p0: %v", err)
	}
	if err := cat.AddPart(ctx, "logs", 1, PartEntry{PartID: "p1-part", Index: "logs"}); err != nil {
		t.Fatalf("AddPart p1: %v", err)
	}

	c0, _ := cat.Load(ctx, "logs", 0)
	c1, _ := cat.Load(ctx, "logs", 1)

	if len(c0.Parts) != 1 || c0.Parts[0].PartID != "p0-part" {
		t.Error("partition 0 should have only p0-part")
	}
	if len(c1.Parts) != 1 || c1.Parts[0].PartID != "p1-part" {
		t.Error("partition 1 should have only p1-part")
	}
}

func TestPartCatalog_ListPartitions(t *testing.T) {
	store := objstore.NewMemStore()
	cat := NewPartCatalog(store, testLogger())
	ctx := context.Background()

	if err := cat.AddPart(ctx, "logs", 0, PartEntry{PartID: "logs-0", Index: "logs"}); err != nil {
		t.Fatalf("AddPart logs/0: %v", err)
	}
	if err := cat.AddPart(ctx, "metrics", 2, PartEntry{PartID: "metrics-2", Index: "metrics"}); err != nil {
		t.Fatalf("AddPart metrics/2: %v", err)
	}

	partitions, err := cat.ListPartitions(ctx)
	if err != nil {
		t.Fatalf("ListPartitions: %v", err)
	}
	if len(partitions) != 2 {
		t.Fatalf("len(partitions) = %d, want 2", len(partitions))
	}
	if partitions[0] != (CatalogPartition{Index: "logs", Partition: 0}) &&
		partitions[1] != (CatalogPartition{Index: "logs", Partition: 0}) {
		t.Fatalf("missing logs partition in %#v", partitions)
	}
	if partitions[0] != (CatalogPartition{Index: "metrics", Partition: 2}) &&
		partitions[1] != (CatalogPartition{Index: "metrics", Partition: 2}) {
		t.Fatalf("missing metrics partition in %#v", partitions)
	}
}

type failingStore struct {
	err error
}

func (s *failingStore) Put(context.Context, string, []byte) error { return nil }
func (s *failingStore) Get(context.Context, string) ([]byte, error) {
	return nil, s.err
}
func (s *failingStore) GetRange(context.Context, string, int64, int64) ([]byte, error) {
	return nil, s.err
}
func (s *failingStore) Delete(context.Context, string) error { return nil }
func (s *failingStore) List(context.Context, string) ([]string, error) {
	return nil, s.err
}
func (s *failingStore) Exists(context.Context, string) (bool, error) { return false, s.err }
func (s *failingStore) Copy(context.Context, string, string) error   { return s.err }
