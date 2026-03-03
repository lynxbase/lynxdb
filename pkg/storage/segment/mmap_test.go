package segment

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMmapSegment_WriteAndRead(t *testing.T) {
	events := generateTestEvents(100)

	dir := t.TempDir()
	path := filepath.Join(dir, "test.lsg")

	// Write segment to file.
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	sw := NewWriter(f)
	written, err := sw.Write(events)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Open via mmap.
	ms, err := OpenSegmentFile(path)
	if err != nil {
		t.Fatalf("OpenSegmentFile: %v", err)
	}
	defer ms.Close()

	// Verify reader works.
	r := ms.Reader()
	if r.EventCount() != int64(len(events)) {
		t.Fatalf("EventCount: got %d, want %d", r.EventCount(), len(events))
	}

	if written <= 0 {
		t.Fatal("expected positive bytes written")
	}

	// Verify Bytes() returns the raw data.
	b := ms.Bytes()
	if int64(len(b)) != written {
		t.Fatalf("Bytes() len: got %d, want %d", len(b), written)
	}

	// Verify full event reconstruction.
	readEvents, err := r.ReadEvents()
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(readEvents) != len(events) {
		t.Fatalf("ReadEvents: got %d, want %d", len(readEvents), len(events))
	}

	for i, orig := range events {
		got := readEvents[i]
		if !got.Time.Equal(orig.Time) {
			t.Errorf("event[%d].Time: mismatch", i)
		}
		if got.Raw != orig.Raw {
			t.Errorf("event[%d].Raw: mismatch", i)
		}
		if got.Host != orig.Host {
			t.Errorf("event[%d].Host: mismatch", i)
		}
	}
}

func TestMmapSegment_BloomFilter(t *testing.T) {
	events := generateTestEvents(50)

	dir := t.TempDir()
	path := filepath.Join(dir, "test.lsg")

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	sw := NewWriter(f)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}
	f.Close()

	ms, err := OpenSegmentFile(path)
	if err != nil {
		t.Fatalf("OpenSegmentFile: %v", err)
	}
	defer ms.Close()

	bf, err := ms.Reader().BloomFilter()
	if err != nil {
		t.Fatalf("BloomFilter: %v", err)
	}
	if bf == nil {
		t.Fatal("expected bloom filter (V2 segment)")
	}

	// "request" and "processed" should be in every event's _raw.
	if !bf.MayContain("request") {
		t.Error("bloom filter should contain 'request'")
	}
	if !bf.MayContain("processed") {
		t.Error("bloom filter should contain 'processed'")
	}

	// A term that definitely doesn't exist.
	if bf.MayContain("zzz_nonexistent_term_xyz") {
		t.Error("bloom filter false positive for 'zzz_nonexistent_term_xyz' (unlikely)")
	}
}

func TestMmapSegment_InvertedIndex(t *testing.T) {
	events := generateTestEvents(50)

	dir := t.TempDir()
	path := filepath.Join(dir, "test.lsg")

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	sw := NewWriter(f)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}
	f.Close()

	ms, err := OpenSegmentFile(path)
	if err != nil {
		t.Fatalf("OpenSegmentFile: %v", err)
	}
	defer ms.Close()

	inv, err := ms.Reader().InvertedIndex()
	if err != nil {
		t.Fatalf("InvertedIndex: %v", err)
	}
	if inv == nil {
		t.Fatal("expected inverted index (V2 segment)")
	}

	// Search for a term that's in all events.
	bm, err := inv.Search("request")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if bm.GetCardinality() == 0 {
		t.Error("expected matches for 'request'")
	}
}

func TestMmapSegment_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.lsg")
	os.WriteFile(path, nil, 0o644)

	_, err := OpenSegmentFile(path)
	if err == nil {
		t.Fatal("expected error for empty file")
	}
}

func TestMmapSegment_CloseIdempotent(t *testing.T) {
	events := generateTestEvents(10)

	dir := t.TempDir()
	path := filepath.Join(dir, "test.lsg")

	f, _ := os.Create(path)
	sw := NewWriter(f)
	sw.Write(events)
	f.Close()

	ms, err := OpenSegmentFile(path)
	if err != nil {
		t.Fatalf("OpenSegmentFile: %v", err)
	}

	// Close should succeed.
	if err := ms.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
