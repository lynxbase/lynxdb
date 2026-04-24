package part

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLayout_PartitionKey_Daily(t *testing.T) {
	l := NewLayout("/data")
	ts := time.Date(2026, 3, 2, 14, 30, 0, 0, time.UTC)

	got := l.PartitionKey(ts)
	want := "2026-03-02"
	if got != want {
		t.Errorf("PartitionKey: got %q, want %q", got, want)
	}
}

func TestLayout_PartitionKey_Hourly(t *testing.T) {
	l := NewLayoutWithGranularity("/data", GranularityHourly)
	ts := time.Date(2026, 3, 2, 14, 30, 0, 0, time.UTC)

	got := l.PartitionKey(ts)
	want := "2026-03-02-14"
	if got != want {
		t.Errorf("PartitionKey: got %q, want %q", got, want)
	}
}

func TestLayout_PartitionDir(t *testing.T) {
	l := NewLayout("/data")
	ts := time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)

	got := l.PartitionDir("main", ts)
	want := "/data/segments/hot/main/2026-03-02"
	if got != want {
		t.Errorf("PartitionDir: got %q, want %q", got, want)
	}
}

func TestLayout_IndexDir(t *testing.T) {
	l := NewLayout("/data")

	got := l.IndexDir("nginx")
	want := "/data/segments/hot/nginx"
	if got != want {
		t.Errorf("IndexDir: got %q, want %q", got, want)
	}
}

func TestLayout_EnsurePartitionDir(t *testing.T) {
	dir := t.TempDir()
	l := NewLayout(dir)
	ts := time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)

	if err := l.EnsurePartitionDir("main", ts); err != nil {
		t.Fatalf("EnsurePartitionDir: %v", err)
	}

	expected := l.PartitionDir("main", ts)
	info, err := os.Stat(expected)
	if err != nil {
		t.Fatalf("dir does not exist: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("expected directory, got file")
	}
}

func TestLayout_ListIndexes(t *testing.T) {
	dir := t.TempDir()
	l := NewLayout(dir)

	for _, idx := range []string{"main", "nginx", "api"} {
		ts := time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)
		if err := l.EnsurePartitionDir(idx, ts); err != nil {
			t.Fatalf("EnsurePartitionDir(%s): %v", idx, err)
		}
	}

	indexes, err := l.ListIndexes()
	if err != nil {
		t.Fatalf("ListIndexes: %v", err)
	}

	want := map[string]bool{"main": true, "nginx": true, "api": true}
	for _, idx := range indexes {
		if !want[idx] {
			t.Errorf("unexpected index: %q", idx)
		}
		delete(want, idx)
	}
	for idx := range want {
		t.Errorf("missing index: %q", idx)
	}
}

func TestLayout_ListIndexes_Empty(t *testing.T) {
	dir := t.TempDir()
	l := NewLayout(dir)

	indexes, err := l.ListIndexes()
	if err != nil {
		t.Fatalf("ListIndexes: %v", err)
	}
	if len(indexes) != 0 {
		t.Errorf("expected empty, got %v", indexes)
	}
}

func TestLayout_ListPartitions(t *testing.T) {
	dir := t.TempDir()
	l := NewLayout(dir)

	dates := []time.Time{
		time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 3, 3, 0, 0, 0, 0, time.UTC),
	}
	for _, ts := range dates {
		if err := l.EnsurePartitionDir("main", ts); err != nil {
			t.Fatalf("EnsurePartitionDir: %v", err)
		}
	}

	partitions, err := l.ListPartitions("main")
	if err != nil {
		t.Fatalf("ListPartitions: %v", err)
	}
	if len(partitions) != 3 {
		t.Fatalf("expected 3 partitions, got %d", len(partitions))
	}
}

func TestLayout_ListParts(t *testing.T) {
	dir := t.TempDir()
	l := NewLayout(dir)
	ts := time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)

	if err := l.EnsurePartitionDir("main", ts); err != nil {
		t.Fatalf("EnsurePartitionDir: %v", err)
	}

	partDir := l.PartitionDir("main", ts)
	for i := 0; i < 3; i++ {
		f, err := os.Create(filepath.Join(partDir, Filename("main", 0, ts.Add(time.Duration(i)*time.Second))))
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		f.Close()
	}

	// Create a non-.lsg file (should be ignored).
	f, err := os.Create(filepath.Join(partDir, "notes.txt"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	f.Close()

	parts, err := l.ListParts("main", l.PartitionKey(ts))
	if err != nil {
		t.Fatalf("ListParts: %v", err)
	}
	if len(parts) != 3 {
		t.Errorf("expected 3 parts, got %d: %v", len(parts), parts)
	}
}

func TestFilename(t *testing.T) {
	ts := time.Date(2026, 3, 2, 14, 30, 0, 0, time.UTC)
	name := Filename("main", 0, ts)

	if filepath.Ext(name) != ".lsg" {
		t.Errorf("expected .lsg extension, got %q", name)
	}
	if name[:5] != "part-" {
		t.Errorf("expected 'part-' prefix, got %q", name)
	}
}

func TestID(t *testing.T) {
	ts := time.Date(2026, 3, 2, 14, 30, 0, 0, time.UTC)
	id := ID("main", 0, ts)
	name := Filename("main", 0, ts)

	// ID should be filename without .lsg.
	if id+".lsg" != name {
		t.Errorf("ID/Filename mismatch: id=%q, name=%q", id, name)
	}
}

func TestLayout_Granularity(t *testing.T) {
	l := NewLayout("/data")
	if l.Granularity() != GranularityDaily {
		t.Errorf("expected daily, got %d", l.Granularity())
	}

	l2 := NewLayoutWithGranularity("/data", GranularityHourly)
	if l2.Granularity() != GranularityHourly {
		t.Errorf("expected hourly, got %d", l2.Granularity())
	}
}

func TestLayout_DataDir(t *testing.T) {
	l := NewLayout("/data/lynxdb")
	if l.DataDir() != "/data/lynxdb" {
		t.Errorf("DataDir: got %q, want %q", l.DataDir(), "/data/lynxdb")
	}
}

func TestS3PartKey(t *testing.T) {
	ts := time.Date(2026, 3, 9, 14, 30, 0, 0, time.UTC)

	tests := []struct {
		name         string
		index        string
		partition    uint32
		timeBucket   time.Time
		partFilename string
		want         string
	}{
		{
			name:         "basic",
			index:        "logs",
			partition:    0,
			timeBucket:   ts,
			partFilename: "part-logs-L0-123456.lsg",
			want:         "parts/logs/t20260309/p0/part-logs-L0-123456.lsg",
		},
		{
			name:         "high partition",
			index:        "nginx",
			partition:    512,
			timeBucket:   ts,
			partFilename: "part-nginx-L1-789.lsg",
			want:         "parts/nginx/t20260309/p512/part-nginx-L1-789.lsg",
		},
		{
			name:         "different date",
			index:        "audit",
			partition:    3,
			timeBucket:   time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC),
			partFilename: "part-audit-L0-1.lsg",
			want:         "parts/audit/t20251231/p3/part-audit-L0-1.lsg",
		},
		{
			name:         "non-UTC time gets normalized",
			index:        "logs",
			partition:    0,
			timeBucket:   time.Date(2026, 3, 9, 23, 0, 0, 0, time.FixedZone("EST", -5*60*60)),
			partFilename: "part.lsg",
			want:         "parts/logs/t20260310/p0/part.lsg", // 23:00 EST = 04:00 UTC next day
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := S3PartKey(tt.index, tt.partition, tt.timeBucket, tt.partFilename)
			if got != tt.want {
				t.Errorf("S3PartKey() = %q, want %q", got, tt.want)
			}
		})
	}
}
