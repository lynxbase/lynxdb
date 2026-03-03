package storage

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/OrlovEvgeny/Lynxdb/pkg/config"
)

func TestEngine_EphemeralIngestAndQuery(t *testing.T) {
	eng := NewEphemeralEngine()
	defer eng.Close()

	lines := []string{
		`{"level":"error","msg":"disk full","source":"api"}`,
		`{"level":"info","msg":"started","source":"api"}`,
		`{"level":"error","msg":"timeout","source":"worker"}`,
		`{"level":"info","msg":"healthy","source":"worker"}`,
		`{"level":"warn","msg":"slow query","source":"db"}`,
	}

	n, err := eng.IngestLines(lines, IngestOpts{Source: "test"})
	if err != nil {
		t.Fatalf("IngestLines: %v", err)
	}
	if n != 5 {
		t.Fatalf("expected 5 events, got %d", n)
	}

	if eng.EventCount() != 5 {
		t.Fatalf("EventCount: got %d, want 5", eng.EventCount())
	}

	ctx := context.Background()

	res, _, err := eng.Query(ctx, `FROM main | stats count`, QueryOpts{})
	if err != nil {
		t.Fatalf("Query stats count: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if cnt, ok := res.Rows[0]["count"]; !ok {
		t.Fatal("stats count: missing 'count' field in result")
	} else if toInt(cnt) != 5 {
		t.Errorf("stats count: got %v, want 5", cnt)
	}

	res, _, err = eng.Query(ctx, `FROM main | where level="error" | stats count`, QueryOpts{})
	if err != nil {
		t.Fatalf("Query filtered: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if cnt, ok := res.Rows[0]["count"]; !ok {
		t.Fatal("filtered stats count: missing 'count' field in result")
	} else if toInt(cnt) != 2 {
		t.Errorf("filtered stats count: got %v, want 2", cnt)
	}

	res, _, err = eng.Query(ctx, `FROM main | stats count by level`, QueryOpts{})
	if err != nil {
		t.Fatalf("Query stats count by level: %v", err)
	}
	if len(res.Rows) != 3 {
		t.Fatalf("expected 3 rows for stats count by level (error, info, warn), got %d", len(res.Rows))
	}
	// Build a map of level→count for order-independent verification.
	levelCounts := make(map[string]int64)
	for _, row := range res.Rows {
		level, _ := row["level"].(string)
		levelCounts[level] = toInt(row["count"])
	}
	if levelCounts["error"] != 2 {
		t.Errorf("level=error count: got %d, want 2", levelCounts["error"])
	}
	if levelCounts["info"] != 2 {
		t.Errorf("level=info count: got %d, want 2", levelCounts["info"])
	}
	if levelCounts["warn"] != 1 {
		t.Errorf("level=warn count: got %d, want 1", levelCounts["warn"])
	}
}

// toInt converts an interface{} (typically int64 or float64 from query results) to int64.
func toInt(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	default:
		return -1
	}
}

func TestEngine_EphemeralIngestReader(t *testing.T) {
	eng := NewEphemeralEngine()
	defer eng.Close()

	data := `{"level":"error","msg":"test1"}
{"level":"info","msg":"test2"}
{"level":"error","msg":"test3"}
`
	n, err := eng.IngestReader(strings.NewReader(data), IngestOpts{Source: "stdin"})
	if err != nil {
		t.Fatalf("IngestReader: %v", err)
	}
	if n != 3 {
		t.Fatalf("expected 3 events, got %d", n)
	}

	ctx := context.Background()
	res, _, err := eng.Query(ctx, `FROM main | stats count`, QueryOpts{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if cnt, ok := res.Rows[0]["count"]; !ok {
		t.Fatal("stats count: missing 'count' field in result")
	} else if toInt(cnt) != 3 {
		t.Errorf("stats count: got %v, want 3", cnt)
	}
}

func TestEngine_EphemeralEmptyQuery(t *testing.T) {
	eng := NewEphemeralEngine()
	defer eng.Close()

	ctx := context.Background()
	res, _, err := eng.Query(ctx, `FROM main | stats count`, QueryOpts{})
	if err != nil {
		t.Fatalf("Query on empty engine: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if cnt, ok := res.Rows[0]["count"]; !ok {
		t.Fatal("stats count: missing 'count' field in result")
	} else if toInt(cnt) != 0 {
		t.Errorf("stats count on empty engine: got %v, want 0", cnt)
	}
}

func TestEngine_ParseError(t *testing.T) {
	eng := NewEphemeralEngine()
	defer eng.Close()

	ctx := context.Background()
	_, _, err := eng.Query(ctx, `||| invalid query |||`, QueryOpts{})
	if err == nil {
		t.Fatal("expected parse error, got nil")
	}
}

func TestEngine_ProfileResolution(t *testing.T) {
	tests := []struct {
		name    string
		cfg     EngineConfig
		profile Profile
	}{
		{
			name:    "ephemeral",
			cfg:     EngineConfig{},
			profile: Ephemeral,
		},
		{
			name: "persistent",
			cfg: EngineConfig{
				DataDir: t.TempDir(),
			},
			profile: Persistent,
		},
		{
			name: "tiered",
			cfg: EngineConfig{
				DataDir: t.TempDir(),
				Storage: config.StorageConfig{S3Bucket: "my-bucket"},
			},
			profile: Tiered,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eng, err := NewEngine(tt.cfg)
			if err != nil {
				t.Fatalf("NewEngine: %v", err)
			}
			defer eng.Close()
			if eng.Profile() != tt.profile {
				t.Errorf("profile: got %v, want %v", eng.Profile(), tt.profile)
			}
		})
	}
}

func TestEngine_EphemeralClose(t *testing.T) {
	eng := NewEphemeralEngine()

	lines := []string{
		`{"msg":"event1"}`,
		`{"msg":"event2"}`,
	}
	_, err := eng.IngestLines(lines, IngestOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if eng.EventCount() != 2 {
		t.Fatalf("expected 2 events before close")
	}

	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}

	// After close, event count should be zero.
	if eng.EventCount() != 0 {
		t.Errorf("EventCount after Close: got %d, want 0", eng.EventCount())
	}

	// Double close is safe.
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestEngine_EphemeralNoFilesLeftAfterClose(t *testing.T) {
	// Snapshot lynxdb-query-* dirs before the test so we only detect
	// dirs leaked by THIS test, not by parallel test runs.
	before := lynxdbQueryDirs(t)

	eng := NewEphemeralEngine()

	// Ingest some data.
	lines := make([]string, 100)
	for i := range lines {
		lines[i] = `{"msg":"test event","index":0}`
	}
	_, err := eng.IngestLines(lines, IngestOpts{})
	if err != nil {
		t.Fatal(err)
	}

	eng.Close()

	after := lynxdbQueryDirs(t)
	for dir := range after {
		if !before[dir] {
			t.Errorf("leaked temp dir after Close: %s", dir)
		}
	}
}

// lynxdbQueryDirs returns the set of lynxdb-query-* entries in os.TempDir().
func lynxdbQueryDirs(t *testing.T) map[string]bool {
	t.Helper()

	entries, err := os.ReadDir(os.TempDir())
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", os.TempDir(), err)
	}

	dirs := make(map[string]bool)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "lynxdb-query-") {
			dirs[e.Name()] = true
		}
	}

	return dirs
}

func TestEngine_EphemeralProfile(t *testing.T) {
	eng := NewEphemeralEngine()
	defer eng.Close()

	if eng.Profile() != Ephemeral {
		t.Errorf("expected Ephemeral profile, got %v", eng.Profile())
	}
	features := Features(eng.Profile())
	if features.PartWriter {
		t.Error("Ephemeral should not have PartWriter")
	}
}
