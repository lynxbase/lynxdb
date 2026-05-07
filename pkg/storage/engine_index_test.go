package storage

import (
	"context"
	"testing"
)

func TestEphemeralQueryNonDefaultIndexSearchesParsedFields(t *testing.T) {
	eng := NewEphemeralEngine()
	lines := []string{
		`{"CommandLine":"whoami","__sigma_index":0}`,
		`{"CommandLine":"hostname","__sigma_index":1}`,
	}
	n, err := eng.IngestLines(context.Background(), lines, IngestOpts{
		Source:     "test",
		SourceType: "json",
		Index:      "security_logs",
	})
	if err != nil {
		t.Fatalf("IngestLines: %v", err)
	}
	if n != len(lines) {
		t.Fatalf("IngestLines accepted %d, want %d", n, len(lines))
	}
	if got := len(eng.events["security_logs"]); got != len(lines) {
		t.Fatalf("security_logs event count = %d, want %d", got, len(lines))
	}

	result, _, err := eng.Query(context.Background(),
		`FROM security_logs | search CommandLine="whoami"`,
		QueryOpts{},
	)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("result row count = %d, want 1; rows=%#v", len(result.Rows), result.Rows)
	}
	if got := result.Rows[0]["__sigma_index"]; got != int64(0) {
		t.Fatalf("__sigma_index = %T(%v), want int64(0)", got, got)
	}
}
