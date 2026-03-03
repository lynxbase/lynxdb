package pipeline

import (
	"context"
	"testing"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
)

func TestRexIterator_Match(t *testing.T) {
	raw := `/archives/edgar/data/12345/0001234567-25-012345.txt`
	events := []*event.Event{
		{Raw: raw, Fields: map[string]event.Value{}},
	}
	scan := NewScanIterator(events, 1024)
	rex, err := NewRexIterator(scan, "_raw", `/archives/edgar/data/(?P<cik>\d+)/`)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	rows, err := CollectAll(ctx, rex)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	cik := rows[0]["cik"]
	if cik.IsNull() || cik.AsString() != "12345" {
		t.Errorf("cik: got %v, want 12345", cik)
	}
}

func TestRexIterator_NoMatch(t *testing.T) {
	events := []*event.Event{
		{Raw: "no match here", Fields: map[string]event.Value{}},
	}
	scan := NewScanIterator(events, 1024)
	rex, err := NewRexIterator(scan, "_raw", `/archives/edgar/data/(?P<cik>\d+)/`)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	rows, err := CollectAll(ctx, rex)
	if err != nil {
		t.Fatal(err)
	}
	cik := rows[0]["cik"]
	if !cik.IsNull() {
		t.Errorf("cik should be null for no match, got %v", cik)
	}
}

func TestRexIterator_OptionalGroupNull(t *testing.T) {
	// Pattern with optional named group: file_ext is optional
	events := []*event.Event{
		{Raw: "/data/file.txt", Fields: map[string]event.Value{}},
		{Raw: "/data/noext", Fields: map[string]event.Value{}},
	}
	scan := NewScanIterator(events, 1024)
	// Pattern matches both, but ext group only captures on first
	rex, err := NewRexIterator(scan, "_raw", `/data/(?P<name>[^.]+)(?:\.(?P<ext>\w+))?$`)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	rows, err := CollectAll(ctx, rex)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	// First row: ext should be "txt"
	if rows[0]["ext"].IsNull() || rows[0]["ext"].AsString() != "txt" {
		t.Errorf("row 0 ext: got %v, want txt", rows[0]["ext"])
	}
	// Second row: ext should be null (optional group didn't match)
	if !rows[1]["ext"].IsNull() {
		t.Errorf("row 1 ext: got %v, want null", rows[1]["ext"])
	}
}

func TestRexIterator_SourceNull(t *testing.T) {
	// Events with null _raw should be skipped
	batch := NewBatch(2)
	batch.AddRow(map[string]event.Value{
		"_raw": event.NullValue(),
	})
	batch.AddRow(map[string]event.Value{
		"_raw": event.StringValue("/data/12345/file.txt"),
	})
	child := &staticIterator{batches: []*Batch{batch}}
	rex, err := NewRexIterator(child, "_raw", `/data/(?P<id>\d+)/`)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	rows, err := CollectAll(ctx, rex)
	if err != nil {
		t.Fatal(err)
	}
	// Row 0: null source, id should remain null
	if !rows[0]["id"].IsNull() {
		t.Errorf("row 0 id: got %v, want null", rows[0]["id"])
	}
	// Row 1: matched
	if rows[1]["id"].IsNull() || rows[1]["id"].AsString() != "12345" {
		t.Errorf("row 1 id: got %v, want 12345", rows[1]["id"])
	}
}

func TestExtractLiteralPrefix(t *testing.T) {
	tests := []struct {
		pattern string
		want    string
	}{
		{`/archives/edgar/data/(?P<cik>\d+)/`, "/archives/edgar/data/"},
		{`\.(?P<ext>\w+)$`, "."},
		{`hello`, "hello"},
		{`\/path\/to\/(?P<x>.+)`, "/path/to/"},
		{`(?P<x>\d+)`, ""},
	}
	for _, tt := range tests {
		got := extractLiteralPrefix(tt.pattern)
		if got != tt.want {
			t.Errorf("extractLiteralPrefix(%q) = %q, want %q", tt.pattern, got, tt.want)
		}
	}
}

// staticIterator is a test helper that returns pre-built batches.
type staticIterator struct {
	batches []*Batch
	idx     int
}

func (s *staticIterator) Init(ctx context.Context) error { return nil }
func (s *staticIterator) Next(ctx context.Context) (*Batch, error) {
	if s.idx >= len(s.batches) {
		return nil, nil
	}
	b := s.batches[s.idx]
	s.idx++

	return b, nil
}
func (s *staticIterator) Close() error        { return nil }
func (s *staticIterator) Schema() []FieldInfo { return nil }
