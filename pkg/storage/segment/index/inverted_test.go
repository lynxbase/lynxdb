package index

import (
	"fmt"
	"testing"
)

func TestInvertedIndex_Basic(t *testing.T) {
	idx := NewInvertedIndex()
	idx.Add(0, "error connection refused from 192.168.1.100")
	idx.Add(1, "warning disk space low on web-01")
	idx.Add(2, "error timeout connecting to database")
	idx.Add(3, "info request processed successfully")

	// Search for "error" should return events 0 and 2.
	results := idx.Search("error")
	if results.GetCardinality() != 2 {
		t.Errorf("search 'error': got %d results, want 2", results.GetCardinality())
	}
	if !results.Contains(0) || !results.Contains(2) {
		t.Errorf("search 'error': expected events 0 and 2, got %v", results.ToArray())
	}

	// Search for "192" (IP is now split into individual octets by the tokenizer).
	results = idx.Search("192")
	if results.GetCardinality() != 1 || !results.Contains(0) {
		t.Errorf("search '192': expected [0], got %v", results.ToArray())
	}

	// Search for non-existent term.
	results = idx.Search("nonexistent")
	if results.GetCardinality() != 0 {
		t.Errorf("search 'nonexistent': expected empty, got %v", results.ToArray())
	}
}

func TestInvertedIndex_EncodeDecode(t *testing.T) {
	idx := NewInvertedIndex()
	idx.Add(0, "error connection refused")
	idx.Add(1, "warning disk space low")
	idx.Add(2, "error timeout database")
	idx.Add(3, "info request processed")
	idx.Add(4, "error connection reset by peer")

	data, err := idx.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	si, err := DecodeInvertedIndex(data)
	if err != nil {
		t.Fatalf("DecodeInvertedIndex: %v", err)
	}

	// Verify all terms from original are searchable.
	tests := []struct {
		term     string
		expected []uint32
	}{
		{"error", []uint32{0, 2, 4}},
		{"connection", []uint32{0, 4}},
		{"warning", []uint32{1}},
		{"info", []uint32{3}},
		{"nonexistent", nil},
	}

	for _, tt := range tests {
		results, err := si.Search(tt.term)
		if err != nil {
			t.Fatalf("Search(%q): %v", tt.term, err)
		}
		got := results.ToArray()
		if len(got) == 0 && len(tt.expected) == 0 {
			continue
		}
		if len(got) != len(tt.expected) {
			t.Errorf("Search(%q): got %v, want %v", tt.term, got, tt.expected)

			continue
		}
		for i := range got {
			if got[i] != tt.expected[i] {
				t.Errorf("Search(%q)[%d]: got %d, want %d", tt.term, i, got[i], tt.expected[i])
			}
		}
	}

	// Test Contains.
	if !si.Contains("error") {
		t.Error("Contains('error'): expected true")
	}
	if si.Contains("nonexistent") {
		t.Error("Contains('nonexistent'): expected false")
	}
}

func TestInvertedIndex_Empty(t *testing.T) {
	idx := NewInvertedIndex()
	data, err := idx.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	si, err := DecodeInvertedIndex(data)
	if err != nil {
		t.Fatalf("DecodeInvertedIndex: %v", err)
	}

	results, err := si.Search("anything")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if results.GetCardinality() != 0 {
		t.Errorf("expected empty results")
	}
}

func TestInvertedIndex_LargeScale(t *testing.T) {
	idx := NewInvertedIndex()
	for i := 0; i < 10000; i++ {
		text := fmt.Sprintf("event %d host=web-%02d level=INFO msg=\"request processed for user %d\"", i, i%10, i%1000)
		idx.Add(uint32(i), text)
	}

	data, err := idx.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	t.Logf("10K events index size: %d bytes", len(data))

	si, err := DecodeInvertedIndex(data)
	if err != nil {
		t.Fatalf("DecodeInvertedIndex: %v", err)
	}

	// "info" should appear in all 10K events.
	results, err := si.Search("info")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if results.GetCardinality() != 10000 {
		t.Errorf("search 'info': got %d, want 10000", results.GetCardinality())
	}

	// "05" should appear in ~1000 events (from "web-05" split into "web"+"05").
	results, err = si.Search("05")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if results.GetCardinality() != 1000 {
		t.Errorf("search '05': got %d, want 1000", results.GetCardinality())
	}
}

func BenchmarkInvertedIndexBuild(b *testing.B) {
	texts := make([]string, 10000)
	for i := range texts {
		texts[i] = fmt.Sprintf("2024-01-01 host=web-%02d level=INFO status=200 msg=\"request processed user=%d\"", i%10, i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := NewInvertedIndex()
		for j, text := range texts {
			idx.Add(uint32(j), text)
		}
	}
}

func BenchmarkInvertedIndexSearch(b *testing.B) {
	idx := NewInvertedIndex()
	for i := 0; i < 10000; i++ {
		text := fmt.Sprintf("2024-01-01 host=web-%02d level=INFO status=200 msg=\"request processed user=%d\"", i%10, i)
		idx.Add(uint32(i), text)
	}
	data, _ := idx.Encode()
	si, _ := DecodeInvertedIndex(data)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = si.Search("info")
	}
}
