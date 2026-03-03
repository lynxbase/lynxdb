package index

import "testing"

// TestBloomFilter_ConsistencyWithInvertedIndex verifies that the bloom filter
// and inverted index agree: any term found by the inverted index must also
// pass the bloom filter check.
func TestBloomFilter_ConsistencyWithInvertedIndex(t *testing.T) {
	logLines := []string{
		"2024-01-15T10:30:00Z host=web-01 level=ERROR msg=\"connection refused\"",
		"2024-01-15T10:30:01Z host=web-02 level=INFO msg=\"request processed\" status=200",
		"2024-01-15T10:30:02Z host=db-01 level=WARN msg=\"slow query\" query_time=5s",
		"2024-01-15T10:30:03Z host=web-01 level=ERROR msg=\"timeout connecting to db\"",
		"2024-01-15T10:30:04Z host=api-gw level=DEBUG msg=\"health check ok\"",
	}

	// Build bloom and inverted index from same events (mimicking writer).
	inv := NewInvertedIndex()
	uniqueTokens := make(map[string]struct{})
	for i, line := range logLines {
		for _, tok := range TokenizeUnique(line) {
			uniqueTokens[tok] = struct{}{}
		}
		inv.Add(uint32(i), line)
	}

	bf := NewBloomFilter(uint(len(uniqueTokens)+100), 0.01)
	for tok := range uniqueTokens {
		bf.Add(tok)
	}

	// Encode + decode inverted index to simulate reading from segment.
	data, err := inv.Encode()
	if err != nil {
		t.Fatalf("inv.Encode: %v", err)
	}
	si, err := DecodeInvertedIndex(data)
	if err != nil {
		t.Fatalf("DecodeInvertedIndex: %v", err)
	}

	// For every unique token, if the inverted index finds it, bloom must also.
	for tok := range uniqueTokens {
		bm, err := si.Search(tok)
		if err != nil {
			t.Fatalf("Search(%q): %v", tok, err)
		}
		if bm.GetCardinality() > 0 && !bf.MayContain(tok) {
			t.Errorf("inverted index found %q (card=%d) but bloom filter says absent",
				tok, bm.GetCardinality())
		}
	}

	// Also verify bloom does not produce false negatives for any known token.
	for tok := range uniqueTokens {
		if !bf.MayContain(tok) {
			t.Errorf("bloom false negative for known token %q", tok)
		}
	}
}

// TestBloomFilter_BuiltInFieldTokens verifies that host/source/sourcetype
// tokens would be in a bloom built the same way as the segment writer.
func TestBloomFilter_BuiltInFieldTokens(t *testing.T) {
	// Simulate what writer.go does: tokenize _raw + built-in fields.
	raw := "2024-01-15T10:30:00Z level=ERROR msg=\"connection refused\""
	host := "web-server-01"
	source := "/var/log/app.log"
	sourceType := "json_events"

	uniqueTokens := make(map[string]struct{})
	for _, tok := range TokenizeUnique(raw) {
		uniqueTokens[tok] = struct{}{}
	}
	for _, field := range []string{host, source, sourceType} {
		for _, tok := range TokenizeUnique(field) {
			uniqueTokens[tok] = struct{}{}
		}
	}

	bf := NewBloomFilter(uint(len(uniqueTokens)+100), 0.01)
	for tok := range uniqueTokens {
		bf.Add(tok)
	}

	// Tokens from host.
	for _, tok := range TokenizeUnique(host) {
		if !bf.MayContain(tok) {
			t.Errorf("host token %q missing from bloom", tok)
		}
	}

	// Tokens from source.
	for _, tok := range TokenizeUnique(source) {
		if !bf.MayContain(tok) {
			t.Errorf("source token %q missing from bloom", tok)
		}
	}

	// Tokens from sourcetype.
	for _, tok := range TokenizeUnique(sourceType) {
		if !bf.MayContain(tok) {
			t.Errorf("sourcetype token %q missing from bloom", tok)
		}
	}

	// Tokens from raw.
	for _, tok := range TokenizeUnique(raw) {
		if !bf.MayContain(tok) {
			t.Errorf("raw token %q missing from bloom", tok)
		}
	}
}
