package index

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestBloomFilter_Basic(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	bf.Add("error")
	bf.Add("warning")
	bf.Add("info")

	if !bf.MayContain("error") {
		t.Error("expected bloom to contain 'error'")
	}
	if !bf.MayContain("warning") {
		t.Error("expected bloom to contain 'warning'")
	}

	// Non-added terms should (mostly) not be found.
	falsePositives := 0
	for i := 0; i < 10000; i++ {
		if bf.MayContain(fmt.Sprintf("nonexistent_%d", i)) {
			falsePositives++
		}
	}
	fpRate := float64(falsePositives) / 10000.0
	t.Logf("false positive rate: %.4f (expected ~0.01)", fpRate)
	if fpRate > 0.05 {
		t.Errorf("false positive rate too high: %.4f", fpRate)
	}
}

func TestBloomFilter_AddTokens(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)
	bf.AddTokens("error connection refused from 192.168.1.100")

	if !bf.MayContain("error") {
		t.Error("expected 'error'")
	}
	if !bf.MayContain("connection") {
		t.Error("expected 'connection'")
	}
	// IP is now split into individual octets by the tokenizer.
	if !bf.MayContain("192") {
		t.Error("expected '192'")
	}
	if !bf.MayContain("168") {
		t.Error("expected '168'")
	}
}

func TestBloomFilter_EncodeDecode(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)
	bf.Add("hello")
	bf.Add("world")

	data, err := bf.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	t.Logf("bloom filter size: %d bytes", len(data))

	decoded, err := DecodeBloomFilter(data)
	if err != nil {
		t.Fatalf("DecodeBloomFilter: %v", err)
	}

	if !decoded.MayContain("hello") {
		t.Error("decoded: expected 'hello'")
	}
	if !decoded.MayContain("world") {
		t.Error("decoded: expected 'world'")
	}
}

func TestBloomFilter_SegmentPruning(t *testing.T) {
	// Simulate: 3 segments, each with a bloom filter.
	segments := make([]*BloomFilter, 3)

	segments[0] = NewBloomFilter(1000, 0.01)
	segments[0].AddTokens("error connection refused from web-01")

	segments[1] = NewBloomFilter(1000, 0.01)
	segments[1].AddTokens("info request processed successfully on web-02")

	segments[2] = NewBloomFilter(1000, 0.01)
	segments[2].AddTokens("error timeout connecting to database on db-01")

	// Search for "error" — should match segments 0 and 2.
	matchingSegments := []int{}
	for i, bf := range segments {
		if bf.MayContain("error") {
			matchingSegments = append(matchingSegments, i)
		}
	}

	if len(matchingSegments) < 2 {
		t.Errorf("expected at least 2 matching segments, got %d", len(matchingSegments))
	}

	// Search for "database" — should match only segment 2.
	matchingSegments = []int{}
	for i, bf := range segments {
		if bf.MayContain("database") {
			matchingSegments = append(matchingSegments, i)
		}
	}
	if len(matchingSegments) != 1 || matchingSegments[0] != 2 {
		t.Errorf("expected [2], got %v", matchingSegments)
	}
}

func TestBloomFilter_MayContainAll(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)
	bf.Add("error")
	bf.Add("connection")
	bf.Add("refused")

	// All present.
	if !bf.MayContainAll([]string{"error", "connection", "refused"}) {
		t.Error("expected MayContainAll to return true for all-present terms")
	}

	// Some missing.
	if bf.MayContainAll([]string{"error", "timeout"}) {
		t.Error("expected MayContainAll to return false when 'timeout' is missing")
	}

	// Empty terms — should always return true.
	if !bf.MayContainAll(nil) {
		t.Error("expected MayContainAll(nil) to return true")
	}
	if !bf.MayContainAll([]string{}) {
		t.Error("expected MayContainAll([]) to return true")
	}
}

func TestBloomFilter_Metadata(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)
	bf.Add("test")

	if bf.BitCount() == 0 {
		t.Error("expected BitCount > 0")
	}
	if bf.HashCount() == 0 {
		t.Error("expected HashCount > 0")
	}
	t.Logf("BitCount=%d HashCount=%d", bf.BitCount(), bf.HashCount())
}

func TestBloomFilter_EncodeDecodeEmpty(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)

	data, err := bf.Encode()
	if err != nil {
		t.Fatalf("Encode empty bloom: %v", err)
	}

	decoded, err := DecodeBloomFilter(data)
	if err != nil {
		t.Fatalf("Decode empty bloom: %v", err)
	}

	if decoded.MayContain("anything") {
		t.Error("empty bloom should not contain 'anything'")
	}
}

func TestBloomFilter_EncodeDecode_SingleElement(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)
	bf.Add("only")

	data, err := bf.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	decoded, err := DecodeBloomFilter(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if !decoded.MayContain("only") {
		t.Error("decoded bloom missing 'only'")
	}
	if decoded.MayContain("other") {
		// Not necessarily an error (could be FP), but log it.
		t.Log("note: false positive for 'other' in single-element bloom")
	}
}

func TestBloomFilter_EncodeDecode_Large(t *testing.T) {
	const n = 100000
	bf := NewBloomFilter(uint(n), 0.01)

	for i := 0; i < n; i++ {
		bf.Add(fmt.Sprintf("term_%d", i))
	}

	data, err := bf.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	t.Logf("100k-element bloom size: %d bytes", len(data))

	decoded, err := DecodeBloomFilter(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	// All original terms must still be present.
	for i := 0; i < n; i++ {
		if !decoded.MayContain(fmt.Sprintf("term_%d", i)) {
			t.Fatalf("false negative at term_%d after decode", i)
		}
	}

	// FP rate after round-trip should remain reasonable.
	falsePositives := 0
	const probes = 100000
	for i := 0; i < probes; i++ {
		if decoded.MayContain(fmt.Sprintf("absent_%d", i)) {
			falsePositives++
		}
	}
	fpRate := float64(falsePositives) / float64(probes)
	t.Logf("FP rate after 100k decode: %.4f", fpRate)
	if fpRate > 0.02 {
		t.Errorf("FP rate too high after round-trip: %.4f (want <= 0.02)", fpRate)
	}
}

func TestBloomFilter_TokenConsistency(t *testing.T) {
	text := "2024-01-15 ERROR connection refused from web-01 host=db-server"

	// Write-time: tokenize and add to bloom.
	bf := NewBloomFilter(1000, 0.01)
	bf.AddTokens(text)

	// Query-time: tokenize the same text and check all tokens are present.
	tokens := TokenizeUnique(text)
	for _, tok := range tokens {
		if !bf.MayContain(tok) {
			t.Errorf("token %q from same text not found in bloom", tok)
		}
	}
}

func TestBloomFilter_SubstringNotMatched(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)
	bf.AddTokens("error connection refused")

	// "err" is NOT a token — only "error" is. Bloom should not contain "err".
	if bf.MayContain("err") {
		// Could be a false positive, but the probability is very low with a
		// properly-sized filter. Log it rather than failing hard.
		t.Log("note: 'err' is a bloom false positive for bloom containing 'error'")
	}

	// "conn" is NOT a token.
	if bf.MayContain("conn") {
		t.Log("note: 'conn' is a bloom false positive")
	}

	// "refuse" is NOT a token.
	if bf.MayContain("refuse") {
		t.Log("note: 'refuse' is a bloom false positive")
	}

	// The actual tokens must be present.
	for _, tok := range []string{"error", "connection", "refused"} {
		if !bf.MayContain(tok) {
			t.Errorf("expected token %q to be in bloom", tok)
		}
	}
}

func TestBloomFilter_RealisticLogLines(t *testing.T) {
	logs := []string{
		`2024-01-15T10:30:00Z host=web-01 level=ERROR msg="Failed to connect to database at 192.168.1.50:5432"`,
		`2024-01-15T10:30:01Z host=web-02 level=INFO msg="Request processed successfully" status=200 latency=42ms`,
		`2024-01-15T10:30:02Z host=db-01 level=WARN msg="Slow query detected" query_time=5.2s table=users`,
		`2024-01-15T10:30:03Z host=web-01 level=ERROR msg="Connection timeout after 30s" remote=10.0.0.1`,
		`2024-01-15T10:30:04Z host=api-gw level=INFO msg="Health check passed" endpoint=/health`,
	}

	bf := NewBloomFilter(1000, 0.01)
	for _, line := range logs {
		bf.AddTokens(line)
	}

	// Expected tokens that should be present.
	// Minor breakers (-:_) now split tokens, so "web-01" → "web"+"01", etc.
	expectedPresent := []string{
		"error", "info", "warn",
		"web", "01", "02", "db", "api", "gw",
		"failed", "connect", "database",
		"request", "processed", "successfully",
		"slow", "query", "detected",
		"connection", "timeout",
		"health", "check", "passed",
		"200", "42ms",
	}
	for _, tok := range expectedPresent {
		if !bf.MayContain(tok) {
			t.Errorf("expected token %q to be in bloom", tok)
		}
	}
}

func TestBloomFilter_SubstringSearchRegression(t *testing.T) {
	// Regression: searching for "004815" must match text containing
	// "0001829126-25-004815" because minor breakers split it into
	// ["0001829126", "25", "004815"].
	bf := NewBloomFilter(1000, 0.01)
	bf.AddTokens("/Archives/edgar/data/1893311/0001829126-25-004815.txt")

	if !bf.MayContain("004815") {
		t.Error("expected bloom to contain '004815' from '0001829126-25-004815'")
	}
	if !bf.MayContain("0001829126") {
		t.Error("expected bloom to contain '0001829126'")
	}
	if !bf.MayContain("25") {
		t.Error("expected bloom to contain '25'")
	}

	// Also verify MayContainAll with tokenized search term.
	searchTokens := Tokenize("004815")
	if !bf.MayContainAll(searchTokens) {
		t.Error("expected MayContainAll(Tokenize(\"004815\")) to return true")
	}
}

func TestBloomFilter_FalsePositiveRate_UnderLoad(t *testing.T) {
	const addCount = 50000
	bf := NewBloomFilter(uint(addCount), 0.01)

	rng := rand.New(rand.NewSource(42))
	for i := 0; i < addCount; i++ {
		bf.Add(fmt.Sprintf("present_%d_%d", i, rng.Int63()))
	}

	// Probe with 100k absent terms.
	falsePositives := 0
	const probes = 100000
	for i := 0; i < probes; i++ {
		if bf.MayContain(fmt.Sprintf("absent_%d_%d", i, rng.Int63())) {
			falsePositives++
		}
	}
	fpRate := float64(falsePositives) / float64(probes)
	t.Logf("FP rate: %.4f (expected <= 0.02)", fpRate)
	if fpRate > 0.02 {
		t.Errorf("FP rate too high: %.4f", fpRate)
	}
}

func BenchmarkBloomFilterAdd(b *testing.B) {
	bf := NewBloomFilter(100000, 0.01)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.Add(fmt.Sprintf("term_%d", i))
	}
}

func BenchmarkBloomFilterCheck(b *testing.B) {
	bf := NewBloomFilter(100000, 0.01)
	for i := 0; i < 100000; i++ {
		bf.Add(fmt.Sprintf("term_%d", i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.MayContain(fmt.Sprintf("term_%d", i%100000))
	}
}
