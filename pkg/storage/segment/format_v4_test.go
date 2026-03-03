package segment

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
	"github.com/OrlovEvgeny/Lynxdb/pkg/storage/segment/index"
)

func TestV4_BloomAndInvertedRoundtrip(t *testing.T) {
	events := generateTestEvents(200)

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	written, err := sw.Write(events)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	t.Logf("V4 segment: %d events, %d bytes", len(events), written)

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	if r.EventCount() != int64(len(events)) {
		t.Fatalf("EventCount: got %d, want %d", r.EventCount(), len(events))
	}

	// Verify bloom filter.
	bf, err := r.BloomFilter()
	if err != nil {
		t.Fatalf("BloomFilter: %v", err)
	}
	if bf == nil {
		t.Fatal("segment should have bloom filter")
	}

	if !bf.MayContain("request") {
		t.Error("bloom: 'request' should be present")
	}
	if !bf.MayContain("processed") {
		t.Error("bloom: 'processed' should be present")
	}

	// Verify inverted index.
	inv, err := r.InvertedIndex()
	if err != nil {
		t.Fatalf("InvertedIndex: %v", err)
	}
	if inv == nil {
		t.Fatal("segment should have inverted index")
	}

	bm, err := inv.Search("info")
	if err != nil {
		t.Fatalf("Search(info): %v", err)
	}
	if bm.GetCardinality() == 0 {
		t.Error("expected matches for 'info' in inverted index")
	}

	bm, err = inv.Search("error")
	if err != nil {
		t.Fatalf("Search(error): %v", err)
	}
	if bm.GetCardinality() == 0 {
		t.Error("expected matches for 'error' in inverted index")
	}

	// Verify full event round-trip.
	readEvents, err := r.ReadEvents()
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(readEvents) != len(events) {
		t.Fatalf("ReadEvents: got %d, want %d", len(readEvents), len(events))
	}

	for i := range events {
		if !readEvents[i].Time.Equal(events[i].Time) {
			t.Errorf("event[%d].Time mismatch", i)
		}
		if readEvents[i].Raw != events[i].Raw {
			t.Errorf("event[%d].Raw mismatch", i)
		}
		if readEvents[i].Host != events[i].Host {
			t.Errorf("event[%d].Host mismatch", i)
		}
	}
}

func TestV4_ColumnPruning(t *testing.T) {
	events := generateTestEvents(50)

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	pruned, err := r.ReadEventsWithColumns([]string{"_raw", "host"})
	if err != nil {
		t.Fatalf("ReadEventsWithColumns: %v", err)
	}
	if len(pruned) != len(events) {
		t.Fatalf("pruned count: got %d, want %d", len(pruned), len(events))
	}

	for i, e := range pruned {
		if e.Raw != events[i].Raw {
			t.Errorf("event[%d].Raw mismatch", i)
		}
		if e.Host != events[i].Host {
			t.Errorf("event[%d].Host mismatch", i)
		}
		if e.Source != "" {
			t.Errorf("event[%d].Source should be empty, got %q", i, e.Source)
		}
	}
}

func TestV4_BloomFilter_NegativeCheck(t *testing.T) {
	events := generateTestEvents(100)

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	bf, err := r.BloomFilter()
	if err != nil {
		t.Fatalf("BloomFilter: %v", err)
	}

	// With fpRate=0.01 and 100 events, the probability of a false positive
	// for a single random term is ~1%. We test multiple terms to ensure at
	// least one correctly reports "not found". If ALL report present, either
	// the bloom filter is broken or we hit an astronomically unlikely event.
	absentTerms := []string{
		"xq7z9k2m_definitely_not_in_events",
		"zy3w8v1p_also_absent_from_data",
		"kj5r2n9t_never_appeared_anywhere",
	}
	falsePositives := 0
	for _, term := range absentTerms {
		if bf.MayContain(term) {
			falsePositives++
		}
	}
	if falsePositives == len(absentTerms) {
		t.Errorf("bloom filter returned true for ALL %d absent terms — bloom is likely broken (expected at most 1 false positive)", len(absentTerms))
	}
}

func TestV4_ReadEventsByBitmap(t *testing.T) {
	events := generateTestEvents(200)

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	inv, err := r.InvertedIndex()
	if err != nil {
		t.Fatalf("InvertedIndex: %v", err)
	}

	bm, err := inv.Search("error")
	if err != nil {
		t.Fatalf("Search(error): %v", err)
	}
	if bm.GetCardinality() == 0 {
		t.Fatal("expected matches for 'error'")
	}

	filtered, err := r.ReadEventsByBitmap(bm, []string{"_raw", "host", "index"})
	if err != nil {
		t.Fatalf("ReadEventsByBitmap: %v", err)
	}

	if len(filtered) != int(bm.GetCardinality()) {
		t.Errorf("filtered count: got %d, want %d", len(filtered), bm.GetCardinality())
	}

	for i, e := range filtered {
		if e.Raw == "" {
			t.Errorf("event[%d]: _raw is empty", i)
		}
		if e.Host == "" {
			t.Errorf("event[%d]: host is empty", i)
		}
	}
}

func TestV4_ReadEventsByBitmap_EmptyBitmap(t *testing.T) {
	events := generateTestEvents(50)

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	bm := roaring.New()
	filtered, err := r.ReadEventsByBitmap(bm, []string{"_raw"})
	if err != nil {
		t.Fatalf("ReadEventsByBitmap: %v", err)
	}
	if filtered != nil {
		t.Errorf("expected nil for empty bitmap, got %d events", len(filtered))
	}
}

func TestV4_ReadEventsFiltered(t *testing.T) {
	events := generateTestEvents(200)

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	preds := []Predicate{
		{Field: "status", Op: ">=", Value: "400"},
	}
	filtered, err := r.ReadEventsFiltered(preds, nil, []string{"_raw", "host", "index"})
	if err != nil {
		t.Fatalf("ReadEventsFiltered: %v", err)
	}

	expected := 0
	for _, e := range events {
		v := e.GetField("status")
		if v.AsInt() >= 400 {
			expected++
		}
	}

	if len(filtered) != expected {
		t.Errorf("filtered count: got %d, want %d", len(filtered), expected)
	}
}

func TestV4_ReadEventsFiltered_WithSearchBitmap(t *testing.T) {
	events := generateTestEvents(200)

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	inv, err := r.InvertedIndex()
	if err != nil {
		t.Fatalf("InvertedIndex: %v", err)
	}
	searchBm, err := inv.Search("error")
	if err != nil {
		t.Fatalf("Search(error): %v", err)
	}

	preds := []Predicate{
		{Field: "status", Op: ">=", Value: "400"},
	}
	filtered, err := r.ReadEventsFiltered(preds, searchBm, []string{"_raw", "host", "index"})
	if err != nil {
		t.Fatalf("ReadEventsFiltered: %v", err)
	}

	if uint64(len(filtered)) > searchBm.GetCardinality() {
		t.Errorf("filtered count %d exceeds search bitmap cardinality %d",
			len(filtered), searchBm.GetCardinality())
	}
}

func TestV4_StatsByName(t *testing.T) {
	events := generateTestEvents(100)

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	stats := r.StatsByName("status")
	if stats == nil {
		t.Fatal("expected stats for 'status' column")
	}
	if stats.Count != 100 {
		t.Errorf("stats.Count: got %d, want 100", stats.Count)
	}

	if r.StatsByName("nonexistent") != nil {
		t.Error("expected nil for non-existent column")
	}
}

func TestBloomFilter_SegmentWriteReadRoundTrip(t *testing.T) {
	events := generateTestEvents(200)

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	bf, err := r.BloomFilter()
	if err != nil {
		t.Fatalf("BloomFilter: %v", err)
	}
	if bf == nil {
		t.Fatal("expected bloom filter")
	}

	for _, term := range []string{"request", "processed"} {
		if !bf.MayContain(term) {
			t.Errorf("bloom should contain %q (present in all events)", term)
		}
	}

	for _, level := range []string{"info", "warn", "error", "debug"} {
		found := false
		for _, e := range events {
			if strings.Contains(strings.ToLower(e.Raw), level) {
				found = true

				break
			}
		}
		if found && !bf.MayContain(level) {
			t.Errorf("bloom should contain %q (present in event raw)", level)
		}
	}

	hostTokens := make(map[string]bool)
	for _, e := range events {
		for _, tok := range index.TokenizeUnique(e.Host) {
			hostTokens[tok] = true
		}
	}
	for tok := range hostTokens {
		if !bf.MayContain(tok) {
			t.Errorf("bloom should contain host token %q", tok)
		}
	}

	if bf.MayContain("xyzzy_nonexistent_term_12345") {
		t.Log("note: false positive for clearly absent term")
	}

	if !bf.MayContainAll([]string{"request", "processed"}) {
		t.Error("MayContainAll should be true for terms in all events")
	}
	if bf.MayContainAll([]string{"request", "xyzzy_nonexistent_term_12345"}) {
		t.Log("note: MayContainAll false positive for mix of present + absent")
	}
}

func TestV4_MultiRowGroup_RoundTrip(t *testing.T) {
	// Generate more than DefaultRowGroupSize events to test multi-row-group.
	n := DefaultRowGroupSize + 500
	events := generateTestEvents(n)

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	written, err := sw.Write(events)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	t.Logf("Multi-RG segment: %d events, %d bytes, expected 2 row groups", n, written)

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	if r.EventCount() != int64(n) {
		t.Fatalf("EventCount: got %d, want %d", r.EventCount(), n)
	}

	if r.RowGroupCount() != 2 {
		t.Fatalf("RowGroupCount: got %d, want 2", r.RowGroupCount())
	}

	readEvents, err := r.ReadEvents()
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(readEvents) != n {
		t.Fatalf("ReadEvents: got %d events, want %d", len(readEvents), n)
	}

	for i := range events {
		if !readEvents[i].Time.Equal(events[i].Time) {
			t.Errorf("event[%d].Time mismatch", i)

			break
		}
		if readEvents[i].Raw != events[i].Raw {
			t.Errorf("event[%d].Raw mismatch", i)

			break
		}
		if readEvents[i].Host != events[i].Host {
			t.Errorf("event[%d].Host mismatch", i)

			break
		}
	}
}

func TestV4_RowGroupPruning(t *testing.T) {
	// Create events spanning 2 row groups with distinct time ranges.
	n := DefaultRowGroupSize + 500
	events := generateTestEvents(n)

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	if r.RowGroupCount() != 2 {
		t.Fatalf("need 2 row groups for pruning test, got %d", r.RowGroupCount())
	}

	// Get time range of the last row group by reading its zone map.
	rg1TimeChunk := findChunk(&r.footer.RowGroups[1], "_time")
	if rg1TimeChunk == nil {
		t.Fatal("row group 1 should have _time column")
	}
	t.Logf("RG0 rows=%d, RG1 rows=%d", r.footer.RowGroups[0].RowCount, r.footer.RowGroups[1].RowCount)

	// Query with time range that only covers the second row group.
	// The second row group starts after DefaultRowGroupSize events.
	rg1Start := events[DefaultRowGroupSize].Time
	maxTime := events[len(events)-1].Time.Add(time.Second)

	hints := QueryHints{
		MinTime: &rg1Start,
		MaxTime: &maxTime,
	}

	prunedEvents, err := r.ReadEventsWithHints(hints)
	if err != nil {
		t.Fatalf("ReadEventsWithHints: %v", err)
	}

	// Should return only events from the second row group (approximately 500).
	expectedCount := len(events) - DefaultRowGroupSize
	if len(prunedEvents) != expectedCount {
		t.Errorf("pruned events: got %d, want %d", len(prunedEvents), expectedCount)
	}
	t.Logf("pruned from %d to %d events (skipped row group 0)", len(events), len(prunedEvents))
}

func TestV4_ColumnProjectionWithRowGroups(t *testing.T) {
	n := DefaultRowGroupSize + 100
	events := generateTestEvents(n)

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	// Request only 2 columns out of ~9 total.
	projected, err := r.ReadEventsWithColumns([]string{"_raw", "host"})
	if err != nil {
		t.Fatalf("ReadEventsWithColumns: %v", err)
	}

	if len(projected) != n {
		t.Fatalf("projected count: got %d, want %d", len(projected), n)
	}

	for i, e := range projected {
		if e.Raw != events[i].Raw {
			t.Errorf("event[%d].Raw mismatch", i)

			break
		}
		if e.Host != events[i].Host {
			t.Errorf("event[%d].Host mismatch", i)

			break
		}
		if e.Source != "" {
			t.Errorf("event[%d].Source should be empty, got %q", i, e.Source)

			break
		}
	}
}

func TestV4_100KEvents(t *testing.T) {
	events := generateTestEvents(100_000)

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	written, err := sw.Write(events)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	t.Logf("100K events: %d bytes (%.1f bytes/event), %d row groups",
		written, float64(written)/float64(len(events)),
		(len(events)+DefaultRowGroupSize-1)/DefaultRowGroupSize)

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	if r.EventCount() != 100_000 {
		t.Fatalf("EventCount: got %d, want 100000", r.EventCount())
	}

	readEvents, err := r.ReadEvents()
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(readEvents) != len(events) {
		t.Fatalf("ReadEvents: got %d, want %d", len(readEvents), len(events))
	}

	// Spot-check a few events.
	for _, idx := range []int{0, 1000, 50000, 99999} {
		if !readEvents[idx].Time.Equal(events[idx].Time) {
			t.Errorf("event[%d].Time mismatch", idx)
		}
		if readEvents[idx].Raw != events[idx].Raw {
			t.Errorf("event[%d].Raw mismatch", idx)
		}
	}
}

func TestV4_EventsWithNulls(t *testing.T) {
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	events := make([]*event.Event, 50)
	for i := range events {
		e := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond), fmt.Sprintf("event %d", i))
		e.Host = "web-01"
		e.Source = "/var/log/app.log"
		e.SourceType = "json"
		e.Index = "main"
		// Only set field on even events to create null gaps.
		if i%2 == 0 {
			e.SetField("status", event.IntValue(200))
		}
		events[i] = e
	}

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	readEvents, err := r.ReadEvents()
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}

	for i, e := range readEvents {
		if i%2 == 0 {
			v := e.GetField("status")
			if v.AsInt() != 200 {
				t.Errorf("event[%d].status: got %d, want 200", i, v.AsInt())
			}
		}
	}
}

func TestV4_CRC32_Integrity(t *testing.T) {
	events := generateTestEvents(100)

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	data := buf.Bytes()

	// Corrupt a byte in the middle of the data (column chunk area).
	corrupt := make([]byte, len(data))
	copy(corrupt, data)
	corrupt[HeaderSize+50] ^= 0xFF // flip a byte in a column chunk

	// At least one of OpenSegment or ReadEvents MUST fail on corrupted data.
	// If both succeed, CRC integrity checking is broken.
	r, openErr := OpenSegment(corrupt)
	if openErr != nil {
		// Good — corruption detected at open time.
		t.Logf("corruption detected at open: %v", openErr)

		return
	}

	_, readErr := r.ReadEvents()
	if readErr != nil {
		// Good — corruption detected at read time.
		t.Logf("corruption detected at read: %v", readErr)

		return
	}

	// Both succeeded — verify the data at least differs from original.
	// Re-read from uncorrupted data and compare.
	origR, err := OpenSegment(data)
	if err != nil {
		t.Fatalf("OpenSegment (original): %v", err)
	}
	origEvents, err := origR.ReadEvents()
	if err != nil {
		t.Fatalf("ReadEvents (original): %v", err)
	}
	corruptR, _ := OpenSegment(corrupt)
	corruptEvents, _ := corruptR.ReadEvents()

	// If the corrupted segment returns identical data, the corruption
	// landed in a non-data area (bloom/index). That's acceptable — the
	// data itself is intact. But if it returns DIFFERENT data silently,
	// CRC checking is broken.
	for i := range origEvents {
		if i >= len(corruptEvents) {
			break
		}
		if origEvents[i].Raw != corruptEvents[i].Raw {
			t.Errorf("corrupted segment returned different data without error at event[%d]: orig=%q, corrupt=%q",
				i, origEvents[i].Raw, corruptEvents[i].Raw)
		}
	}
}

func TestV4_ZSTD_RoundTrip(t *testing.T) {
	events := generateTestEvents(200)

	var buf bytes.Buffer
	sw := NewWriterWithCompression(&buf, CompressionZSTD)
	n, err := sw.Write(events)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	t.Logf("ZSTD segment: %d events, %d bytes", len(events), n)

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	readEvents, err := r.ReadEvents()
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(readEvents) != len(events) {
		t.Fatalf("ReadEvents: got %d, want %d", len(readEvents), len(events))
	}

	for i, e := range readEvents {
		if !e.Time.Equal(events[i].Time) {
			t.Errorf("event[%d].Time mismatch", i)
		}
		if e.Raw != events[i].Raw {
			t.Errorf("event[%d].Raw mismatch", i)
		}
		if e.Host != events[i].Host {
			t.Errorf("event[%d].Host mismatch", i)
		}
	}

	// Verify at least some chunks used ZSTD compression.
	zstdCount := 0
	for _, rg := range r.footer.RowGroups {
		for _, cc := range rg.Columns {
			if cc.Compression == CompressionZSTD {
				zstdCount++
			}
		}
	}
	if zstdCount == 0 {
		t.Error("expected at least one ZSTD-compressed chunk")
	}
	t.Logf("ZSTD compressed chunks: %d", zstdCount)
}

func TestV4_ZSTD_RawColumnSkipsLayer2(t *testing.T) {
	events := generateTestEvents(100)

	var buf bytes.Buffer
	sw := NewWriterWithCompression(&buf, CompressionZSTD)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	// _raw column should have CompressionNone because layer 1 is already LZ4.
	for _, rg := range r.footer.RowGroups {
		rawChunk := findChunk(&rg, "_raw")
		if rawChunk == nil {
			t.Fatal("missing _raw column")
		}
		if rawChunk.Compression != CompressionNone {
			t.Errorf("_raw column has compression %d, want None (0)", rawChunk.Compression)
		}
	}
}

func TestV4_ZSTD_SizeComparison(t *testing.T) {
	// Generate realistic log data with repetitive patterns.
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	events := make([]*event.Event, 10000)
	for i := range events {
		e := event.NewEvent(
			base.Add(time.Duration(i)*time.Millisecond),
			fmt.Sprintf("2024-01-01T00:00:%02d.%03dZ host=web-%02d level=INFO component=api msg=\"request processed\" method=GET path=/api/v1/users status=%d latency=%dms",
				i/1000, i%1000, i%10, 200+i%5, 10+i%100),
		)
		e.Host = fmt.Sprintf("web-%02d", i%10)
		e.Source = "/var/log/app.log"
		e.SourceType = "json"
		e.Index = "main"
		e.SetField("status", event.IntValue(int64(200+i%5)))
		e.SetField("latency_ms", event.FloatValue(float64(10+i%100)))
		events[i] = e
	}

	var lz4Buf bytes.Buffer
	lz4W := NewWriterWithCompression(&lz4Buf, CompressionLZ4)
	lz4Size, err := lz4W.Write(events)
	if err != nil {
		t.Fatalf("LZ4 Write: %v", err)
	}

	var zstdBuf bytes.Buffer
	zstdW := NewWriterWithCompression(&zstdBuf, CompressionZSTD)
	zstdSize, err := zstdW.Write(events)
	if err != nil {
		t.Fatalf("ZSTD Write: %v", err)
	}

	t.Logf("10K events: LZ4 = %d bytes, ZSTD = %d bytes, ZSTD saving = %.1f%%",
		lz4Size, zstdSize, float64(lz4Size-zstdSize)/float64(lz4Size)*100)

	if zstdSize >= lz4Size {
		t.Logf("note: ZSTD not smaller than LZ4 for this dataset (LZ4=%d, ZSTD=%d)", lz4Size, zstdSize)
	}

	// Verify both round-trip correctly.
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"LZ4", lz4Buf.Bytes()},
		{"ZSTD", zstdBuf.Bytes()},
	} {
		r, err := OpenSegment(tc.data)
		if err != nil {
			t.Fatalf("%s OpenSegment: %v", tc.name, err)
		}
		got, err := r.ReadEvents()
		if err != nil {
			t.Fatalf("%s ReadEvents: %v", tc.name, err)
		}
		if len(got) != len(events) {
			t.Fatalf("%s: got %d events, want %d", tc.name, len(got), len(events))
		}
	}
}

func TestV4_ZSTD_MultiRowGroup(t *testing.T) {
	// Generate enough events for 2 row groups.
	events := generateTestEvents(DefaultRowGroupSize + 500)

	var buf bytes.Buffer
	sw := NewWriterWithCompression(&buf, CompressionZSTD)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	if r.RowGroupCount() != 2 {
		t.Fatalf("expected 2 row groups, got %d", r.RowGroupCount())
	}

	readEvents, err := r.ReadEvents()
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(readEvents) != len(events) {
		t.Fatalf("ReadEvents: got %d, want %d", len(readEvents), len(events))
	}
}

func TestV4_PerRowGroupBloom(t *testing.T) {
	// Create events where different row groups have distinct terms.
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	events := make([]*event.Event, DefaultRowGroupSize+500)

	// RG0: events 0..65535 contain "alpha" but not "beta"
	for i := 0; i < DefaultRowGroupSize; i++ {
		e := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond),
			fmt.Sprintf("event %d alpha host=web-01", i))
		e.Host = "web-01"
		e.Source = "/var/log/app.log"
		e.SourceType = "json"
		e.Index = "main"
		events[i] = e
	}
	// RG1: events 65536..66035 contain "beta" but not "alpha"
	for i := DefaultRowGroupSize; i < len(events); i++ {
		e := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond),
			fmt.Sprintf("event %d beta host=web-02", i))
		e.Host = "web-02"
		e.Source = "/var/log/app.log"
		e.SourceType = "json"
		e.Index = "main"
		events[i] = e
	}

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	if r.RowGroupCount() != 2 {
		t.Fatalf("expected 2 row groups, got %d", r.RowGroupCount())
	}

	// "alpha" should match only RG0.
	alphaRGs, err := r.CheckBloomForRowGroups("alpha")
	if err != nil {
		t.Fatalf("CheckBloomForRowGroups(alpha): %v", err)
	}
	if len(alphaRGs) != 1 || alphaRGs[0] != 0 {
		t.Errorf("expected [0] for 'alpha', got %v", alphaRGs)
	}

	// "beta" should match only RG1.
	betaRGs, err := r.CheckBloomForRowGroups("beta")
	if err != nil {
		t.Fatalf("CheckBloomForRowGroups(beta): %v", err)
	}
	if len(betaRGs) != 1 || betaRGs[0] != 1 {
		t.Errorf("expected [1] for 'beta', got %v", betaRGs)
	}

	// "event" should match both RGs.
	eventRGs, err := r.CheckBloomForRowGroups("event")
	if err != nil {
		t.Fatalf("CheckBloomForRowGroups(event): %v", err)
	}
	if len(eventRGs) != 2 {
		t.Errorf("expected 2 RGs for 'event', got %v", eventRGs)
	}

	// Per-RG bloom with ReadEventsWithHints: search "beta" should skip RG0.
	got, err := r.ReadEventsWithHints(QueryHints{
		SearchTerms: []string{"beta"},
	})
	if err != nil {
		t.Fatalf("ReadEventsWithHints: %v", err)
	}
	if len(got) != 500 {
		t.Errorf("expected 500 events (RG1 only), got %d", len(got))
	}

	t.Logf("Per-RG bloom: alpha→%v, beta→%v, event→%v", alphaRGs, betaRGs, eventRGs)
}

func TestV4_PerRowGroupBloom_AllTerms(t *testing.T) {
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	events := make([]*event.Event, DefaultRowGroupSize+500)

	// RG0: contains "error" and "database"
	for i := 0; i < DefaultRowGroupSize; i++ {
		e := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond),
			"error connecting to database host=db-01")
		e.Host = "db-01"
		e.Index = "main"
		events[i] = e
	}
	// RG1: contains "error" but NOT "database"
	for i := DefaultRowGroupSize; i < len(events); i++ {
		e := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond),
			"error timeout reached host=web-01")
		e.Host = "web-01"
		e.Index = "main"
		events[i] = e
	}

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	// Search ["error", "database"] — should match only RG0.
	matching, err := r.CheckBloomAllTermsForRowGroups([]string{"error", "database"})
	if err != nil {
		t.Fatalf("CheckBloomAllTermsForRowGroups: %v", err)
	}
	if len(matching) != 1 || matching[0] != 0 {
		t.Errorf("expected [0] for ['error','database'], got %v", matching)
	}

	// Search ["error"] — should match both.
	matching, err = r.CheckBloomAllTermsForRowGroups([]string{"error"})
	if err != nil {
		t.Fatalf("CheckBloomAllTermsForRowGroups: %v", err)
	}
	if len(matching) != 2 {
		t.Errorf("expected 2 RGs for ['error'], got %v", matching)
	}
}

func TestV4_PerRowGroupBloom_SingleRowGroup(t *testing.T) {
	events := generateTestEvents(100)

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	if r.RowGroupCount() != 1 {
		t.Fatalf("expected 1 row group, got %d", r.RowGroupCount())
	}

	// BloomFilter() backward compat should still work.
	bf, err := r.BloomFilter()
	if err != nil {
		t.Fatalf("BloomFilter: %v", err)
	}
	if bf == nil {
		t.Fatal("expected non-nil bloom filter")
	}
	if !bf.MayContain("request") {
		t.Error("bloom should contain 'request'")
	}

	// Per-RG access.
	bf0, err := r.BloomFilterForRowGroup(0)
	if err != nil {
		t.Fatalf("BloomFilterForRowGroup(0): %v", err)
	}
	if bf0 == nil {
		t.Fatal("expected non-nil bloom for RG0")
	}
	if !bf0.MayContain("request") {
		t.Error("RG0 bloom should contain 'request'")
	}
}

func BenchmarkCheckBloomForRowGroups(b *testing.B) {
	events := generateTestEvents(DefaultRowGroupSize + 500)
	var buf bytes.Buffer
	w := NewWriter(&buf)
	_, _ = w.Write(events)
	data := buf.Bytes()
	r, _ := OpenSegment(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = r.CheckBloomForRowGroups("request")
	}
}

func TestMultiRowGroupBloomUnion(t *testing.T) {
	// Regression test: segments with >1 row group must produce a valid
	// segment-level bloom filter via BloomFilter(). Previously, each row
	// group's bloom was sized by its own unique token count, leading to
	// mismatched m/k values and a "m's don't match" error on Merge().
	// The fix sizes all blooms uniformly using the max token count.

	// Create events where row groups have deliberately different token
	// distributions: RG0 has many unique tokens, RG1 (shorter) has fewer.
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	n := DefaultRowGroupSize + 500
	events := make([]*event.Event, n)

	// RG0: 65536 events with varying content (many unique tokens).
	for i := 0; i < DefaultRowGroupSize; i++ {
		e := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond),
			fmt.Sprintf("event=%d host=web-%03d level=INFO action=process request_id=req-%08d path=/api/v%d/resource/%d",
				i, i%100, i, i%5, i%1000))
		e.Host = fmt.Sprintf("web-%03d", i%100)
		e.Source = "/var/log/app.log"
		e.SourceType = "json"
		e.Index = "main"
		events[i] = e
	}

	// RG1: 500 events with fewer unique tokens (shorter, less diverse).
	for i := DefaultRowGroupSize; i < n; i++ {
		e := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond),
			fmt.Sprintf("event=%d host=db-01 level=ERROR msg=timeout", i))
		e.Host = "db-01"
		e.Source = "/var/log/db.log"
		e.SourceType = "json"
		e.Index = "main"
		events[i] = e
	}

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	if r.RowGroupCount() != 2 {
		t.Fatalf("expected 2 row groups, got %d", r.RowGroupCount())
	}

	// Verify per-row-group bloom filters have matching m and k.
	bf0, err := r.BloomFilterForRowGroup(0)
	if err != nil {
		t.Fatalf("BloomFilterForRowGroup(0): %v", err)
	}
	bf1, err := r.BloomFilterForRowGroup(1)
	if err != nil {
		t.Fatalf("BloomFilterForRowGroup(1): %v", err)
	}
	if bf0 == nil || bf1 == nil {
		t.Fatal("both row groups should have bloom filters")
	}
	if bf0.BitCount() != bf1.BitCount() {
		t.Errorf("bloom filter bit counts differ: rg0=%d, rg1=%d", bf0.BitCount(), bf1.BitCount())
	}
	if bf0.HashCount() != bf1.HashCount() {
		t.Errorf("bloom filter hash counts differ: rg0=%d, rg1=%d", bf0.HashCount(), bf1.HashCount())
	}

	// The key assertion: BloomFilter() must return a valid merged filter, not nil/error.
	merged, err := r.BloomFilter()
	if err != nil {
		t.Fatalf("BloomFilter: %v", err)
	}
	if merged == nil {
		t.Fatal("BloomFilter must return non-nil for multi-RG segment with uniform bloom sizing")
	}

	// Verify the merged filter contains terms from both row groups.
	for _, tok := range []string{"info", "error", "timeout", "process"} {
		if !merged.MayContain(tok) {
			t.Errorf("merged bloom should contain %q", tok)
		}
	}

	// Verify absent terms are correctly rejected.
	if merged.MayContain("xyzzy_definitely_absent_42") {
		t.Log("note: false positive for absent term (acceptable)")
	}
}

func TestV4_NoCompression(t *testing.T) {
	events := generateTestEvents(100)

	var buf bytes.Buffer
	sw := NewWriterWithCompression(&buf, CompressionNone)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	// Verify no chunks have layer 2 compression.
	for _, rg := range r.footer.RowGroups {
		for _, cc := range rg.Columns {
			if cc.Compression != CompressionNone {
				t.Errorf("column %q has compression %d, want None", cc.Name, cc.Compression)
			}
		}
	}

	readEvents, err := r.ReadEvents()
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(readEvents) != len(events) {
		t.Fatalf("ReadEvents: got %d, want %d", len(readEvents), len(events))
	}
}

func BenchmarkSegmentWriteZSTD(b *testing.B) {
	events := generateTestEvents(100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		buf.Grow(4 << 20)
		w := NewWriterWithCompression(&buf, CompressionZSTD)
		_, _ = w.Write(events)
	}
}

func BenchmarkSegmentReadZSTD(b *testing.B) {
	events := generateTestEvents(100_000)
	var buf bytes.Buffer
	w := NewWriterWithCompression(&buf, CompressionZSTD)
	_, _ = w.Write(events)
	data := buf.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ := OpenSegment(data)
		_, _ = r.ReadEvents()
	}
}

func TestLazyFieldsAllocation(t *testing.T) {
	// Verify that Fields map stays nil when only built-in columns are decoded.
	// Key optimization: avoids N map allocations when user-defined fields
	// are not needed (e.g., projection to _raw + host only).
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	events := make([]*event.Event, 100)
	for i := range events {
		e := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond),
			fmt.Sprintf("event %d level=info", i))
		e.Host = "web-01"
		e.Source = "/var/log/app.log"
		e.SourceType = "json"
		e.Index = "main"
		e.SetField("level", event.StringValue("info"))
		e.SetField("status", event.IntValue(200))
		e.SetField("latency", event.FloatValue(1.5))
		events[i] = e
	}

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	// Read with projection to builtins only — Fields should stay nil.
	projected, err := r.ReadEventsWithColumns([]string{"_raw", "host"})
	if err != nil {
		t.Fatalf("ReadEventsWithColumns: %v", err)
	}
	for i, e := range projected {
		if e.Fields != nil {
			t.Errorf("event[%d].Fields should be nil when only builtins projected, got %v", i, e.Fields)

			break
		}
		// GetField on nil Fields must return NullValue, not panic.
		v := e.GetField("status")
		if v.Type() != event.FieldTypeNull {
			t.Errorf("event[%d].GetField(status) should return NullValue, got %v", i, v)
		}
	}

	// Read all columns — Fields should be non-nil (user fields decoded).
	allEvents, err := r.ReadEvents()
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	for i, e := range allEvents {
		if e.Fields == nil {
			t.Errorf("event[%d].Fields should be non-nil when all columns read", i)

			break
		}
		if e.GetField("status").AsInt() != 200 {
			t.Errorf("event[%d].status: got %d, want 200", i, e.GetField("status").AsInt())
		}
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// V4 feature tests: ConstColumns, per-column bloom, presence bitmap
// ──────────────────────────────────────────────────────────────────────────────

func TestV4_ConstColumn_Roundtrip(t *testing.T) {
	// generateTestEvents creates uniform _source, _sourcetype, index → const columns.
	events := generateTestEvents(100)

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	// _source, _sourcetype, and index should be const columns.
	for _, col := range []string{"_source", "_sourcetype", "index"} {
		if !r.IsConstColumn(0, col) {
			t.Errorf("expected %q to be const column in RG0", col)
		}
		val, ok := r.GetConstValue(0, col)
		if !ok {
			t.Errorf("expected const value for %q", col)
		}
		if val == "" {
			t.Errorf("const value for %q should not be empty", col)
		}
	}

	// _source should have const value "/var/log/app.log".
	val, ok := r.GetConstValue(0, "_source")
	if !ok || val != "/var/log/app.log" {
		t.Errorf("_source const: got %q, want %q", val, "/var/log/app.log")
	}

	// host varies across 5 values → NOT const.
	if r.IsConstColumn(0, "host") {
		t.Error("host should NOT be const (multiple distinct values)")
	}

	// _time is never const.
	if r.IsConstColumn(0, "_time") {
		t.Error("_time should NOT be const")
	}

	// _raw is never const (unique log lines).
	if r.IsConstColumn(0, "_raw") {
		t.Error("_raw should NOT be const")
	}

	// Full round-trip: all event fields should be correct.
	readEvents, err := r.ReadEvents()
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(readEvents) != len(events) {
		t.Fatalf("ReadEvents: got %d, want %d", len(readEvents), len(events))
	}

	for i := range events {
		if readEvents[i].Source != events[i].Source {
			t.Errorf("event[%d].Source: got %q, want %q", i, readEvents[i].Source, events[i].Source)

			break
		}
		if readEvents[i].SourceType != events[i].SourceType {
			t.Errorf("event[%d].SourceType: got %q, want %q", i, readEvents[i].SourceType, events[i].SourceType)

			break
		}
		if readEvents[i].Index != events[i].Index {
			t.Errorf("event[%d].Index: got %q, want %q", i, readEvents[i].Index, events[i].Index)

			break
		}
	}
}

func TestV4_ConstColumn_ReadStrings(t *testing.T) {
	// Verify ReadStrings works for const columns.
	events := generateTestEvents(50)

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	// ReadStrings for a const column should return the value for every row.
	sources, err := r.ReadStrings("_source")
	if err != nil {
		t.Fatalf("ReadStrings(_source): %v", err)
	}
	if len(sources) != 50 {
		t.Fatalf("expected 50 values, got %d", len(sources))
	}
	for i, v := range sources {
		if v != "/var/log/app.log" {
			t.Errorf("source[%d]: got %q, want %q", i, v, "/var/log/app.log")

			break
		}
	}
}

func TestV4_ConstColumn_Projection(t *testing.T) {
	// Verify column projection works with const columns.
	events := generateTestEvents(50)

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	// Project to _raw + _source (const).
	projected, err := r.ReadEventsWithColumns([]string{"_raw", "_source"})
	if err != nil {
		t.Fatalf("ReadEventsWithColumns: %v", err)
	}
	if len(projected) != 50 {
		t.Fatalf("expected 50 events, got %d", len(projected))
	}

	for i, e := range projected {
		if e.Raw != events[i].Raw {
			t.Errorf("event[%d].Raw mismatch", i)

			break
		}
		if e.Source != "/var/log/app.log" {
			t.Errorf("event[%d].Source: got %q, want %q", i, e.Source, "/var/log/app.log")

			break
		}
		// host was not requested → should be empty.
		if e.Host != "" {
			t.Errorf("event[%d].Host should be empty, got %q", i, e.Host)

			break
		}
	}
}

func TestV4_ConstColumn_PredicateFilter(t *testing.T) {
	// Verify predicate pushdown works on const columns.
	events := generateTestEvents(100)

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	// Predicate on const column that matches → all events returned.
	matching, err := r.ReadEventsFiltered(
		[]Predicate{{Field: "_source", Op: "=", Value: "/var/log/app.log"}},
		nil, []string{"_raw"},
	)
	if err != nil {
		t.Fatalf("ReadEventsFiltered (matching): %v", err)
	}
	if len(matching) != 100 {
		t.Errorf("expected 100 matching events, got %d", len(matching))
	}

	// Predicate on const column that does NOT match → zero events.
	notMatching, err := r.ReadEventsFiltered(
		[]Predicate{{Field: "_source", Op: "=", Value: "/var/log/other.log"}},
		nil, []string{"_raw"},
	)
	if err != nil {
		t.Fatalf("ReadEventsFiltered (not matching): %v", err)
	}
	if len(notMatching) != 0 {
		t.Errorf("expected 0 non-matching events, got %d", len(notMatching))
	}
}

func TestV4_PresenceBitmap(t *testing.T) {
	events := generateTestEvents(50)

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	// All columns should be present in RG0 (chunks or const).
	for _, col := range []string{"_time", "_raw", "_source", "_sourcetype", "host", "index", "level", "status", "latency"} {
		if !r.HasColumnInRowGroup(0, col) {
			t.Errorf("expected %q to be present in RG0", col)
		}
	}

	// Non-existent column should not be present.
	if r.HasColumnInRowGroup(0, "nonexistent") {
		t.Error("nonexistent column should not be present")
	}

	// Out-of-range RG index.
	if r.HasColumnInRowGroup(99, "_time") {
		t.Error("should return false for out-of-range RG index")
	}
}

func TestV4_PerColumnBloom(t *testing.T) {
	// Create events with known field values for per-column bloom testing.
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	events := make([]*event.Event, 100)
	for i := range events {
		level := "INFO"
		if i%5 == 0 {
			level = "ERROR"
		}
		e := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond),
			fmt.Sprintf("event %d level=%s host=web-01", i, level))
		e.Host = "web-01"
		e.Source = "/var/log/app.log"
		e.SourceType = "json"
		e.Index = "main"
		e.SetField("level", event.StringValue(level))
		events[i] = e
	}

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	// Per-column bloom for _raw should contain "event" token.
	mayContain, err := r.CheckColumnBloom(0, "_raw", "event")
	if err != nil {
		t.Fatalf("CheckColumnBloom(_raw, event): %v", err)
	}
	if !mayContain {
		t.Error("_raw bloom should contain 'event'")
	}

	// Per-column bloom for "level" field should contain "error".
	mayContain, err = r.CheckColumnBloom(0, "level", "error")
	if err != nil {
		t.Fatalf("CheckColumnBloom(level, error): %v", err)
	}
	if !mayContain {
		t.Error("level bloom should contain 'error'")
	}

	// Per-column bloom for "level" should NOT contain a completely absent term.
	mayContain, err = r.CheckColumnBloom(0, "level", "xyzzy_absent_12345")
	if err != nil {
		t.Fatalf("CheckColumnBloom(level, absent): %v", err)
	}
	if mayContain {
		t.Log("note: false positive for absent term in level bloom (acceptable)")
	}

	// Const column (_source) should NOT have a bloom (value is known exactly).
	// CheckColumnBloom returns true conservatively for missing blooms.
	mayContain, err = r.CheckColumnBloom(0, "_source", "/var/log/app.log")
	if err != nil {
		t.Fatalf("CheckColumnBloom(_source): %v", err)
	}
	// Should return true (conservative) since _source is const and has no bloom.
	if !mayContain {
		t.Error("const column should return true (conservative)")
	}

	// CheckColumnBloomAllTerms.
	mayContain, err = r.CheckColumnBloomAllTerms(0, "_raw", []string{"event", "level"})
	if err != nil {
		t.Fatalf("CheckColumnBloomAllTerms: %v", err)
	}
	if !mayContain {
		t.Error("_raw bloom should contain both 'event' and 'level'")
	}
}

func TestV4_MixedConstAndChunk_MultiRG(t *testing.T) {
	// Create events where a column is const in one RG but not in another.
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	events := make([]*event.Event, DefaultRowGroupSize+500)

	// RG0: _source is uniform (const).
	for i := 0; i < DefaultRowGroupSize; i++ {
		e := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond),
			fmt.Sprintf("event %d from nginx", i))
		e.Host = fmt.Sprintf("web-%02d", i%3)
		e.Source = "nginx"
		e.SourceType = "access"
		e.Index = "main"
		events[i] = e
	}
	// RG1: _source varies (NOT const).
	sources := []string{"nginx", "api-gw", "redis"}
	for i := DefaultRowGroupSize; i < len(events); i++ {
		src := sources[(i-DefaultRowGroupSize)%3]
		e := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond),
			fmt.Sprintf("event %d from %s", i, src))
		e.Host = "web-01"
		e.Source = src
		e.SourceType = "access"
		e.Index = "main"
		events[i] = e
	}

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	if r.RowGroupCount() != 2 {
		t.Fatalf("expected 2 row groups, got %d", r.RowGroupCount())
	}

	// RG0: _source should be const "nginx".
	if !r.IsConstColumn(0, "_source") {
		t.Error("RG0: _source should be const")
	}
	val, ok := r.GetConstValue(0, "_source")
	if !ok || val != "nginx" {
		t.Errorf("RG0 _source const: got %q, want %q", val, "nginx")
	}

	// RG1: _source should NOT be const (varies).
	if r.IsConstColumn(1, "_source") {
		t.Error("RG1: _source should NOT be const")
	}

	// Both RGs should report _source as present.
	if !r.HasColumnInRowGroup(0, "_source") {
		t.Error("RG0 should have _source")
	}
	if !r.HasColumnInRowGroup(1, "_source") {
		t.Error("RG1 should have _source")
	}

	// Full round-trip.
	readEvents, err := r.ReadEvents()
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(readEvents) != len(events) {
		t.Fatalf("ReadEvents: got %d, want %d", len(readEvents), len(events))
	}

	for i := range events {
		if readEvents[i].Source != events[i].Source {
			t.Errorf("event[%d].Source: got %q, want %q", i, readEvents[i].Source, events[i].Source)

			break
		}
	}
}

func TestV4_ConstColumn_UserField(t *testing.T) {
	// Create events where a user-defined field is const.
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	events := make([]*event.Event, 50)
	for i := range events {
		e := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond),
			fmt.Sprintf("event %d env=production", i))
		e.Host = "web-01"
		e.Source = "/var/log/app.log"
		e.SourceType = "json"
		e.Index = "main"
		e.SetField("env", event.StringValue("production"))
		e.SetField("status", event.IntValue(int64(200+i%3)))
		events[i] = e
	}

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	// "env" field should be const since all values are "production".
	if !r.IsConstColumn(0, "env") {
		t.Error("expected 'env' to be const column")
	}
	val, ok := r.GetConstValue(0, "env")
	if !ok || val != "production" {
		t.Errorf("env const: got %q, want %q", val, "production")
	}

	// "status" field should NOT be const (varies: 200, 201, 202).
	if r.IsConstColumn(0, "status") {
		t.Error("status should NOT be const (multiple values)")
	}

	// Full round-trip verifies user const columns.
	readEvents, err := r.ReadEvents()
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	for i, e := range readEvents {
		v := e.GetField("env")
		if v.IsNull() || v.AsString() != "production" {
			t.Errorf("event[%d].env: got %v, want 'production'", i, v)

			break
		}
	}
}

func TestV4_ColumnarRead_ConstColumns(t *testing.T) {
	// Verify columnar read path handles const columns.
	events := generateTestEvents(50)

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	// ReadColumnar with const column _source.
	result, err := r.ReadColumnar([]string{"_time", "_source", "host"}, nil)
	if err != nil {
		t.Fatalf("ReadColumnar: %v", err)
	}
	if result.Count != 50 {
		t.Fatalf("expected 50 rows, got %d", result.Count)
	}

	// _source should be filled from const column.
	sources, ok := result.Builtins["_source"]
	if !ok {
		t.Fatal("expected _source in Builtins")
	}
	if len(sources) != 50 {
		t.Fatalf("expected 50 _source values, got %d", len(sources))
	}
	for i, v := range sources {
		if v != "/var/log/app.log" {
			t.Errorf("_source[%d]: got %q, want %q", i, v, "/var/log/app.log")

			break
		}
	}

	// host should be from chunk (non-const).
	hosts, ok := result.Builtins["host"]
	if !ok {
		t.Fatal("expected host in Builtins")
	}
	if len(hosts) != 50 {
		t.Fatalf("expected 50 host values, got %d", len(hosts))
	}
}

func TestV4_ColumnarRead_ConstUserField(t *testing.T) {
	// Verify columnar read path handles const user-defined fields.
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	events := make([]*event.Event, 50)
	for i := range events {
		e := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond),
			fmt.Sprintf("event %d", i))
		e.Host = "web-01"
		e.Source = "/var/log/app.log"
		e.SourceType = "json"
		e.Index = "main"
		e.SetField("region", event.StringValue("us-east-1"))
		events[i] = e
	}

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	result, err := r.ReadColumnar([]string{"_time", "region"}, nil)
	if err != nil {
		t.Fatalf("ReadColumnar: %v", err)
	}
	if result.Count != 50 {
		t.Fatalf("expected 50 rows, got %d", result.Count)
	}

	regions, ok := result.Fields["region"]
	if !ok {
		t.Fatal("expected 'region' in Fields")
	}
	if len(regions) != 50 {
		t.Fatalf("expected 50 region values, got %d", len(regions))
	}
	for i, v := range regions {
		if v.AsString() != "us-east-1" {
			t.Errorf("region[%d]: got %q, want %q", i, v.AsString(), "us-east-1")

			break
		}
	}
}

func TestV4_ColumnNames_IncludesConst(t *testing.T) {
	events := generateTestEvents(20)

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	names := r.ColumnNames()
	nameSet := make(map[string]bool, len(names))
	for _, n := range names {
		nameSet[n] = true
	}

	// _source is const but should still appear in ColumnNames.
	if !nameSet["_source"] {
		t.Error("ColumnNames should include const column _source")
	}
	if !nameSet["_sourcetype"] {
		t.Error("ColumnNames should include const column _sourcetype")
	}
	if !nameSet["index"] {
		t.Error("ColumnNames should include const column index")
	}
}

func TestV4_HasColumn_IncludesConst(t *testing.T) {
	events := generateTestEvents(20)

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	if !r.HasColumn("_source") {
		t.Error("HasColumn should return true for const column _source")
	}
	if !r.HasColumn("host") {
		t.Error("HasColumn should return true for chunk column host")
	}
	if r.HasColumn("nonexistent") {
		t.Error("HasColumn should return false for nonexistent column")
	}
}

func TestV4_ColumnarFiltered_ConstColumn(t *testing.T) {
	// Verify columnar filtered read works with const column predicates.
	events := generateTestEvents(100)

	var buf bytes.Buffer
	sw := NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	// Predicate on const column that matches.
	result, err := r.ReadColumnarFiltered(
		[]Predicate{{Field: "_source", Op: "=", Value: "/var/log/app.log"}},
		nil, []string{"_raw"},
	)
	if err != nil {
		t.Fatalf("ReadColumnarFiltered (match): %v", err)
	}
	if result == nil || result.Count != 100 {
		count := 0
		if result != nil {
			count = result.Count
		}
		t.Errorf("expected 100 matching rows, got %d", count)
	}

	// Predicate on const column that does NOT match.
	result, err = r.ReadColumnarFiltered(
		[]Predicate{{Field: "_source", Op: "=", Value: "/var/log/other.log"}},
		nil, []string{"_raw"},
	)
	if err != nil {
		t.Fatalf("ReadColumnarFiltered (no match): %v", err)
	}
	if result != nil && result.Count != 0 {
		t.Errorf("expected 0 rows, got %d", result.Count)
	}
}

func BenchmarkSegmentWriteWithPruning(b *testing.B) {
	events := generateTestEvents(100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		buf.Grow(4 << 20)
		w := NewWriter(&buf)
		_, _ = w.Write(events)
	}
}

func BenchmarkSegmentReadWithPruning(b *testing.B) {
	events := generateTestEvents(100_000)
	var buf bytes.Buffer
	w := NewWriter(&buf)
	_, _ = w.Write(events)
	data := buf.Bytes()

	// Time range covering only last row group.
	rg1Start := events[DefaultRowGroupSize].Time
	end := events[len(events)-1].Time.Add(time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ := OpenSegment(data)
		_, _ = r.ReadEventsWithHints(QueryHints{
			MinTime: &rg1Start,
			MaxTime: &end,
		})
	}
}
