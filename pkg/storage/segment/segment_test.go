package segment

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
)

func TestSegmentWriteRead_Basic(t *testing.T) {
	events := generateTestEvents(100)

	var buf bytes.Buffer
	w := NewWriter(&buf)
	written, err := w.Write(events)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if written != int64(buf.Len()) {
		t.Fatalf("written=%d, buf.Len()=%d", written, buf.Len())
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	if r.EventCount() != int64(len(events)) {
		t.Fatalf("EventCount: got %d, want %d", r.EventCount(), len(events))
	}

	// Verify timestamps.
	timestamps, err := r.ReadTimestamps()
	if err != nil {
		t.Fatalf("ReadTimestamps: %v", err)
	}
	for i, ts := range timestamps {
		if !ts.Equal(events[i].Time) {
			t.Errorf("timestamp[%d]: got %v, want %v", i, ts, events[i].Time)
		}
	}

	// Verify raw values.
	rawValues, err := r.ReadStrings("_raw")
	if err != nil {
		t.Fatalf("ReadStrings(_raw): %v", err)
	}
	for i, raw := range rawValues {
		if raw != events[i].Raw {
			t.Errorf("raw[%d]: got %q, want %q", i, raw, events[i].Raw)
		}
	}

	// Verify host values.
	hosts, err := r.ReadStrings("host")
	if err != nil {
		t.Fatalf("ReadStrings(host): %v", err)
	}
	for i, h := range hosts {
		if h != events[i].Host {
			t.Errorf("host[%d]: got %q, want %q", i, h, events[i].Host)
		}
	}
}

func TestSegmentWriteRead_10KEvents(t *testing.T) {
	events := generateTestEvents(10000)

	var buf bytes.Buffer
	w := NewWriter(&buf)
	written, err := w.Write(events)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	t.Logf("10K events: %d bytes written (%.2f bytes/event)", written, float64(written)/float64(len(events)))

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	if r.EventCount() != 10000 {
		t.Fatalf("EventCount: got %d, want 10000", r.EventCount())
	}

	// Full event reconstruction.
	readEvents, err := r.ReadEvents()
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}

	if len(readEvents) != len(events) {
		t.Fatalf("ReadEvents: got %d events, want %d", len(readEvents), len(events))
	}

	for i := range events {
		orig := events[i]
		got := readEvents[i]

		if !got.Time.Equal(orig.Time) {
			t.Errorf("event[%d].Time: got %v, want %v", i, got.Time, orig.Time)
		}
		if got.Raw != orig.Raw {
			t.Errorf("event[%d].Raw: got %q, want %q", i, got.Raw, orig.Raw)
		}
		if got.Host != orig.Host {
			t.Errorf("event[%d].Host: got %q, want %q", i, got.Host, orig.Host)
		}
		if got.Source != orig.Source {
			t.Errorf("event[%d].Source: got %q, want %q", i, got.Source, orig.Source)
		}
		if got.SourceType != orig.SourceType {
			t.Errorf("event[%d].SourceType: got %q, want %q", i, got.SourceType, orig.SourceType)
		}
		if got.Index != orig.Index {
			t.Errorf("event[%d].Index: got %q, want %q", i, got.Index, orig.Index)
		}

		// Verify user fields.
		origLevel := orig.GetField("level")
		gotLevel := got.GetField("level")
		if !origLevel.IsNull() && gotLevel.AsString() != origLevel.AsString() {
			t.Errorf("event[%d].level: got %q, want %q", i, gotLevel, origLevel)
		}

		origStatus := orig.GetField("status")
		gotStatus := got.GetField("status")
		if !origStatus.IsNull() && gotStatus.AsInt() != origStatus.AsInt() {
			t.Errorf("event[%d].status: got %d, want %d", i, gotStatus.AsInt(), origStatus.AsInt())
		}

		origLatency := orig.GetField("latency")
		gotLatency := got.GetField("latency")
		if !origLatency.IsNull() {
			if math.Abs(gotLatency.AsFloat()-origLatency.AsFloat()) > 1e-10 {
				t.Errorf("event[%d].latency: got %v, want %v", i, gotLatency.AsFloat(), origLatency.AsFloat())
			}
		}
	}
}

func TestSegmentWriteRead_ColumnNames(t *testing.T) {
	events := generateTestEvents(10)

	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	names := r.ColumnNames()
	expected := map[string]bool{
		"_time": true, "_raw": true, "_source": true,
		"_sourcetype": true, "host": true, "index": true,
		"level": true, "status": true, "latency": true,
	}
	for _, name := range names {
		if !expected[name] {
			t.Errorf("unexpected column %q", name)
		}
		delete(expected, name)
	}
	for name := range expected {
		t.Errorf("missing column %q", name)
	}
}

func TestSegmentWriteRead_HasColumn(t *testing.T) {
	events := generateTestEvents(10)

	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	if !r.HasColumn("_time") {
		t.Error("expected _time column")
	}
	if !r.HasColumn("host") {
		t.Error("expected host column")
	}
	if r.HasColumn("nonexistent") {
		t.Error("unexpected nonexistent column")
	}
}

func TestSegmentWriteRead_Stats(t *testing.T) {
	events := generateTestEvents(100)

	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	stats := r.Stats()
	if len(stats) == 0 {
		t.Fatal("expected stats")
	}

	for _, s := range stats {
		if s.Count != 100 {
			t.Errorf("stat %q: count=%d, want 100", s.Name, s.Count)
		}
	}
}

func TestSegment_EmptyEvents(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	_, err := w.Write(nil)
	if !errors.Is(err, ErrNoEvents) {
		t.Errorf("expected ErrNoEvents, got %v", err)
	}
}

func TestSegment_InvalidMagic(t *testing.T) {
	_, err := OpenSegment([]byte("JUNK" + "xxxxxxxxxxxx"))
	if err == nil {
		t.Fatal("expected error for invalid magic")
	}
}

func TestSegment_ColumnNotFound(t *testing.T) {
	events := generateTestEvents(10)
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	_, err = r.ReadStrings("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent column")
	}
}

func generateTestEvents(n int) []*event.Event {
	rng := rand.New(rand.NewSource(42))
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	hosts := []string{"web-01", "web-02", "web-03", "db-01", "cache-01"}
	levels := []string{"INFO", "WARN", "ERROR", "DEBUG"}

	events := make([]*event.Event, n)
	ts := base
	for i := 0; i < n; i++ {
		ts = ts.Add(time.Duration(90+rng.Intn(20)) * time.Millisecond)
		host := hosts[rng.Intn(len(hosts))]
		level := levels[rng.Intn(len(levels))]
		status := int64(200 + rng.Intn(4)*100)
		latency := rng.Float64() * 500

		raw := fmt.Sprintf("%s host=%s level=%s status=%d latency=%.2f msg=\"request processed\"",
			ts.Format(time.RFC3339Nano), host, level, status, latency)

		e := event.NewEvent(ts, raw)
		e.Host = host
		e.Source = "/var/log/app.log"
		e.SourceType = "json"
		e.Index = "main"
		e.SetField("level", event.StringValue(level))
		e.SetField("status", event.IntValue(status))
		e.SetField("latency", event.FloatValue(latency))
		events[i] = e
	}

	return events
}

func TestPrimaryIndex_WriteReadRoundtrip(t *testing.T) {
	// Generate events with a "level" field that we'll use as sort key.
	events := generateTestEvents(100)

	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.SetSortKey([]string{"level"})
	if _, err := w.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	// The primary index should be present.
	idx, err := r.PrimaryIndex()
	if err != nil {
		t.Fatalf("PrimaryIndex: %v", err)
	}
	if idx == nil {
		t.Fatal("expected primary index, got nil")
	}

	if idx.Interval != PrimaryIndexInterval {
		t.Errorf("Interval: got %d, want %d", idx.Interval, PrimaryIndexInterval)
	}
	if len(idx.SortFields) != 1 || idx.SortFields[0] != "level" {
		t.Errorf("SortFields: got %v, want [level]", idx.SortFields)
	}

	// 100 events / 8192 interval = 1 entry (at row 0).
	if len(idx.Entries) != 1 {
		t.Fatalf("Entries: got %d, want 1", len(idx.Entries))
	}
	if idx.Entries[0].RowOffset != 0 {
		t.Errorf("entry[0].RowOffset: got %d, want 0", idx.Entries[0].RowOffset)
	}
	if len(idx.Entries[0].SortKeyValues) != 1 {
		t.Errorf("entry[0].SortKeyValues: got %d values, want 1", len(idx.Entries[0].SortKeyValues))
	}
}

func TestPrimaryIndex_LargeDataset(t *testing.T) {
	// Use 20000 events to get multiple primary index entries.
	events := generateTestEvents(20000)

	// Sort by host to create a meaningful sort key.
	sortKey := []string{"host"}
	sortedEvents := make([]*event.Event, len(events))
	copy(sortedEvents, events)
	for i := 1; i < len(sortedEvents); i++ {
		for j := i; j > 0; j-- {
			vi := sortedEvents[j].GetField("host").String()
			vj := sortedEvents[j-1].GetField("host").String()
			if vi < vj {
				sortedEvents[j], sortedEvents[j-1] = sortedEvents[j-1], sortedEvents[j]
			}
		}
	}
	// Just use sort.Slice for the actual test.
	events = generateTestEvents(20000)
	hostEvents := make([]*event.Event, len(events))
	copy(hostEvents, events)
	// Sort by host field.
	hostMap := make(map[int]string, len(hostEvents))
	for i, e := range hostEvents {
		hostMap[i] = e.Host
	}
	// Stable sort by host
	indices := make([]int, len(hostEvents))
	for i := range indices {
		indices[i] = i
	}
	// We can just pre-sort the events array
	sorted := make([]*event.Event, len(hostEvents))
	copy(sorted, hostEvents)
	// Use insertion-order-preserving sort
	for i := 1; i < len(sorted); i++ {
		key := sorted[i]
		keyHost := key.Host
		j := i - 1
		for j >= 0 && sorted[j].Host > keyHost {
			sorted[j+1] = sorted[j]
			j--
		}
		sorted[j+1] = key
	}

	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.SetSortKey(sortKey)
	if _, err := w.Write(sorted); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	idx, err := r.PrimaryIndex()
	if err != nil {
		t.Fatalf("PrimaryIndex: %v", err)
	}
	if idx == nil {
		t.Fatal("expected primary index")
	}

	// 20000 / 8192 = 2 full intervals + remainder = 3 entries (rows 0, 8192, 16384).
	expectedEntries := (20000 + PrimaryIndexInterval - 1) / PrimaryIndexInterval
	if len(idx.Entries) != expectedEntries {
		t.Errorf("Entries: got %d, want %d", len(idx.Entries), expectedEntries)
	}

	// Verify row offsets are at expected intervals.
	for i, entry := range idx.Entries {
		expectedOffset := uint32(i * PrimaryIndexInterval)
		if entry.RowOffset != expectedOffset {
			t.Errorf("entry[%d].RowOffset: got %d, want %d", i, entry.RowOffset, expectedOffset)
		}
	}

	// Verify SeekBySortKey returns a valid range.
	startRow, endRow, err := r.SeekBySortKey([]string{"web-01"})
	if err != nil {
		t.Fatalf("SeekBySortKey: %v", err)
	}
	if startRow < 0 || endRow > 20000 || startRow > endRow {
		t.Errorf("SeekBySortKey: invalid range [%d, %d)", startRow, endRow)
	}
}

func TestPrimaryIndex_NoPrimaryIndex(t *testing.T) {
	events := generateTestEvents(50)

	var buf bytes.Buffer
	w := NewWriter(&buf)
	// No SetSortKey — should produce no primary index.
	if _, err := w.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	idx, err := r.PrimaryIndex()
	if err != nil {
		t.Fatalf("PrimaryIndex: %v", err)
	}
	if idx != nil {
		t.Errorf("expected nil primary index, got %+v", idx)
	}

	// SeekBySortKey should return full range.
	start, end, err := r.SeekBySortKey([]string{"anything"})
	if err != nil {
		t.Fatalf("SeekBySortKey: %v", err)
	}
	if start != 0 || end != 50 {
		t.Errorf("SeekBySortKey without index: got [%d, %d), want [0, 50)", start, end)
	}
}

func TestPrimaryIndex_MultiFieldSortKey(t *testing.T) {
	events := generateTestEvents(100)

	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.SetSortKey([]string{"host", "level"})
	if _, err := w.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	idx, err := r.PrimaryIndex()
	if err != nil {
		t.Fatalf("PrimaryIndex: %v", err)
	}
	if idx == nil {
		t.Fatal("expected primary index")
	}
	if len(idx.SortFields) != 2 {
		t.Errorf("SortFields: got %d, want 2", len(idx.SortFields))
	}
	if idx.SortFields[0] != "host" || idx.SortFields[1] != "level" {
		t.Errorf("SortFields: got %v, want [host level]", idx.SortFields)
	}
	// Each entry should have 2 sort key values.
	for i, entry := range idx.Entries {
		if len(entry.SortKeyValues) != 2 {
			t.Errorf("entry[%d].SortKeyValues: got %d, want 2", i, len(entry.SortKeyValues))
		}
	}
}

func TestPrimaryIndex_EncodeDecode(t *testing.T) {
	idx := &PrimaryIndex{
		Interval:   8192,
		SortFields: []string{"host", "status"},
		Entries: []PrimaryIndexEntry{
			{RowOffset: 0, SortKeyValues: []string{"web-01", "200"}},
			{RowOffset: 8192, SortKeyValues: []string{"web-02", "500"}},
			{RowOffset: 16384, SortKeyValues: []string{"web-03", "301"}},
		},
	}

	data := EncodePrimaryIndex(idx)
	decoded, err := DecodePrimaryIndex(data)
	if err != nil {
		t.Fatalf("DecodePrimaryIndex: %v", err)
	}

	if decoded.Interval != idx.Interval {
		t.Errorf("Interval: got %d, want %d", decoded.Interval, idx.Interval)
	}
	if len(decoded.SortFields) != len(idx.SortFields) {
		t.Fatalf("SortFields len: got %d, want %d", len(decoded.SortFields), len(idx.SortFields))
	}
	for i := range idx.SortFields {
		if decoded.SortFields[i] != idx.SortFields[i] {
			t.Errorf("SortFields[%d]: got %q, want %q", i, decoded.SortFields[i], idx.SortFields[i])
		}
	}
	if len(decoded.Entries) != len(idx.Entries) {
		t.Fatalf("Entries len: got %d, want %d", len(decoded.Entries), len(idx.Entries))
	}
	for i := range idx.Entries {
		if decoded.Entries[i].RowOffset != idx.Entries[i].RowOffset {
			t.Errorf("entry[%d].RowOffset: got %d, want %d", i, decoded.Entries[i].RowOffset, idx.Entries[i].RowOffset)
		}
		for j := range idx.Entries[i].SortKeyValues {
			if decoded.Entries[i].SortKeyValues[j] != idx.Entries[i].SortKeyValues[j] {
				t.Errorf("entry[%d].SortKeyValues[%d]: got %q, want %q", i, j, decoded.Entries[i].SortKeyValues[j], idx.Entries[i].SortKeyValues[j])
			}
		}
	}
}

func BenchmarkSegmentWrite(b *testing.B) {
	events := generateTestEvents(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		buf.Grow(1 << 20) // 1MB
		w := NewWriter(&buf)
		_, _ = w.Write(events)
	}
}

func BenchmarkSegmentRead(b *testing.B) {
	events := generateTestEvents(10000)
	var buf bytes.Buffer
	w := NewWriter(&buf)
	_, _ = w.Write(events)
	data := buf.Bytes()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ := OpenSegment(data)
		_, _ = r.ReadEvents()
	}
}

func BenchmarkSegmentScan(b *testing.B) {
	events := generateTestEvents(10000)
	var buf bytes.Buffer
	w := NewWriter(&buf)
	_, _ = w.Write(events)
	data := buf.Bytes()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ := OpenSegment(data)
		_, _ = r.ReadTimestamps()
		_, _ = r.ReadStrings("host")
	}
}
