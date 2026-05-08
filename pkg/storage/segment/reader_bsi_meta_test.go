package segment

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/storage/segment/index"
)

func TestIntegration_Reader_LoadRangeMeta_IntAndFloat_ReturnsEncodedBounds(t *testing.T) {
	events := makeRangeMetaEventsForReaderTest(t)
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.SetRowGroupSize(len(events))
	w.SetIndexConfig(IndexConfig{
		ProfileOverrides: map[string]IndexProfile{
			"bytes":   IndexProfileRangeBSI,
			"latency": IndexProfileRangeBSI,
		},
	})
	if _, err := w.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	bytesMeta, ok, err := r.LoadRangeMeta(0, "bytes")
	if err != nil {
		t.Fatalf("LoadRangeMeta(bytes): %v", err)
	}
	if !ok {
		t.Fatal("LoadRangeMeta(bytes) ok = false, want true")
	}
	wantBytes := rangeMeta{ValueKind: index.RangeBSIValueInt, MinValue: 100, MaxValue: 200}
	if bytesMeta != wantBytes {
		t.Fatalf("bytes meta = %+v, want %+v", bytesMeta, wantBytes)
	}

	latencyMeta, ok, err := r.LoadRangeMeta(0, "latency")
	if err != nil {
		t.Fatalf("LoadRangeMeta(latency): %v", err)
	}
	if !ok {
		t.Fatal("LoadRangeMeta(latency) ok = false, want true")
	}
	wantLatency := rangeMeta{
		ValueKind: index.RangeBSIValueFloat64Bits,
		MinValue:  index.FloatToOrderedInt64(0.5),
		MaxValue:  index.FloatToOrderedInt64(999.9),
	}
	if latencyMeta != wantLatency {
		t.Fatalf("latency meta = %+v, want %+v", latencyMeta, wantLatency)
	}

	idx, err := r.LoadRangeBSI(0, "latency")
	if err != nil {
		t.Fatalf("LoadRangeBSI(latency): %v", err)
	}
	if idx == nil {
		t.Fatal("LoadRangeBSI(latency) = nil, want BSI after metadata load")
	}

	missing, ok, err := r.LoadRangeMeta(0, "missing")
	if err != nil {
		t.Fatalf("LoadRangeMeta(missing): %v", err)
	}
	if ok {
		t.Fatalf("LoadRangeMeta(missing) = (%+v, true), want false", missing)
	}
	if missing != (rangeMeta{}) {
		t.Fatalf("missing meta = %+v, want zero value", missing)
	}
}

func makeRangeMetaEventsForReaderTest(t *testing.T) []*event.Event {
	t.Helper()
	base := time.Date(2026, 5, 8, 13, 0, 0, 0, time.UTC)
	bytesValues := []int64{100, 125, 150, 175, 200}
	latencyValues := []float64{0.5, 10.25, 42.75, 500.125, 999.9}
	events := make([]*event.Event, len(bytesValues))
	for i := range events {
		e := event.NewEvent(base.Add(time.Duration(i)*time.Second), fmt.Sprintf("bytes=%d latency=%.3f", bytesValues[i], latencyValues[i]))
		e.SetField("bytes", event.IntValue(bytesValues[i]))
		e.SetField("latency", event.FloatValue(latencyValues[i]))
		e.Host = "meta-host"
		e.Source = "/var/log/meta.log"
		e.SourceType = "json"
		e.Index = "main"
		events[i] = e
	}
	return events
}
