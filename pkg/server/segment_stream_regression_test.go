package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/config"
	"github.com/lynxbase/lynxdb/pkg/event"
)

// TestStreamingPipelineReadsFlushedSegments checks that the streaming scan
// reads data already flushed to .lsg segments, not only buffered events.
//
// The test ingests events, flushes the batcher so the buffer is empty and the
// data lives only in segments, then runs a query through SubmitQuery and
// asserts that the segment-resident events are returned and that the scan
// statistics report segment reads.
func TestStreamingPipelineReadsFlushedSegments(t *testing.T) {
	queryCfg := config.DefaultConfig().Query
	queryCfg.SpillDir = t.TempDir()
	fsync := false

	e := NewEngine(Config{
		DataDir: t.TempDir(),
		Storage: config.DefaultConfig().Storage,
		Logger:  discardLogger(),
		Query:   queryCfg,
		Ingest:  config.IngestConfig{FSync: &fsync},
	})

	ctx, cancel := context.WithCancel(context.Background())
	if err := e.Start(ctx); err != nil {
		cancel()
		t.Fatalf("engine start: %v", err)
	}
	defer cancel()
	defer func() {
		if err := e.Shutdown(5 * time.Second); err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	}()

	const eventCount = 16
	base := time.Now().UTC()
	events := make([]*event.Event, eventCount)
	for i := range events {
		ev := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond), fmt.Sprintf("segment-event-%d", i))
		ev.Index = "main"
		ev.Fields = map[string]event.Value{
			"level": event.StringValue("error"),
		}
		events[i] = ev
	}
	if err := e.Ingest(events); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	// Flush the batcher: data moves from the in-memory buffer to segments.
	if err := e.FlushBatcher(); err != nil {
		t.Fatalf("flush batcher: %v", err)
	}
	if n := e.BufferedEventCount(); n != 0 {
		t.Fatalf("buffered events after flush = %d, want 0", n)
	}
	if n := e.SegmentCount(); n == 0 {
		t.Fatalf("segment count after flush = 0, want > 0")
	}

	snap := submitAndWait(t, e, "source=main | where level = \"error\"")
	if snap.Status != JobStatusDone {
		t.Fatalf("query status = %s, error=%s", snap.Status, snap.Error)
	}
	if got := len(snap.Results); got != eventCount {
		t.Fatalf("query over flushed segments returned %d rows, want %d (segment data lost)", got, eventCount)
	}
	if snap.Stats.SegmentsScanned == 0 {
		t.Fatalf("SegmentsScanned = 0, want > 0 (segments were not read)")
	}

	// Aggregation over segment-resident data must see every event too.
	aggSnap := submitAndWait(t, e, "source=main | stats count() as c")
	if aggSnap.Status != JobStatusDone {
		t.Fatalf("agg query status = %s, error=%s", aggSnap.Status, aggSnap.Error)
	}
	if len(aggSnap.Results) != 1 {
		t.Fatalf("agg query returned %d rows, want 1", len(aggSnap.Results))
	}
	if c, ok := aggSnap.Results[0].Fields["c"]; !ok {
		t.Fatalf("agg row missing count column: %v", aggSnap.Results[0].Fields)
	} else if fmt.Sprintf("%v", c) != fmt.Sprintf("%d", eventCount) {
		t.Fatalf("agg count = %v, want %d", c, eventCount)
	}
}
