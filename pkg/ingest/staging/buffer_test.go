package staging

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/memgov"
)

func TestBuffer_AddBelowLimits_BuffersUntilFlush(t *testing.T) {
	sink := &recordingSink{}
	buf := NewBuffer(testConfig(), sink.call, memgov.NopAccount(), nil)
	defer closeBuffer(t, buf)

	if err := buf.Add(context.Background(), events("hello")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if sink.calls() != 0 {
		t.Fatalf("sink calls = %d, want 0", sink.calls())
	}
	if err := buf.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if sink.calls() != 1 {
		t.Fatalf("sink calls = %d, want 1", sink.calls())
	}
}

func TestBuffer_AddOverByteLimit_TriggersInlineFlush(t *testing.T) {
	sink := &recordingSink{}
	cfg := testConfig()
	cfg.MaxBytes = 20
	buf := NewBuffer(cfg, sink.call, memgov.NopAccount(), nil)
	defer closeBuffer(t, buf)

	if err := buf.Add(context.Background(), events("123456789")); err != nil {
		t.Fatalf("Add first: %v", err)
	}
	if err := buf.Add(context.Background(), events("abcdefghi")); err != nil {
		t.Fatalf("Add second: %v", err)
	}
	if sink.calls() == 0 {
		t.Fatal("expected inline size flush")
	}
}

func TestBuffer_AddSingleHugeBatch_ReturnsOverflow(t *testing.T) {
	metrics := &recordingMetrics{}
	cfg := testConfig()
	cfg.MaxBytes = 4
	buf := NewBuffer(cfg, (&recordingSink{}).call, memgov.NopAccount(), metrics)
	defer closeBuffer(t, buf)

	if err := buf.Add(context.Background(), events("too-large")); !errors.Is(err, ErrBufferOverflow) {
		t.Fatalf("Add error = %v, want ErrBufferOverflow", err)
	}
	if metrics.overflows != 1 {
		t.Fatalf("overflows = %d, want 1", metrics.overflows)
	}
}

func TestBuffer_FlushUsesTempIOMemoryAccount(t *testing.T) {
	gov := memgov.NewGovernor(memgov.GovernorConfig{})
	acct := memgov.NewClassAccount(gov, memgov.ClassTempIO)
	sink := &recordingSink{}
	buf := NewBuffer(testConfig(), sink.call, acct, nil)
	defer closeBuffer(t, buf)

	if err := buf.Add(context.Background(), events("tracked")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if got := gov.ClassUsage(memgov.ClassTempIO).Allocated; got == 0 {
		t.Fatal("expected temp-io allocation after Add")
	}
	if err := buf.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := gov.ClassUsage(memgov.ClassTempIO).Allocated; got != 0 {
		t.Fatalf("temp-io allocation = %d, want 0 after flush", got)
	}
}

func TestBuffer_SinkTransientError_RetriesAndRecordsFlush(t *testing.T) {
	metrics := &recordingMetrics{}
	sink := &recordingSink{failures: 1}
	cfg := testConfig()
	cfg.FlushRetries = 2
	cfg.FlushBackoffMax = 0
	buf := NewBuffer(cfg, sink.call, memgov.NopAccount(), metrics)
	defer closeBuffer(t, buf)

	if err := buf.Add(context.Background(), events("retry")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := buf.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if sink.calls() != 2 {
		t.Fatalf("sink calls = %d, want 2", sink.calls())
	}
	if metrics.flushes != 1 {
		t.Fatalf("flushes = %d, want 1", metrics.flushes)
	}
}

func TestBuffer_SinkPermanentError_DropsAfterRetries(t *testing.T) {
	metrics := &recordingMetrics{}
	sink := &recordingSink{failures: 10}
	cfg := testConfig()
	cfg.FlushRetries = 2
	cfg.FlushBackoffMax = 0
	buf := NewBuffer(cfg, sink.call, memgov.NopAccount(), metrics)
	defer closeBuffer(t, buf)

	if err := buf.Add(context.Background(), events("drop")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := buf.Flush(context.Background()); err == nil {
		t.Fatal("expected sink error")
	}
	if sink.calls() != 3 {
		t.Fatalf("sink calls = %d, want 3", sink.calls())
	}
	if metrics.dropped != 1 {
		t.Fatalf("dropped = %d, want 1", metrics.dropped)
	}
}

func TestBuffer_CloseDrainsAndStops(t *testing.T) {
	sink := &recordingSink{}
	buf := NewBuffer(testConfig(), sink.call, memgov.NopAccount(), nil)

	if err := buf.Add(context.Background(), events("close")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := buf.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if sink.calls() != 1 {
		t.Fatalf("sink calls = %d, want 1", sink.calls())
	}
}

func testConfig() Config {
	return Config{
		Enabled:           true,
		MaxBytes:          1024,
		MaxAge:            time.Hour,
		MaxInflightEvents: 100,
		FlushRetries:      0,
		FlushBackoffMax:   0,
	}
}

func events(raws ...string) []*event.Event {
	out := make([]*event.Event, 0, len(raws))
	for _, raw := range raws {
		out = append(out, event.NewEvent(time.Now(), raw))
	}
	return out
}

func closeBuffer(t *testing.T, b *Buffer) {
	t.Helper()
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

type recordingSink struct {
	mu       sync.Mutex
	n        int
	failures int
}

func (s *recordingSink) call(context.Context, []*event.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n++
	if s.failures > 0 {
		s.failures--
		return errors.New("sink failed")
	}
	return nil
}

func (s *recordingSink) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n
}

type recordingMetrics struct {
	flushes   int
	overflows int
	dropped   int
}

func (m *recordingMetrics) SetState(int64, int, float64) {}

func (m *recordingMetrics) RecordFlush(string, int64) {
	m.flushes++
}

func (m *recordingMetrics) RecordOverflow() {
	m.overflows++
}

func (m *recordingMetrics) RecordDropped(_ string, events int) {
	m.dropped += events
}
