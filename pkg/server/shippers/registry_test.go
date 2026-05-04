package shippers

import (
	"testing"
	"time"
)

func TestRegistry_RingBuffer_BoundedSize(t *testing.T) {
	now := time.Unix(100, 0)
	reg := NewRegistry(2)
	reg.now = func() time.Time { return now }

	reg.Observe(RequestInfo{UserAgent: "Filebeat/8.15.0", RemoteIP: "10.0.0.1", Endpoint: "/_bulk"}, 1)
	now = now.Add(time.Second)
	reg.Observe(RequestInfo{UserAgent: "Vector/0.40.0", RemoteIP: "10.0.0.2", Endpoint: "/_bulk"}, 1)
	now = now.Add(time.Second)
	reg.Observe(RequestInfo{UserAgent: "Fluent-Bit v3.1.4", RemoteIP: "10.0.0.3", Endpoint: "/_bulk"}, 1)

	got := reg.List()
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	for _, obs := range got {
		if obs.Tool == "filebeat" {
			t.Fatalf("oldest entry was not evicted: %#v", got)
		}
	}
}

func TestRegistry_LastSeenAndEventsPerMinute(t *testing.T) {
	now := time.Unix(100, 0)
	reg := NewRegistry(8)
	reg.now = func() time.Time { return now }
	info := RequestInfo{UserAgent: "Vector/0.40.0", RemoteIP: "10.0.0.2", Endpoint: "/_bulk"}

	reg.Observe(info, 10)
	now = now.Add(30 * time.Second)
	reg.Observe(info, 5)

	got := reg.List()
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	if got[0].EventsPerMin != 15 {
		t.Fatalf("events_per_min = %d, want 15", got[0].EventsPerMin)
	}
	if !got[0].LastSeen.Equal(now) {
		t.Fatalf("last_seen = %s, want %s", got[0].LastSeen, now)
	}
}
