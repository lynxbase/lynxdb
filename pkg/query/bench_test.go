package query

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/engine/physical"
	"github.com/lynxbase/lynxdb/pkg/event"
)

// benchEvents builds a deterministic web-log dataset.
func benchEvents(n int) map[string][]*event.Event {
	hosts := []string{"web-1", "web-2", "web-3", "web-4", "web-5", "web-6", "web-7", "web-8"}
	levels := []string{"INFO", "INFO", "INFO", "WARN", "ERROR"}
	events := make([]*event.Event, n)
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		status := 200
		switch {
		case i%17 == 0:
			status = 500
		case i%7 == 0:
			status = 404
		}
		level := levels[i%len(levels)]
		raw := fmt.Sprintf("%s GET /api/item/%d status=%d bytes=%d", level, i%1000, status, (i*37)%5000)
		if level == "ERROR" {
			raw += " error connection reset"
		}
		e := event.NewEvent(start.Add(time.Duration(i)*time.Second), raw)
		e.Index = "main"
		e.SetField("host", event.StringValue(hosts[i%len(hosts)]))
		e.SetField("status", event.IntValue(int64(status)))
		e.SetField("bytes", event.IntValue(int64((i*37)%5000)))
		e.SetField("level", event.StringValue(level))
		events[i] = e
	}
	return map[string][]*event.Event{"main": events}
}

var benchQueries = []struct {
	name string
	ppl  string
}{
	{"FilterStatsBy", "source=main | where status >= 500 | stats count() by host"},
	{"SearchTerm", "search source=main error | stats count()"},
	{"EvalSortHead", "source=main | eval kb = bytes / 1024 | sort - kb | head 10"},
	{"TimeBuckets", "source=main | stats count() by span(_time, 1m)"},
	{"MultiAgg", "source=main | stats avg(bytes), max(bytes), dc(host), percentile(bytes, 95) by level"},
}

func BenchmarkPPL(b *testing.B) {
	store := physical.MapStore(benchEvents(50000))
	for _, q := range benchQueries {
		c, err := Compile(q.ppl)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(q.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := Run(context.Background(), c, store, Options{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCompile(b *testing.B) {
	for _, q := range benchQueries {
		b.Run(q.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := Compile(q.ppl); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
