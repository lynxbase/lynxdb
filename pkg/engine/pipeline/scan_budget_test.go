package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/memgov"
)

// makeSizedEvents creates n events with a _raw field of the given size.
func makeSizedEvents(n, rawSize int) []*event.Event {
	raw := strings.Repeat("x", rawSize)
	events := make([]*event.Event, n)
	for i := 0; i < n; i++ {
		ev := event.NewEvent(time.Now(), raw)
		ev.SetField("idx", event.IntValue(int64(i)))
		events[i] = ev
	}

	return events
}

func TestScanReturnsErrorOnGenuineBudgetPressure(t *testing.T) {
	// 1000 events, each ~200 bytes raw + 128 base + field overhead ≈ 350 bytes.
	// Budget of 50KB — after Shrink only one batch is tracked at a time (~32*350=11KB),
	// but the budget is cumulative across operators. With a tiny budget, Grow should
	// fail and return an explicit error, not silent EOF.
	events := makeSizedEvents(1000, 200)

	// Very small budget: 2KB — too small even for a single batch of 32 events.
	acct := memgov.NewTestBudget("test", 2*1024).NewAccount("scan")
	iter := NewScanIteratorWithBudget(events, 32, acct)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	var sawError bool
	for {
		_, err := iter.Next(ctx)
		if err != nil {
			sawError = true

			break
		}
	}
	iter.Close()

	if !sawError {
		t.Fatal("expected an error when budget is exceeded, not silent EOF")
	}
}

func TestScanCompletesWithBudgetTracking(t *testing.T) {
	// 100 events with small raw. Budget is large enough — all events returned.
	// Verify that acct.Used() reflects only one batch (not cumulative throughput).
	events := makeSizedEvents(100, 50)

	// Large budget — no pressure.
	acct := memgov.NewTestBudget("test", 0).NewAccount("scan")
	iter := NewScanIteratorWithBudget(events, 32, acct)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 100 {
		t.Fatalf("expected 100 rows, got %d", len(result))
	}

	// After Close, the account should be zeroed (last batch shrunk).
	iter.Close()
	if acct.Used() != 0 {
		t.Fatalf("expected acct.Used()=0 after Close, got %d", acct.Used())
	}
}

func TestEstimateEventSize(t *testing.T) {
	tests := []struct {
		name     string
		rawSize  int
		fields   int
		minBytes int64
		maxBytes int64
	}{
		{"tiny event", 10, 0, 128, 300},
		{"medium event", 500, 5, 500, 1200},
		{"large raw", 10000, 2, 10000, 11000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev := event.NewEvent(time.Now(), strings.Repeat("x", tt.rawSize))
			ev.Host = "web-01"
			ev.Source = "/var/log/app.log"
			for i := 0; i < tt.fields; i++ {
				ev.SetField("field_"+strings.Repeat("a", 5), event.StringValue(strings.Repeat("v", 20)))
			}

			size := event.EstimateEventSize(ev)
			if size < tt.minBytes {
				t.Errorf("estimate %d < min %d", size, tt.minBytes)
			}
			if size > tt.maxBytes {
				t.Errorf("estimate %d > max %d", size, tt.maxBytes)
			}
		})
	}
}

func TestScanTightBudgetWithSortSpill(t *testing.T) {
	// Integration test: scan and sort share a tight BudgetAdapter.
	// The budget is large enough for the sort to spill and reclaim capacity,
	// but small enough that scan batches cause pressure. Verifies the
	// sort-spill-on-child-budget-exceeded path handles real budget sharing.
	const (
		numEvents = 200
		rawSize   = 512 // ~512B per event
		batchSize = 32
	)

	events := makeSizedEvents(numEvents, rawSize)

	// Budget: 30KB. Each event is ~512B + overhead ≈ 700B.
	// A batch of 32 events is ~22KB. Sort accumulates rows and eventually
	// hits the limit, triggering spill. After spill, scan can continue.
	monitor := memgov.NewTestBudget("test", 30*1024)
	scanAcct := monitor.NewAccount("scan")
	sortAcct := monitor.NewAccount("sort")

	scan := NewScanIteratorWithBudget(events, batchSize, scanAcct)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sortIter := NewSortIteratorWithSpill(scan, []SortField{{Name: "idx", Desc: false}}, batchSize, sortAcct, mgr)

	ctx := context.Background()
	if err := sortIter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, sortIter)
	if err != nil {
		t.Fatalf("expected query to complete with budget sharing + spill, got error: %v", err)
	}

	if len(result) != numEvents {
		t.Fatalf("expected %d rows, got %d", numEvents, len(result))
	}

	// Verify sorted ascending by idx.
	for i := 0; i < len(result); i++ {
		got := result[i]["idx"].AsInt()
		if got != int64(i) {
			t.Fatalf("row %d: expected idx=%d, got %d", i, i, got)
		}
	}
}

func TestScanMinBatchBudgetError(t *testing.T) {
	// When the budget is impossibly small (cannot fit even a single event),
	// the scan should return a clear actionable error, not hang or panic.
	events := makeSizedEvents(10, 1024)

	// Budget: 100 bytes — far too small for any event (~1.2KB each).
	acct := memgov.NewTestBudget("test", 100).NewAccount("scan")
	iter := NewScanIteratorWithBudget(events, 32, acct)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	_, err := iter.Next(ctx)
	if err == nil {
		t.Fatal("expected error when budget cannot fit any events")
	}

	// Verify the error is budget-related.
	if !memgov.IsBudgetExceeded(err) {
		t.Fatalf("expected BudgetExceededError, got: %v", err)
	}

	// Verify the error message is actionable.
	errMsg := err.Error()
	if !strings.Contains(errMsg, "memory") {
		t.Errorf("error should mention memory, got: %s", errMsg)
	}
}
