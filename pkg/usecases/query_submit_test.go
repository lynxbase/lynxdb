package usecases

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/config"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/planner"
	"github.com/lynxbase/lynxdb/pkg/server"
)

// newSubmitTestEngine builds a started in-memory engine seeded with a few
// events so Submit() has something to execute.
func newSubmitTestEngine(t *testing.T) *server.Engine {
	t.Helper()

	queryCfg := config.DefaultConfig().Query
	queryCfg.SpillDir = t.TempDir()

	e := server.NewEngine(server.Config{
		DataDir: "",
		Storage: config.DefaultConfig().Storage,
		Logger:  slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		Query:   queryCfg,
	})
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("engine start: %v", err)
	}
	t.Cleanup(func() { _ = e.Shutdown(5 * time.Second) })

	now := time.Now()
	events := make([]*event.Event, 8)
	for i := range events {
		events[i] = &event.Event{
			Time:   now.Add(time.Duration(i) * time.Millisecond),
			Raw:    "submit test event",
			Source: "submit-test",
			Index:  "submit-test",
			Fields: map[string]event.Value{
				"source": event.StringValue("submit-test"),
				"level":  event.StringValue("info"),
			},
		}
	}
	if err := e.Ingest(events); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	return e
}

// doneAwaitingEngine wraps a real engine and blocks SubmitQuery until the job
// has completed before returning it. This makes the timer-vs-Done tie in
// Submit() deterministic: by the time Submit reaches its select, job.Done() is
// already closed, so a tiny SyncTimeout/Wait means BOTH the timer and the done
// channel are ready. Go selects randomly among ready cases, so without the
// non-blocking re-check the timer branch would return a 202 for an
// already-finished job.
type doneAwaitingEngine struct {
	*server.Engine
}

func (d doneAwaitingEngine) SubmitQuery(ctx context.Context, params server.QueryParams) (*server.SearchJob, error) {
	job, err := d.Engine.SubmitQuery(ctx, params)
	if err != nil {
		return nil, err
	}
	<-job.Done()

	return job, nil
}

// TestSubmit_SyncReturnsInlineResultForFastQuery locks in the basic sync
// contract: a query that completes within the window returns inline results
// (no JobID) with advisory metadata attached via the buildSync closure.
func TestSubmit_SyncReturnsInlineResultForFastQuery(t *testing.T) {
	e := newSubmitTestEngine(t)
	cfg := config.DefaultConfig().Query
	cfg.SyncTimeout = 30 * time.Second
	svc := NewQueryService(planner.New(), e, cfg)

	res, err := svc.Submit(context.Background(), SubmitRequest{
		Query: "source=submit-test | head 10",
		Mode:  QueryModeSync,
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if !res.Done {
		t.Fatalf("expected Done result, got job handle %q", res.JobID)
	}
	if res.JobID != "" {
		t.Errorf("expected empty JobID on sync result, got %q", res.JobID)
	}
	if res.QueryID == "" {
		t.Error("expected QueryID to be populated on sync result")
	}
}

// TestSubmit_SyncReturnsResultWhenJobDoneAtTimer exercises the timer-vs-Done
// race: with an already-completed job and a 1ns SyncTimeout, the select sees
// both cases ready. The non-blocking re-check must return the inline result
// every time rather than handing back a 202 the client would have to poll.
func TestSubmit_SyncReturnsResultWhenJobDoneAtTimer(t *testing.T) {
	e := newSubmitTestEngine(t)
	cfg := config.DefaultConfig().Query
	cfg.SyncTimeout = time.Nanosecond
	svc := NewQueryService(planner.New(), doneAwaitingEngine{e}, cfg)

	for i := 0; i < 100; i++ {
		res, err := svc.Submit(context.Background(), SubmitRequest{
			Query: "source=submit-test | head 10",
			Mode:  QueryModeSync,
		})
		if err != nil {
			t.Fatalf("iter %d: Submit: %v", i, err)
		}
		if !res.Done || res.JobID != "" {
			t.Fatalf("iter %d: job completed before the timer but Submit returned a 202 job handle (Done=%v, JobID=%q)",
				i, res.Done, res.JobID)
		}
	}
}

// TestSubmit_HybridReturnsResultWhenJobDoneAtTimer is the hybrid-mode analogue
// of the timer-vs-Done race test.
func TestSubmit_HybridReturnsResultWhenJobDoneAtTimer(t *testing.T) {
	e := newSubmitTestEngine(t)
	svc := NewQueryService(planner.New(), doneAwaitingEngine{e}, config.DefaultConfig().Query)

	for i := 0; i < 100; i++ {
		res, err := svc.Submit(context.Background(), SubmitRequest{
			Query: "source=submit-test | head 10",
			Mode:  QueryModeHybrid,
			Wait:  time.Nanosecond,
		})
		if err != nil {
			t.Fatalf("iter %d: Submit: %v", i, err)
		}
		if !res.Done || res.JobID != "" {
			t.Fatalf("iter %d: hybrid job completed before the wait expired but Submit returned a 202 (Done=%v, JobID=%q)",
				i, res.Done, res.JobID)
		}
	}
}

// TestSubmit_AsyncReturnsJobHandle confirms async mode still returns a job
// handle immediately (no inline result).
func TestSubmit_AsyncReturnsJobHandle(t *testing.T) {
	e := newSubmitTestEngine(t)
	svc := NewQueryService(planner.New(), e, config.DefaultConfig().Query)

	res, err := svc.Submit(context.Background(), SubmitRequest{
		Query: "source=submit-test | head 10",
		Mode:  QueryModeAsync,
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if res.Done {
		t.Error("expected a job handle for async mode, got an inline result")
	}
	if res.JobID == "" {
		t.Error("expected JobID to be populated for async mode")
	}
}
