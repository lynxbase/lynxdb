package server

import (
	"log/slog"
	"strings"
	"testing"
)

func TestFailJobOnPanicMarksJobFailed(t *testing.T) {
	e := &Engine{logger: slog.Default()}
	job := newSearchJob("from x | stats dc(y)", ResultTypeAggregate)

	func() {
		defer e.failJobOnPanic(job)
		panic("assignment to entry in nil map")
	}()

	snap := job.Snapshot()
	if snap.Status != JobStatusError {
		t.Fatalf("status = %q, want %q", snap.Status, JobStatusError)
	}
	if !strings.Contains(snap.Error, "assignment to entry in nil map") {
		t.Fatalf("error = %q, want panic value", snap.Error)
	}
	select {
	case <-job.Done():
	default:
		t.Fatal("job done channel not closed")
	}
}

func TestFailJobOnPanicWithoutPanicLeavesJobRunning(t *testing.T) {
	e := &Engine{logger: slog.Default()}
	job := newSearchJob("from x", ResultTypeEvents)

	func() {
		defer e.failJobOnPanic(job)
	}()

	if got := job.Snapshot().Status; got != JobStatusRunning {
		t.Fatalf("status = %q, want %q", got, JobStatusRunning)
	}
}
