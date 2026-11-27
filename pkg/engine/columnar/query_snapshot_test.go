package columnar

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/ppl"
)

// pausedScan stops when the query executor installs its scan cancellation,
// after the subquery has acquired its input and before the outer scan runs.
type pausedScan struct {
	context.Context
	pause func()
}

func (c pausedScan) Done() <-chan struct{} {
	c.pause()
	return c.Context.Done()
}

func TestSubqueryAndOuterScanKeepTheirSource(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.Store.MergeFanIn = -1
	db, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := appendTo(db, "web", lines("web", 0, 1)); err != nil {
		t.Fatal(err)
	}
	compiled, err := ppl.Compile("source=web | where host in [ source=web | fields host ] | stats count() as n")
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	resume := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	ctx := pausedScan{Context: t.Context(), pause: sync.OnceFunc(func() {
		close(entered)
		<-resume
	})}

	done := make(chan error, 1)
	go func() {
		res, err := db.Query(ctx, compiled.Plan, Options{Workers: 1})
		if err == nil {
			if got := render(res.Maps()); got != "n=1" {
				err = fmt.Errorf("query already reading the index saw %s", got)
			}
		}
		done <- err
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("query ended before entering its scan: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("query did not reach its read")
	}
	if _, err := db.DropPartition(t.Context(), "web", testNow.Truncate(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if got := ask(t, db, "source=web | stats count() as n"); got != "n=0" {
		t.Fatalf("new query saw %s", got)
	}
	unblock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
