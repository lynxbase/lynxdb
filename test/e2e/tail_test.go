//go:build e2e

package e2e

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/client"
)

func TestE2E_Tail_ReceivesEvents(t *testing.T) {
	h := NewHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var received atomic.Int64
	gotFirst := make(chan struct{}, 1)

	tailDone := make(chan error, 1)
	go func() {
		err := h.Client().Tail(ctx, `FROM main`, "", 10, func(ev client.SSEEvent) error {
			if received.Add(1) == 1 {
				select {
				case gotFirst <- struct{}{}:
				default:
				}
			}
			return nil
		})
		tailDone <- err
	}()

	// Ingest events in a loop until at least one is received or timeout.
	ingestTicker := time.NewTicker(100 * time.Millisecond)
	defer ingestTicker.Stop()

	for {
		select {
		case <-gotFirst:
			// At least one event received — test passes.
			cancel()
			<-tailDone
			t.Logf("tail received %d events", received.Load())
			return
		case <-ingestTicker.C:
			events := []client.IngestEvent{
				{
					Event:  `{"host":"tail-test","message":"tail event"}`,
					Host:   "tail-test",
					Fields: map[string]interface{}{"message": "tail event"},
				},
			}
			_, _ = h.Client().IngestEvents(ctx, events)
		case <-ctx.Done():
			cancel()
			<-tailDone
			t.Fatalf("tail did not receive any events within timeout (received=%d)", received.Load())
		}
	}
}
