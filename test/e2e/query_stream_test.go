//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/client"
)

func TestE2E_QueryStream_NDJSON_DeliversEvents(t *testing.T) {
	h := NewHarness(t)
	h.IngestFile("idx_ssh", "testdata/logs/OpenSSH_2k.log")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var count int
	meta, err := h.Client().QueryStream(ctx, client.QueryRequest{
		Q:     `FROM idx_ssh | HEAD 10`,
		Limit: 10,
	}, func(msg json.RawMessage) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("QueryStream: %v", err)
	}
	if count == 0 {
		t.Error("expected to receive at least 1 streamed event")
	}
	if meta != nil {
		t.Logf("stream meta: total=%d, scanned=%d, took_ms=%d", meta.Total, meta.Scanned, meta.TookMS)
	}
}

func TestE2E_QueryStream_EmptyResult_ZeroTotal(t *testing.T) {
	h := NewHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var count int
	meta, err := h.Client().QueryStream(ctx, client.QueryRequest{
		Q: `FROM nonexistent_stream_idx | HEAD 10`,
	}, func(msg json.RawMessage) error {
		count++
		return nil
	})
	if err != nil {
		// Some implementations may return an error for nonexistent index.
		t.Logf("QueryStream on nonexistent index returned error: %v", err)
		return
	}
	if meta != nil && meta.Total != 0 {
		t.Errorf("expected total=0 for empty result, got %d", meta.Total)
	}
	if count != 0 {
		t.Errorf("expected 0 streamed events, got %d", count)
	}
}
