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

	var rows []map[string]interface{}
	meta, err := h.Client().QueryStream(ctx, client.QueryRequest{
		Q: `source=idx_ssh | head 10`,
	}, func(msg json.RawMessage) error {
		var row map[string]interface{}
		if err := json.Unmarshal(msg, &row); err != nil {
			return err
		}
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		t.Fatalf("QueryStream: %v", err)
	}
	if len(rows) != 10 {
		t.Fatalf("expected 10 streamed events, got %d", len(rows))
	}
	if meta == nil || meta.Total != 10 {
		t.Errorf("expected stream meta total=10, got %+v", meta)
	}
	// head without sort follows time order: the log starts at Dec 10 06:55:46.
	want := "Dec 10 06:55:46 LabSZ sshd[24200]: reverse mapping checking getaddrinfo for ns.marryaldkfaczcz.com [173.234.31.186] failed - POSSIBLE BREAK-IN ATTEMPT!"
	if rows[0]["_raw"] != want {
		t.Errorf("expected the first log line, got %v", rows[0]["_raw"])
	}
}

func TestE2E_QueryStream_EmptyResult_ZeroTotal(t *testing.T) {
	h := NewHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var count int
	meta, err := h.Client().QueryStream(ctx, client.QueryRequest{
		Q: `source=nonexistent_stream_idx | head 10`,
	}, func(msg json.RawMessage) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("QueryStream on a nonexistent index: %v", err)
	}
	if meta == nil || meta.Total != 0 {
		t.Errorf("expected stream meta total=0, got %+v", meta)
	}
	if count != 0 {
		t.Errorf("expected 0 streamed events, got %d", count)
	}
}
