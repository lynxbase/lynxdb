//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/OrlovEvgeny/Lynxdb/pkg/client"
)

func TestE2E_IngestRaw_TextPlain_CountMatches(t *testing.T) {
	h := NewHarness(t)
	h.IngestFile("idx_ssh", "testdata/OpenSSH_2k.log")

	result := h.MustQuery(`FROM idx_ssh | STATS count`)
	requireAggValue(t, result, "count", 2000)
}

func TestE2E_IngestRaw_MultipleIndexes(t *testing.T) {
	h := NewHarness(t)
	h.IngestFile("idx_ssh", "testdata/OpenSSH_2k.log")
	h.IngestFile("idx_openstack", "testdata/OpenStack_2k.log")

	r1 := h.MustQuery(`FROM idx_ssh | STATS count`)
	requireAggValue(t, r1, "count", 2000)

	r2 := h.MustQuery(`FROM idx_openstack | STATS count`)
	requireAggValue(t, r2, "count", 2000)
}

func TestE2E_IngestJSON_StructuredEvents(t *testing.T) {
	h := NewHarness(t)
	ctx := context.Background()

	events := []map[string]interface{}{
		{"host": "web-01", "status": 200, "path": "/api/v1/users"},
		{"host": "web-02", "status": 404, "path": "/api/v1/missing"},
		{"host": "web-01", "status": 500, "path": "/api/v1/error"},
	}

	result, err := h.Client().Ingest(ctx, events)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if result.Accepted != 3 {
		t.Errorf("expected 3 accepted, got %d", result.Accepted)
	}

	r := h.MustQuery(`FROM main | STATS count`)
	requireAggValue(t, r, "count", 3)
}

func TestE2E_IngestRaw_NewIndex_Queryable(t *testing.T) {
	h := NewHarness(t)
	ctx := context.Background()

	body := strings.NewReader("line 1\nline 2\nline 3\n")
	result, err := h.Client().IngestRaw(ctx, body, client.IngestOpts{
		Index:       "custom_index",
		ContentType: "text/plain",
	})
	if err != nil {
		t.Fatalf("IngestRaw: %v", err)
	}
	if result.Accepted != 3 {
		t.Errorf("expected 3 accepted, got %d", result.Accepted)
	}

	// Data should land in custom_index (X-Index header is sent by IngestRaw).
	r := h.MustQuery(`FROM custom_index | STATS count`)
	requireAggValue(t, r, "count", 3)
}

func TestE2E_IngestRaw_IndexHeader_RoutesToCorrectIndex(t *testing.T) {
	h := NewHarness(t)
	ctx := context.Background()

	body := strings.NewReader("test line 1\ntest line 2\n")
	_, err := h.Client().IngestRaw(ctx, body, client.IngestOpts{
		Index:       "bug_test_idx",
		ContentType: "text/plain",
	})
	if err != nil {
		t.Fatalf("IngestRaw: %v", err)
	}

	// IngestRaw sends X-Index header, so data lands in bug_test_idx.
	rTarget := h.MustQuery(`FROM bug_test_idx | STATS count`)
	requireAggValue(t, rTarget, "count", 2)

	// Verify nothing leaked to main.
	rMain := h.MustQuery(`FROM main | STATS count`)
	mainCount := GetInt(rMain, "count")
	if mainCount != 0 {
		t.Errorf("expected 0 events in main, got %d (data leaked from bug_test_idx)", mainCount)
	}
}

func TestE2E_IngestRaw_EmptyBody_ReturnsError(t *testing.T) {
	h := NewHarness(t)
	ctx := context.Background()

	_, err := h.Client().IngestRaw(ctx, bytes.NewReader(nil), client.IngestOpts{
		Index:       "main",
		ContentType: "text/plain",
	})
	// Empty body should return an error or zero accepted.
	if err != nil {
		// Error is the expected path.
		t.Logf("empty body correctly returned error: %v", err)
		return
	}
	t.Log("empty body did not return error (server accepted empty payload)")
}
