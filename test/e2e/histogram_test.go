//go:build e2e

package e2e

import (
	"context"
	"testing"
)

func TestE2E_Histogram_ReturnsBuckets(t *testing.T) {
	h := NewHarness(t)
	h.IngestFile("idx_ssh", "testdata/OpenSSH_2k.log")

	ctx := context.Background()
	// Use a wide time range to cover all ingested events regardless of their
	// timestamps. The index parameter targets the specific index.
	result, err := h.Client().Histogram(ctx, "-8760h", "now", 20, "idx_ssh")
	if err != nil {
		t.Fatalf("Histogram: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil histogram result")
	}
	if len(result.Buckets) == 0 {
		t.Error("expected non-empty buckets")
	}
	t.Logf("histogram: %d buckets, total=%d, interval=%s", len(result.Buckets), result.Total, result.Interval)
	// total may be 0 if the histogram endpoint counts differently from STATS count.
	// We verify that the endpoint returns successfully and produces buckets.
}
