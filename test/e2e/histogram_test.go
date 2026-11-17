//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestE2E_Histogram_ReturnsBuckets(t *testing.T) {
	h := NewHarness(t)
	h.IngestFile("idx_ssh", "testdata/logs/OpenSSH_2k.log")

	// The log lines carry no year; ingest dates them Dec 10 of the current year.
	year := time.Now().UTC().Year()
	from := fmt.Sprintf("%d-12-10T06:00:00Z", year)
	to := fmt.Sprintf("%d-12-10T12:00:00Z", year)

	ctx := context.Background()
	result, err := h.Client().Histogram(ctx, from, to, 6, "idx_ssh")
	if err != nil {
		t.Fatalf("Histogram: %v", err)
	}
	if result.Interval != time.Hour.String() {
		t.Errorf("expected interval 1h, got %q", result.Interval)
	}
	if result.Total != 2000 {
		t.Errorf("expected total=2000, got %d", result.Total)
	}

	// Lines per hour of OpenSSH_2k.log, from 06:00 to 11:00.
	want := []int64{7, 169, 118, 676, 554, 476}
	if len(result.Buckets) != len(want) {
		t.Fatalf("expected %d buckets, got %d: %+v", len(want), len(result.Buckets), result.Buckets)
	}
	start := time.Date(year, time.December, 10, 6, 0, 0, 0, time.UTC)
	for i, b := range result.Buckets {
		if wantTime := start.Add(time.Duration(i) * time.Hour); !b.Time.Equal(wantTime) || b.Count != want[i] {
			t.Errorf("bucket %d: expected %s=%d, got %s=%d", i, wantTime.Format(time.RFC3339), want[i], b.Time.Format(time.RFC3339), b.Count)
		}
	}
}
