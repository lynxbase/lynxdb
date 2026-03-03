//go:build e2e

package e2e

import (
	"context"
	"testing"
)

func TestE2E_Cache_StatsAndClear(t *testing.T) {
	h := NewHarness(t)
	ctx := context.Background()

	// Get initial cache stats.
	stats1, err := h.Client().CacheStats(ctx)
	if err != nil {
		t.Fatalf("CacheStats (initial): %v", err)
	}
	t.Logf("initial cache stats: %v", stats1)

	// Clear cache.
	err = h.Client().CacheClear(ctx)
	if err != nil {
		t.Fatalf("CacheClear: %v", err)
	}

	// Get cache stats after clear.
	stats2, err := h.Client().CacheStats(ctx)
	if err != nil {
		t.Fatalf("CacheStats (after clear): %v", err)
	}
	t.Logf("cache stats after clear: %v", stats2)
}
