package planner

import (
	"fmt"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/server"
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

func makePlan(query string) *Plan {
	return &Plan{
		RawQuery:   query,
		ResultType: server.ResultTypeEvents,
		Program:    &spl2.Program{Main: &spl2.Query{}},
	}
}

func TestPlanCache_HitMiss(t *testing.T) {
	cache := NewPlanCache(100, 5*time.Minute)

	plan := makePlan("search error")
	cache.Put("search error", plan)

	// Cache hit.
	got, ok := cache.Get("search error")
	if !ok {
		t.Fatal("expected cache hit, got miss")
	}
	if got.RawQuery != "search error" {
		t.Fatalf("expected RawQuery %q, got %q", "search error", got.RawQuery)
	}

	// Cache miss for nonexistent query.
	_, ok = cache.Get("search warning")
	if ok {
		t.Fatal("expected cache miss for nonexistent query, got hit")
	}
}

func TestPlanCache_TTLExpiry(t *testing.T) {
	cache := NewPlanCache(100, 1*time.Millisecond)

	plan := makePlan("search error")
	cache.Put("search error", plan)

	time.Sleep(5 * time.Millisecond)

	_, ok := cache.Get("search error")
	if ok {
		t.Fatal("expected cache miss after TTL expiry, got hit")
	}
}

func TestPlanCache_DeepClone(t *testing.T) {
	cache := NewPlanCache(100, 5*time.Minute)

	plan := makePlan("search error")
	cache.Put("search error", plan)

	// Get the plan and mutate the returned copy.
	got, ok := cache.Get("search error")
	if !ok {
		t.Fatal("expected cache hit")
	}
	got.RawQuery = "MUTATED"

	// Get again and verify the cached plan is NOT mutated.
	got2, ok := cache.Get("search error")
	if !ok {
		t.Fatal("expected cache hit on second get")
	}
	if got2.RawQuery != "search error" {
		t.Fatalf("cached plan was mutated: expected RawQuery %q, got %q", "search error", got2.RawQuery)
	}
}

func TestPlanCache_EvictionCLOCK(t *testing.T) {
	cache := NewPlanCache(3, 5*time.Minute)

	for i := 0; i < 4; i++ {
		q := fmt.Sprintf("search query%d", i)
		cache.Put(q, makePlan(q))
	}

	// Verify the map size does not exceed capacity.
	cache.mu.RLock()
	mapSize := len(cache.entries)
	cache.mu.RUnlock()
	if mapSize > 3 {
		t.Fatalf("expected at most 3 entries after eviction, got %d", mapSize)
	}

	// Verify at least 3 plans are accessible.
	hits := 0
	for i := 0; i < 4; i++ {
		q := fmt.Sprintf("search query%d", i)
		if _, ok := cache.Get(q); ok {
			hits++
		}
	}
	if hits < 3 {
		t.Fatalf("expected at least 3 accessible plans, got %d", hits)
	}
}

func TestPlanCache_NormalizeQuery(t *testing.T) {
	cache := NewPlanCache(100, 5*time.Minute)

	plan := makePlan("SEARCH  error")
	cache.Put("SEARCH  error", plan)

	// Get with normalized form: lowercase and collapsed whitespace.
	got, ok := cache.Get("search error")
	if !ok {
		t.Fatal("expected cache hit after normalization, got miss")
	}
	if got.RawQuery != "SEARCH  error" {
		t.Fatalf("expected original RawQuery %q, got %q", "SEARCH  error", got.RawQuery)
	}
}
