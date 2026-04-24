package pipeline

import (
	"context"
	"fmt"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/memgov"
)

func TestDedupSpillTransition(t *testing.T) {
	// Create rows with 200 unique values. With a tiny budget, the dedup
	// should spill to disk and continue deduplicating correctly.
	n := 1000
	rows := make([]map[string]event.Value, n)
	for i := 0; i < n; i++ {
		rows[i] = map[string]event.Value{
			"key": event.StringValue(fmt.Sprintf("val%d", i%200)),
			"seq": event.IntValue(int64(i)),
		}
	}

	child := NewRowScanIterator(rows, 64)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// Budget is small enough to force a spill after ~50 entries.
	// estimatedDedupHashEntryBytes = 56, so 56*50 = 2800 bytes.
	acct := memgov.NewTestBudget("test", 3*1024).NewAccount("dedup")
	iter := newDedupIteratorWithSpill(child, []string{"key"}, 1, acct, mgr)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	// Should have exactly 200 unique values.
	if len(result) != 200 {
		t.Fatalf("expected 200 unique rows, got %d", len(result))
	}

	// Verify all values are unique.
	seen := make(map[string]bool)
	for _, row := range result {
		key := row["key"].String()
		if seen[key] {
			t.Errorf("duplicate key found: %s", key)
		}
		seen[key] = true
	}

	// Verify spill actually occurred.
	// CollectAll calls Close(), which clears externalSet, so we check
	// the persisted spill metrics instead.
	di := findDedupIterator(iter)
	if di == nil {
		t.Fatal("could not find DedupIterator in chain")
	}
	if di.spilledEntries == 0 {
		t.Error("expected spilledEntries > 0")
	}

	// ResourceStats should report spill (safe to call after Close).
	rs := di.ResourceStats()
	if rs.SpilledRows == 0 {
		t.Error("ResourceStats.SpilledRows should be > 0")
	}
	if rs.SpillBytes == 0 {
		t.Error("ResourceStats.SpillBytes should be > 0")
	}
}

func TestDedupSpillWithLimit(t *testing.T) {
	// Test dedup with limit=3 and spill. Each unique key should appear at most 3 times.
	n := 600
	rows := make([]map[string]event.Value, n)
	for i := 0; i < n; i++ {
		rows[i] = map[string]event.Value{
			"key": event.StringValue(fmt.Sprintf("val%d", i%50)),
			"seq": event.IntValue(int64(i)),
		}
	}

	child := NewRowScanIterator(rows, 64)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// Small budget to force spill.
	acct := memgov.NewTestBudget("test", 2*1024).NewAccount("dedup")
	iter := newDedupIteratorWithSpill(child, []string{"key"}, 3, acct, mgr)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	// Should have at most 50 * 3 = 150 rows.
	if len(result) != 150 {
		t.Fatalf("expected 150 rows (50 keys * limit 3), got %d", len(result))
	}

	// Verify no key appears more than 3 times.
	counts := make(map[string]int)
	for _, row := range result {
		key := row["key"].String()
		counts[key]++
	}
	for key, count := range counts {
		if count > 3 {
			t.Errorf("key %s appeared %d times, expected <= 3", key, count)
		}
	}
}

func TestDedupSpillExactMode(t *testing.T) {
	// Test exact mode dedup with spill.
	n := 500
	rows := make([]map[string]event.Value, n)
	for i := 0; i < n; i++ {
		rows[i] = map[string]event.Value{
			"key": event.StringValue(fmt.Sprintf("val%d", i%100)),
		}
	}

	child := NewRowScanIterator(rows, 64)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// Small budget.
	acct := memgov.NewTestBudget("test", 4*1024).NewAccount("dedup")
	iter := newDedupIteratorExactWithSpill(child, []string{"key"}, 1, acct, mgr)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 100 {
		t.Fatalf("expected 100 unique rows, got %d", len(result))
	}
}

func TestDedupNoSpillSmallDataset(t *testing.T) {
	// With a large budget, dedup should not spill.
	n := 100
	rows := make([]map[string]event.Value, n)
	for i := 0; i < n; i++ {
		rows[i] = map[string]event.Value{
			"key": event.StringValue(fmt.Sprintf("val%d", i%20)),
		}
	}

	child := NewRowScanIterator(rows, 64)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// Large budget — no spill expected.
	acct := memgov.NewTestBudget("test", 1*1024*1024).NewAccount("dedup")
	iter := newDedupIteratorWithSpill(child, []string{"key"}, 1, acct, mgr)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 20 {
		t.Fatalf("expected 20 unique rows, got %d", len(result))
	}

	// No spill should have occurred.
	di := findDedupIterator(iter)
	if di == nil {
		t.Fatal("could not find DedupIterator in chain")
	}
	if di.spilledEntries != 0 {
		t.Errorf("expected no spill for small dataset, got %d spilled entries", di.spilledEntries)
	}
}

func TestDedupSpillMultiField(t *testing.T) {
	// Test dedup with multiple fields and spill.
	// Use (i/8)%10 for host so all 10*8=80 (host, source) combos are generated.
	// (i%10, i%8) only produces 40 combos due to gcd(10,8)=2.
	n := 800
	rows := make([]map[string]event.Value, n)
	for i := 0; i < n; i++ {
		rows[i] = map[string]event.Value{
			"host":   event.StringValue(fmt.Sprintf("host%d", (i/8)%10)),
			"source": event.StringValue(fmt.Sprintf("src%d", i%8)),
		}
	}

	child := NewRowScanIterator(rows, 64)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// Small budget to force spill.
	acct := memgov.NewTestBudget("test", 2*1024).NewAccount("dedup")
	iter := newDedupIteratorWithSpill(child, []string{"host", "source"}, 1, acct, mgr)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	// 10 hosts * 8 sources = 80 unique combinations.
	if len(result) != 80 {
		t.Fatalf("expected 80 unique rows, got %d", len(result))
	}
}

func TestDedupSpillWithoutSpillManager(t *testing.T) {
	// Without a SpillManager, budget exceeded should return an error.
	n := 500
	rows := make([]map[string]event.Value, n)
	for i := 0; i < n; i++ {
		rows[i] = map[string]event.Value{
			"key": event.StringValue(fmt.Sprintf("val%d", i)),
		}
	}

	child := NewRowScanIterator(rows, 64)
	// Tiny budget, no spill manager.
	acct := memgov.NewTestBudget("test", 1*1024).NewAccount("dedup")
	iter := NewDedupIteratorWithBudget(child, []string{"key"}, 1, acct)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	_, err := CollectAll(ctx, iter)
	if err == nil {
		t.Fatal("expected budget exceeded error, got nil")
	}
	if !memgov.IsBudgetExceeded(err) {
		t.Fatalf("expected BudgetExceededError, got: %v", err)
	}
}

func TestExternalDedupSetBasics(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	seenHash := map[uint64]int{
		100: 1,
		200: 1,
		300: 1,
	}

	eds, err := newExternalDedupSet(seenHash, nil, false, mgr)
	if err != nil {
		t.Fatal(err)
	}
	defer eds.close()

	// Known hashes should be found.
	if !eds.containsHash(100) {
		t.Error("expected containsHash(100) = true")
	}
	if !eds.containsHash(200) {
		t.Error("expected containsHash(200) = true")
	}
	if !eds.containsHash(300) {
		t.Error("expected containsHash(300) = true")
	}

	// Unknown hashes should not be found.
	if eds.containsHash(999) {
		t.Error("expected containsHash(999) = false")
	}

	// Add a new hash.
	if err := eds.addHash(999); err != nil {
		t.Fatal(err)
	}
	if !eds.containsHash(999) {
		t.Error("expected containsHash(999) = true after add")
	}
}

func TestExternalDedupSetFlushBuffer(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	eds, err := newExternalDedupSet(nil, nil, false, mgr)
	if err != nil {
		t.Fatal(err)
	}
	defer eds.close()

	// Add enough hashes to trigger a buffer flush (bufferMax = 65536).
	// Use a smaller set and manually trigger flush.
	for i := uint64(0); i < 100; i++ {
		if addErr := eds.addHash(i * 17); addErr != nil {
			t.Fatal(addErr)
		}
	}

	// Force a flush by calling flushBuffer.
	if flushErr := eds.flushBuffer(); flushErr != nil {
		t.Fatal(flushErr)
	}

	// All hashes should still be findable after flush.
	for i := uint64(0); i < 100; i++ {
		if !eds.containsHash(i * 17) {
			t.Errorf("expected containsHash(%d) = true after flush", i*17)
		}
	}

	// Hashes not added should not be found.
	if eds.containsHash(1) {
		t.Error("expected containsHash(1) = false")
	}
}

// findDedupIterator walks the iterator chain to find a DedupIterator.
func findDedupIterator(iter Iterator) *DedupIterator {
	switch it := iter.(type) {
	case *DedupIterator:
		return it
	case *InstrumentedIterator:
		return findDedupIterator(it.inner)
	default:
		return nil
	}
}
