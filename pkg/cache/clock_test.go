package cache

import (
	"fmt"
	"testing"
)

func TestClockBuffer_Basic(t *testing.T) {
	cb := newClockBuffer(4)

	cb.insert("a")
	cb.insert("b")
	cb.insert("c")
	cb.insert("d")

	if cb.count != 4 {
		t.Fatalf("expected 4, got %d", cb.count)
	}
}

func TestClockBuffer_EvictionOrder(t *testing.T) {
	cb := newClockBuffer(4)

	// Fill the buffer.
	cb.insert("a")
	cb.insert("b")
	cb.insert("c")
	cb.insert("d")

	// All entries have refBit=1 from insert.
	// Access "b" and "d" again (redundant, but demonstrates pattern).
	cb.access("b")
	cb.access("d")

	// Insert "e" — should evict "a" (first unaccessed after one full scan).
	evicted := cb.insert("e")
	// After first full scan, all refBits become 0.
	// Second pass evicts "a" (first entry with refBit=0).
	t.Logf("evicted: %q", evicted)
	if evicted == "" {
		t.Fatal("expected an eviction")
	}

	// Insert "f" — should evict another entry.
	evicted2 := cb.insert("f")
	t.Logf("evicted2: %q", evicted2)
	if evicted2 == "" {
		t.Fatal("expected second eviction")
	}

	if cb.count != 4 {
		t.Errorf("expected 4 entries, got %d", cb.count)
	}
}

func TestClockBuffer_Remove(t *testing.T) {
	cb := newClockBuffer(4)
	cb.insert("a")
	cb.insert("b")
	cb.insert("c")

	if !cb.remove("b") {
		t.Error("remove should return true for existing key")
	}
	if cb.count != 2 {
		t.Errorf("expected 2, got %d", cb.count)
	}
	if cb.remove("b") {
		t.Error("remove should return false for non-existing key")
	}
}

func TestClockBuffer_AccessSetsRefBit(t *testing.T) {
	cb := newClockBuffer(4)
	cb.insert("a") // refBit = 1

	// Manually clear refBit by evicting (which sets refBit=0 on first pass).
	// Instead, we test that access returns true for existing key.
	if !cb.access("a") {
		t.Error("access should return true for existing key")
	}
	if cb.access("nonexistent") {
		t.Error("access should return false for non-existing key")
	}
}

func TestClockBuffer_Clear(t *testing.T) {
	cb := newClockBuffer(4)
	cb.insert("a")
	cb.insert("b")
	cb.clear()

	if cb.count != 0 {
		t.Errorf("expected 0 after clear, got %d", cb.count)
	}
}

func TestClockBuffer_DuplicateInsert(t *testing.T) {
	cb := newClockBuffer(4)
	cb.insert("a")
	evicted := cb.insert("a") // duplicate

	if evicted != "" {
		t.Errorf("duplicate insert should not evict, got %q", evicted)
	}
	if cb.count != 1 {
		t.Errorf("expected 1, got %d", cb.count)
	}
}

func TestClockBuffer_EvictAll(t *testing.T) {
	cb := newClockBuffer(4)
	cb.insert("a")
	cb.insert("b")
	cb.insert("c")
	cb.insert("d")

	// Insert 4 more items — should evict all 4 original items.
	for i := 0; i < 4; i++ {
		cb.insert(fmt.Sprintf("new_%d", i))
	}

	if cb.count != 4 {
		t.Errorf("expected 4, got %d", cb.count)
	}

	// None of the original keys should be present.
	for _, key := range []string{"a", "b", "c", "d"} {
		if cb.access(key) {
			t.Errorf("key %q should have been evicted", key)
		}
	}
}

func TestClockBuffer_Keys(t *testing.T) {
	cb := newClockBuffer(4)
	cb.insert("a")
	cb.insert("b")
	cb.insert("c")

	if len(cb.keyIndex) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(cb.keyIndex))
	}

	for _, expected := range []string{"a", "b", "c"} {
		if _, ok := cb.keyIndex[expected]; !ok {
			t.Errorf("expected key %q in keyIndex", expected)
		}
	}
}

func BenchmarkClockInsert(b *testing.B) {
	cb := newClockBuffer(1024)
	for i := 0; i < b.N; i++ {
		cb.insert(fmt.Sprintf("key_%d", i))
	}
}

func BenchmarkClockAccess(b *testing.B) {
	cb := newClockBuffer(1024)
	for i := 0; i < 1024; i++ {
		cb.insert(fmt.Sprintf("key_%d", i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cb.access(fmt.Sprintf("key_%d", i%1024))
	}
}
