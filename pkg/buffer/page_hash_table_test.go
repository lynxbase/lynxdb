package buffer

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"testing"
)

func hashKey(key []byte) uint64 {
	h := fnv.New64a()
	h.Write(key)

	return h.Sum64()
}

func TestPageHashTable_PutGet(t *testing.T) {
	bp := newTestPool(t, 16)
	alloc := NewOperatorPageAllocator(bp, "ht-test")
	defer alloc.ReleaseAll()

	ht := NewPageHashTable(alloc)

	key := []byte("group-key-1")
	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, 42)
	h := hashKey(key)

	ref, err := ht.Put(h, key, value)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !ref.IsValid() {
		t.Fatal("Put returned invalid ref")
	}

	if ht.Len() != 1 {
		t.Errorf("Len = %d, want 1", ht.Len())
	}

	// Get.
	gotRef, ok := ht.Get(h, key)
	if !ok {
		t.Fatal("Get returned miss for existing key")
	}

	// Resolve and read value.
	resolved := bp.Resolve(gotRef)
	if resolved == nil {
		t.Fatal("Resolve returned nil")
	}
	gotVal := binary.LittleEndian.Uint64(resolved)
	if gotVal != 42 {
		t.Errorf("value = %d, want 42", gotVal)
	}
}

func TestPageHashTable_PutUpdate(t *testing.T) {
	bp := newTestPool(t, 16)
	alloc := NewOperatorPageAllocator(bp, "ht-update")
	defer alloc.ReleaseAll()

	ht := NewPageHashTable(alloc)

	key := []byte("counter")
	val1 := make([]byte, 8)
	binary.LittleEndian.PutUint64(val1, 100)
	h := hashKey(key)

	_, err := ht.Put(h, key, val1)
	if err != nil {
		t.Fatalf("Put v1: %v", err)
	}

	// Update with same-size value.
	val2 := make([]byte, 8)
	binary.LittleEndian.PutUint64(val2, 200)
	ref2, err := ht.Put(h, key, val2)
	if err != nil {
		t.Fatalf("Put v2: %v", err)
	}

	// Should not create a new entry.
	if ht.Len() != 1 {
		t.Errorf("Len = %d, want 1 (update, not insert)", ht.Len())
	}

	resolved := bp.Resolve(ref2)
	if resolved == nil {
		t.Fatal("Resolve returned nil after update")
	}
	gotVal := binary.LittleEndian.Uint64(resolved)
	if gotVal != 200 {
		t.Errorf("value after update = %d, want 200", gotVal)
	}
}

func TestPageHashTable_GetMiss(t *testing.T) {
	bp := newTestPool(t, 8)
	alloc := NewOperatorPageAllocator(bp, "ht-miss")
	defer alloc.ReleaseAll()

	ht := NewPageHashTable(alloc)

	key := []byte("nonexistent")
	h := hashKey(key)

	_, ok := ht.Get(h, key)
	if ok {
		t.Error("Get should return false for missing key")
	}
}

func TestPageHashTable_MultipleEntries(t *testing.T) {
	bp := newTestPool(t, 32)
	alloc := NewOperatorPageAllocator(bp, "ht-multi")
	defer alloc.ReleaseAll()

	ht := NewPageHashTable(alloc)

	const n = 100
	for i := 0; i < n; i++ {
		key := make([]byte, 4)
		binary.LittleEndian.PutUint32(key, uint32(i))
		val := make([]byte, 8)
		binary.LittleEndian.PutUint64(val, uint64(i*10))
		h := hashKey(key)

		if _, err := ht.Put(h, key, val); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	if ht.Len() != n {
		t.Errorf("Len = %d, want %d", ht.Len(), n)
	}

	// Verify all entries.
	for i := 0; i < n; i++ {
		key := make([]byte, 4)
		binary.LittleEndian.PutUint32(key, uint32(i))
		h := hashKey(key)

		ref, ok := ht.Get(h, key)
		if !ok {
			t.Errorf("Get %d: miss", i)

			continue
		}
		resolved := bp.Resolve(ref)
		if resolved == nil {
			t.Errorf("Resolve %d: nil", i)

			continue
		}
		gotVal := binary.LittleEndian.Uint64(resolved)
		if gotVal != uint64(i*10) {
			t.Errorf("value[%d] = %d, want %d", i, gotVal, i*10)
		}
	}
}

func TestPageHashTable_HashCollision(t *testing.T) {
	bp := newTestPool(t, 16)
	alloc := NewOperatorPageAllocator(bp, "ht-collision")
	defer alloc.ReleaseAll()

	ht := NewPageHashTable(alloc)

	// Use the same hash for two different keys (simulated collision).
	sameHash := uint64(12345)
	key1 := []byte("key-alpha")
	key2 := []byte("key-beta")
	val1 := []byte("value-1!")
	val2 := []byte("value-2!")

	if _, err := ht.Put(sameHash, key1, val1); err != nil {
		t.Fatalf("Put key1: %v", err)
	}
	if _, err := ht.Put(sameHash, key2, val2); err != nil {
		t.Fatalf("Put key2: %v", err)
	}

	if ht.Len() != 2 {
		t.Errorf("Len = %d, want 2 (two entries with same hash)", ht.Len())
	}

	// Both should be retrievable.
	ref1, ok := ht.Get(sameHash, key1)
	if !ok {
		t.Fatal("Get key1: miss")
	}
	if !bytes.Equal(bp.Resolve(ref1), val1) {
		t.Errorf("value1 mismatch")
	}

	ref2, ok := ht.Get(sameHash, key2)
	if !ok {
		t.Fatal("Get key2: miss")
	}
	if !bytes.Equal(bp.Resolve(ref2), val2) {
		t.Errorf("value2 mismatch")
	}
}

func TestPageHashTable_ForEach(t *testing.T) {
	bp := newTestPool(t, 16)
	alloc := NewOperatorPageAllocator(bp, "ht-foreach")
	defer alloc.ReleaseAll()

	ht := NewPageHashTable(alloc)

	keys := []string{"aaa", "bbb", "ccc"}
	for _, k := range keys {
		kb := []byte(k)
		val := []byte("v-" + k)
		h := hashKey(kb)
		if _, err := ht.Put(h, kb, val); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}

	var visited int
	ht.ForEach(func(hash uint64, key []byte, valRef PageRef) bool {
		visited++

		return true // continue
	})
	if visited != 3 {
		t.Errorf("ForEach visited %d entries, want 3", visited)
	}
}

func TestPageHashTable_ForEach_EarlyStop(t *testing.T) {
	bp := newTestPool(t, 16)
	alloc := NewOperatorPageAllocator(bp, "ht-stop")
	defer alloc.ReleaseAll()

	ht := NewPageHashTable(alloc)

	for i := 0; i < 10; i++ {
		key := make([]byte, 4)
		binary.LittleEndian.PutUint32(key, uint32(i))
		val := []byte("val")
		if _, err := ht.Put(hashKey(key), key, val); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	var visited int
	ht.ForEach(func(hash uint64, key []byte, valRef PageRef) bool {
		visited++

		return visited < 3 // stop after 3
	})
	if visited != 3 {
		t.Errorf("ForEach with early stop visited %d entries, want 3", visited)
	}
}

func TestPageHashTable_Clear(t *testing.T) {
	bp := newTestPool(t, 16)
	alloc := NewOperatorPageAllocator(bp, "ht-clear")
	defer alloc.ReleaseAll()

	ht := NewPageHashTable(alloc)

	key := []byte("test")
	val := []byte("data1234")
	if _, err := ht.Put(hashKey(key), key, val); err != nil {
		t.Fatalf("Put: %v", err)
	}

	ht.Clear()

	if ht.Len() != 0 {
		t.Errorf("Len = %d, want 0 after Clear", ht.Len())
	}
	if ht.PageCount() != 0 {
		t.Errorf("PageCount = %d, want 0 after Clear", ht.PageCount())
	}
}

func TestPageHashTable_PageCount(t *testing.T) {
	bp := newTestPool(t, 32)
	alloc := NewOperatorPageAllocator(bp, "ht-pages")
	defer alloc.ReleaseAll()

	ht := NewPageHashTable(alloc)

	// First entry should allocate one page.
	key := []byte("key1")
	val := []byte("val1val1")
	if _, err := ht.Put(hashKey(key), key, val); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if ht.PageCount() != 1 {
		t.Errorf("PageCount = %d, want 1", ht.PageCount())
	}
}
