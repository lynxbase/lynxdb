package buffer

import (
	"testing"
	"unsafe"
)

func TestAllocateOffHeap(t *testing.T) {
	size := 4096
	ptr, err := allocateOffHeap(size)
	if err != nil {
		t.Fatalf("allocateOffHeap(%d): %v", size, err)
	}
	if ptr == nil {
		t.Fatal("allocateOffHeap returned nil pointer")
	}

	// Write and read back to verify the memory is usable.
	data := unsafe.Slice((*byte)(ptr), size)
	data[0] = 0xDE
	data[1] = 0xAD
	data[size-1] = 0xFF

	if data[0] != 0xDE || data[1] != 0xAD {
		t.Error("write/read failed at start of allocation")
	}
	if data[size-1] != 0xFF {
		t.Error("write/read failed at end of allocation")
	}

	// Free the memory.
	if err := freeOffHeap(ptr, size); err != nil {
		t.Fatalf("freeOffHeap: %v", err)
	}
}

func TestAllocateOffHeap_InvalidSize(t *testing.T) {
	_, err := allocateOffHeap(0)
	if err == nil {
		t.Error("allocateOffHeap(0) should fail")
	}

	_, err = allocateOffHeap(-1)
	if err == nil {
		t.Error("allocateOffHeap(-1) should fail")
	}
}

func TestFreeOffHeap_Nil(t *testing.T) {
	if err := freeOffHeap(nil, 0); err != nil {
		t.Errorf("freeOffHeap(nil) should succeed, got: %v", err)
	}
}

func TestMmapAvailable(t *testing.T) {
	// Verify it returns a bool without panicking.
	_ = mmapAvailable()
}

func TestAllocateOffHeap_LargeAllocation(t *testing.T) {
	// Allocate 1MB to verify larger sizes work.
	size := 1 << 20
	ptr, err := allocateOffHeap(size)
	if err != nil {
		t.Fatalf("allocateOffHeap(%d): %v", size, err)
	}

	// Write pattern to verify the entire range is accessible.
	data := unsafe.Slice((*byte)(ptr), size)
	for i := 0; i < size; i += 4096 {
		data[i] = byte(i >> 12)
	}

	// Verify pattern.
	for i := 0; i < size; i += 4096 {
		expected := byte(i >> 12)
		if data[i] != expected {
			t.Errorf("data[%d] = %d, want %d", i, data[i], expected)

			break
		}
	}

	if err := freeOffHeap(ptr, size); err != nil {
		t.Fatalf("freeOffHeap: %v", err)
	}
}

func TestPoolWithOffHeap(t *testing.T) {
	if !mmapAvailable() {
		t.Fatal("test requires mmap support")
	}

	bp, err := NewPool(PoolConfig{
		MaxPages:      4,
		PageSize:      PageSize64KB,
		EnableOffHeap: true,
	})
	if err != nil {
		t.Fatalf("NewPool with off-heap: %v", err)
	}
	defer func() { _ = bp.Close() }()

	if !bp.Stats().OffHeap {
		t.Error("expected off-heap mode")
	}

	// Allocate and use a page.
	p, err := bp.AllocPage(OwnerQueryOperator, "test")
	if err != nil {
		t.Fatalf("AllocPage: %v", err)
	}

	if err := p.WriteAt([]byte{0xCA, 0xFE}, 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}

	var buf [2]byte
	if err := p.ReadAt(buf[:], 0); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if buf[0] != 0xCA || buf[1] != 0xFE {
		t.Errorf("ReadAt = %x, want CAFE", buf)
	}
	p.Unpin()
}
