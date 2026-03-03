//go:build !unix

package buffer

import (
	"fmt"
	"unsafe"
)

// allocateOffHeap falls back to Go heap allocation on non-Unix platforms.
// The memory IS visible to the Go GC, so GC pause benefits do not apply.
// This provides functional correctness without platform-specific syscalls.
func allocateOffHeap(size int) (unsafe.Pointer, error) {
	if size <= 0 {
		return nil, fmt.Errorf("buffer.allocateOffHeap: invalid size %d", size)
	}

	buf := make([]byte, size)

	return unsafe.Pointer(&buf[0]), nil
}

// freeOffHeap is a no-op on non-Unix platforms — Go GC handles deallocation.
// The caller must ensure no references to the underlying slice remain.
func freeOffHeap(_ unsafe.Pointer, _ int) error {
	return nil
}

// mmapAvailable reports whether off-heap mmap allocation is available.
func mmapAvailable() bool {
	return false
}
