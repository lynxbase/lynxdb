//go:build !unix

package bufmgr

import (
	"fmt"
	"unsafe"
)

// allocateOffHeap falls back to Go heap allocation on non-Unix platforms.
func allocateOffHeap(size int) (unsafe.Pointer, error) {
	if size <= 0 {
		return nil, fmt.Errorf("bufmgr.allocateOffHeap: invalid size %d", size)
	}

	buf := make([]byte, size)

	return unsafe.Pointer(&buf[0]), nil
}

// freeOffHeap is a no-op on non-Unix platforms.
func freeOffHeap(_ unsafe.Pointer, _ int) error {
	return nil
}

// mmapAvailable reports whether off-heap mmap allocation is available.
func mmapAvailable() bool {
	return false
}
