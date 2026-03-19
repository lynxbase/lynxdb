//go:build unix

package bufmgr

import (
	"fmt"
	"unsafe"

	"golang.org/x/sys/unix"
)

// allocateOffHeap allocates off-heap memory via anonymous mmap.
// The memory is not visible to the Go garbage collector, reducing GC pressure.
func allocateOffHeap(size int) (unsafe.Pointer, error) {
	if size <= 0 {
		return nil, fmt.Errorf("bufmgr.allocateOffHeap: invalid size %d", size)
	}

	data, err := unix.Mmap(-1, 0, size,
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_PRIVATE|unix.MAP_ANON)
	if err != nil {
		return nil, fmt.Errorf("bufmgr.allocateOffHeap: mmap(%d bytes): %w", size, err)
	}

	return unsafe.Pointer(&data[0]), nil
}

// freeOffHeap releases off-heap memory.
func freeOffHeap(ptr unsafe.Pointer, size int) error {
	if ptr == nil {
		return nil
	}

	data := unsafe.Slice((*byte)(ptr), size)
	if err := unix.Munmap(data); err != nil {
		return fmt.Errorf("bufmgr.freeOffHeap: munmap(%d bytes): %w", size, err)
	}

	return nil
}

// mmapAvailable reports whether off-heap mmap allocation is available.
func mmapAvailable() bool {
	return true
}
