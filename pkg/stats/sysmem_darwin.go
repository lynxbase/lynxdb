//go:build darwin

package stats

import (
	"syscall"
	"unsafe"
)

// TotalSystemMemory returns the total physical memory on macOS via
// the hw.memsize sysctl. Returns 0 if detection fails.
func TotalSystemMemory() int64 {
	// CTL_HW = 6, HW_MEMSIZE = 24
	mib := [2]int32{6, 24}
	var memsize uint64
	size := unsafe.Sizeof(memsize)

	_, _, errno := syscall.Syscall6(
		syscall.SYS___SYSCTL,
		uintptr(unsafe.Pointer(&mib[0])),
		2,
		uintptr(unsafe.Pointer(&memsize)),
		uintptr(unsafe.Pointer(&size)),
		0,
		0,
	)
	if errno != 0 {
		return 0
	}

	return int64(memsize)
}
