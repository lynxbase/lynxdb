//go:build windows

package stats

import (
	"syscall"
	"unsafe"
)

// memoryStatusEx corresponds to the Windows MEMORYSTATUSEX structure.
// See: https://learn.microsoft.com/en-us/windows/win32/api/sysinfoapi/ns-sysinfoapi-memorystatusex
type memoryStatusEx struct {
	dwLength                uint32
	dwMemoryLoad            uint32
	ullTotalPhys            uint64
	ullAvailPhys            uint64
	ullTotalPageFile        uint64
	ullAvailPageFile        uint64
	ullTotalVirtual         uint64
	ullAvailVirtual         uint64
	ullAvailExtendedVirtual uint64
}

// TotalSystemMemory returns the total physical memory on Windows via
// kernel32.GlobalMemoryStatusEx. Returns 0 if detection fails.
func TotalSystemMemory() int64 {
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	globalMemoryStatusEx := kernel32.NewProc("GlobalMemoryStatusEx")

	var ms memoryStatusEx
	ms.dwLength = uint32(unsafe.Sizeof(ms))

	ret, _, _ := globalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&ms)))
	if ret == 0 {
		return 0
	}

	return int64(ms.ullTotalPhys)
}
