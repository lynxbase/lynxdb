//go:build darwin

package colstore

import "golang.org/x/sys/unix"

// physicalMemory returns the bytes of memory of the machine, zero when
// unknown.
func physicalMemory() uint64 {
	n, err := unix.SysctlUint64("hw.memsize")
	if err != nil {
		return 0
	}

	return n
}
