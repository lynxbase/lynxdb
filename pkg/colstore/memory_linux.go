//go:build linux

package colstore

import (
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// physicalMemory returns the bytes of memory the process may use: the
// machine's, or less when a cgroup sets a limit; zero when unknown.
func physicalMemory() uint64 {
	var info unix.Sysinfo_t
	if unix.Sysinfo(&info) != nil {
		return 0
	}
	total := uint64(info.Totalram) * uint64(info.Unit)
	if limit, err := os.ReadFile("/sys/fs/cgroup/memory.max"); err == nil {
		if n, err := strconv.ParseUint(strings.TrimSpace(string(limit)), 10, 64); err == nil && n > 0 {
			total = min(total, n)
		}
	}

	return total
}
