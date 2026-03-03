//go:build linux

package stats

import (
	"os"
	"strconv"
	"strings"
	"syscall"
)

// unlimitedCgroupSentinel is the threshold above which cgroup memory values
// are treated as "unlimited" (kernel uses maxint-like values as sentinel).
const unlimitedCgroupSentinel int64 = 1 << 62

// TotalSystemMemory returns the effective memory limit on Linux.
// It checks cgroup v2 first, then cgroup v1, then falls back to syscall.Sysinfo.
// Returns 0 if detection fails.
func TotalSystemMemory() int64 {
	// cgroup v2: /sys/fs/cgroup/memory.max
	if v := readCgroupInt("/sys/fs/cgroup/memory.max"); v > 0 && v < unlimitedCgroupSentinel {
		return v
	}

	// cgroup v1: /sys/fs/cgroup/memory/memory.limit_in_bytes
	if v := readCgroupInt("/sys/fs/cgroup/memory/memory.limit_in_bytes"); v > 0 && v < unlimitedCgroupSentinel {
		return v
	}

	// Fallback: syscall.Sysinfo
	var info syscall.Sysinfo_t
	if err := syscall.Sysinfo(&info); err != nil {
		return 0
	}

	return int64(info.Totalram) * int64(info.Unit)
}

// readCgroupInt reads a cgroup file and parses the first line as an int64.
// Returns 0 on any failure (file not found, parse error, "max" sentinel).
func readCgroupInt(path string) int64 {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}

	s := strings.TrimSpace(string(data))
	if s == "" || s == "max" {
		return 0
	}

	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}

	return v
}
