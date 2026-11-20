//go:build unix

package colstore

import (
	"runtime"
	"syscall"
	"time"
)

// cpuTime returns the user and system CPU time the process has used, which
// includes the garbage collector.
func cpuTime() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}

	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// peakRSS returns the most memory the process has held resident.
func peakRSS() uint64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	if runtime.GOOS == "darwin" {
		return uint64(ru.Maxrss)
	}

	return uint64(ru.Maxrss) << 10
}
