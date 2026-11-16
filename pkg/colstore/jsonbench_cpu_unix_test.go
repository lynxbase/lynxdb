//go:build unix

package colstore

import (
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
