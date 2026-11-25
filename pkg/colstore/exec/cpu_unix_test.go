//go:build unix

package exec_test

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

// pageFaults returns the page faults the process has taken, those served
// from memory and those that read a device.
func pageFaults() int64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}

	return int64(ru.Minflt) + int64(ru.Majflt)
}
