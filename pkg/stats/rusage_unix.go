//go:build !windows

package stats

import (
	"syscall"
	"time"
)

// CPUSnapshot holds a point-in-time CPU usage snapshot from getrusage(2).
type CPUSnapshot struct {
	utime syscall.Timeval
	stime syscall.Timeval
}

// TakeCPUSnapshot captures the current process CPU usage.
func TakeCPUSnapshot() CPUSnapshot {
	var ru syscall.Rusage
	// RUSAGE_SELF includes all threads in the process.
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)

	return CPUSnapshot{utime: ru.Utime, stime: ru.Stime}
}

// ApplyCPUStats computes the CPU time delta between before and after snapshots
// and populates the QueryStats fields.
func ApplyCPUStats(st *QueryStats, before, after CPUSnapshot) {
	st.CPUTimeUser = tvDelta(before.utime, after.utime)
	st.CPUTimeSys = tvDelta(before.stime, after.stime)
}

func tvDelta(before, after syscall.Timeval) time.Duration {
	b := time.Duration(before.Sec)*time.Second + time.Duration(before.Usec)*time.Microsecond
	a := time.Duration(after.Sec)*time.Second + time.Duration(after.Usec)*time.Microsecond
	d := a - b
	if d < 0 {
		return 0
	}

	return d
}
