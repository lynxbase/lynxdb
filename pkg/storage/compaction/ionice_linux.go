//go:build linux

package compaction

import (
	"runtime"
	"syscall"
)

const (
	sysIOPrioSet      = 289 // __NR_ioprio_set on x86_64/arm64
	ioprioWhoProcess  = 1
	ioprioClassShift  = 13
	ioprioClassIdle   = 3
	ioprioIdlePrioBE7 = (ioprioClassIdle << ioprioClassShift) | 0
)

// SetCompactionIOPriority pins the current goroutine to its OS thread and
// sets IOPRIO_CLASS_IDLE via ioprio_set(2). This ensures compaction I/O
// yields to query I/O on Linux with CFQ/BFQ schedulers.
//
// The caller MUST NOT call runtime.UnlockOSThread afterwards — the goroutine
// must remain pinned for the ioprio to stay effective.
func SetCompactionIOPriority() {
	runtime.LockOSThread()
	// ioprio_set(IOPRIO_WHO_PROCESS, 0 /* self */, IOPRIO_CLASS_IDLE|0)
	syscall.Syscall(uintptr(sysIOPrioSet), ioprioWhoProcess, 0, ioprioIdlePrioBE7) //nolint:errcheck
}
