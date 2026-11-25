//go:build !unix

package exec_test

import "time"

// cpuTime reports no CPU time where the process cannot read its own.
func cpuTime() time.Duration { return 0 }

// pageFaults reports no page faults where the process cannot read its own.
func pageFaults() int64 { return 0 }
