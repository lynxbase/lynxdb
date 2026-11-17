//go:build !unix

package exec_test

import "time"

// cpuTime reports no CPU time where the process cannot read its own.
func cpuTime() time.Duration { return 0 }
