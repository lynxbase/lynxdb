//go:build !unix

package colstore

import "time"

// cpuTime reports no CPU time where the process cannot read its own.
func cpuTime() time.Duration { return 0 }

// peakRSS reports nothing where the process cannot read its own.
func peakRSS() uint64 { return 0 }
