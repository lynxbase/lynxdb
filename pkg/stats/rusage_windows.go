//go:build windows

package stats

// CPUSnapshot is a no-op on Windows where getrusage is unavailable.
type CPUSnapshot struct{}

// TakeCPUSnapshot returns an empty snapshot on Windows.
func TakeCPUSnapshot() CPUSnapshot {
	return CPUSnapshot{}
}

// ApplyCPUStats is a no-op on Windows.
func ApplyCPUStats(_ *QueryStats, _, _ CPUSnapshot) {}
