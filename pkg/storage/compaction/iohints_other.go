//go:build !linux

package compaction

// AdviseSequential is a no-op on non-Linux platforms where fadvise is not available.
func AdviseSequential(_ uintptr) {}

// AdviseDontNeed is a no-op on non-Linux platforms where fadvise is not available.
func AdviseDontNeed(_ uintptr) {}
