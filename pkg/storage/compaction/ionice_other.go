//go:build !linux

package compaction

// SetCompactionIOPriority is a no-op on non-Linux platforms where ioprio_set
// is not available.
func SetCompactionIOPriority() {}
