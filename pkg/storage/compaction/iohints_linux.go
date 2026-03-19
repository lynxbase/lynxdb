//go:build linux

package compaction

import "golang.org/x/sys/unix"

// AdviseSequential hints the OS to read-ahead aggressively for compaction.
func AdviseSequential(fd uintptr) {
	_ = unix.Fadvise(int(fd), 0, 0, unix.FADV_SEQUENTIAL)
}

// AdviseDontNeed hints the OS to drop pages from the page cache after compaction
// read, preventing cold compaction data from polluting the cache used by queries.
func AdviseDontNeed(fd uintptr) {
	_ = unix.Fadvise(int(fd), 0, 0, unix.FADV_DONTNEED)
}
