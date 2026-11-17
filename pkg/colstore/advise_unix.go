//go:build unix

package colstore

import (
	"os"

	"golang.org/x/sys/unix"
)

var pageSize = os.Getpagesize()

// adviseRandom tells the kernel that a mapped part is read at scattered
// places, so that a page fault brings in the page asked for and not a window
// of neighbours that nobody reads.
func adviseRandom(data []byte) {
	if len(data) > 0 {
		_ = unix.Madvise(data, unix.MADV_RANDOM)
	}
}

// adviseWillNeed asks for n bytes at off of a mapped part in one request,
// ahead of the faults that reading them page by page would take.
func adviseWillNeed(data []byte, off, n int) {
	start := off &^ (pageSize - 1)
	end := min(off+n, len(data))
	if start < end {
		_ = unix.Madvise(data[start:end], unix.MADV_WILLNEED)
	}
}

// adviseDontNeed lets go of the resident pages of a mapped part; they are
// read from the file again when touched.
func adviseDontNeed(data []byte) {
	if len(data) > 0 {
		_ = unix.Madvise(data, unix.MADV_DONTNEED)
	}
}
