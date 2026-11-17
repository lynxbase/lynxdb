//go:build linux

package colstore

import (
	"os"

	"golang.org/x/sys/unix"
)

// startWriteback asks the kernel to begin writing n bytes at off of f to the
// device and does not wait for them: without it the device idles until a
// sync asks for the whole file at once.
func startWriteback(f *os.File, off, n int64) {
	_ = unix.SyncFileRange(int(f.Fd()), off, n, unix.SYNC_FILE_RANGE_WRITE)
}
