//go:build !linux

package colstore

import "os"

// startWriteback does nothing where the system writes on its own schedule.
func startWriteback(*os.File, int64, int64) {}
