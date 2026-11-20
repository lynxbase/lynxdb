//go:build !linux && !darwin

package colstore

// physicalMemory returns zero: the memory of the machine is unknown here.
func physicalMemory() uint64 { return 0 }
