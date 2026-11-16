package mem

import "unsafe"

func sizeOf[T any](v T) int { return int(unsafe.Sizeof(v)) }
