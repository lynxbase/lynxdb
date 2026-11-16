//go:build !goexperiment.arenas

package mem

import (
	"sync"
	"unsafe"
)

const slabSize = 1 << 20

var slabPool = sync.Pool{New: func() any {
	// []uint64 keeps every carved slice 8-byte aligned.
	s := make([]uint64, slabSize/8)

	return &s
}}

// Arena releases everything allocated from it in one call and must not be
// shared between goroutines.
type Arena struct {
	slabs []*[]uint64
	large [][]uint64
	off   int // byte offset into the last slab
	size  int
}

// NewArena returns an empty arena backed by pooled slabs.
func NewArena() *Arena {
	return &Arena{off: slabSize}
}

// Slice returns a zeroed slice owned by a.
func Slice[T Scalar](a *Arena, length, capacity int) []T {
	var zero T
	n := capacity * sizeOf(zero)
	if n == 0 {
		return nil
	}
	a.size += n
	p := a.alloc(n)

	return unsafe.Slice((*T)(p), capacity)[:length]
}

func (a *Arena) alloc(n int) unsafe.Pointer {
	n = (n + 7) &^ 7
	if n > slabSize/4 {
		buf := make([]uint64, n/8)
		a.large = append(a.large, buf)

		return unsafe.Pointer(unsafe.SliceData(buf))
	}
	if a.off+n > slabSize {
		a.slabs = append(a.slabs, slabPool.Get().(*[]uint64))
		a.off = 0
	}
	slab := *a.slabs[len(a.slabs)-1]
	words := slab[a.off/8 : (a.off+n)/8]
	clear(words)
	a.off += n

	return unsafe.Pointer(unsafe.SliceData(words))
}

// Free releases every allocation; they must not be used afterwards.
func (a *Arena) Free() {
	for _, s := range a.slabs {
		slabPool.Put(s)
	}
	clear(a.slabs)
	a.slabs = a.slabs[:0]
	clear(a.large)
	a.large = a.large[:0]
	a.off = slabSize
	a.size = 0
}

// Reset frees every allocation and makes a usable again.
func (a *Arena) Reset() { a.Free() }

// Size reports the bytes handed out since the last Reset.
func (a *Arena) Size() int { return a.size }

// RuntimeBacked reports whether allocations bypass the garbage collector.
const RuntimeBacked = false
