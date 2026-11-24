package mem

import (
	"math/bits"
	"sync"
	"unsafe"
)

// Sized pools slices whose capacity is a power of two, a pool per capacity,
// for buffers that grow by doubling: the buffer that one user outgrows is
// the next one of another, which an arena would hold until it is freed.
type Sized[T Scalar] struct {
	classes [bits.UintSize]sync.Pool
}

// Get returns a slice of length n whose capacity is the least power of two
// that holds it and whose elements hold unspecified values.
func (p *Sized[T]) Get(n int) []T {
	if n <= 0 {
		return nil
	}
	class := bits.Len(uint(n - 1))
	// A pointer goes into the pool's interface without an allocation, which
	// a slice does not.
	if v, ok := p.classes[class].Get().(*T); ok {
		return unsafe.Slice(v, 1<<class)[:n]
	}

	return make([]T, n, 1<<class)
}

// Put takes back a slice that Get returned; the caller must not use it
// afterwards. Slices of any other capacity are left to the collector.
func (p *Sized[T]) Put(s []T) {
	c := cap(s)
	if c == 0 || c&(c-1) != 0 {
		return
	}
	p.classes[bits.Len(uint(c-1))].Put(unsafe.SliceData(s[:c]))
}

// Grow returns s with capacity for at least need more elements, moving it to
// a larger slice of the pool when it has to, and giving the old one back.
func (p *Sized[T]) Grow(s []T, need int) []T {
	if cap(s)-len(s) >= need {
		return s
	}
	out := p.Get(max(len(s)+need, 2*cap(s)))[:len(s)]
	copy(out, s)
	p.Put(s)

	return out
}
