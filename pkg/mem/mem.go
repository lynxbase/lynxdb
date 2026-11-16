// Package mem provides region allocators and typed object pools for memory
// whose lifetime is a query, a request or a block.
package mem

import "sync"

// Scalar is a pointer-free element type that an Arena can hand out in bulk.
type Scalar interface {
	~int8 | ~int16 | ~int32 | ~int64 | ~int |
		~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uint |
		~float32 | ~float64
}

// Grow returns s with capacity for at least need more elements, moving it
// inside a when it has to be reallocated.
func Grow[T Scalar](a *Arena, s []T, need int) []T {
	if cap(s)-len(s) >= need {
		return s
	}
	newCap := 2 * cap(s)
	if newCap < len(s)+need {
		newCap = len(s) + need
	}
	if newCap < 16 {
		newCap = 16
	}
	out := Raw[T](a, len(s), newCap)
	copy(out, s)

	return out
}

// CopyBytes stores a copy of b in a.
func (a *Arena) CopyBytes(b []byte) []byte {
	out := Raw[byte](a, len(b), len(b))
	copy(out, b)

	return out
}

// Pool is a typed sync.Pool that stores pointers, so Put does not allocate.
type Pool[T any] struct {
	p sync.Pool
	// Reset prepares v for reuse and reports whether it is worth keeping.
	Reset func(v *T) bool
}

// Get returns a pooled value or a zero one.
func (p *Pool[T]) Get() *T {
	if v, ok := p.p.Get().(*T); ok {
		return v
	}

	return new(T)
}

// Put returns v to the pool unless Reset rejects it.
func (p *Pool[T]) Put(v *T) {
	if v == nil {
		return
	}
	if p.Reset != nil && !p.Reset(v) {
		return
	}
	p.p.Put(v)
}
