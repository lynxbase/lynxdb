//go:build goexperiment.arenas

package mem

import "arena"

// Arena releases everything allocated from it in one call and must not be
// shared between goroutines.
type Arena struct {
	a    *arena.Arena
	size int
}

// NewArena returns an empty arena backed by the runtime arena allocator.
func NewArena() *Arena {
	return &Arena{a: arena.NewArena()}
}

// Slice returns a zeroed slice owned by a.
func Slice[T Scalar](a *Arena, length, capacity int) []T {
	var zero T
	a.size += capacity * sizeOf(zero)

	return arena.MakeSlice[T](a.a, length, capacity)
}

// Raw returns a slice owned by a whose elements hold unspecified values.
func Raw[T Scalar](a *Arena, length, capacity int) []T {
	return Slice[T](a, length, capacity)
}

// Free releases every allocation; using them afterwards faults.
func (a *Arena) Free() {
	if a.a != nil {
		a.a.Free()
		a.a = nil
	}
	a.size = 0
}

// Reset frees every allocation and makes a usable again.
func (a *Arena) Reset() {
	a.Free()
	a.a = arena.NewArena()
}

// Size reports the bytes handed out since the last Reset.
func (a *Arena) Size() int { return a.size }

// RuntimeBacked reports whether allocations bypass the garbage collector.
const RuntimeBacked = true
