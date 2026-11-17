package exec

import (
	"encoding/binary"
	"math/bits"

	"github.com/lynxbase/lynxdb/pkg/mem"
)

const (
	// heapMinShift is the size of the smallest block, 16 bytes, as a shift.
	heapMinShift = 4
	heapClasses  = 28
	// heapHeader holds the length of a block's state.
	heapHeader   = 4
	heapChunk    = 4 << 10
	heapMaxChunk = 1 << 20
)

// stateRef addresses a state of a stateHeap: the size class of its block in
// the top byte, then the chunk number plus one, then the offset. Zero is no
// state.
type stateRef uint64

// stateHeap stores the states of groups that vary in size: byte strings that
// grow, shrink and go away one by one. Blocks come from an arena in powers
// of two, and a block that is released or outgrown serves the next state of
// its size, so the heap holds at most twice the bytes of the live states
// plus the blocks waiting for reuse. A state has no pointers into the heap,
// which lets it be copied into a spill record and merged from one as it is.
type stateHeap struct {
	arena  *mem.Arena
	chunks [][]byte
	// cur is the chunk that new blocks are cut from, at offset at.
	cur, at int
	free    [heapClasses]stateRef
}

func (h *stateHeap) init(a *mem.Arena) { h.arena = a }

// reset drops every state and keeps the chunks for the next ones.
func (h *stateHeap) reset() {
	h.cur, h.at = 0, 0
	clear(h.free[:])
}

func (h *stateHeap) block(ref stateRef) (b []byte, size int) {
	c := h.chunks[int(ref>>32&0xffffff)-1]
	off := int(uint32(ref))
	size = 1 << (uint(ref>>56) + heapMinShift)

	return c[off : off+size : off+size], size
}

// get returns the bytes of a state, with the capacity of its block.
func (h *stateHeap) get(ref stateRef) []byte {
	if ref == 0 {
		return nil
	}
	b, _ := h.block(ref)

	return b[heapHeader : heapHeader+int(binary.LittleEndian.Uint32(b))]
}

// alloc returns a state of n bytes whose content is undefined.
func (h *stateHeap) alloc(n int) (stateRef, []byte) {
	class := 0
	if need := n + heapHeader; need > 1<<heapMinShift {
		class = bits.Len(uint(need-1)) - heapMinShift
	}
	size := 1 << (class + heapMinShift)
	ref := h.free[class]
	var b []byte
	if ref != 0 {
		b, _ = h.block(ref)
		h.free[class] = stateRef(binary.LittleEndian.Uint64(b))
	} else {
		if len(h.chunks) == 0 || h.at+size > len(h.chunks[h.cur]) {
			h.nextChunk(size)
		}
		ref = stateRef(class)<<56 | stateRef(h.cur+1)<<32 | stateRef(h.at)
		b = h.chunks[h.cur][h.at : h.at+size : h.at+size]
		h.at += size
	}
	binary.LittleEndian.PutUint32(b, uint32(n))

	return ref, b[heapHeader : heapHeader+n]
}

// nextChunk makes the current chunk one with room for size bytes.
func (h *stateHeap) nextChunk(size int) {
	if len(h.chunks) != 0 {
		h.cur++
	}
	h.at = 0
	for i := h.cur; i < len(h.chunks); i++ {
		if len(h.chunks[i]) >= size {
			h.chunks[h.cur], h.chunks[i] = h.chunks[i], h.chunks[h.cur]

			return
		}
	}
	n := min(heapChunk<<min(len(h.chunks), 8), heapMaxChunk)
	h.chunks = append(h.chunks, mem.Raw[byte](h.arena, max(n, size), max(n, size)))
	last := len(h.chunks) - 1
	h.chunks[h.cur], h.chunks[last] = h.chunks[last], h.chunks[h.cur]
}

// resize returns the state with n bytes, of which it keeps the first ones;
// the ref changes when the state moves to a larger block.
func (h *stateHeap) resize(ref stateRef, n int) (stateRef, []byte) {
	if ref == 0 {
		return h.alloc(n)
	}
	b, size := h.block(ref)
	if n+heapHeader <= size {
		binary.LittleEndian.PutUint32(b, uint32(n))

		return ref, b[heapHeader : heapHeader+n]
	}
	old := b[heapHeader : heapHeader+int(binary.LittleEndian.Uint32(b))]
	// A state that grows a little at a time moves a logarithmic number of
	// times.
	grown, out := h.alloc(max(n, 2*len(old)))
	copy(out, old)
	h.release(ref)
	b, _ = h.block(grown)
	binary.LittleEndian.PutUint32(b, uint32(n))

	return grown, b[heapHeader : heapHeader+n]
}

// set makes v the content of a state.
func (h *stateHeap) set(ref stateRef, v []byte) stateRef {
	if ref != 0 {
		if b, size := h.block(ref); len(v)+heapHeader <= size {
			binary.LittleEndian.PutUint32(b, uint32(len(v)))
			copy(b[heapHeader:], v)

			return ref
		}
		h.release(ref)
	}
	ref, b := h.alloc(len(v))
	copy(b, v)

	return ref
}

// release hands the block of a state to the next one of its size.
func (h *stateHeap) release(ref stateRef) {
	if ref == 0 {
		return
	}
	b, _ := h.block(ref)
	class := int(ref >> 56)
	binary.LittleEndian.PutUint64(b, uint64(h.free[class]))
	h.free[class] = ref
}
