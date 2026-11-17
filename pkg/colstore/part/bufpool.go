package part

import (
	"math/bits"

	"github.com/lynxbase/lynxdb/pkg/mem"
)

// Buffers for decoded string data are pooled by capacity, four classes to
// the octave, so that a buffer one block gives back fits a column of
// another block or another merge.
const (
	minBufShift = 8
	bufOctaves  = 23 // up to maxSectionSize
)

type byteBuf struct{ b []byte }

var bufPools [4*bufOctaves + 1]mem.Pool[byteBuf]

// bufClass returns the class of the smallest capacity that holds n bytes.
func bufClass(n int) int {
	if n <= 1<<minBufShift {
		return 0
	}
	e := bits.Len(uint(n - 1))
	quarter := (n - 1<<(e-1) + 1<<(e-3) - 1) >> (e - 3)

	return 4*(e-1-minBufShift) + quarter
}

// bufSize returns the capacity of the buffers of a class.
func bufSize(class int) int {
	octave, quarter := class/4, class%4

	return (4 + quarter) << (minBufShift + octave - 2)
}

// getBuf returns an empty buffer with room for n bytes.
func getBuf(n int) *byteBuf {
	class := bufClass(n)
	if class >= len(bufPools) {
		return &byteBuf{b: make([]byte, 0, n)}
	}
	buf := bufPools[class].Get()
	if cap(buf.b) < n {
		buf.b = make([]byte, 0, bufSize(class))
	}

	return buf
}

// putBuf gives buf back; the caller must not use it afterwards.
func putBuf(buf *byteBuf) {
	if class := bufClass(cap(buf.b)); class < len(bufPools) && bufSize(class) == cap(buf.b) {
		bufPools[class].Put(buf)
	}
}
