//go:build goexperiment.simd && go1.27

package vec

import (
	"simd"
	"unsafe"
)

// SIMD reports whether the wide kernels use hardware vectors.
const SIMD = true

func asInt8(s []uint8) []int8 {
	return unsafe.Slice((*int8)(unsafe.Pointer(unsafe.SliceData(s))), len(s))
}

func maskEq8Wide(dst, src []uint8, c uint8) int {
	bc := simd.BroadcastUint8s(c)
	width := bc.Len()
	out := asInt8(dst)
	i := 0
	for ; i+width <= len(src); i += width {
		simd.LoadUint8s(src[i:]).Equal(bc).ToInt8s().Store(out[i:])
	}

	return i
}

func maskAndWide(dst, a, b []uint8) int {
	var probe simd.Uint8s
	width := probe.Len()
	i := 0
	for ; i+width <= len(a); i += width {
		simd.LoadUint8s(a[i:]).And(simd.LoadUint8s(b[i:])).Store(dst[i:])
	}

	return i
}

func maskCountWide(mask []uint8) (count, consumed int) {
	var probe simd.Int8s
	width := probe.Len()
	in := asInt8(mask)
	var lanes [64]int8
	i := 0
	for i+width <= len(in) {
		// Each vector adds at most 1 to a lane, so 127 of them cannot overflow it.
		steps := (len(in) - i) / width
		if steps > 127 {
			steps = 127
		}
		var acc simd.Int8s
		for s := 0; s < steps; s++ {
			acc = acc.Sub(simd.LoadInt8s(in[i:]))
			i += width
		}
		acc.Store(lanes[:])
		for _, v := range lanes[:width] {
			count += int(v)
		}
	}

	return count, i
}
