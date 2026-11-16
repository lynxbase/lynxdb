//go:build !(goexperiment.simd && go1.27)

package vec

// SIMD reports whether the wide kernels use hardware vectors.
const SIMD = false

func maskEq8Wide(_, _ []uint8, _ uint8) int { return 0 }

func maskAndWide(_, _, _ []uint8) int { return 0 }

func maskCountWide(_ []uint8) (count, consumed int) { return 0, 0 }
