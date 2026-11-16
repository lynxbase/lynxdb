package part

import (
	"math/bits"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

// spread copies src, which holds one value per set bit of valid, to the
// positions of those bits in dst.
func spread[T any](dst, src []T, valid vec.Bitmap) {
	k := 0
	for wi, w := range valid {
		base := wi << 6
		if w == ^uint64(0) {
			copy(dst[base:base+64], src[k:k+64])
			k += 64

			continue
		}
		for w != 0 {
			start := bits.TrailingZeros64(w)
			run := bits.TrailingZeros64(^(w >> uint(start)))
			copy(dst[base+start:base+start+run], src[k:k+run])
			k += run
			if start+run == 64 {
				break
			}
			w &^= 1<<uint(start+run) - 1
		}
	}
}
