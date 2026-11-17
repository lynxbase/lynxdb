package exec

import (
	"encoding/binary"
	"math"
)

// condition returns bytes that two filters share only when they pass the
// same rows, or false for a filter with a function, which has no name.
func condition(dst []byte, e *Expr) ([]byte, bool) {
	if e.Op == OpFunc {
		return dst, false
	}
	dst = append(dst, byte(e.Op), byte(e.Kind), byte(e.Value.Kind), 0)
	if e.Value.Bool {
		dst[len(dst)-1] = 1
	}
	dst = binary.LittleEndian.AppendUint64(dst, uint64(e.Value.Int))
	dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(e.Value.Float))
	dst = binary.AppendUvarint(dst, uint64(len(e.Path)))
	dst = binary.AppendUvarint(append(dst, e.Path...), uint64(len(e.Value.Bytes)))
	dst = e.Pattern.AppendKey(append(dst, e.Value.Bytes...))
	dst = binary.AppendUvarint(dst, uint64(len(e.Args)))
	for _, a := range e.Args {
		var ok bool
		if dst, ok = condition(dst, a); !ok {
			return dst, false
		}
	}

	return dst, true
}
