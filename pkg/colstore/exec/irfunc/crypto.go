package irfunc

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"hash"

	"github.com/lynxbase/lynxdb/pkg/ir"
)

// digest writes the hex digest of the text of its argument, reusing one
// hash per function.
func digest(newHash func() hash.Hash) func() impl {
	return func() impl {
		h := newHash()
		var sum []byte
		return strFn(func(c *ctx, s string) val {
			h.Reset()
			h.Write(stringBytes(s))
			sum = h.Sum(sum[:0])
			start := len(c.out)
			c.out = hex.AppendEncode(c.out, sum)
			return c.result(start)
		})
	}
}

func init() {
	def("md5", func(*ir.Call) (func() impl, error) { return digest(md5.New), nil })
	def("sha1", func(*ir.Call) (func() impl, error) { return digest(sha1.New), nil })
	def("sha2", func(call *ir.Call) (func() impl, error) {
		bits, _ := constInt(call, 1)
		var h func() hash.Hash
		switch bits {
		case 224:
			h = sha256.New224
		case 0, 256:
			h = sha256.New
		case 384:
			h = sha512.New384
		case 512:
			h = sha512.New
		default:
			return nil, errorf("sha2 bits must be 224, 256, 384 or 512")
		}
		mk := digest(h)
		return func() impl { return first(mk()) }, nil
	})
}
