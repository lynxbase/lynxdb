package irfunc

import (
	"net"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/ir"
)

var typeNames = [...]string{
	kNull: "null", kStr: "string", kInt: "int", kFloat: "float", kBool: "bool",
	kTime: "timestamp", kDur: "duration", kArray: "array", kObject: "object",
}

func init() {
	fixed("is_null", func(_ *ctx, a []val) val { return boolVal(a[0].k == kNull) })
	fixed("is_empty", func(c *ctx, a []val) val {
		if a[0].k == kNull {
			return boolVal(true)
		}
		return boolVal(a[0].k == kStr && a[0].s == "")
	})
	fixed("is_blank", func(_ *ctx, a []val) val {
		if a[0].k == kNull {
			return boolVal(true)
		}
		return boolVal(a[0].k == kStr && strings.TrimSpace(a[0].s) == "")
	})
	fixed("coalesce", func(_ *ctx, a []val) val {
		for _, v := range a {
			if v.k != kNull {
				return v
			}
		}
		return null
	})
	fixed("typeof", func(_ *ctx, a []val) val { return strVal(typeNames[a[0].k]) })
	def("cidrmatch", func(*ir.Call) (func() impl, error) {
		return func() impl {
			var lastText string
			var last *net.IPNet
			return strict(func(c *ctx, a []val) val {
				ipText, ok1 := c.str(a[0])
				cidr, ok2 := c.str(a[1])
				if !ok1 || !ok2 {
					return null
				}
				ip := net.ParseIP(strings.TrimSpace(ipText))
				if cidr != lastText || last == nil {
					_, block, err := net.ParseCIDR(strings.TrimSpace(cidr))
					if err != nil {
						return null
					}
					lastText, last = strings.Clone(cidr), block
				}
				if ip == nil {
					return null
				}
				return boolVal(last.Contains(ip))
			})
		}, nil
	})
}
