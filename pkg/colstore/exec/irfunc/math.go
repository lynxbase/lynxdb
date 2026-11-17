package irfunc

import (
	"hash/crc32"
	"math"
	"strconv"
	"strings"
)

func floatFn(f func(float64) float64) impl {
	return strict(func(_ *ctx, a []val) val {
		x, ok := numFloat(a[0])
		if !ok {
			return null
		}
		r := f(x)
		if math.IsNaN(r) || math.IsInf(r, 0) {
			return null
		}
		return floatVal(r)
	})
}

func positive(f func(float64) float64) func(float64) float64 {
	return func(x float64) float64 {
		if x <= 0 {
			return math.NaN()
		}
		return f(x)
	}
}

func roundHalfAway(x float64) float64 {
	if x < 0 {
		return -math.Floor(-x + 0.5)
	}

	return math.Floor(x + 0.5)
}

func init() {
	fixed("abs", strict(func(_ *ctx, a []val) val {
		if a[0].k == kInt {
			n := a[0].n
			if n < 0 {
				n = -n
			}
			return intVal(n)
		}
		f, ok := numFloat(a[0])
		if !ok {
			return null
		}
		return floatVal(math.Abs(f))
	}))
	roundTo := func(f func(float64) float64) impl {
		return strict(func(_ *ctx, a []val) val {
			if a[0].k == kInt {
				return a[0]
			}
			x, ok := numFloat(a[0])
			if !ok || math.IsNaN(x) || math.IsInf(x, 0) {
				return null
			}
			return intVal(int64(f(x)))
		})
	}
	fixed("ceil", roundTo(math.Ceil))
	fixed("floor", roundTo(math.Floor))
	fixed("round", strict(func(_ *ctx, a []val) val {
		d := int64(0)
		if len(a) > 1 {
			var ok bool
			if d, ok = numInt(a[1]); !ok {
				return null
			}
		}
		if a[0].k == kInt {
			if d >= 0 {
				return a[0]
			}
			p := math.Pow(10, float64(-d))
			return intVal(int64(roundHalfAway(float64(a[0].n)/p) * p))
		}
		x, ok := numFloat(a[0])
		if !ok {
			return null
		}
		p := math.Pow(10, float64(d))
		return floatVal(roundHalfAway(x*p) / p)
	}))
	fixed("rint", floatFn(math.RoundToEven))
	fixed("sqrt", floatFn(math.Sqrt))
	fixed("cbrt", floatFn(math.Cbrt))
	fixed("exp", floatFn(math.Exp))
	fixed("expm1", floatFn(math.Expm1))
	fixed("ln", floatFn(positive(math.Log)))
	fixed("log2", floatFn(positive(math.Log2)))
	fixed("log10", floatFn(positive(math.Log10)))
	fixed("log", strict(func(_ *ctx, a []val) val {
		x, ok := numFloat(a[len(a)-1])
		if !ok || x <= 0 {
			return null
		}
		if len(a) == 1 {
			return floatVal(math.Log(x))
		}
		b, ok := numFloat(a[0])
		if !ok || b <= 0 || b == 1 {
			return null
		}
		return floatVal(math.Log(x) / math.Log(b))
	}))
	fixed("pow", strict(func(_ *ctx, a []val) val {
		x, ok1 := numFloat(a[0])
		y, ok2 := numFloat(a[1])
		r := math.Pow(x, y)
		if !ok1 || !ok2 || math.IsNaN(r) || math.IsInf(r, 0) {
			return null
		}
		return floatVal(r)
	}))
	fixed("mod", func(_ *ctx, a []val) val {
		x, y := numericPair(a[0], a[1])
		switch {
		case x.k == kInt && y.k == kInt:
			if y.n == 0 {
				return null
			}
			return intVal(x.n % y.n)
		case x.k == kDur && y.k == kDur:
			if y.n == 0 {
				return null
			}
			return durVal(x.dur() % y.dur())
		}
		return null
	})
	fixed("sign", strict(func(_ *ctx, a []val) val {
		x, ok := numFloat(a[0])
		if !ok {
			return null
		}
		switch {
		case x > 0:
			return intVal(1)
		case x < 0:
			return intVal(-1)
		}
		return intVal(0)
	}))
	for name, f := range map[string]func(float64) float64{
		"sin": math.Sin, "cos": math.Cos, "tan": math.Tan, "asin": math.Asin, "acos": math.Acos,
		"sinh": math.Sinh, "cosh": math.Cosh, "degrees": func(x float64) float64 { return x * 180 / math.Pi },
		"radians": func(x float64) float64 { return x * math.Pi / 180 }, "cot": func(x float64) float64 { return 1 / math.Tan(x) },
	} {
		fixed(name, floatFn(f))
	}
	atan := strict(func(_ *ctx, a []val) val {
		y, ok := numFloat(a[0])
		if !ok {
			return null
		}
		if len(a) == 1 {
			return floatVal(math.Atan(y))
		}
		x, ok := numFloat(a[1])
		if !ok {
			return null
		}
		return floatVal(math.Atan2(y, x))
	})
	fixed("atan", atan)
	fixed("atan2", atan)
	fixed("pi", func(*ctx, []val) val { return floatVal(math.Pi) })
	fixed("e", func(*ctx, []val) val { return floatVal(math.E) })
	fixed("conv", strict(func(c *ctx, a []val) val {
		s, ok := c.str(a[0])
		from, ok1 := numInt(a[1])
		to, ok2 := numInt(a[2])
		if !ok || !ok1 || !ok2 || from < 2 || from > 36 || to < 2 || to > 36 {
			return null
		}
		n, err := strconv.ParseInt(strings.TrimSpace(s), int(from), 64)
		if err != nil {
			return null
		}
		start := len(c.out)
		c.out = strconv.AppendInt(c.out, n, int(to))
		return c.result(start)
	}))
	fixed("crc32", strFn(func(_ *ctx, s string) val { return intVal(int64(crc32.ChecksumIEEE(stringBytes(s)))) }))
	sumAvg := func(avg bool) impl {
		return strict(func(_ *ctx, a []val) val {
			allInt := true
			var fsum float64
			var isum int64
			for _, v := range a {
				if v.k == kInt {
					isum += v.n
					fsum += float64(v.n)
					continue
				}
				f, ok := numFloat(v)
				if !ok {
					return null
				}
				allInt = false
				fsum += f
			}
			if avg {
				return floatVal(fsum / float64(len(a)))
			}
			if allInt {
				return intVal(isum)
			}
			return floatVal(fsum)
		})
	}
	fixed("scalar_sum", sumAvg(false))
	fixed("scalar_avg", sumAvg(true))
	extreme := func(greater bool) impl {
		return func(_ *ctx, a []val) val {
			best := null
			for _, v := range a {
				if v.k == kNull {
					continue
				}
				if best.k == kNull {
					best = v
					continue
				}
				x, y := numericPair(v, best)
				cmp, isNull := strictCompare(x, y)
				if isNull {
					continue
				}
				if (greater && cmp > 0) || (!greater && cmp < 0) {
					best = v
				}
			}
			return best
		}
	}
	fixed("greatest", extreme(true))
	fixed("least", extreme(false))
	fixed("num_bucket", strict(func(_ *ctx, a []val) val {
		if a[0].k == kInt && a[1].k == kInt && a[1].n > 0 {
			x, w := a[0].n, a[1].n
			q := x / w
			if x%w != 0 && x < 0 {
				q--
			}
			return intVal(q * w)
		}
		x, ok1 := numFloat(a[0])
		w, ok2 := numFloat(a[1])
		if !ok1 || !ok2 || w <= 0 {
			return null
		}
		return floatVal(math.Floor(x/w) * w)
	}))
}
