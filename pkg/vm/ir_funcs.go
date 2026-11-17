package vm

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"hash"
	"hash/crc32"
	"math"
	"math/rand/v2"
	"net"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ir"
)

// irEmitter compiles a call of one registered IR function.
type irEmitter func(c *irCompiler, call *ir.Call) error

// irFuncs maps every scalar function of the IR registry to its bytecode.
var irFuncs = map[string]irEmitter{}

func (c *irCompiler) call(n *ir.Call) error {
	emit, ok := irFuncs[n.Func.Name]
	if !ok {
		return fmt.Errorf("vm: function %s has no implementation", n.Func.Name)
	}
	return emit(c, n)
}

// natively compiles the arguments and calls f with all of them.
func natively(f NativeFunc) irEmitter {
	return func(c *irCompiler, n *ir.Call) error {
		if err := c.compileAll(n.Args); err != nil {
			return err
		}
		c.native(f, len(n.Args))
		return nil
	}
}

// strict wraps f so that any NULL argument yields NULL.
func strict(f NativeFunc) NativeFunc {
	return func(a []event.Value) event.Value {
		for _, v := range a {
			if v.IsNull() {
				return event.NullValue()
			}
		}
		return f(a)
	}
}

func opcode(op Opcode) irEmitter {
	return func(c *irCompiler, n *ir.Call) error {
		if err := c.compileAll(n.Args); err != nil {
			return err
		}
		c.prog.EmitOp(op)
		return nil
	}
}

// constStringArg returns the string literal argument i.
func constStringArg(n *ir.Call, i int) (string, bool) {
	if i >= len(n.Args) {
		return "", false
	}
	lit, ok := n.Args[i].(*ir.Literal)
	if !ok {
		return "", false
	}
	s, ok := lit.V.(string)
	return s, ok
}

func constIntArg(n *ir.Call, i int) (int64, bool) {
	if i >= len(n.Args) {
		return 0, false
	}
	lit, ok := n.Args[i].(*ir.Literal)
	if !ok {
		return 0, false
	}
	switch v := lit.V.(type) {
	case int64:
		return v, true
	case float64:
		return int64(v), true
	}
	return 0, false
}

func floatFn(f func(float64) float64) NativeFunc {
	return strict(func(a []event.Value) event.Value {
		x, ok := numFloat(a[0])
		if !ok {
			return event.NullValue()
		}
		r := f(x)
		if math.IsNaN(r) || math.IsInf(r, 0) {
			return event.NullValue()
		}
		return event.FloatValue(r)
	})
}

func strFn(f func(string) event.Value) NativeFunc {
	return strict(func(a []event.Value) event.Value {
		s, ok := str(a[0])
		if !ok {
			return event.NullValue()
		}
		return f(s)
	})
}

func init() {
	registerMathFuncs()
	registerStringFuncs()
	registerConditionFuncs()
	registerConversionFuncs()
	registerCryptoFuncs()
	registerCollectionFuncs()
	registerDatetimeFuncs()
	registerJSONFuncs()
	irFuncs["cidrmatch"] = natively(strict(cidrMatch))
	irFuncs["typeof"] = natively(func(a []event.Value) event.Value { return event.StringValue(typeName(a[0])) })
}

func typeName(v event.Value) string {
	switch v.Type() {
	case event.FieldTypeNull:
		return "null"
	case event.FieldTypeString:
		return "string"
	case event.FieldTypeInt:
		return "int"
	case event.FieldTypeFloat:
		return "float"
	case event.FieldTypeBool:
		return "bool"
	case event.FieldTypeTimestamp:
		return "timestamp"
	case event.FieldTypeDuration:
		return "duration"
	case event.FieldTypeArray:
		return "array"
	case event.FieldTypeObject:
		return "object"
	}
	return "unknown"
}

func cidrMatch(a []event.Value) event.Value {
	ipText, ok1 := str(a[0])
	cidr, ok2 := str(a[1])
	if !ok1 || !ok2 {
		return event.NullValue()
	}
	ip := net.ParseIP(strings.TrimSpace(ipText))
	_, block, err := net.ParseCIDR(strings.TrimSpace(cidr))
	if ip == nil || err != nil {
		return event.NullValue()
	}
	return event.BoolValue(block.Contains(ip))
}

func registerMathFuncs() {
	irFuncs["abs"] = natively(strict(func(a []event.Value) event.Value {
		if a[0].Type() == event.FieldTypeInt {
			n := a[0].AsInt()
			if n < 0 {
				n = -n
			}
			return event.IntValue(n)
		}
		f, ok := numFloat(a[0])
		if !ok {
			return event.NullValue()
		}
		return event.FloatValue(math.Abs(f))
	}))
	roundTo := func(f func(float64) float64) NativeFunc {
		return strict(func(a []event.Value) event.Value {
			if a[0].Type() == event.FieldTypeInt {
				return a[0]
			}
			x, ok := numFloat(a[0])
			if !ok || math.IsNaN(x) || math.IsInf(x, 0) {
				return event.NullValue()
			}
			return event.IntValue(toInt(f(x)))
		})
	}
	irFuncs["ceil"] = natively(roundTo(math.Ceil))
	irFuncs["floor"] = natively(roundTo(math.Floor))
	irFuncs["round"] = natively(strict(func(a []event.Value) event.Value {
		d := int64(0)
		if len(a) > 1 {
			var ok bool
			if d, ok = numInt(a[1]); !ok {
				return event.NullValue()
			}
		}
		if a[0].Type() == event.FieldTypeInt {
			if d >= 0 {
				return a[0]
			}
			p := math.Pow(10, float64(-d))
			return event.IntValue(int64(roundHalfAway(float64(a[0].AsInt())/p) * p))
		}
		x, ok := numFloat(a[0])
		if !ok {
			return event.NullValue()
		}
		p := math.Pow(10, float64(d))
		return event.FloatValue(roundHalfAway(x*p) / p)
	}))
	irFuncs["rint"] = natively(floatFn(math.RoundToEven))
	irFuncs["sqrt"] = natively(floatFn(math.Sqrt))
	irFuncs["cbrt"] = natively(floatFn(math.Cbrt))
	irFuncs["exp"] = natively(floatFn(math.Exp))
	irFuncs["expm1"] = natively(floatFn(math.Expm1))
	positive := func(f func(float64) float64) func(float64) float64 {
		return func(x float64) float64 {
			if x <= 0 {
				return math.NaN()
			}
			return f(x)
		}
	}
	irFuncs["ln"] = natively(floatFn(positive(math.Log)))
	irFuncs["log2"] = natively(floatFn(positive(math.Log2)))
	irFuncs["log10"] = natively(floatFn(positive(math.Log10)))
	irFuncs["log"] = natively(strict(func(a []event.Value) event.Value {
		x, ok := numFloat(a[len(a)-1])
		if !ok || x <= 0 {
			return event.NullValue()
		}
		if len(a) == 1 {
			return event.FloatValue(math.Log(x))
		}
		b, ok := numFloat(a[0])
		if !ok || b <= 0 || b == 1 {
			return event.NullValue()
		}
		return event.FloatValue(math.Log(x) / math.Log(b))
	}))
	irFuncs["pow"] = natively(strict(func(a []event.Value) event.Value {
		x, ok1 := numFloat(a[0])
		y, ok2 := numFloat(a[1])
		r := math.Pow(x, y)
		if !ok1 || !ok2 || math.IsNaN(r) || math.IsInf(r, 0) {
			return event.NullValue()
		}
		return event.FloatValue(r)
	}))
	irFuncs["mod"] = func(c *irCompiler, n *ir.Call) error {
		if err := c.compileAll(n.Args); err != nil {
			return err
		}
		c.prog.EmitOp(OpModStrict)
		return nil
	}
	irFuncs["sign"] = natively(strict(func(a []event.Value) event.Value {
		x, ok := numFloat(a[0])
		if !ok {
			return event.NullValue()
		}
		switch {
		case x > 0:
			return event.IntValue(1)
		case x < 0:
			return event.IntValue(-1)
		}
		return event.IntValue(0)
	}))
	for name, f := range map[string]func(float64) float64{
		"sin": math.Sin, "cos": math.Cos, "tan": math.Tan, "asin": math.Asin, "acos": math.Acos,
		"sinh": math.Sinh, "cosh": math.Cosh, "degrees": func(x float64) float64 { return x * 180 / math.Pi },
		"radians": func(x float64) float64 { return x * math.Pi / 180 }, "cot": func(x float64) float64 { return 1 / math.Tan(x) },
	} {
		irFuncs[name] = natively(floatFn(f))
	}
	atan := natively(strict(func(a []event.Value) event.Value {
		y, ok := numFloat(a[0])
		if !ok {
			return event.NullValue()
		}
		if len(a) == 1 {
			return event.FloatValue(math.Atan(y))
		}
		x, ok := numFloat(a[1])
		if !ok {
			return event.NullValue()
		}
		return event.FloatValue(math.Atan2(y, x))
	}))
	irFuncs["atan"] = atan
	irFuncs["atan2"] = atan
	irFuncs["pi"] = natively(func([]event.Value) event.Value { return event.FloatValue(math.Pi) })
	irFuncs["e"] = natively(func([]event.Value) event.Value { return event.FloatValue(math.E) })
	irFuncs["rand"] = func(c *irCompiler, n *ir.Call) error {
		var r *rand.Rand
		if seed, ok := constIntArg(n, 0); ok {
			r = rand.New(rand.NewPCG(uint64(seed), uint64(seed)))
		}
		c.native(func([]event.Value) event.Value {
			if r != nil {
				return event.FloatValue(r.Float64())
			}
			return event.FloatValue(rand.Float64())
		}, 0)
		return nil
	}
	irFuncs["conv"] = natively(strict(func(a []event.Value) event.Value {
		s, ok := str(a[0])
		from, ok1 := numInt(a[1])
		to, ok2 := numInt(a[2])
		if !ok || !ok1 || !ok2 || from < 2 || from > 36 || to < 2 || to > 36 {
			return event.NullValue()
		}
		n, err := strconv.ParseInt(strings.TrimSpace(s), int(from), 64)
		if err != nil {
			return event.NullValue()
		}
		return event.StringValue(strconv.FormatInt(n, int(to)))
	}))
	irFuncs["crc32"] = natively(strFn(func(s string) event.Value { return event.IntValue(int64(crc32.ChecksumIEEE([]byte(s)))) }))
	sumAvg := func(avg bool) NativeFunc {
		return strict(func(a []event.Value) event.Value {
			allInt := true
			var fsum float64
			var isum int64
			for _, v := range a {
				if v.Type() == event.FieldTypeInt {
					isum += v.AsInt()
					fsum += float64(v.AsInt())
					continue
				}
				f, ok := numFloat(v)
				if !ok {
					return event.NullValue()
				}
				allInt = false
				fsum += f
			}
			if avg {
				return event.FloatValue(fsum / float64(len(a)))
			}
			if allInt {
				return event.IntValue(isum)
			}
			return event.FloatValue(fsum)
		})
	}
	irFuncs["scalar_sum"] = natively(sumAvg(false))
	irFuncs["scalar_avg"] = natively(sumAvg(true))
	extreme := func(greater bool) NativeFunc {
		return func(a []event.Value) event.Value {
			best := event.NullValue()
			for _, v := range a {
				if v.IsNull() {
					continue
				}
				if best.IsNull() {
					best = v
					continue
				}
				x, y := numericPair(v, best)
				cmp, isNull := strictCompare(x, y, nil)
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
	irFuncs["greatest"] = natively(extreme(true))
	irFuncs["least"] = natively(extreme(false))
	irFuncs["num_bucket"] = natively(strict(func(a []event.Value) event.Value {
		if a[0].Type() == event.FieldTypeInt && a[1].Type() == event.FieldTypeInt && a[1].AsInt() > 0 {
			x, w := a[0].AsInt(), a[1].AsInt()
			q := x / w
			if x%w != 0 && x < 0 {
				q--
			}
			return event.IntValue(q * w)
		}
		x, ok1 := numFloat(a[0])
		w, ok2 := numFloat(a[1])
		if !ok1 || !ok2 || w <= 0 {
			return event.NullValue()
		}
		return event.FloatValue(math.Floor(x/w) * w)
	}))
}

func roundHalfAway(x float64) float64 {
	if x < 0 {
		return -math.Floor(-x + 0.5)
	}
	return math.Floor(x + 0.5)
}

func registerStringFuncs() {
	irFuncs["concat"] = natively(strict(func(a []event.Value) event.Value {
		var b strings.Builder
		for _, v := range a {
			s, _ := str(v)
			b.WriteString(s)
		}
		return event.StringValue(b.String())
	}))
	irFuncs["concat_ws"] = natively(func(a []event.Value) event.Value {
		sep, ok := str(a[0])
		if !ok {
			return event.NullValue()
		}
		parts := make([]string, 0, len(a)-1)
		for _, v := range a[1:] {
			if s, ok := str(v); ok {
				parts = append(parts, s)
			}
		}
		return event.StringValue(strings.Join(parts, sep))
	})
	irFuncs["length"] = natively(strFn(func(s string) event.Value { return event.IntValue(int64(utf8.RuneCountInString(s))) }))
	irFuncs["lower"] = natively(strFn(func(s string) event.Value { return event.StringValue(strings.ToLower(s)) }))
	irFuncs["upper"] = natively(strFn(func(s string) event.Value { return event.StringValue(strings.ToUpper(s)) }))
	irFuncs["trim"] = natively(strFn(func(s string) event.Value { return event.StringValue(strings.TrimSpace(s)) }))
	irFuncs["ltrim"] = natively(strFn(func(s string) event.Value { return event.StringValue(strings.TrimLeftFunc(s, unicode.IsSpace)) }))
	irFuncs["rtrim"] = natively(strFn(func(s string) event.Value { return event.StringValue(strings.TrimRightFunc(s, unicode.IsSpace)) }))
	irFuncs["reverse"] = natively(strFn(func(s string) event.Value {
		r := []rune(s)
		for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
			r[i], r[j] = r[j], r[i]
		}
		return event.StringValue(string(r))
	}))
	leftRight := func(left bool) NativeFunc {
		return strict(func(a []event.Value) event.Value {
			s, ok := str(a[0])
			n, ok2 := numInt(a[1])
			if !ok || !ok2 {
				return event.NullValue()
			}
			r := []rune(s)
			if n < 0 {
				n = 0
			}
			if int(n) > len(r) {
				n = int64(len(r))
			}
			if left {
				return event.StringValue(string(r[:n]))
			}
			return event.StringValue(string(r[len(r)-int(n):]))
		})
	}
	irFuncs["left"] = natively(leftRight(true))
	irFuncs["right"] = natively(leftRight(false))
	irFuncs["substring"] = natively(strict(func(a []event.Value) event.Value {
		s, ok := str(a[0])
		start, ok2 := numInt(a[1])
		if !ok || !ok2 {
			return event.NullValue()
		}
		r := []rune(s)
		var from int64
		switch {
		case start > 0:
			from = start - 1
		case start < 0:
			from = int64(len(r)) + start
		default:
			return event.StringValue("")
		}
		if from < 0 || from >= int64(len(r)) {
			return event.StringValue("")
		}
		to := int64(len(r))
		if len(a) > 2 {
			l, ok := numInt(a[2])
			if !ok {
				return event.NullValue()
			}
			if l < 0 {
				return event.StringValue("")
			}
			if from+l < to {
				to = from + l
			}
		}
		return event.StringValue(string(r[from:to]))
	}))
	irFuncs["locate"] = natively(strict(func(a []event.Value) event.Value {
		sub, ok := str(a[0])
		s, ok2 := str(a[1])
		if !ok || !ok2 {
			return event.NullValue()
		}
		r := []rune(s)
		start := int64(1)
		if len(a) > 2 {
			if start, ok = numInt(a[2]); !ok {
				return event.NullValue()
			}
		}
		if start < 1 || start > int64(len(r))+1 {
			return event.IntValue(0)
		}
		i := strings.Index(string(r[start-1:]), sub)
		if i < 0 {
			return event.IntValue(0)
		}
		return event.IntValue(start + int64(utf8.RuneCountInString(string(r[start-1:])[:i])))
	}))
	irFuncs["ascii"] = natively(strFn(func(s string) event.Value {
		r, ok := firstRune(s)
		if !ok {
			return event.IntValue(0)
		}
		return event.IntValue(int64(r))
	}))
	irFuncs["strcmp"] = natively(strict(func(a []event.Value) event.Value {
		x, ok := str(a[0])
		y, ok2 := str(a[1])
		if !ok || !ok2 {
			return event.NullValue()
		}
		return event.IntValue(int64(strings.Compare(x, y)))
	}))
	irFuncs["like"] = func(c *irCompiler, n *ir.Call) error {
		pattern, ok := constStringArg(n, 1)
		if !ok {
			return fmt.Errorf("vm: like needs a constant pattern")
		}
		cs := false
		if len(n.Args) > 2 {
			if lit, ok := n.Args[2].(*ir.Literal); ok {
				cs, _ = lit.V.(bool)
			}
		}
		re, err := regexp.Compile(likeToRegex(pattern, cs))
		if err != nil {
			return err
		}
		if err := c.compile(n.Args[0]); err != nil {
			return err
		}
		c.native(strFn(func(s string) event.Value { return event.BoolValue(re.MatchString(s)) }), 1)
		return nil
	}
	irFuncs["regexp_match"] = func(c *irCompiler, n *ir.Call) error {
		pattern, ok := constStringArg(n, 1)
		if !ok {
			return fmt.Errorf("vm: regexp_match needs a constant pattern")
		}
		f, err := regexMatcher(pattern, true)
		if err != nil {
			return err
		}
		if err := c.compile(n.Args[0]); err != nil {
			return err
		}
		c.native(strict(f), 1)
		return nil
	}
	irFuncs["regexp_replace"] = func(c *irCompiler, n *ir.Call) error {
		pattern, ok := constStringArg(n, 1)
		if !ok {
			return fmt.Errorf("vm: regexp_replace needs a constant pattern")
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return err
		}
		if err := c.compile(n.Args[0]); err != nil {
			return err
		}
		if err := c.compile(n.Args[2]); err != nil {
			return err
		}
		c.native(strict(func(a []event.Value) event.Value {
			s, ok := str(a[0])
			r, ok2 := str(a[1])
			if !ok || !ok2 {
				return event.NullValue()
			}
			return event.StringValue(re.ReplaceAllString(s, javaGroupRefs(r)))
		}), 2)
		return nil
	}
	irFuncs["regexp_extract"] = func(c *irCompiler, n *ir.Call) error {
		pattern, ok := constStringArg(n, 1)
		if !ok {
			return fmt.Errorf("vm: regexp_extract needs a constant pattern")
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return err
		}
		group, _ := constIntArg(n, 2)
		if err := c.compile(n.Args[0]); err != nil {
			return err
		}
		c.native(strFn(func(s string) event.Value {
			m := re.FindStringSubmatch(s)
			if m == nil || int(group) >= len(m) {
				return event.NullValue()
			}
			return event.StringValue(m[group])
		}), 1)
		return nil
	}
	irFuncs["contains"] = natively(strict(func(a []event.Value) event.Value {
		s, ok := str(a[0])
		sub, ok2 := str(a[1])
		if !ok || !ok2 {
			return event.NullValue()
		}
		return event.BoolValue(strings.Contains(s, sub))
	}))
	irFuncs["wildcard_replace"] = func(c *irCompiler, n *ir.Call) error {
		pattern, ok := constStringArg(n, 1)
		repl, ok2 := constStringArg(n, 2)
		if !ok || !ok2 {
			return fmt.Errorf("vm: wildcard_replace needs constant pattern and replacement")
		}
		f := wildcardReplacer(pattern, repl)
		if err := c.compile(n.Args[0]); err != nil {
			return err
		}
		c.native(f, 1)
		return nil
	}
}

// javaGroupRefs converts \1 group references to Go's ${1} form; $1 is
// already understood.
var backslashGroup = regexp.MustCompile(`\\(\d+)`)

func javaGroupRefs(r string) string {
	return backslashGroup.ReplaceAllString(r, `$${$1}`)
}

// likeToRegex converts a SQL LIKE pattern to an anchored regular expression.
func likeToRegex(p string, caseSensitive bool) string {
	var b strings.Builder
	if !caseSensitive {
		b.WriteString("(?i)")
	}
	b.WriteString("(?s)^")
	for i := 0; i < len(p); i++ {
		switch ch := p[i]; ch {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteString(".")
		case '\\':
			if i+1 < len(p) {
				i++
				b.WriteString(regexp.QuoteMeta(string(p[i])))
			} else {
				b.WriteString(`\\`)
			}
		default:
			r, size := utf8.DecodeRuneInString(p[i:])
			b.WriteString(regexp.QuoteMeta(string(r)))
			i += size - 1
		}
	}
	b.WriteString("$")
	return b.String()
}

// wildcardReplacer implements wildcard_replace: literal patterns replace
// every occurrence; '*' patterns match the whole value and carry the
// matched text into the replacement's '*'.
func wildcardReplacer(pattern, repl string) NativeFunc {
	type piece struct {
		text string
		wild bool
	}
	split := func(s string) []piece {
		var out []piece
		var cur strings.Builder
		for i := 0; i < len(s); i++ {
			switch {
			case s[i] == '\\' && i+1 < len(s) && (s[i+1] == '*' || s[i+1] == '\\'):
				cur.WriteByte(s[i+1])
				i++
			case s[i] == '*':
				out = append(out, piece{text: cur.String()})
				cur.Reset()
				out = append(out, piece{wild: true})
			default:
				cur.WriteByte(s[i])
			}
		}
		return append(out, piece{text: cur.String()})
	}
	pp, rp := split(pattern), split(repl)
	wild := false
	for _, p := range pp {
		wild = wild || p.wild
	}
	if !wild {
		lit := pp[0].text
		var rb strings.Builder
		for _, p := range rp {
			if p.wild {
				rb.WriteByte('*')
			} else {
				rb.WriteString(p.text)
			}
		}
		out := rb.String()
		return func(a []event.Value) event.Value {
			if a[0].IsNull() {
				return a[0]
			}
			s, ok := str(a[0])
			if !ok || lit == "" {
				return a[0]
			}
			return event.StringValue(strings.ReplaceAll(s, lit, out))
		}
	}
	var re strings.Builder
	re.WriteString("(?s)^")
	for _, p := range pp {
		if p.wild {
			re.WriteString("(.*?)")
		} else {
			re.WriteString(regexp.QuoteMeta(p.text))
		}
	}
	re.WriteString("$")
	rx := regexp.MustCompile(re.String())
	return func(a []event.Value) event.Value {
		if a[0].IsNull() {
			return a[0]
		}
		s, ok := str(a[0])
		if !ok {
			return a[0]
		}
		m := rx.FindStringSubmatch(s)
		if m == nil {
			return a[0]
		}
		var b strings.Builder
		g := 1
		for _, p := range rp {
			if p.wild {
				if g < len(m) {
					b.WriteString(m[g])
				}
				g++
				continue
			}
			b.WriteString(p.text)
		}
		return event.StringValue(b.String())
	}
}

func registerConditionFuncs() {
	irFuncs["is_null"] = opcode(OpIsNull)
	irFuncs["is_empty"] = natively(func(a []event.Value) event.Value {
		if a[0].IsNull() {
			return event.BoolValue(true)
		}
		s, _ := str(a[0])
		return event.BoolValue(a[0].Type() == event.FieldTypeString && s == "")
	})
	irFuncs["is_blank"] = natively(func(a []event.Value) event.Value {
		if a[0].IsNull() {
			return event.BoolValue(true)
		}
		return event.BoolValue(a[0].Type() == event.FieldTypeString && strings.TrimSpace(a[0].AsString()) == "")
	})
	irFuncs["coalesce"] = func(c *irCompiler, n *ir.Call) error {
		if err := c.compileAll(n.Args); err != nil {
			return err
		}
		c.prog.EmitOp(OpCoalesce, len(n.Args))
		return nil
	}
}

func registerConversionFuncs() {
	irFuncs["tostring"] = func(c *irCompiler, n *ir.Call) error {
		format, _ := constStringArg(n, 1)
		if err := c.compile(n.Args[0]); err != nil {
			return err
		}
		c.native(toStringFormat(strings.ToLower(format)), 1)
		return nil
	}
	irFuncs["tonumber"] = func(c *irCompiler, n *ir.Call) error {
		base := int64(10)
		if b, ok := constIntArg(n, 1); ok {
			base = b
		}
		if err := c.compile(n.Args[0]); err != nil {
			return err
		}
		c.native(strict(func(a []event.Value) event.Value {
			if a[0].Type() == event.FieldTypeInt || a[0].Type() == event.FieldTypeFloat {
				return a[0]
			}
			s, ok := str(a[0])
			if !ok {
				return event.NullValue()
			}
			if base == 10 {
				if v, ok := parseNumber(s); ok {
					return v
				}
				return event.NullValue()
			}
			if base < 2 || base > 36 {
				return event.NullValue()
			}
			v, err := strconv.ParseInt(strings.TrimSpace(s), int(base), 64)
			if err != nil {
				return event.NullValue()
			}
			return event.IntValue(v)
		}), 1)
		return nil
	}
	registerConvertCommandFuncs()
}

func toStringFormat(format string) NativeFunc {
	return strict(func(a []event.Value) event.Value {
		switch format {
		case "hex":
			if n, ok := numInt(a[0]); ok {
				return event.StringValue("0x" + strconv.FormatInt(n, 16))
			}
		case "binary":
			if n, ok := numInt(a[0]); ok {
				return event.StringValue(strconv.FormatInt(n, 2))
			}
		case "commas":
			if f, ok := numFloat(a[0]); ok {
				return event.StringValue(commas(f, a[0].Type() == event.FieldTypeInt))
			}
		case "duration":
			if n, ok := numInt(a[0]); ok {
				d := n
				sign := ""
				if d < 0 {
					sign, d = "-", -d
				}
				return event.StringValue(fmt.Sprintf("%s%02d:%02d:%02d", sign, d/3600, d%3600/60, d%60))
			}
		}
		if s, ok := str(a[0]); ok {
			return event.StringValue(s)
		}
		return event.NullValue()
	})
}

func commas(f float64, isInt bool) string {
	neg := f < 0
	if neg {
		f = -f
	}
	var intPart, frac string
	if isInt {
		intPart = strconv.FormatInt(int64(f), 10)
	} else {
		s := strconv.FormatFloat(f, 'f', 2, 64)
		intPart, frac, _ = strings.Cut(s, ".")
	}
	var b strings.Builder
	for i, r := range intPart {
		if i > 0 && (len(intPart)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(r)
	}
	out := b.String()
	if frac != "" {
		out += "." + frac
	}
	if neg {
		out = "-" + out
	}
	return out
}

func registerCryptoFuncs() {
	digest := func(h func() hash.Hash) irEmitter {
		return natively(strFn(func(s string) event.Value {
			d := h()
			d.Write([]byte(s))
			return event.StringValue(hex.EncodeToString(d.Sum(nil)))
		}))
	}
	irFuncs["md5"] = digest(md5.New)
	irFuncs["sha1"] = digest(sha1.New)
	irFuncs["sha2"] = func(c *irCompiler, n *ir.Call) error {
		bits, _ := constIntArg(n, 1)
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
			return fmt.Errorf("vm: sha2 bits must be 224, 256, 384 or 512")
		}
		return digest(h)(c, &ir.Call{Func: n.Func, Args: n.Args[:1], T: n.T})
	}
}

func registerCollectionFuncs() {
	irFuncs["array"] = func(c *irCompiler, n *ir.Call) error {
		if err := c.compileAll(n.Args); err != nil {
			return err
		}
		c.prog.EmitOp(OpArrayBuild, len(n.Args))
		return nil
	}
	length := func(a []event.Value) event.Value {
		if a[0].Type() != event.FieldTypeArray {
			return event.NullValue()
		}
		return event.IntValue(int64(len(a[0].AsArray())))
	}
	irFuncs["array_length"] = natively(length)
	lambdaOp := func(op Opcode, maxParams int) irEmitter {
		return func(c *irCompiler, n *ir.Call) error {
			if err := c.compile(n.Args[0]); err != nil {
				return err
			}
			l, ok := n.Args[1].(*ir.Lambda)
			if !ok || len(l.Params) < 1 || len(l.Params) > maxParams {
				if maxParams == 1 {
					return fmt.Errorf("vm: %s needs a one-parameter lambda", n.Func.Name)
				}
				return fmt.Errorf("vm: %s needs a lambda of one or %d parameters", n.Func.Name, maxParams)
			}
			idx, err := c.compileLambda(l)
			if err != nil {
				return err
			}
			c.prog.EmitOp(op, idx)
			return nil
		}
	}
	irFuncs["forall"] = lambdaOp(OpArrayAll, 1)
	irFuncs["exists"] = lambdaOp(OpArrayAny, 1)
	irFuncs["filter"] = lambdaOp(OpArrayFilter, 1)
	irFuncs["transform"] = lambdaOp(OpArrayMap, 2)
	irFuncs["reduce"] = func(c *irCompiler, n *ir.Call) error {
		if err := c.compile(n.Args[0]); err != nil {
			return err
		}
		if err := c.compile(n.Args[1]); err != nil {
			return err
		}
		l, ok := n.Args[2].(*ir.Lambda)
		if !ok || len(l.Params) != 2 {
			return fmt.Errorf("vm: reduce needs a two-parameter lambda")
		}
		idx, err := c.compileLambda(l)
		if err != nil {
			return err
		}
		c.prog.EmitOp(OpArrayReduce, idx)
		if len(n.Args) > 3 {
			fin, ok := n.Args[3].(*ir.Lambda)
			if !ok || len(fin.Params) != 1 {
				return fmt.Errorf("vm: the reduce finish function needs one parameter")
			}
			fidx, err := c.compileLambda(fin)
			if err != nil {
				return err
			}
			c.prog.EmitOp(OpArrayBuild, 1)
			c.prog.EmitOp(OpArrayMap, fidx)
			c.constant(event.IntValue(0))
			c.prog.EmitOp(OpIndex)
		}
		return nil
	}
	irFuncs["mvjoin"] = natively(strict(func(a []event.Value) event.Value {
		sep, ok := str(a[1])
		if !ok {
			return event.NullValue()
		}
		elems := []event.Value{a[0]}
		if a[0].Type() == event.FieldTypeArray {
			elems = a[0].AsArray()
		}
		parts := make([]string, 0, len(elems))
		for _, e := range elems {
			if s, ok := str(e); ok {
				parts = append(parts, s)
			}
		}
		return event.StringValue(strings.Join(parts, sep))
	}))
	irFuncs["mvappend"] = natively(func(a []event.Value) event.Value {
		var out []event.Value
		for _, v := range a {
			switch v.Type() {
			case event.FieldTypeNull:
			case event.FieldTypeArray:
				for _, e := range v.AsArray() {
					if !e.IsNull() {
						out = append(out, e)
					}
				}
			default:
				out = append(out, v)
			}
		}
		if out == nil {
			out = []event.Value{}
		}
		return event.ArrayValue(out)
	})
	irFuncs["split"] = natively(strict(func(a []event.Value) event.Value {
		s, ok := str(a[0])
		sep, ok2 := str(a[1])
		if !ok || !ok2 {
			return event.NullValue()
		}
		parts := strings.Split(s, sep)
		out := make([]event.Value, len(parts))
		for i, p := range parts {
			out[i] = event.StringValue(p)
		}
		return event.ArrayValue(out)
	}))
	irFuncs["mvdedup"] = natively(strict(func(a []event.Value) event.Value {
		if a[0].Type() != event.FieldTypeArray {
			return a[0]
		}
		return execArrayDistinct(a[0])
	}))
	irFuncs["mvfind"] = func(c *irCompiler, n *ir.Call) error {
		pattern, ok := constStringArg(n, 1)
		if !ok {
			return fmt.Errorf("vm: mvfind needs a constant regex")
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return err
		}
		if err := c.compile(n.Args[0]); err != nil {
			return err
		}
		c.native(strict(func(a []event.Value) event.Value {
			elems := []event.Value{a[0]}
			if a[0].Type() == event.FieldTypeArray {
				elems = a[0].AsArray()
			}
			for i, e := range elems {
				if s, ok := str(e); ok && re.MatchString(s) {
					return event.IntValue(int64(i))
				}
			}
			return event.NullValue()
		}), 1)
		return nil
	}
	irFuncs["mvindex"] = natively(strict(func(a []event.Value) event.Value {
		elems := []event.Value{a[0]}
		if a[0].Type() == event.FieldTypeArray {
			elems = a[0].AsArray()
		}
		n := int64(len(elems))
		norm := func(v event.Value) (int64, bool) {
			i, ok := numInt(v)
			if !ok {
				return 0, false
			}
			if i < 0 {
				i += n
			}
			return i, true
		}
		start, ok := norm(a[1])
		if !ok {
			return event.NullValue()
		}
		if len(a) == 2 {
			if start < 0 || start >= n {
				return event.NullValue()
			}
			return elems[start]
		}
		end, ok := norm(a[2])
		if !ok {
			return event.NullValue()
		}
		if start < 0 {
			start = 0
		}
		if end >= n {
			end = n - 1
		}
		if start > end {
			return event.NullValue()
		}
		return event.ArrayValue(append([]event.Value(nil), elems[start:end+1]...))
	}))
	irFuncs["mvzip"] = natively(strict(func(a []event.Value) event.Value {
		sep := ","
		if len(a) > 2 {
			s, ok := str(a[2])
			if !ok {
				return event.NullValue()
			}
			sep = s
		}
		asArr := func(v event.Value) []event.Value {
			if v.Type() == event.FieldTypeArray {
				return v.AsArray()
			}
			return []event.Value{v}
		}
		l, r := asArr(a[0]), asArr(a[1])
		n := min(len(l), len(r))
		out := make([]event.Value, n)
		for i := 0; i < n; i++ {
			x, _ := str(l[i])
			y, _ := str(r[i])
			out[i] = event.StringValue(x + sep + y)
		}
		return event.ArrayValue(out)
	}))
}
