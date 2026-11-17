package irfunc

import (
	"regexp"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/lynxbase/lynxdb/pkg/ir"
)

func strFn(f func(c *ctx, s string) val) impl {
	return strict(func(c *ctx, a []val) val {
		s, ok := c.str(a[0])
		if !ok {
			return null
		}
		return f(c, s)
	})
}

// mapRunes writes s with every character mapped, as strings.Map does: an
// invalid byte becomes U+FFFD.
func mapRunes(dst []byte, s string, f func(rune) rune) []byte {
	for i := 0; i < len(s); i++ {
		if s[i] >= utf8.RuneSelf {
			for _, r := range s[i:] {
				dst = utf8.AppendRune(dst, f(r))
			}
			return dst
		}
		dst = append(dst, byte(f(rune(s[i]))))
	}

	return dst
}

// changeCase writes s in lower or upper case, ASCII by arithmetic.
func changeCase(dst []byte, s string, upper bool) []byte {
	from, f := byte('A'), unicode.ToLower
	if upper {
		from, f = 'a', unicode.ToUpper
	}
	dst = slices.Grow(dst, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= utf8.RuneSelf {
			return mapRunes(dst, s[i:], f)
		}
		if c-from < 26 {
			c ^= 0x20
		}
		dst = append(dst, c)
	}

	return dst
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= utf8.RuneSelf {
			return false
		}
	}

	return true
}

func isSpace(r rune) bool { return unicode.IsSpace(r) }

func init() {
	fixed("concat", strict(func(c *ctx, a []val) val {
		start := len(c.out)
		for _, v := range a {
			s, _ := c.str(v)
			c.out = append(c.out, s...)
		}
		return c.result(start)
	}))
	fixed("concat_ws", func(c *ctx, a []val) val {
		sep, ok := c.str(a[0])
		if !ok {
			return null
		}
		start := len(c.out)
		n := 0
		for _, v := range a[1:] {
			if s, ok := c.str(v); ok {
				if n > 0 {
					c.out = append(c.out, sep...)
				}
				c.out = append(c.out, s...)
				n++
			}
		}
		return c.result(start)
	})
	fixed("length", strFn(func(_ *ctx, s string) val { return intVal(int64(utf8.RuneCountInString(s))) }))
	fixed("lower", strFn(func(c *ctx, s string) val {
		start := len(c.out)
		c.out = changeCase(c.out, s, false)
		return c.result(start)
	}))
	fixed("upper", strFn(func(c *ctx, s string) val {
		start := len(c.out)
		c.out = changeCase(c.out, s, true)
		return c.result(start)
	}))
	fixed("trim", strFn(func(_ *ctx, s string) val { return strVal(strings.TrimSpace(s)) }))
	fixed("ltrim", strFn(func(_ *ctx, s string) val { return strVal(strings.TrimLeftFunc(s, isSpace)) }))
	fixed("rtrim", strFn(func(_ *ctx, s string) val { return strVal(strings.TrimRightFunc(s, isSpace)) }))
	fixed("reverse", strFn(func(c *ctx, s string) val {
		start := len(c.out)
		if isASCII(s) {
			c.out = slices.Grow(c.out, len(s))[:start+len(s)]
			for i := 0; i < len(s); i++ {
				c.out[start+len(s)-1-i] = s[i]
			}
			return c.result(start)
		}
		c.rs = c.rs[:0]
		for _, r := range s {
			c.rs = append(c.rs, r)
		}
		for i := len(c.rs) - 1; i >= 0; i-- {
			c.out = utf8.AppendRune(c.out, c.rs[i])
		}
		return c.result(start)
	}))
	leftRight := func(left bool) impl {
		return strict(func(c *ctx, a []val) val {
			s, ok := c.str(a[0])
			n, ok2 := numInt(a[1])
			if !ok || !ok2 {
				return null
			}
			count := int64(utf8.RuneCountInString(s))
			if n < 0 {
				n = 0
			}
			if n > count {
				n = count
			}
			if left {
				return c.runes(s, 0, n)
			}
			return c.runes(s, count-n, count)
		})
	}
	fixed("left", leftRight(true))
	fixed("right", leftRight(false))
	fixed("substring", strict(func(c *ctx, a []val) val {
		s, ok := c.str(a[0])
		start, ok2 := numInt(a[1])
		if !ok || !ok2 {
			return null
		}
		count := int64(utf8.RuneCountInString(s))
		var from int64
		switch {
		case start > 0:
			from = start - 1
		case start < 0:
			from = count + start
		default:
			return strVal("")
		}
		if from < 0 || from >= count {
			return strVal("")
		}
		to := count
		if len(a) > 2 {
			l, ok := numInt(a[2])
			if !ok {
				return null
			}
			if l < 0 {
				return strVal("")
			}
			if from+l < to {
				to = from + l
			}
			// A length that overflows the end makes the row engine fail.
			if to < from {
				return null
			}
		}
		return c.runes(s, from, to)
	}))
	fixed("locate", strict(func(c *ctx, a []val) val {
		sub, ok := c.str(a[0])
		s, ok2 := c.str(a[1])
		if !ok || !ok2 {
			return null
		}
		count := int64(utf8.RuneCountInString(s))
		start := int64(1)
		if len(a) > 2 {
			if start, ok = numInt(a[2]); !ok {
				return null
			}
		}
		if start < 1 || start > count+1 {
			return intVal(0)
		}
		rest := c.runes(s, start-1, count).s
		i := strings.Index(rest, sub)
		if i < 0 {
			return intVal(0)
		}
		return intVal(start + int64(utf8.RuneCountInString(rest[:i])))
	}))
	fixed("ascii", strFn(func(_ *ctx, s string) val {
		for _, r := range s {
			return intVal(int64(r))
		}
		return intVal(0)
	}))
	fixed("strcmp", strict(func(c *ctx, a []val) val {
		x, ok := c.str(a[0])
		y, ok2 := c.str(a[1])
		if !ok || !ok2 {
			return null
		}
		return intVal(int64(strings.Compare(x, y)))
	}))
	def("like", func(call *ir.Call) (func() impl, error) {
		pattern, ok := constString(call, 1)
		if !ok {
			return nil, errorf("like needs a constant pattern")
		}
		cs := false
		if len(call.Args) > 2 {
			if lit, ok := call.Args[2].(*ir.Literal); ok {
				cs, _ = lit.V.(bool)
			}
		}
		re, err := regexp.Compile(likeToRegex(pattern, cs))
		if err != nil {
			return nil, err
		}
		f := strFn(func(_ *ctx, s string) val { return boolVal(re.MatchString(s)) })
		return func() impl { return first(f) }, nil
	})
	def("regexp_match", func(call *ir.Call) (func() impl, error) {
		pattern, ok := constString(call, 1)
		if !ok {
			return nil, errorf("regexp_match needs a constant pattern")
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, err
		}
		f := strFn(func(_ *ctx, s string) val { return boolVal(re.MatchString(s)) })
		return func() impl { return first(f) }, nil
	})
	def("regexp_replace", func(call *ir.Call) (func() impl, error) {
		pattern, ok := constString(call, 1)
		if !ok {
			return nil, errorf("regexp_replace needs a constant pattern")
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, err
		}
		f := strict(func(c *ctx, a []val) val {
			s, ok := c.str(a[0])
			r, ok2 := c.str(a[1])
			if !ok || !ok2 {
				return null
			}
			return strVal(re.ReplaceAllString(s, javaGroupRefs(r)))
		})
		return func() impl {
			var two [2]val
			return func(c *ctx, a []val) val {
				two[0], two[1] = a[0], a[2]
				return f(c, two[:])
			}
		}, nil
	})
	def("regexp_extract", func(call *ir.Call) (func() impl, error) {
		pattern, ok := constString(call, 1)
		if !ok {
			return nil, errorf("regexp_extract needs a constant pattern")
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, err
		}
		group, _ := constInt(call, 2)
		f := strFn(func(_ *ctx, s string) val {
			m := re.FindStringSubmatchIndex(s)
			if m == nil || group < 0 || int(group) >= len(m)/2 {
				return null
			}
			if m[2*group] < 0 {
				return strVal("")
			}
			return strVal(s[m[2*group]:m[2*group+1]])
		})
		return func() impl { return first(f) }, nil
	})
	pair := func(name string, test func(s, t string) bool) {
		fixed(name, strict(func(c *ctx, a []val) val {
			s, ok := c.str(a[0])
			t, ok2 := c.str(a[1])
			if !ok || !ok2 {
				return null
			}
			return boolVal(test(s, t))
		}))
	}
	pair("contains", strings.Contains)
	pair("starts_with", strings.HasPrefix)
	pair("ends_with", strings.HasSuffix)
	def("wildcard_replace", func(call *ir.Call) (func() impl, error) {
		pattern, ok := constString(call, 1)
		repl, ok2 := constString(call, 2)
		if !ok || !ok2 {
			return nil, errorf("wildcard_replace needs a constant pattern and replacement")
		}
		f, err := wildcardReplacer(pattern, repl)
		if err != nil {
			return nil, err
		}
		return func() impl { return first(f) }, nil
	})
}

// runes returns the characters from..to of s.
func (c *ctx) runes(s string, from, to int64) val {
	i, start, end := int64(0), -1, len(s)
	for off := range s {
		if i == from {
			start = off
		}
		if i == to {
			end = off
			break
		}
		i++
	}
	if start < 0 {
		start = len(s)
	}
	if !utf8.ValidString(s[start:end]) {
		at := len(c.out)
		c.out = mapRunes(c.out, s[start:end], func(r rune) rune { return r })
		return c.result(at)
	}

	return strVal(s[start:end])
}

var backslashGroup = regexp.MustCompile(`\\(\d+)`)

// javaGroupRefs converts \1 group references to Go's ${1} form.
func javaGroupRefs(r string) string {
	if !strings.Contains(r, `\`) {
		return r
	}

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

// wildcardReplacer implements wildcard_replace: a pattern without '*'
// replaces every occurrence; one with '*' matches the whole value and
// carries the matched text into the replacement's '*'.
func wildcardReplacer(pattern, repl string) (impl, error) {
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
		with := rb.String()
		return func(c *ctx, a []val) val {
			if a[0].isNull() {
				return a[0]
			}
			s, _ := c.str(a[0])
			if lit == "" {
				return a[0]
			}
			return strVal(strings.ReplaceAll(s, lit, with))
		}, nil
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
	rx, err := regexp.Compile(re.String())
	if err != nil {
		return nil, err
	}

	return func(c *ctx, a []val) val {
		if a[0].isNull() {
			return a[0]
		}
		s, _ := c.str(a[0])
		m := rx.FindStringSubmatch(s)
		if m == nil {
			return a[0]
		}
		start := len(c.out)
		g := 1
		for _, p := range rp {
			if p.wild {
				if g < len(m) {
					c.out = append(c.out, m[g]...)
				}
				g++
				continue
			}
			c.out = append(c.out, p.text...)
		}
		return c.result(start)
	}, nil
}
