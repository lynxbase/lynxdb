package shred

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/bits"
	"strconv"
	"unicode/utf16"
	"unicode/utf8"
	"unsafe"

	"github.com/lynxbase/lynxdb/pkg/colstore/variant"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// SyntaxError reports malformed JSON at a byte offset of the document.
type SyntaxError struct {
	Offset int
	Msg    string
}

func (e *SyntaxError) Error() string {
	return fmt.Sprintf("shred: invalid JSON at offset %d: %s", e.Offset, e.Msg)
}

func (b *Batch) syntax(msg string) error { return &SyntaxError{Offset: b.pos, Msg: msg} }

func (b *Batch) skipSpace() {
	// Every blank is at most a space; most JSON has none between tokens.
	if b.pos < len(b.src) && b.src[b.pos] > ' ' {
		return
	}
	b.skipBlanks()
}

func (b *Batch) skipBlanks() {
	src, i := b.src, b.pos
	for i < len(src) {
		if c := src[i]; c != ' ' && c != '\n' && c != '\t' && c != '\r' {
			break
		}
		i++
	}
	b.pos = i
}

// object shreds the members of the object whose '{' was just consumed.
func (b *Batch) object(n *node, depth int) error {
	b.skipSpace()
	if b.pos < len(b.src) && b.src[b.pos] == '}' {
		b.pos++
		if n != b.root {
			c := b.put(n.cols, n.path, vec.KindVariant)
			var mark int
			c.Data, mark = variant.BeginObject(c.Data)
			c.Data = variant.End(c.Data, mark)
			c.Ends = append(c.Ends, uint32(len(c.Data)))
		}

		return nil
	}
	hint := 0
	for {
		if b.pos >= len(b.src) || b.src[b.pos] != '"' {
			return b.syntax("expected an object key")
		}
		b.pos++
		var err error
		var key []byte
		child := b.nextKey(n, &hint)
		if child == nil {
			var escaped bool
			if key, escaped, err = b.scanString(); err != nil {
				return err
			}
			if escaped {
				if b.scratch, err = unescape(b.scratch[:0], key); err != nil {
					return b.syntax(err.Error())
				}
				key = b.scratch
			}
			child = b.child(n, key, &hint)
		}
		b.skipSpace()
		if b.pos >= len(b.src) || b.src[b.pos] != ':' {
			return b.syntax("expected ':' after an object key")
		}
		b.pos++
		b.skipSpace()
		if child == nil {
			err = b.overflow(n, key)
		} else {
			err = b.value(child, depth+1)
		}
		if err != nil {
			return err
		}
		b.skipSpace()
		if b.pos >= len(b.src) {
			return b.syntax("unterminated object")
		}
		switch b.src[b.pos] {
		case ',':
			b.pos++
			b.skipSpace()
		case '}':
			b.pos++

			return nil
		default:
			return b.syntax("expected ',' or '}'")
		}
	}
}

// overflow keeps a member that has no column in the row's rest object.
func (b *Batch) overflow(parent *node, key []byte) error {
	if b.restMark == 0 {
		b.rest, b.restMark = variant.BeginObject(b.rest[:0])
	}
	// key may alias scratch, which nested values reuse.
	path := make([]byte, 0, len(parent.path)+1+len(key))
	if parent != b.root {
		path = append(append(path, parent.path...), '.')
	}
	path = append(path, key...)
	b.rest = variant.AppendKey(b.rest, path)
	var err error
	b.rest, err = b.variantValue(b.rest, 0)

	return err
}

func (b *Batch) value(n *node, depth int) error {
	if b.pos >= len(b.src) {
		return b.syntax("expected a value")
	}
	switch c := b.src[b.pos]; c {
	case '"':
		b.pos++
		s, escaped, err := b.scanString()
		if err != nil {
			return err
		}
		col := b.put(n.cols, n.path, vec.KindString)
		if escaped {
			if col.Data, err = unescape(col.Data, s); err != nil {
				col.Ends = append(col.Ends, uint32(len(col.Data)))

				return b.syntax(err.Error())
			}
		} else {
			col.Data = append(col.Data, s...)
		}
		col.Ends = append(col.Ends, uint32(len(col.Data)))

		return nil
	case '{':
		if depth >= maxDepth {
			return b.variantInto(n)
		}
		if pc := n.cols; pc.seq == b.seq && pc.last.lastRow == int32(b.Rows) {
			pc.last.pop()
		}
		b.pos++

		return b.object(n, depth)
	case '[':
		return b.variantInto(n)
	case 't':
		if err := b.literal("true"); err != nil {
			return err
		}
		col := b.put(n.cols, n.path, vec.KindBool)
		col.Bools = append(col.Bools, 1)

		return nil
	case 'f':
		if err := b.literal("false"); err != nil {
			return err
		}
		col := b.put(n.cols, n.path, vec.KindBool)
		col.Bools = append(col.Bools, 0)

		return nil
	case 'n':
		if err := b.literal("null"); err != nil {
			return err
		}
		b.put(n.cols, n.path, vec.KindNull)

		return nil
	default:
		num, err := b.number()
		if err != nil {
			return err
		}
		switch {
		case num.digits != nil:
			col := b.put(n.cols, n.path, vec.KindVariant)
			col.Data = variant.AppendNumber(col.Data, num.digits)
			col.Ends = append(col.Ends, uint32(len(col.Data)))
		case num.isInt:
			col := b.put(n.cols, n.path, vec.KindInt)
			col.Ints = append(col.Ints, num.i)
		default:
			col := b.put(n.cols, n.path, vec.KindFloat)
			col.Floats = append(col.Floats, num.f)
		}

		return nil
	}
}

func (b *Batch) variantInto(n *node) error {
	col := b.put(n.cols, n.path, vec.KindVariant)
	var err error
	col.Data, err = b.variantValue(col.Data, 0)
	col.Ends = append(col.Ends, uint32(len(col.Data)))

	return err
}

// variantValue appends the binary form of the JSON value at the cursor.
func (b *Batch) variantValue(dst []byte, depth int) ([]byte, error) {
	if b.pos >= len(b.src) {
		return dst, b.syntax("expected a value")
	}
	if depth > 4*maxDepth {
		return dst, b.syntax("value nested too deeply")
	}
	switch c := b.src[b.pos]; c {
	case '"':
		b.pos++
		s, escaped, err := b.scanString()
		if err != nil {
			return dst, err
		}
		if escaped {
			if b.scratch, err = unescape(b.scratch[:0], s); err != nil {
				return dst, b.syntax(err.Error())
			}
			s = b.scratch
		}

		return variant.AppendString(dst, s), nil
	case '[':
		b.pos++
		dst, mark := variant.BeginArray(dst)
		b.skipSpace()
		if b.pos < len(b.src) && b.src[b.pos] == ']' {
			b.pos++

			return variant.End(dst, mark), nil
		}
		for {
			var err error
			if dst, err = b.variantValue(dst, depth+1); err != nil {
				return dst, err
			}
			b.skipSpace()
			if b.pos >= len(b.src) {
				return dst, b.syntax("unterminated array")
			}
			switch b.src[b.pos] {
			case ',':
				b.pos++
				b.skipSpace()
			case ']':
				b.pos++

				return variant.End(dst, mark), nil
			default:
				return dst, b.syntax("expected ',' or ']'")
			}
		}
	case '{':
		b.pos++
		dst, mark := variant.BeginObject(dst)
		b.skipSpace()
		if b.pos < len(b.src) && b.src[b.pos] == '}' {
			b.pos++

			return variant.End(dst, mark), nil
		}
		for {
			if b.pos >= len(b.src) || b.src[b.pos] != '"' {
				return dst, b.syntax("expected an object key")
			}
			b.pos++
			key, escaped, err := b.scanString()
			if err != nil {
				return dst, err
			}
			if escaped {
				if b.scratch, err = unescape(b.scratch[:0], key); err != nil {
					return dst, b.syntax(err.Error())
				}
				key = b.scratch
			}
			dst = variant.AppendKey(dst, key)
			b.skipSpace()
			if b.pos >= len(b.src) || b.src[b.pos] != ':' {
				return dst, b.syntax("expected ':' after an object key")
			}
			b.pos++
			b.skipSpace()
			if dst, err = b.variantValue(dst, depth+1); err != nil {
				return dst, err
			}
			b.skipSpace()
			if b.pos >= len(b.src) {
				return dst, b.syntax("unterminated object")
			}
			switch b.src[b.pos] {
			case ',':
				b.pos++
				b.skipSpace()
			case '}':
				b.pos++

				return variant.End(dst, mark), nil
			default:
				return dst, b.syntax("expected ',' or '}'")
			}
		}
	case 't':
		return variant.AppendBool(dst, true), b.literal("true")
	case 'f':
		return variant.AppendBool(dst, false), b.literal("false")
	case 'n':
		return variant.AppendNull(dst), b.literal("null")
	default:
		num, err := b.number()
		switch {
		case err != nil:
			return dst, err
		case num.digits != nil:
			return variant.AppendNumber(dst, num.digits), nil
		case num.isInt:
			return variant.AppendInt(dst, num.i), nil
		}

		return variant.AppendFloat(dst, num.f), nil
	}
}

func (b *Batch) literal(word string) error {
	if len(b.src)-b.pos < len(word) || string(b.src[b.pos:b.pos+len(word)]) != word {
		return b.syntax("invalid literal")
	}
	b.pos += len(word)

	return nil
}

const (
	lowBits  = 0x0101010101010101
	highBits = 0x8080808080808080
)

// quoteOrEscape sets the high bit of the bytes of w that are a quote or a
// backslash; above the lowest such byte it may set others.
func quoteOrEscape(w uint64) uint64 {
	q, e := w^(lowBits*'"'), w^(lowBits*'\\')

	return (q-lowBits)&^q&highBits | (e-lowBits)&^e&highBits
}

// wordScan is how far scanString looks eight bytes at a time before it
// leaves a long string to bytes.IndexByte.
const wordScan = 64

// scanString returns the bytes between the opening quote, already consumed,
// and the closing one, and whether they contain escapes.
func (b *Batch) scanString() (s []byte, escaped bool, err error) {
	src, start := b.src, b.pos
	i := start
	// Most keys and values are short; words of eight bytes beat a call for
	// them.
	for limit := min(start+wordScan, len(src)-8); i <= limit; i += 8 {
		if m := quoteOrEscape(binary.LittleEndian.Uint64(src[i:])); m != 0 {
			i += bits.TrailingZeros64(m) >> 3
			if src[i] == '\\' {
				return b.scanEscapedString(start, i)
			}
			b.pos = i + 1

			return src[start:i], false, nil
		}
	}
	if i > len(src)-8 {
		for ; i < len(src); i++ {
			switch src[i] {
			case '"':
				b.pos = i + 1

				return src[start:i], false, nil
			case '\\':
				return b.scanEscapedString(start, i)
			}
		}

		return nil, false, b.syntax("unterminated string")
	}
	end := bytes.IndexByte(src[i:], '"')
	if end < 0 {
		return nil, false, b.syntax("unterminated string")
	}
	end += i
	if esc := bytes.IndexByte(src[i:end], '\\'); esc >= 0 {
		return b.scanEscapedString(start, i+esc)
	}
	b.pos = end + 1

	return src[start:end], false, nil
}

func (b *Batch) scanEscapedString(start, i int) ([]byte, bool, error) {
	src := b.src
	for i < len(src) {
		switch src[i] {
		case '\\':
			i += 2
		case '"':
			b.pos = i + 1

			return src[start:i], true, nil
		default:
			i++
		}
	}

	return nil, false, b.syntax("unterminated string")
}

func unescape(dst, s []byte) ([]byte, error) {
	for i := 0; i < len(s); {
		c := s[i]
		if c != '\\' {
			j := bytes.IndexByte(s[i:], '\\')
			if j < 0 {
				return append(dst, s[i:]...), nil
			}
			dst = append(dst, s[i:i+j]...)
			i += j

			continue
		}
		if i+1 >= len(s) {
			return dst, errBadEscape
		}
		switch e := s[i+1]; e {
		case '"', '\\', '/':
			dst = append(dst, e)
		case 'b':
			dst = append(dst, '\b')
		case 'f':
			dst = append(dst, '\f')
		case 'n':
			dst = append(dst, '\n')
		case 'r':
			dst = append(dst, '\r')
		case 't':
			dst = append(dst, '\t')
		case 'u':
			r, ok := hex4(s[i+2:])
			if !ok {
				return dst, errBadEscape
			}
			i += 4
			if utf16.IsSurrogate(r) {
				// A lone surrogate becomes U+FFFD, as encoding/json does.
				if i+7 < len(s) && s[i+2] == '\\' && s[i+3] == 'u' {
					if lo, ok := hex4(s[i+4:]); ok {
						if full := utf16.DecodeRune(r, lo); full != utf8.RuneError {
							r = full
							i += 6
						}
					}
				}
				if utf16.IsSurrogate(r) {
					r = utf8.RuneError
				}
			}
			dst = utf8.AppendRune(dst, r)
		default:
			return dst, errBadEscape
		}
		i += 2
	}

	return dst, nil
}

var errBadEscape = fmt.Errorf("invalid string escape")

func hex4(s []byte) (rune, bool) {
	if len(s) < 4 {
		return 0, false
	}
	var r rune
	for _, c := range s[:4] {
		switch {
		case c >= '0' && c <= '9':
			c -= '0'
		case c >= 'a' && c <= 'f':
			c -= 'a' - 10
		case c >= 'A' && c <= 'F':
			c -= 'A' - 10
		default:
			return 0, false
		}
		r = r<<4 | rune(c)
	}

	return r, true
}

// number is a parsed JSON number: an int64, a float64, or the digits of an
// integer that fits neither exactly.
type number struct {
	i      int64
	f      float64
	digits []byte
	isInt  bool
}

// number parses the JSON number at the cursor.
func (b *Batch) number() (number, error) {
	src, start := b.src, b.pos
	p := start
	neg := false
	if p < len(src) && src[p] == '-' {
		neg = true
		p++
	}
	var u uint64
	digits := p
	for p < len(src) {
		d := src[p] - '0'
		if d > 9 {
			break
		}
		u = u*10 + uint64(d)
		p++
	}
	digits = p - digits
	if digits == 0 {
		return number{}, b.syntax("invalid value")
	}
	fractional := p < len(src) && (src[p] == '.' || src[p] == 'e' || src[p] == 'E')
	if !fractional && digits <= 18 {
		b.pos = p
		if neg {
			return number{i: -int64(u), isInt: true}, nil
		}

		return number{i: int64(u), isInt: true}, nil
	}
	for p < len(src) {
		c := src[p]
		if (c < '0' || c > '9') && c != '.' && c != 'e' && c != 'E' && c != '+' && c != '-' {
			break
		}
		p++
	}
	tok := unsafe.String(unsafe.SliceData(src[start:p]), p-start)
	b.pos = p
	if !fractional {
		v, perr := strconv.ParseInt(tok, 10, 64)
		if perr == nil {
			return number{i: v, isInt: true}, nil
		}
		if isRange(perr) {
			// A float64 would drop the low digits of an integer this large.
			return number{digits: b.integerText(src[start:p])}, nil
		}
	}
	f, perr := strconv.ParseFloat(tok, 64)
	if perr != nil && !isRange(perr) {
		b.pos = start

		return number{}, b.syntax("invalid number")
	}

	return number{f: f}, nil
}

// integerText returns an integer literal without the leading zeros that
// JSON does not allow.
func (b *Batch) integerText(lit []byte) []byte {
	sign := 0
	if lit[0] == '-' {
		sign = 1
	}
	zeros := 0
	for sign+zeros < len(lit)-1 && lit[sign+zeros] == '0' {
		zeros++
	}
	if zeros == 0 {
		return lit
	}
	b.scratch = append(append(b.scratch[:0], lit[:sign]...), lit[sign+zeros:]...)

	return b.scratch
}

func isRange(err error) bool {
	ne, ok := err.(*strconv.NumError)

	return ok && ne.Err == strconv.ErrRange
}
