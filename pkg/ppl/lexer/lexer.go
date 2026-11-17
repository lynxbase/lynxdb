// Package lexer tokenizes OpenSearch Piped Processing Language (PPL) text.
//
// Token rules follow the official OpenSearch PPL lexer grammar: keywords are
// case-insensitive and recognized by the parser from Ident tokens, so every
// keyword the grammar allows as an identifier stays usable as a field name;
// identifiers may contain '-', '*' and digits after the first character and
// may start with '@' or '*'; strings use single or double quotes; backticks
// delimit identifiers; '//' and '/* */' are comments.
//
// Token texts and, where no unescaping is needed, token values are slices of
// the input, so tokenizing allocates only the token slice.
package lexer

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Kind is a token kind.
type Kind uint8

const (
	EOF Kind = iota
	Illegal
	// Ident is a regular identifier or keyword.
	Ident
	// QuotedIdent is a backtick-delimited identifier; Value holds the
	// unescaped name.
	QuotedIdent
	// String is a single- or double-quoted string; Value holds the
	// unescaped text.
	String
	Integer
	// Decimal is a number with a fraction and no suffix.
	Decimal
	// Double is a number with a D suffix (1.5d).
	Double
	// Float is a number with an F suffix (1.5f).
	Float
	// Span is an integer or decimal followed by a time unit (5m, 1.5h).
	Span
	// NumericIdent is digits followed by identifier characters (123abc).
	NumericIdent
	// TimeSnap is '@' followed by a snap unit (@d, @w1).
	TimeSnap
	Pipe
	Comma
	Dot
	LParen
	RParen
	LBracket
	RBracket
	LBrace
	RBrace
	Eq
	EqEq
	NotEq
	Lt
	Le
	Gt
	Ge
	Plus
	Minus
	Star
	Slash
	Percent
	Arrow
	Bang
	Colon
	Caret
	Question
	Semicolon
)

var kindNames = [...]string{
	EOF: "end of query", Illegal: "illegal character", Ident: "identifier", QuotedIdent: "quoted identifier",
	String: "string", Integer: "integer", Decimal: "decimal", Double: "double", Float: "float",
	Span: "span", NumericIdent: "identifier", TimeSnap: "time snap",
	Pipe: "'|'", Comma: "','", Dot: "'.'", LParen: "'('", RParen: "')'", LBracket: "'['", RBracket: "']'",
	LBrace: "'{'", RBrace: "'}'", Eq: "'='", EqEq: "'=='", NotEq: "'!='", Lt: "'<'", Le: "'<='", Gt: "'>'",
	Ge: "'>='", Plus: "'+'", Minus: "'-'", Star: "'*'", Slash: "'/'", Percent: "'%'", Arrow: "'->'",
	Bang: "'!'", Colon: "':'", Caret: "'^'", Question: "'?'", Semicolon: "';'",
}

func (k Kind) String() string {
	if int(k) < len(kindNames) {
		return kindNames[k]
	}
	return "token"
}

// Pos is a position in the query text. Offset is a byte offset; Line and
// Col are 1-based, Col counting characters.
type Pos struct {
	Offset int
	Line   int
	Col    int
}

func (p Pos) String() string { return strconv.Itoa(p.Line) + ":" + strconv.Itoa(p.Col) }

// Token is one lexical token.
type Token struct {
	Kind Kind
	// SpaceBefore reports whether white space or a comment precedes the
	// token.
	SpaceBefore bool
	nonASCII    bool
	Text        string
	Value       string
	Pos         Pos
	End         Pos
}

// Is reports whether the token is the identifier kw, compared without regard
// to case; kw must be lower-case ASCII.
func (t *Token) Is(kw string) bool {
	if t.Kind != Ident {
		return false
	}
	if t.nonASCII {
		return strings.EqualFold(t.Text, kw)
	}
	return len(t.Text) == len(kw) && equalFoldLower(t.Text, kw)
}

// equalFoldLower reports whether s equals the lower-case ASCII word kw
// without regard to case; both have the same length.
func equalFoldLower(s, kw string) bool {
	for i := 0; i < len(kw); i++ {
		c := s[i]
		if c != kw[i] && c|0x20 != kw[i] {
			return false
		}
	}
	return true
}

// Error is a lexical error.
type Error struct {
	Pos Pos
	Msg string
}

func (e *Error) Error() string { return e.Pos.String() + ": " + e.Msg }

type lexer struct {
	src  string
	base int
	off  int
	line int
	col  int
	toks []Token
	errs []*Error
}

// Tokenize splits src into tokens, ending with an EOF token. Lexical errors
// are returned alongside the tokens; an erroneous character becomes an
// Illegal token.
func Tokenize(src string) ([]Token, []*Error) {
	return AppendTokens(nil, src)
}

// AppendTokens is Tokenize appending to dst, so a caller can reuse a token
// buffer across queries.
func AppendTokens(dst []Token, src string) ([]Token, []*Error) {
	if cap(dst)-len(dst) < len(src)/3+2 {
		grown := make([]Token, len(dst), len(dst)+len(src)/3+8)
		copy(grown, dst)
		dst = grown
	}
	base := len(dst)
	l := lexer{src: src, base: base, line: 1, col: 1, toks: dst}
	space := false
	for {
		if l.skipSpaceAndComments() {
			space = true
		}
		if l.off >= len(l.src) {
			p := l.pos()
			l.toks = append(l.toks, Token{Kind: EOF, Pos: p, End: p, SpaceBefore: true})
			return l.toks, l.errs
		}
		l.toks = append(l.toks, Token{})
		tok := &l.toks[len(l.toks)-1]
		l.next(tok)
		tok.SpaceBefore = space || len(l.toks) == base+1
		space = false
	}
}

func (l *lexer) pos() Pos { return Pos{Offset: l.off, Line: l.line, Col: l.col} }

func (l *lexer) peekByte(n int) byte {
	if l.off+n < len(l.src) {
		return l.src[l.off+n]
	}
	return 0
}

// advance moves past n bytes, extended to the end of the last character,
// keeping the line and column current.
func (l *lexer) advance(n int) {
	end := l.off + n
	for l.off < end && l.off < len(l.src) {
		c := l.src[l.off]
		if c < utf8.RuneSelf {
			l.off++
			if c == '\n' {
				l.line++
				l.col = 1
			} else {
				l.col++
			}
			continue
		}
		_, size := utf8.DecodeRuneInString(l.src[l.off:])
		l.off += size
		l.col++
	}
}

// advanceASCII moves past n bytes known to be ASCII and not newlines.
func (l *lexer) advanceASCII(n int) {
	l.off += n
	l.col += n
}

func (l *lexer) skipSpaceAndComments() bool {
	skipped := false
	for l.off < len(l.src) {
		c := l.src[l.off]
		switch {
		case c == ' ' || c == '\t' || c == '\r' || c == '\f':
			l.advanceASCII(1)
		case c == '\n':
			l.off++
			l.line++
			l.col = 1
		case c == '/' && l.peekByte(1) == '/':
			end := strings.IndexByte(l.src[l.off:], '\n')
			if end < 0 {
				end = len(l.src) - l.off
			}
			l.advance(end)
		case c == '/' && l.peekByte(1) == '*':
			start := l.pos()
			end := strings.Index(l.src[l.off+2:], "*/")
			if end < 0 {
				l.advance(len(l.src) - l.off)
				l.errs = append(l.errs, &Error{Pos: start, Msg: "unterminated block comment"})
			} else {
				l.advance(end + 4)
			}
		default:
			if c < utf8.RuneSelf {
				if c != '\v' {
					return skipped
				}
				l.advanceASCII(1)
				break
			}
			r, size := utf8.DecodeRuneInString(l.src[l.off:])
			if !unicode.IsSpace(r) {
				return skipped
			}
			l.advance(size)
		}
		skipped = true
	}
	return skipped
}

// Character classes of ASCII bytes.
const (
	clsIdentStart = 1 << iota
	clsIdentPart
	clsDigit
)

var class = func() (t [256]uint8) {
	for c := 'a'; c <= 'z'; c++ {
		t[c] |= clsIdentStart | clsIdentPart
		t[c-'a'+'A'] |= clsIdentStart | clsIdentPart
	}
	for c := '0'; c <= '9'; c++ {
		t[c] |= clsIdentPart | clsDigit
	}
	t['_'] |= clsIdentStart | clsIdentPart
	t['*'] |= clsIdentStart | clsIdentPart
	t['@'] |= clsIdentStart
	t['-'] |= clsIdentPart
	return t
}()

func isIdentStart(c byte) bool { return class[c]&clsIdentStart != 0 }

func isIdentPart(c byte) bool { return class[c]&clsIdentPart != 0 }

func isDigit(c byte) bool { return class[c]&clsDigit != 0 }

var punct = func() (t [256]Kind) {
	for c, k := range map[byte]Kind{
		'|': Pipe, ',': Comma, '.': Dot, '(': LParen, ')': RParen, '[': LBracket, ']': RBracket,
		'{': LBrace, '}': RBrace, '=': Eq, '<': Lt, '>': Gt, '+': Plus, '-': Minus, '/': Slash,
		'%': Percent, '!': Bang, ':': Colon, '^': Caret, '?': Question, ';': Semicolon,
	} {
		t[c] = k
	}
	return t
}()

// emit fills tok for the text from start to the current offset.
func (l *lexer) emit(tok *Token, kind Kind, start Pos, value string) {
	tok.Kind = kind
	tok.Text = l.src[start.Offset:l.off]
	tok.Value = value
	tok.Pos = start
	tok.End = l.pos()
}

func (l *lexer) next(tok *Token) {
	start := l.pos()
	c := l.src[l.off]
	switch {
	case c == '`':
		l.quoted(tok, '`', QuotedIdent)
		return
	case c == '\'' || c == '"':
		l.quoted(tok, c, String)
		return
	case isDigit(c) || (c == '.' && isDigit(l.peekByte(1)) && !l.prevAllowsMember()):
		l.number(tok)
		return
	case c == '@' && l.isTimeSnap():
		l.timeSnap(tok)
		return
	case c == '*' && !isIdentPart(l.peekByte(1)):
		l.advanceASCII(1)
		l.emit(tok, Star, start, "")
		return
	case isIdentStart(c):
		l.ident(tok)
		return
	}
	if k := l.twoCharOp(c); k != EOF {
		l.advanceASCII(2)
		l.emit(tok, k, start, "")
		return
	}
	if k := punct[c]; k != EOF {
		l.advanceASCII(1)
		l.emit(tok, k, start, "")
		return
	}
	r, size := utf8.DecodeRuneInString(l.src[l.off:])
	l.advance(size)
	l.errs = append(l.errs, &Error{Pos: start, Msg: fmt.Sprintf("unexpected character %q", r)})
	l.emit(tok, Illegal, start, "")
}

func (l *lexer) twoCharOp(c byte) Kind {
	d := l.peekByte(1)
	switch {
	case c == '=' && d == '=':
		return EqEq
	case c == '!' && d == '=', c == '<' && d == '>':
		return NotEq
	case c == '<' && d == '=':
		return Le
	case c == '>' && d == '=':
		return Ge
	case c == '-' && d == '>':
		return Arrow
	}
	return EOF
}

// prevAllowsMember reports whether a '.' directly after the previous token
// is a member separator rather than the start of a decimal such as .5.
func (l *lexer) prevAllowsMember() bool {
	if len(l.toks)-2 < l.base {
		return false
	}
	prev := &l.toks[len(l.toks)-2]
	if prev.End.Offset != l.off {
		return false
	}
	switch prev.Kind {
	case Ident, QuotedIdent, RParen, RBracket, NumericIdent:
		return true
	}
	return false
}

func (l *lexer) quoted(tok *Token, q byte, kind Kind) {
	start := l.pos()
	body := l.off + 1
	i := body
	for i < len(l.src) {
		c := l.src[i]
		if c == q && (i+1 >= len(l.src) || l.src[i+1] != q) {
			l.advance(i + 1 - l.off)
			l.emit(tok, kind, start, l.src[body:i])
			return
		}
		if c == '\\' || c == q || c >= utf8.RuneSelf {
			break
		}
		i++
	}
	l.quotedSlow(tok, q, kind, start)
}

// quotedSlow unescapes a quoted token that has escapes, doubled quotes or
// non-ASCII text; invalid UTF-8 becomes U+FFFD.
func (l *lexer) quotedSlow(tok *Token, q byte, kind Kind, start Pos) {
	l.advance(1)
	b := make([]byte, 0, len(l.src)-l.off)
	for l.off < len(l.src) {
		c := l.src[l.off]
		if c == '\\' && l.off+1 < len(l.src) {
			next := l.src[l.off+1]
			switch next {
			case 'n':
				b = append(b, '\n')
			case 't':
				b = append(b, '\t')
			case 'r':
				b = append(b, '\r')
			case '\\', '\'', '"', '`':
				b = append(b, next)
			default:
				b = append(b, '\\', next)
			}
			l.advance(2)
			continue
		}
		if c == q {
			if l.peekByte(1) == q {
				b = append(b, q)
				l.advance(2)
				continue
			}
			l.advance(1)
			l.emit(tok, kind, start, string(b))
			return
		}
		r, size := utf8.DecodeRuneInString(l.src[l.off:])
		b = utf8.AppendRune(b, r)
		l.advance(size)
	}
	what := "string"
	if kind == QuotedIdent {
		what = "quoted identifier"
	}
	l.errs = append(l.errs, &Error{Pos: start, Msg: "unterminated " + what})
	l.emit(tok, Illegal, start, string(b))
}

// isSpanUnit reports whether the lower-case word is a span time unit.
func isSpanUnit(w string) bool {
	switch w {
	case "milliseconds", "millisecond", "microseconds", "seconds", "minutes", "quarters", "quarter",
		"second", "minute", "months", "month", "hours", "weeks", "years", "hour", "week", "year",
		"days", "secs", "mins", "hrs", "qtrs", "yrs", "sec", "min", "day", "mon", "qtr", "ms", "us",
		"cs", "ds", "hr", "yr", "s", "m", "h", "d", "w", "q", "y":
		return true
	}
	return false
}

// isSnapUnit reports whether the lower-case word is a time snap unit.
func isSnapUnit(w string) bool {
	switch w {
	case "seconds", "second", "minutes", "minute", "quarters", "quarter", "months", "month", "hours", "hour",
		"weeks", "week", "years", "year", "days", "day", "secs", "sec", "mins", "min", "hrs", "hr", "mon",
		"qtrs", "qtr", "yrs", "yr", "s", "m", "h", "d", "w", "q", "y":
		return true
	}
	return false
}

// lowerASCII returns s in lower case, allocating only when s has upper-case
// letters.
func lowerASCII(s string) string {
	for i := 0; i < len(s); i++ {
		if c := s[i]; c >= 'A' && c <= 'Z' {
			return strings.ToLower(s)
		}
	}
	return s
}

func (l *lexer) number(tok *Token) {
	start := l.pos()
	src := l.src
	i := l.off
	for i < len(src) && isDigit(src[i]) {
		i++
	}
	hasFrac := false
	if i < len(src) && src[i] == '.' && i+1 < len(src) && isDigit(src[i+1]) {
		hasFrac = true
		i++
		for i < len(src) && isDigit(src[i]) {
			i++
		}
	}
	if !hasFrac && i < len(src) && (src[i] == 'e' || src[i] == 'E') {
		j := i + 1
		if j < len(src) && (src[j] == '+' || src[j] == '-') {
			j++
		}
		if j < len(src) && isDigit(src[j]) {
			for j < len(src) && isDigit(src[j]) {
				j++
			}
			if j >= len(src) || !isIdentPart(src[j]) {
				l.advanceASCII(j - l.off)
				l.emit(tok, Decimal, start, src[start.Offset:l.off])
				return
			}
		}
	}
	numText := src[l.off:i]
	end := i
	for end < len(src) && isIdentPart(src[end]) {
		end++
	}
	suffix := src[i:end]
	if suffix == "" {
		l.advanceASCII(i - l.off)
		if hasFrac {
			l.emit(tok, Decimal, start, numText)
			return
		}
		l.emit(tok, Integer, start, numText)
		return
	}
	lower := lowerASCII(suffix)
	if hasFrac && (lower == "d" || lower == "f") {
		l.advanceASCII(end - l.off)
		if lower == "f" {
			l.emit(tok, Float, start, numText)
			return
		}
		l.emit(tok, Double, start, numText)
		return
	}
	if isSpanUnit(lower) {
		l.advanceASCII(end - l.off)
		value := src[start.Offset:end]
		if lower != suffix {
			value = numText + lower
		}
		l.emit(tok, Span, start, value)
		return
	}
	if hasFrac {
		l.advanceASCII(i - l.off)
		l.emit(tok, Decimal, start, numText)
		return
	}
	l.advanceASCII(end - l.off)
	l.emit(tok, NumericIdent, start, src[start.Offset:l.off])
}

func (l *lexer) isTimeSnap() bool {
	_, ok := l.snapLength()
	return ok
}

func (l *lexer) snapLength() (int, bool) {
	rest := l.src[l.off+1:]
	end := 0
	for end < len(rest) && isIdentPart(rest[end]) {
		end++
	}
	word := rest[:end]
	if len(word) == 2 && word[0]|0x20 == 'w' && word[1] >= '0' && word[1] <= '7' {
		return 1 + end, true
	}
	if len(word) > len("quarters") {
		return 0, false
	}
	var buf [8]byte
	for i := 0; i < len(word); i++ {
		c := word[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		buf[i] = c
	}
	if isSnapUnit(string(buf[:len(word)])) {
		return 1 + end, true
	}
	return 0, false
}

func (l *lexer) timeSnap(tok *Token) {
	start := l.pos()
	n, _ := l.snapLength()
	l.advanceASCII(n)
	l.emit(tok, TimeSnap, start, lowerASCII(l.src[start.Offset+1:l.off]))
}

func (l *lexer) ident(tok *Token) {
	start := l.pos()
	src := l.src
	i := l.off + 1
	ascii := true
	for i < len(src) {
		c := src[i]
		if isIdentPart(c) {
			i++
			continue
		}
		if c >= utf8.RuneSelf {
			r, size := utf8.DecodeRuneInString(src[i:])
			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				i += size
				ascii = false
				continue
			}
		}
		break
	}
	// An identifier never ends with '-': "a- b" is a minus.
	for i > l.off+1 && src[i-1] == '-' {
		i--
	}
	if ascii {
		l.advanceASCII(i - l.off)
	} else {
		l.advance(i - l.off)
	}
	text := src[start.Offset:l.off]
	l.emit(tok, Ident, start, text)
	tok.nonASCII = !ascii
}
