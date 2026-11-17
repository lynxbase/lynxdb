// Package lexer tokenizes OpenSearch Piped Processing Language (PPL) text.
//
// Token rules follow the official OpenSearch PPL lexer grammar: keywords are
// case-insensitive and recognized by the parser from Ident tokens, so every
// keyword the grammar allows as an identifier stays usable as a field name;
// identifiers may contain '-', '*' and digits after the first character and
// may start with '@' or '*'; strings use single or double quotes; backticks
// delimit identifiers; '//' and '/* */' are comments.
package lexer

import (
	"fmt"
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

func (p Pos) String() string { return fmt.Sprintf("%d:%d", p.Line, p.Col) }

// Token is one lexical token.
type Token struct {
	Kind  Kind
	Text  string
	Value string
	Pos   Pos
	End   Pos
	// SpaceBefore reports whether white space or a comment precedes the
	// token.
	SpaceBefore bool
}

// Is reports whether the token is the identifier kw, compared without regard
// to case.
func (t Token) Is(kw string) bool {
	return t.Kind == Ident && strings.EqualFold(t.Text, kw)
}

// Error is a lexical error.
type Error struct {
	Pos Pos
	Msg string
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Pos, e.Msg) }

type lexer struct {
	src       string
	off       int
	line, col int
	toks      []Token
	errs      []*Error
}

// Tokenize splits src into tokens, ending with an EOF token. Lexical errors
// are returned alongside the tokens; an erroneous character becomes an
// Illegal token.
func Tokenize(src string) ([]Token, []*Error) {
	l := &lexer{src: src, line: 1, col: 1}
	space := false
	for {
		if l.skipSpaceAndComments() {
			space = true
		}
		if l.off >= len(l.src) {
			l.toks = append(l.toks, Token{Kind: EOF, Pos: l.pos(), End: l.pos(), SpaceBefore: true})
			return l.toks, l.errs
		}
		tok := l.next()
		tok.SpaceBefore = space || len(l.toks) == 0
		l.toks = append(l.toks, tok)
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

func (l *lexer) advance(n int) {
	for i := 0; i < n && l.off < len(l.src); {
		r, size := utf8.DecodeRuneInString(l.src[l.off:])
		l.off += size
		i += size
		if r == '\n' {
			l.line++
			l.col = 1
		} else {
			l.col++
		}
	}
}

func (l *lexer) skipSpaceAndComments() bool {
	skipped := false
	for l.off < len(l.src) {
		c := l.src[l.off]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f':
			l.advance(1)
		case c == '/' && l.peekByte(1) == '/':
			for l.off < len(l.src) && l.src[l.off] != '\n' {
				l.advance(1)
			}
		case c == '/' && l.peekByte(1) == '*':
			start := l.pos()
			l.advance(2)
			closed := false
			for l.off < len(l.src) {
				if l.src[l.off] == '*' && l.peekByte(1) == '/' {
					l.advance(2)
					closed = true
					break
				}
				l.advance(1)
			}
			if !closed {
				l.errs = append(l.errs, &Error{Pos: start, Msg: "unterminated block comment"})
			}
		default:
			r, _ := utf8.DecodeRuneInString(l.src[l.off:])
			if unicode.IsSpace(r) {
				l.advance(utf8.RuneLen(r))
				continue
			}
			return skipped
		}
		skipped = true
	}
	return skipped
}

func isIdentStart(c byte) bool {
	return c == '_' || c == '@' || c == '*' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdentPart(c byte) bool {
	return isIdentStart(c) && c != '@' || c == '-' || (c >= '0' && c <= '9')
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func (l *lexer) emit(kind Kind, start Pos, value string) Token {
	return Token{Kind: kind, Text: l.src[start.Offset:l.off], Value: value, Pos: start, End: l.pos()}
}

func (l *lexer) next() Token {
	start := l.pos()
	c := l.src[l.off]
	switch {
	case c == '`':
		return l.quoted('`', QuotedIdent)
	case c == '\'' || c == '"':
		return l.quoted(c, String)
	case isDigit(c) || (c == '.' && isDigit(l.peekByte(1)) && !l.prevAllowsMember()):
		return l.number()
	case c == '@' && l.isTimeSnap():
		return l.timeSnap()
	case c == '*' && !isIdentPart(l.peekByte(1)):
		l.advance(1)
		return l.emit(Star, start, "")
	case isIdentStart(c):
		return l.ident()
	}
	two := ""
	if l.off+1 < len(l.src) {
		two = l.src[l.off : l.off+2]
	}
	switch two {
	case "==":
		l.advance(2)
		return l.emit(EqEq, start, "")
	case "!=", "<>":
		l.advance(2)
		return l.emit(NotEq, start, "")
	case "<=":
		l.advance(2)
		return l.emit(Le, start, "")
	case ">=":
		l.advance(2)
		return l.emit(Ge, start, "")
	case "->":
		l.advance(2)
		return l.emit(Arrow, start, "")
	}
	single := map[byte]Kind{
		'|': Pipe, ',': Comma, '.': Dot, '(': LParen, ')': RParen, '[': LBracket, ']': RBracket,
		'{': LBrace, '}': RBrace, '=': Eq, '<': Lt, '>': Gt, '+': Plus, '-': Minus, '/': Slash,
		'%': Percent, '!': Bang, ':': Colon, '^': Caret, '?': Question, ';': Semicolon,
	}
	if k, ok := single[c]; ok {
		l.advance(1)
		return l.emit(k, start, "")
	}
	r, size := utf8.DecodeRuneInString(l.src[l.off:])
	l.advance(size)
	l.errs = append(l.errs, &Error{Pos: start, Msg: fmt.Sprintf("unexpected character %q", r)})
	return l.emit(Illegal, start, "")
}

// prevAllowsMember reports whether a '.' directly after the previous token
// is a member separator rather than the start of a decimal such as .5.
func (l *lexer) prevAllowsMember() bool {
	if len(l.toks) == 0 {
		return false
	}
	prev := l.toks[len(l.toks)-1]
	if prev.End.Offset != l.off {
		return false
	}
	switch prev.Kind {
	case Ident, QuotedIdent, RParen, RBracket, NumericIdent:
		return true
	}
	return false
}

func (l *lexer) quoted(q byte, kind Kind) Token {
	start := l.pos()
	l.advance(1)
	var b strings.Builder
	for l.off < len(l.src) {
		c := l.src[l.off]
		if c == '\\' && l.off+1 < len(l.src) {
			next := l.src[l.off+1]
			switch next {
			case 'n':
				b.WriteByte('\n')
			case 't':
				b.WriteByte('\t')
			case 'r':
				b.WriteByte('\r')
			case '\\', '\'', '"', '`':
				b.WriteByte(next)
			default:
				b.WriteByte('\\')
				b.WriteByte(next)
			}
			l.advance(2)
			continue
		}
		if c == q {
			if l.peekByte(1) == q {
				b.WriteByte(q)
				l.advance(2)
				continue
			}
			l.advance(1)
			return l.emit(kind, start, b.String())
		}
		r, size := utf8.DecodeRuneInString(l.src[l.off:])
		b.WriteRune(r)
		l.advance(size)
	}
	what := "string"
	if kind == QuotedIdent {
		what = "quoted identifier"
	}
	l.errs = append(l.errs, &Error{Pos: start, Msg: "unterminated " + what})
	return l.emit(Illegal, start, b.String())
}

var spanUnits = []string{
	"milliseconds", "millisecond", "microseconds", "seconds", "minutes", "quarters", "quarter",
	"second", "minute", "months", "month", "hours", "weeks", "years", "hour", "week", "year",
	"days", "secs", "mins", "hrs", "qtrs", "yrs", "sec", "min", "day", "mon", "qtr", "ms", "us",
	"cs", "ds", "hr", "yr", "s", "m", "h", "d", "w", "q", "y",
}

func (l *lexer) number() Token {
	start := l.pos()
	i := l.off
	for i < len(l.src) && isDigit(l.src[i]) {
		i++
	}
	hasFrac := false
	if i < len(l.src) && l.src[i] == '.' && i+1 < len(l.src) && isDigit(l.src[i+1]) {
		hasFrac = true
		i++
		for i < len(l.src) && isDigit(l.src[i]) {
			i++
		}
	}
	if !hasFrac && i < len(l.src) && (l.src[i] == 'e' || l.src[i] == 'E') {
		j := i + 1
		if j < len(l.src) && (l.src[j] == '+' || l.src[j] == '-') {
			j++
		}
		if j < len(l.src) && isDigit(l.src[j]) {
			for j < len(l.src) && isDigit(l.src[j]) {
				j++
			}
			if j >= len(l.src) || !isIdentPart(l.src[j]) {
				l.advance(j - l.off)
				return l.emit(Decimal, start, l.src[start.Offset:l.off])
			}
		}
	}
	numText := l.src[l.off:i]
	end := i
	for end < len(l.src) && isIdentPart(l.src[end]) {
		end++
	}
	suffix := l.src[i:end]
	if suffix == "" {
		l.advance(i - l.off)
		if hasFrac {
			return l.emit(Decimal, start, numText)
		}
		return l.emit(Integer, start, numText)
	}
	lower := strings.ToLower(suffix)
	if hasFrac && (lower == "d" || lower == "f") {
		l.advance(end - l.off)
		if lower == "f" {
			return l.emit(Float, start, numText)
		}
		return l.emit(Double, start, numText)
	}
	for _, u := range spanUnits {
		if lower == u {
			l.advance(end - l.off)
			return l.emit(Span, start, numText+lower)
		}
	}
	if hasFrac {
		l.advance(i - l.off)
		return l.emit(Decimal, start, numText)
	}
	l.advance(end - l.off)
	return l.emit(NumericIdent, start, l.src[start.Offset:l.off])
}

var snapUnits = []string{
	"seconds", "second", "minutes", "minute", "quarters", "quarter", "months", "month", "hours", "hour",
	"weeks", "week", "years", "year", "days", "day", "secs", "sec", "mins", "min", "hrs", "hr", "mon",
	"qtrs", "qtr", "yrs", "yr", "s", "m", "h", "d", "w", "q", "y",
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
	word := strings.ToLower(rest[:end])
	if len(word) == 2 && word[0] == 'w' && word[1] >= '0' && word[1] <= '7' {
		return 1 + end, true
	}
	for _, u := range snapUnits {
		if word == u {
			return 1 + end, true
		}
	}
	return 0, false
}

func (l *lexer) timeSnap() Token {
	start := l.pos()
	n, _ := l.snapLength()
	l.advance(n)
	return l.emit(TimeSnap, start, strings.ToLower(l.src[start.Offset+1:l.off]))
}

func (l *lexer) ident() Token {
	start := l.pos()
	i := l.off + 1
	for i < len(l.src) {
		c := l.src[i]
		if isIdentPart(c) {
			i++
			continue
		}
		if c >= utf8.RuneSelf {
			r, size := utf8.DecodeRuneInString(l.src[i:])
			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				i += size
				continue
			}
		}
		break
	}
	// An identifier never ends with '-': "a- b" is a minus.
	for i > l.off+1 && l.src[i-1] == '-' {
		i--
	}
	l.advance(i - l.off)
	return l.emit(Ident, start, l.src[start.Offset:l.off])
}
