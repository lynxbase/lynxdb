package spl2

import (
	"fmt"
	"strings"
	"unicode"
)

// Lexer tokenizes SPL2 input.
type Lexer struct {
	input  string
	pos    int
	tokens []Token
}

// NewLexer creates a new lexer for the given input.
func NewLexer(input string) *Lexer {
	return &Lexer{input: input}
}

// Tokenize scans the entire input and returns all tokens.
func (l *Lexer) Tokenize() ([]Token, error) {
	for {
		tok, err := l.next()
		if err != nil {
			return nil, err
		}
		l.tokens = append(l.tokens, tok)
		if tok.Type == TokenEOF {
			break
		}
	}

	return l.tokens, nil
}

func (l *Lexer) next() (Token, error) {
	l.skipWhitespace()

	if l.pos >= len(l.input) {
		return Token{Type: TokenEOF, Pos: l.pos}, nil
	}

	ch := l.input[l.pos]
	startPos := l.pos

	switch {
	case ch == '|':
		l.pos++

		return Token{Type: TokenPipe, Literal: "|", Pos: startPos}, nil
	case ch == ',':
		l.pos++

		return Token{Type: TokenComma, Literal: ",", Pos: startPos}, nil
	case ch == '(':
		l.pos++

		return Token{Type: TokenLParen, Literal: "(", Pos: startPos}, nil
	case ch == ')':
		l.pos++

		return Token{Type: TokenRParen, Literal: ")", Pos: startPos}, nil
	case ch == '[':
		l.pos++

		return Token{Type: TokenLBracket, Literal: "[", Pos: startPos}, nil
	case ch == ']':
		l.pos++

		return Token{Type: TokenRBracket, Literal: "]", Pos: startPos}, nil
	case ch == ';':
		l.pos++

		return Token{Type: TokenSemicolon, Literal: ";", Pos: startPos}, nil
	case ch == '$':
		l.pos++

		return Token{Type: TokenDollar, Literal: "$", Pos: startPos}, nil
	case ch == '+':
		l.pos++

		return Token{Type: TokenPlus, Literal: "+", Pos: startPos}, nil
	case ch == '/':
		l.pos++

		return Token{Type: TokenSlash, Literal: "/", Pos: startPos}, nil
	case ch == '*':
		l.pos++

		return Token{Type: TokenStar, Literal: "*", Pos: startPos}, nil
	case ch == '=':
		l.pos++
		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.pos++

			return Token{Type: TokenEq, Literal: "==", Pos: startPos}, nil
		}
		if l.pos < len(l.input) && l.input[l.pos] == '~' {
			l.pos++

			return Token{Type: TokenRegexMatch, Literal: "=~", Pos: startPos}, nil
		}

		return Token{Type: TokenEq, Literal: "=", Pos: startPos}, nil
	case ch == '!':
		l.pos++
		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.pos++

			return Token{Type: TokenNeq, Literal: "!=", Pos: startPos}, nil
		}
		if l.pos < len(l.input) && l.input[l.pos] == '~' {
			l.pos++

			return Token{Type: TokenRegexNotMatch, Literal: "!~", Pos: startPos}, nil
		}

		return Token{}, fmt.Errorf("unexpected character '!' at position %d (expected '=' or '~' after '!')", startPos)
	case ch == '<':
		l.pos++
		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.pos++

			return Token{Type: TokenLte, Literal: "<=", Pos: startPos}, nil
		}

		return Token{Type: TokenLt, Literal: "<", Pos: startPos}, nil
	case ch == '>':
		l.pos++
		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.pos++

			return Token{Type: TokenGte, Literal: ">=", Pos: startPos}, nil
		}

		return Token{Type: TokenGt, Literal: ">", Pos: startPos}, nil
	case ch == '"':
		return l.readString()
	case ch == '?':
		l.pos++
		if l.pos < len(l.input) && l.input[l.pos] == '?' {
			l.pos++

			return Token{Type: TokenDoubleQuestion, Literal: "??", Pos: startPos}, nil
		}
		if l.pos < len(l.input) && l.input[l.pos] == '.' {
			l.pos++

			return Token{Type: TokenDotQuestion, Literal: "?.", Pos: startPos}, nil
		}

		return Token{Type: TokenQuestionMark, Literal: "?", Pos: startPos}, nil
	case ch == '%':
		l.pos++

		return Token{Type: TokenPercent, Literal: "%", Pos: startPos}, nil
	case ch == '.':
		// .. range operator (e.g., -7d..-1d)
		if l.pos+1 < len(l.input) && l.input[l.pos+1] == '.' {
			l.pos += 2

			return Token{Type: TokenDot, Literal: "..", Pos: startPos}, nil
		}
		// Single dot: treat as identifier part for paths like items[*].id.
		// Fall through to identifier handling.
		if isIdentPart(ch) {
			return l.readIdentOrGlob()
		}
		l.pos++

		return Token{Type: TokenDot, Literal: ".", Pos: startPos}, nil
	case ch == '-':
		// -- line comment: skip to end of line.
		if l.pos+1 < len(l.input) && l.input[l.pos+1] == '-' {
			l.skipLineComment()

			return l.next()
		}
		// Duration literal: -Nd, -Nh, -Nm, -Ns, -Nw (e.g., -1h, -7d, -30m).
		if dur, ok := l.tryReadDuration(); ok {
			return dur, nil
		}
		// Could be negative number or minus operator.
		// Negative number: if previous token is an operator, keyword, comma, lparen, or start.
		if l.pos+1 < len(l.input) && isDigit(l.input[l.pos+1]) && l.isNegativeNumberContext() {
			return l.readNumber()
		}
		l.pos++

		return Token{Type: TokenMinus, Literal: "-", Pos: startPos}, nil
	case ch == '@':
		l.pos++

		return Token{Type: TokenAt, Literal: "@", Pos: startPos}, nil
	case isDigit(ch):
		return l.readNumber()
	case ch == 'f':
		if l.pos+1 < len(l.input) && l.input[l.pos+1] == '"' {
			return l.readFString()
		}
		return l.readIdentOrGlob()
	case isIdentStart(ch):
		return l.readIdentOrGlob()
	default:
		// Characters like '.' that are valid inside identifiers but not at the
		// start. This handles cases like ".0.1" remaining after a number parse
		// inside TERM(127.0.0.1) or similar constructs in search expressions.
		if isIdentPart(ch) {
			return l.readIdentOrGlob()
		}
		l.pos++

		return Token{}, fmt.Errorf("unexpected character %q at position %d", ch, startPos)
	}
}

// isNegativeNumberContext returns true if the previous token suggests a negative number rather than subtraction.
func (l *Lexer) isNegativeNumberContext() bool {
	if len(l.tokens) == 0 {
		return true
	}
	prev := l.tokens[len(l.tokens)-1]
	switch prev.Type {
	case TokenEq, TokenNeq, TokenLt, TokenLte, TokenGt, TokenGte,
		TokenLParen, TokenComma, TokenPipe, TokenPlus, TokenMinus,
		TokenSlash, TokenStar, TokenPercent, TokenDoubleQuestion,
		TokenRegexMatch, TokenRegexNotMatch:
		return true
	}
	// After keywords like WHERE, EVAL, etc.
	switch prev.Type {
	case TokenWhere, TokenEval, TokenAnd, TokenOr, TokenNot, TokenIn:
		return true
	}

	return false
}

func (l *Lexer) readString() (Token, error) {
	startPos := l.pos
	l.pos++ // skip opening quote

	var sb strings.Builder
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '\\' && l.pos+1 < len(l.input) {
			l.pos++
			switch l.input[l.pos] {
			case '"':
				sb.WriteByte('"')
			case '\\':
				sb.WriteByte('\\')
			case 'n':
				sb.WriteByte('\n')
			case 't':
				sb.WriteByte('\t')
			default:
				sb.WriteByte('\\')
				sb.WriteByte(l.input[l.pos])
			}
			l.pos++

			continue
		}
		if ch == '"' {
			l.pos++ // skip closing quote

			return Token{Type: TokenString, Literal: sb.String(), Pos: startPos}, nil
		}
		sb.WriteByte(ch)
		l.pos++
	}

	return Token{}, fmt.Errorf("unterminated string at position %d", startPos)
}

func (l *Lexer) readFString() (Token, error) {
	startPos := l.pos
	l.pos += 2 // skip 'f"'

	var parts []FStringPart
	var literalBuf strings.Builder

	for l.pos < len(l.input) {
		ch := l.input[l.pos]

		if ch == '"' {
			l.pos++
			if literalBuf.Len() > 0 {
				parts = append(parts, FStringPart{Literal: literalBuf.String()})
			}

			return Token{Type: TokenFString, Parts: parts, Pos: startPos}, nil
		}

		if ch == '\\' && l.pos+1 < len(l.input) {
			next := l.input[l.pos+1]
			l.pos += 2
			switch next {
			case 'n':
				literalBuf.WriteByte('\n')
			case 't':
				literalBuf.WriteByte('\t')
			case '{', '}', '"', '\\':
				literalBuf.WriteByte(next)
			default:
				literalBuf.WriteByte('\\')
				literalBuf.WriteByte(next)
			}

			continue
		}

		if ch == '{' {
			l.pos++
			if l.pos < len(l.input) && l.input[l.pos] == '{' {
				// Escaped brace {{
				literalBuf.WriteByte('{')
				l.pos++

				continue
			}

			// Start interpolation
			if literalBuf.Len() > 0 {
				parts = append(parts, FStringPart{Literal: literalBuf.String()})
				literalBuf.Reset()
			}

			exprStart := l.pos
			for l.pos < len(l.input) && l.input[l.pos] != '}' && l.input[l.pos] != '"' {
				l.pos++
			}
			if l.pos >= len(l.input) || l.input[l.pos] == '"' {
				return Token{}, fmt.Errorf("unterminated f-string expression at position %d", startPos)
			}

			parts = append(parts, FStringPart{Expr: strings.TrimSpace(l.input[exprStart:l.pos])})
			l.pos++ // skip '}'

			continue
		}

		if ch == '}' && l.pos+1 < len(l.input) && l.input[l.pos+1] == '}' {
			// Escaped brace }}
			literalBuf.WriteByte('}')
			l.pos += 2

			continue
		}

		literalBuf.WriteByte(ch)
		l.pos++
	}

	return Token{}, fmt.Errorf("unterminated f-string at position %d", startPos)
}

func (l *Lexer) readNumber() (Token, error) {
	startPos := l.pos

	if l.input[l.pos] == '-' {
		l.pos++
	}

	digitStart := l.pos
	for l.pos < len(l.input) && (isDigit(l.input[l.pos]) || l.input[l.pos] == '_') {
		l.pos++
	}

	// Decimal point (also allow underscores after decimal digits).
	if l.pos < len(l.input) && l.input[l.pos] == '.' && l.pos+1 < len(l.input) && isDigit(l.input[l.pos+1]) {
		l.pos++ // skip dot
		for l.pos < len(l.input) && (isDigit(l.input[l.pos]) || l.input[l.pos] == '_') {
			l.pos++
		}
	}

	return Token{Type: TokenNumber, Literal: l.input[startPos:digitStart] + strings.ReplaceAll(l.input[digitStart:l.pos], "_", ""), Pos: startPos}, nil
}

func (l *Lexer) readIdentOrGlob() (Token, error) {
	startPos := l.pos

	for l.pos < len(l.input) && isIdentPart(l.input[l.pos]) {
		l.pos++
	}

	literal := l.input[startPos:l.pos]

	// Wildcard characters → glob token.
	if strings.ContainsAny(literal, "*?") {
		return Token{Type: TokenGlob, Literal: literal, Pos: startPos}, nil
	}

	// Keywords (case-insensitive).
	lower := strings.ToLower(literal)
	if tokType := lookupKeyword(lower); tokType != TokenIdent {
		return Token{Type: tokType, Literal: literal, Pos: startPos}, nil
	}

	return Token{Type: TokenIdent, Literal: literal, Pos: startPos}, nil
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) && (l.input[l.pos] == ' ' || l.input[l.pos] == '\t' || l.input[l.pos] == '\n' || l.input[l.pos] == '\r') {
		l.pos++
	}
}

// skipLineComment advances past a -- line comment to the end of the line.
func (l *Lexer) skipLineComment() {
	for l.pos < len(l.input) && l.input[l.pos] != '\n' {
		l.pos++
	}
}

// tryReadDuration attempts to read a duration literal like -1h, -7d, -30m@h.
// Returns the token and true if a duration was found, otherwise returns
// a zero token and false (caller should fall through to normal minus handling).
func (l *Lexer) tryReadDuration() (Token, bool) {
	if l.pos >= len(l.input) || l.input[l.pos] != '-' {
		return Token{}, false
	}

	startPos := l.pos
	i := l.pos + 1

	// Read digits.
	digitStart := i
	for i < len(l.input) && isDigit(l.input[i]) {
		i++
	}
	if i == digitStart || i >= len(l.input) {
		return Token{}, false
	}

	// Read unit: s, m, h, d, w.
	unit := l.input[i]
	if unit != 's' && unit != 'm' && unit != 'h' && unit != 'd' && unit != 'w' {
		return Token{}, false
	}
	i++

	// Optional snap-to: @h, @d, @w, @m (e.g., -1h@h means "1 hour ago, snapped to hour start").
	lit := l.input[startPos:i]
	if i < len(l.input) && l.input[i] == '@' {
		i++
		if i < len(l.input) {
			snap := l.input[i]
			if snap == 'h' || snap == 'd' || snap == 'w' || snap == 'm' || snap == 's' {
				i++
				lit = l.input[startPos:i]
			}
		}
	}

	// Verify the next character is not an identifier continuation
	// (e.g., "-1hour" should not be a duration, "-1h" should be).
	if i < len(l.input) && isIdentPart(l.input[i]) && l.input[i] != '|' && l.input[i] != ']' {
		return Token{}, false
	}

	l.pos = i

	return Token{Type: TokenDuration, Literal: lit, Pos: startPos}, true
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isIdentStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isIdentPart(ch byte) bool {
	r := rune(ch)

	return unicode.IsLetter(r) || unicode.IsDigit(r) || ch == '_' || ch == '-' || ch == '.' || ch == '*' || ch == ':'
}
