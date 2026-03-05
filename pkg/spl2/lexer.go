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
	case ch == '-':
		// Could be negative number or minus operator.
		// Negative number: if previous token is an operator, keyword, comma, lparen, or start.
		if l.pos+1 < len(l.input) && isDigit(l.input[l.pos+1]) && l.isNegativeNumberContext() {
			return l.readNumber()
		}
		l.pos++

		return Token{Type: TokenMinus, Literal: "-", Pos: startPos}, nil
	case isDigit(ch):
		return l.readNumber()
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
		TokenSlash, TokenStar, TokenRegexMatch, TokenRegexNotMatch:
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

func (l *Lexer) readNumber() (Token, error) {
	startPos := l.pos

	if l.input[l.pos] == '-' {
		l.pos++
	}

	for l.pos < len(l.input) && isDigit(l.input[l.pos]) {
		l.pos++
	}

	// Decimal point.
	if l.pos < len(l.input) && l.input[l.pos] == '.' && l.pos+1 < len(l.input) && isDigit(l.input[l.pos+1]) {
		l.pos++ // skip dot
		for l.pos < len(l.input) && isDigit(l.input[l.pos]) {
			l.pos++
		}
	}

	return Token{Type: TokenNumber, Literal: l.input[startPos:l.pos], Pos: startPos}, nil
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

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isIdentStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isIdentPart(ch byte) bool {
	r := rune(ch)

	return unicode.IsLetter(r) || unicode.IsDigit(r) || ch == '_' || ch == '-' || ch == '.' || ch == '*' || ch == '?' || ch == ':'
}
