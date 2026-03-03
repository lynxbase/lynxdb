package spl2

import (
	"fmt"
	"strings"
)

// SearchTokenType represents token types for the search expression lexer.
type SearchTokenType int

const (
	STokWord   SearchTokenType = iota // bare word (may contain wildcards, dots, digits, etc.)
	STokQuoted                        // "quoted string"
	STokAND                           // AND
	STokOR                            // OR
	STokNOT                           // NOT
	STokIN                            // IN
	STokEq                            // =
	STokNeq                           // !=
	STokLt                            // <
	STokLte                           // <=
	STokGt                            // >
	STokGte                           // >=
	STokLParen                        // (
	STokRParen                        // )
	STokComma                         // ,
	STokCASE                          // CASE(content) — content stored in Literal
	STokTERM                          // TERM(content) — content stored in Literal
	STokLike                          // LIKE
	STokEOF
)

var searchTokenNames = map[SearchTokenType]string{
	STokWord:   "WORD",
	STokQuoted: "QUOTED",
	STokAND:    "AND",
	STokOR:     "OR",
	STokNOT:    "NOT",
	STokIN:     "IN",
	STokEq:     "EQ",
	STokNeq:    "NEQ",
	STokLt:     "LT",
	STokLte:    "LTE",
	STokGt:     "GT",
	STokGte:    "GTE",
	STokLParen: "LPAREN",
	STokRParen: "RPAREN",
	STokComma:  "COMMA",
	STokCASE:   "CASE",
	STokTERM:   "TERM",
	STokLike:   "LIKE",
	STokEOF:    "EOF",
}

func (t SearchTokenType) String() string {
	if s, ok := searchTokenNames[t]; ok {
		return s
	}

	return fmt.Sprintf("STOK(%d)", t)
}

// SearchToken represents a token from the search expression lexer.
type SearchToken struct {
	Type    SearchTokenType
	Literal string
	Pos     int
}

// SearchLexer tokenizes search expressions with rules specific to the
// SPL2 search command (different from the main SPL2 lexer).
type SearchLexer struct {
	input  string
	pos    int
	tokens []SearchToken
}

// NewSearchLexer creates a new search expression lexer.
func NewSearchLexer(input string) *SearchLexer {
	return &SearchLexer{input: input}
}

// Tokenize scans the entire input and returns all tokens.
func (l *SearchLexer) Tokenize() ([]SearchToken, error) {
	for {
		tok, err := l.next()
		if err != nil {
			return nil, err
		}
		l.tokens = append(l.tokens, tok)
		if tok.Type == STokEOF {
			break
		}
	}

	return l.tokens, nil
}

func (l *SearchLexer) next() (SearchToken, error) {
	l.skipWhitespace()

	if l.pos >= len(l.input) {
		return SearchToken{Type: STokEOF, Pos: l.pos}, nil
	}

	ch := l.input[l.pos]
	startPos := l.pos

	switch {
	case ch == '(':
		l.pos++

		return SearchToken{Type: STokLParen, Literal: "(", Pos: startPos}, nil
	case ch == ')':
		l.pos++

		return SearchToken{Type: STokRParen, Literal: ")", Pos: startPos}, nil
	case ch == ',':
		l.pos++

		return SearchToken{Type: STokComma, Literal: ",", Pos: startPos}, nil
	case ch == '=' && l.pos+1 < len(l.input) && l.input[l.pos+1] == '=':
		l.pos += 2

		return SearchToken{Type: STokEq, Literal: "==", Pos: startPos}, nil
	case ch == '=':
		l.pos++

		return SearchToken{Type: STokEq, Literal: "=", Pos: startPos}, nil
	case ch == '!' && l.pos+1 < len(l.input) && l.input[l.pos+1] == '=':
		l.pos += 2

		return SearchToken{Type: STokNeq, Literal: "!=", Pos: startPos}, nil
	case ch == '<':
		l.pos++
		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.pos++

			return SearchToken{Type: STokLte, Literal: "<=", Pos: startPos}, nil
		}

		return SearchToken{Type: STokLt, Literal: "<", Pos: startPos}, nil
	case ch == '>':
		l.pos++
		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.pos++

			return SearchToken{Type: STokGte, Literal: ">=", Pos: startPos}, nil
		}

		return SearchToken{Type: STokGt, Literal: ">", Pos: startPos}, nil
	case ch == '"':
		return l.readQuotedString()
	default:
		return l.readWord()
	}
}

func (l *SearchLexer) readQuotedString() (SearchToken, error) {
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

			return SearchToken{Type: STokQuoted, Literal: sb.String(), Pos: startPos}, nil
		}
		sb.WriteByte(ch)
		l.pos++
	}

	return SearchToken{}, fmt.Errorf("unterminated string at position %d", startPos)
}

func (l *SearchLexer) readWord() (SearchToken, error) {
	startPos := l.pos

	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if isSearchWordBreaker(ch) {
			break
		}
		l.pos++
	}

	if l.pos == startPos {
		return SearchToken{}, fmt.Errorf("unexpected character %q at position %d", l.input[l.pos], l.pos)
	}

	literal := l.input[startPos:l.pos]
	upper := strings.ToUpper(literal)

	// Check for CASE() and TERM() directives — word immediately followed by (
	if (upper == "CASE" || upper == "TERM") && l.pos < len(l.input) && l.input[l.pos] == '(' {
		return l.readDirective(startPos, upper)
	}

	// Check for keywords (case-insensitive)
	switch upper {
	case "AND":
		return SearchToken{Type: STokAND, Literal: literal, Pos: startPos}, nil
	case "OR":
		return SearchToken{Type: STokOR, Literal: literal, Pos: startPos}, nil
	case "NOT":
		return SearchToken{Type: STokNOT, Literal: literal, Pos: startPos}, nil
	case "IN":
		return SearchToken{Type: STokIN, Literal: literal, Pos: startPos}, nil
	case "LIKE":
		return SearchToken{Type: STokLike, Literal: literal, Pos: startPos}, nil
	}

	return SearchToken{Type: STokWord, Literal: literal, Pos: startPos}, nil
}

func (l *SearchLexer) readDirective(startPos int, directive string) (SearchToken, error) {
	l.pos++ // skip (

	// Read content until matching ), handling nested parens
	depth := 1
	contentStart := l.pos
	for l.pos < len(l.input) && depth > 0 {
		switch l.input[l.pos] {
		case '(':
			depth++
		case ')':
			depth--
		}
		if depth > 0 {
			l.pos++
		}
	}

	if depth != 0 {
		return SearchToken{}, fmt.Errorf("unterminated %s() at position %d", directive, startPos)
	}

	content := l.input[contentStart:l.pos]
	l.pos++ // skip closing )

	tokType := STokCASE
	if directive == "TERM" {
		tokType = STokTERM
	}

	return SearchToken{Type: tokType, Literal: content, Pos: startPos}, nil
}

// isSearchWordBreaker returns true if the character breaks a word token.
// Words in search expressions can contain letters, digits, dots, hyphens,
// underscores, colons, wildcards (*), slashes, and other non-operator chars.
func isSearchWordBreaker(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' ||
		ch == '=' || ch == '!' || ch == '<' || ch == '>' ||
		ch == '(' || ch == ')' || ch == ',' || ch == '"'
}

func (l *SearchLexer) skipWhitespace() {
	for l.pos < len(l.input) && (l.input[l.pos] == ' ' || l.input[l.pos] == '\t' ||
		l.input[l.pos] == '\n' || l.input[l.pos] == '\r') {
		l.pos++
	}
}
