package spl2

import (
	"fmt"
	"strings"
)

// ParseSearchExpression parses a search expression string into an AST.
// The search command has different precedence rules than WHERE:
//
//	NOT  (highest, unary)
//	OR   (binds tighter than AND — opposite of most languages!)
//	AND  (lowest, also implicit between adjacent terms)
func ParseSearchExpression(input string) (SearchExpr, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil, fmt.Errorf("search: empty expression")
	}

	lexer := NewSearchLexer(input)
	tokens, err := lexer.Tokenize()
	if err != nil {
		return nil, fmt.Errorf("search lexer: %w", err)
	}

	p := &searchParser{tokens: tokens}
	expr, err := p.parseExpr(0)
	if err != nil {
		return nil, err
	}

	if p.peek().Type != STokEOF {
		return nil, fmt.Errorf("search: unexpected token %q at position %d",
			p.peek().Literal, p.peek().Pos)
	}

	return expr, nil
}

type searchParser struct {
	tokens []SearchToken
	pos    int
}

func (p *searchParser) peek() SearchToken {
	if p.pos >= len(p.tokens) {
		return SearchToken{Type: STokEOF}
	}

	return p.tokens[p.pos]
}

func (p *searchParser) peekAt(offset int) SearchToken {
	idx := p.pos + offset
	if idx >= len(p.tokens) {
		return SearchToken{Type: STokEOF}
	}

	return p.tokens[idx]
}

func (p *searchParser) advance() SearchToken {
	tok := p.peek()
	if tok.Type != STokEOF {
		p.pos++
	}

	return tok
}

// Precedence levels (HIGHER = TIGHTER binding):
//
//	NOT  = unary (handled in parseUnary)
//	OR   = 20
//	AND  = 10 (also implicit between adjacent terms)
const (
	precSearchAND = 10
	precSearchOR  = 20
)

// parseExpr is the main expression parser using precedence climbing.
func (p *searchParser) parseExpr(minPrec int) (SearchExpr, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	for {
		tok := p.peek()

		switch {
		case tok.Type == STokOR:
			if precSearchOR < minPrec {
				return left, nil
			}
			p.advance() // consume OR
			right, err := p.parseExpr(precSearchOR + 1)
			if err != nil {
				return nil, err
			}
			left = &SearchOrExpr{Left: left, Right: right}

		case tok.Type == STokAND:
			if precSearchAND < minPrec {
				return left, nil
			}
			p.advance() // consume AND
			right, err := p.parseExpr(precSearchAND + 1)
			if err != nil {
				return nil, err
			}
			left = &SearchAndExpr{Left: left, Right: right}

		case p.isStartOfPrimary(tok):
			// Implicit AND between adjacent terms
			if precSearchAND < minPrec {
				return left, nil
			}
			right, err := p.parseExpr(precSearchAND + 1)
			if err != nil {
				return nil, err
			}
			left = &SearchAndExpr{Left: left, Right: right}

		default:
			return left, nil
		}
	}
}

func (p *searchParser) isStartOfPrimary(tok SearchToken) bool {
	switch tok.Type {
	case STokWord, STokQuoted, STokNOT, STokLParen, STokCASE, STokTERM:
		return true
	}

	return false
}

func (p *searchParser) parseUnary() (SearchExpr, error) {
	if p.peek().Type == STokNOT {
		p.advance() // consume NOT
		operand, err := p.parseUnary()
		if err != nil {
			return nil, err
		}

		return &SearchNotExpr{Operand: operand}, nil
	}

	return p.parsePrimary()
}

func (p *searchParser) parsePrimary() (SearchExpr, error) {
	tok := p.peek()

	switch tok.Type {
	case STokLParen:
		p.advance() // consume (
		expr, err := p.parseExpr(0)
		if err != nil {
			return nil, err
		}
		if p.peek().Type != STokRParen {
			return nil, fmt.Errorf("search: expected ), got %q at position %d",
				p.peek().Literal, p.peek().Pos)
		}
		p.advance() // consume )

		return expr, nil

	case STokCASE:
		p.advance()

		return &SearchKeywordExpr{
			Value:         tok.Literal,
			HasWildcard:   strings.Contains(tok.Literal, "*"),
			CaseSensitive: true,
		}, nil

	case STokTERM:
		p.advance()

		return &SearchKeywordExpr{
			Value:       tok.Literal,
			IsTermMatch: true,
		}, nil

	case STokQuoted:
		p.advance()

		return &SearchKeywordExpr{
			Value:       tok.Literal,
			HasWildcard: strings.Contains(tok.Literal, "*"),
		}, nil

	case STokWord:
		// Check if this is a field comparison: word followed by operator
		next := p.peekAt(1)
		if isSearchComparisonOp(next.Type) {
			return p.parseFieldComparison()
		}
		// Check if this is field IN (...): word followed by IN
		if next.Type == STokIN {
			return p.parseFieldIn()
		}
		// It's a keyword (bare word) search on _raw
		p.advance()

		return &SearchKeywordExpr{
			Value:       tok.Literal,
			HasWildcard: strings.Contains(tok.Literal, "*"),
		}, nil

	default:
		return nil, fmt.Errorf("search: unexpected token %s %q at position %d",
			tok.Type, tok.Literal, tok.Pos)
	}
}

func isSearchComparisonOp(t SearchTokenType) bool {
	return t == STokEq || t == STokNeq || t == STokLt || t == STokLte ||
		t == STokGt || t == STokGte || t == STokLike
}

func (p *searchParser) parseFieldComparison() (SearchExpr, error) {
	field := p.advance() // consume field name
	opTok := p.advance() // consume operator

	op := searchTokenToCompareOp(opTok.Type)

	// Read value — can be word, quoted string, or CASE() directive
	valTok := p.peek()
	var value string
	var hasWildcard bool
	var caseSensitive bool

	switch valTok.Type {
	case STokWord:
		p.advance()
		value = valTok.Literal
		hasWildcard = strings.Contains(value, "*")
	case STokQuoted:
		p.advance()
		value = valTok.Literal
		hasWildcard = strings.Contains(value, "*")
	case STokCASE:
		p.advance()
		value = valTok.Literal
		hasWildcard = strings.Contains(value, "*")
		caseSensitive = true
	default:
		return nil, fmt.Errorf("search: expected value after %s%s, got %s %q at position %d",
			field.Literal, opTok.Literal, valTok.Type, valTok.Literal, valTok.Pos)
	}

	return &SearchCompareExpr{
		Field:         field.Literal,
		Op:            op,
		Value:         value,
		HasWildcard:   hasWildcard,
		CaseSensitive: caseSensitive,
	}, nil
}

func (p *searchParser) parseFieldIn() (SearchExpr, error) {
	field := p.advance() // consume field name
	p.advance()          // consume IN

	if p.peek().Type != STokLParen {
		return nil, fmt.Errorf("search: expected ( after IN, got %s %q at position %d",
			p.peek().Type, p.peek().Literal, p.peek().Pos)
	}
	p.advance() // consume (

	var values []SearchInValue
	for p.peek().Type != STokRParen {
		if len(values) > 0 {
			if p.peek().Type != STokComma {
				return nil, fmt.Errorf("search: expected , or ) in IN list, got %s %q at position %d",
					p.peek().Type, p.peek().Literal, p.peek().Pos)
			}
			p.advance() // consume ,
		}

		valTok := p.peek()
		switch valTok.Type {
		case STokWord:
			p.advance()
			values = append(values, SearchInValue{
				Value:       valTok.Literal,
				HasWildcard: strings.Contains(valTok.Literal, "*"),
			})
		case STokQuoted:
			p.advance()
			values = append(values, SearchInValue{
				Value:       valTok.Literal,
				HasWildcard: strings.Contains(valTok.Literal, "*"),
			})
		default:
			return nil, fmt.Errorf("search: expected value in IN list, got %s %q at position %d",
				valTok.Type, valTok.Literal, valTok.Pos)
		}
	}

	if p.peek().Type != STokRParen {
		return nil, fmt.Errorf("search: expected ) to close IN list")
	}
	p.advance() // consume )

	return &SearchInExpr{Field: field.Literal, Values: values}, nil
}

func searchTokenToCompareOp(t SearchTokenType) CompareOp {
	switch t {
	case STokEq:
		return OpEq
	case STokNeq:
		return OpNotEq
	case STokLt:
		return OpLt
	case STokLte:
		return OpLte
	case STokGt:
		return OpGt
	case STokGte:
		return OpGte
	case STokLike:
		return OpLike
	default:
		return OpEq
	}
}
