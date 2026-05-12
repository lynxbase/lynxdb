package spl2

import "strings"

// QueryLint is a post-parse warning for syntactically valid queries.
type QueryLint struct {
	Code     string `json:"code"`
	Message  string `json:"message"`
	Position int    `json:"position"`
}

const (
	LintCountWithoutParens = "L013"
	LintMixedSearchAndOr   = "L030"
)

// LintQuery parses input and returns RFC lint warnings for valid queries.
func LintQuery(input string) ([]QueryLint, error) {
	prog, err := ParseProgram(input)
	if err != nil {
		return nil, err
	}

	return LintProgram(input, prog)
}

// LintProgram returns RFC lint warnings for an already parsed program.
func LintProgram(input string, prog *Program) ([]QueryLint, error) {
	if prog == nil {
		return nil, nil
	}

	lexer := NewLexer(input)
	tokens, err := lexer.Tokenize()
	if err != nil {
		return nil, err
	}

	lints := lintCountWithoutParens(tokens)
	lints = append(lints, lintMixedSearchAndOr(input, tokens)...)

	return lints, nil
}

func lintCountWithoutParens(tokens []Token) []QueryLint {
	var lints []QueryLint
	inAggCommand := false
	afterBy := false

	for i, tok := range tokens {
		switch tok.Type {
		case TokenPipe, TokenRBracket, TokenEOF:
			inAggCommand = false
			afterBy = false
			continue
		}

		if isAggregateCommandToken(tok.Type) {
			inAggCommand = true
			afterBy = false
			continue
		}

		if !inAggCommand {
			continue
		}

		if tok.Type == TokenBy {
			afterBy = true
			continue
		}

		if afterBy {
			continue
		}

		if strings.EqualFold(tok.Literal, "count") && peekTokenType(tokens, i+1) != TokenLParen {
			lints = append(lints, QueryLint{
				Code:     LintCountWithoutParens,
				Message:  "`count` is a function; use `count()`",
				Position: tok.Pos,
			})
		}
	}

	return lints
}

func isAggregateCommandToken(t TokenType) bool {
	switch t {
	case TokenStats, TokenTimechart, TokenStreamstats, TokenEventstats,
		TokenRunning, TokenEnrich, TokenEvery, TokenImpact:
		return true
	default:
		return false
	}
}

func peekTokenType(tokens []Token, idx int) TokenType {
	if idx < 0 || idx >= len(tokens) {
		return TokenEOF
	}

	return tokens[idx].Type
}

func lintMixedSearchAndOr(input string, tokens []Token) []QueryLint {
	var lints []QueryLint

	for i := 0; i < len(tokens); i++ {
		if tokens[i].Type != TokenSearch {
			continue
		}
		startIdx := i + 1
		if startIdx >= len(tokens) {
			continue
		}
		startPos := tokens[startIdx].Pos
		endPos := len(input)
		for j := startIdx; j < len(tokens); j++ {
			switch tokens[j].Type {
			case TokenPipe, TokenRBracket, TokenSemicolon, TokenEOF:
				endPos = tokens[j].Pos
				j = len(tokens)
			}
		}
		if startPos >= endPos || startPos >= len(input) {
			continue
		}

		raw := strings.TrimSpace(input[startPos:endPos])
		if raw == "" {
			continue
		}
		if lint, ok := lintSearchMixedAndOr(raw, startPos); ok {
			lints = append(lints, lint)
		}
	}

	return lints
}

func lintSearchMixedAndOr(raw string, basePos int) (QueryLint, bool) {
	lexer := NewSearchLexer(raw)
	tokens, err := lexer.Tokenize()
	if err != nil {
		return QueryLint{}, false
	}

	sawAnd := false
	sawOr := false
	firstOpPos := -1
	prevPrimary := false

	for i := 0; i < len(tokens); i++ {
		tok := tokens[i]
		if tok.Type == STokEOF {
			break
		}
		switch tok.Type {
		case STokAND:
			sawAnd = true
			if firstOpPos < 0 {
				firstOpPos = tok.Pos
			}
			prevPrimary = false
		case STokOR:
			sawOr = true
			if firstOpPos < 0 {
				firstOpPos = tok.Pos
			}
			prevPrimary = false
		default:
			next, ok := consumeSearchPrimary(tokens, i)
			if !ok {
				prevPrimary = false
				continue
			}
			if prevPrimary {
				sawAnd = true
				if firstOpPos < 0 {
					firstOpPos = tok.Pos
				}
			}
			prevPrimary = true
			i = next - 1
		}
	}

	if !sawAnd || !sawOr {
		return QueryLint{}, false
	}

	expr, err := ParseSearchExpression(raw)
	canonical := raw
	if err == nil {
		canonical = expr.String()
	}
	if firstOpPos < 0 {
		firstOpPos = 0
	}

	return QueryLint{
		Code:     LintMixedSearchAndOr,
		Message:  "This parses as " + canonical + "; add parentheses to make it explicit",
		Position: basePos + firstOpPos,
	}, true
}

func consumeSearchPrimary(tokens []SearchToken, idx int) (int, bool) {
	if idx >= len(tokens) {
		return idx, false
	}

	switch tokens[idx].Type {
	case STokNOT:
		next, ok := consumeSearchPrimary(tokens, idx+1)
		return next, ok
	case STokLParen:
		depth := 1
		for i := idx + 1; i < len(tokens); i++ {
			switch tokens[i].Type {
			case STokLParen:
				depth++
			case STokRParen:
				depth--
				if depth == 0 {
					return i + 1, true
				}
			case STokEOF:
				return i, false
			}
		}
		return len(tokens), false
	case STokCASE, STokTERM, STokQuoted:
		return idx + 1, true
	case STokWord:
		return consumeSearchWordPrimary(tokens, idx), true
	default:
		return idx, false
	}
}

func consumeSearchWordPrimary(tokens []SearchToken, idx int) int {
	next := idx + 1
	if next >= len(tokens) {
		return next
	}
	if isSearchComparisonOp(tokens[next].Type) {
		next++
		if next < len(tokens) {
			next = consumeSearchValue(tokens, next)
		}
		return next
	}
	if tokens[next].Type == STokIN {
		next++
		if next < len(tokens) && tokens[next].Type == STokLParen {
			next++
			for next < len(tokens) && tokens[next].Type != STokRParen && tokens[next].Type != STokEOF {
				next++
			}
			if next < len(tokens) && tokens[next].Type == STokRParen {
				next++
			}
		}
	}

	return next
}

func consumeSearchValue(tokens []SearchToken, idx int) int {
	switch tokens[idx].Type {
	case STokCASE:
		return idx + 1
	default:
		return idx + 1
	}
}
