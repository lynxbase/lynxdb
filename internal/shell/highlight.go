package shell

import (
	"strings"

	"github.com/lynxbase/lynxdb/pkg/spl2"
)

// HighlightSPL2 applies syntax highlighting to a query string using the
// SPL2 lexer's token stream. The result is suitable for rendering in the
// shell scrollback viewport.
func HighlightSPL2(input string, theme *ShellTheme) string {
	tokens, err := spl2.NewLexer(input).Tokenize()
	if err != nil {
		// On lexer error, return input unstyled.
		return input
	}

	var b strings.Builder
	b.Grow(len(input) * 2)

	lastEnd := 0

	for _, tok := range tokens {
		if tok.Type == spl2.TokenEOF {
			break
		}

		// Preserve whitespace between tokens.
		if tok.Pos > lastEnd {
			b.WriteString(input[lastEnd:tok.Pos])
		}

		// For string tokens the Literal is the unquoted/unescaped content
		// (e.g. postgres for "postgres"), but the raw input span includes
		// the quotes and escape sequences. Use the raw span so the
		// highlighted output matches the input exactly.
		literal := tok.Literal
		endPos := tok.Pos + len(tok.Literal)

		if tok.Type == spl2.TokenString {
			endPos = stringRawEnd(input, tok.Pos)
			literal = input[tok.Pos:endPos]
		}

		style := tokenStyle(tok.Type, theme)
		b.WriteString(style.Render(literal))

		lastEnd = endPos
	}

	// Append any trailing text.
	if lastEnd < len(input) {
		b.WriteString(input[lastEnd:])
	}

	return b.String()
}

// stringRawEnd returns the byte offset just past the closing quote of a
// quoted string that starts at pos in input. It handles backslash escapes
// so that \" does not terminate the scan early.
func stringRawEnd(input string, pos int) int {
	i := pos + 1 // skip opening quote

	for i < len(input) {
		if input[i] == '\\' && i+1 < len(input) {
			i += 2 // skip escape sequence

			continue
		}

		if input[i] == '"' {
			return i + 1 // past closing quote
		}

		i++
	}

	return i // unterminated string — consume to end
}

func tokenStyle(tt spl2.TokenType, t *ShellTheme) *styleWrapper {
	switch tt {
	// Commands.
	case spl2.TokenFrom, spl2.TokenWhere, spl2.TokenSearch, spl2.TokenStats,
		spl2.TokenEval, spl2.TokenSort, spl2.TokenHead, spl2.TokenTail,
		spl2.TokenTimechart, spl2.TokenRex, spl2.TokenFields, spl2.TokenTable,
		spl2.TokenDedup, spl2.TokenRename, spl2.TokenBin, spl2.TokenStreamstats,
		spl2.TokenEventstats, spl2.TokenJoin, spl2.TokenAppend, spl2.TokenMultisearch,
		spl2.TokenTransaction, spl2.TokenXyseries, spl2.TokenTop, spl2.TokenRare,
		spl2.TokenFillnull, spl2.TokenMaterialize, spl2.TokenViews, spl2.TokenDropview,
		spl2.TokenUnpackJSON, spl2.TokenUnpackLogfmt, spl2.TokenUnpackSyslog,
		spl2.TokenUnpackCombined, spl2.TokenUnpackCLF, spl2.TokenUnpackKV,
		spl2.TokenJson, spl2.TokenUnroll, spl2.TokenPackJson, spl2.TokenIndex,
		spl2.TokenLet, spl2.TokenKeep, spl2.TokenOmit, spl2.TokenSelect,
		spl2.TokenGroup, spl2.TokenCompute, spl2.TokenOrder, spl2.TokenTake,
		spl2.TokenRank, spl2.TokenEnrich, spl2.TokenParse, spl2.TokenExplode,
		spl2.TokenPack, spl2.TokenLookup:
		return &styleWrapper{t.Command}

	// Keywords.
	case spl2.TokenBy, spl2.TokenAs, spl2.TokenAnd, spl2.TokenOr,
		spl2.TokenNot, spl2.TokenIn, spl2.TokenSpan, spl2.TokenLike,
		spl2.TokenBetween, spl2.TokenIs, spl2.TokenNull, spl2.TokenTrue,
		spl2.TokenFalse, spl2.TokenUsing, spl2.TokenExtract, spl2.TokenPer,
		spl2.TokenOn, spl2.TokenInto, spl2.TokenAsc, spl2.TokenDesc:
		return &styleWrapper{t.Keyword}

	// Literals.
	case spl2.TokenString:
		return &styleWrapper{t.String}
	case spl2.TokenNumber:
		return &styleWrapper{t.Number}

	// Operators.
	case spl2.TokenEq, spl2.TokenNeq, spl2.TokenLt, spl2.TokenLte,
		spl2.TokenGt, spl2.TokenGte, spl2.TokenRegexMatch, spl2.TokenRegexNotMatch:
		return &styleWrapper{t.Operator}

	// Pipe.
	case spl2.TokenPipe:
		return &styleWrapper{t.Pipe}

	// Default — identifiers, punctuation, etc.
	default:
		return &styleWrapper{t.Field}
	}
}

// styleWrapper avoids importing lipgloss.Style directly in switch arms.
type styleWrapper struct {
	s interface{ Render(strs ...string) string }
}

func (w *styleWrapper) Render(s string) string {
	return w.s.Render(s)
}
