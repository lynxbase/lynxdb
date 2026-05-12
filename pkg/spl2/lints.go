package spl2

import "strings"

// QueryLint is a post-parse warning for syntactically valid queries.
type QueryLint struct {
	Code     string `json:"code"`
	Message  string `json:"message"`
	Position int    `json:"position"`
}

const (
	LintLeadingWildcard    = "L001"
	LintDefaultSource      = "L002"
	LintIndexRewrite       = "L003"
	LintRawExactCompare    = "L005"
	LintOptionAfterArg     = "L010"
	LintDoubleQuotedName   = "L012"
	LintCountWithoutParens = "L013"
	LintUnsupportedCommand = "L021"
	LintDeprecatedSort     = "L022"
	LintMixedSearchAndOr   = "L030"
	LintDeepSearchNesting  = "L031"
	LintReservedFieldName  = "L034"
	LintDefaultMetricField = "L036"
)

// LintQuery parses input and returns RFC lint warnings for valid queries.
func LintQuery(input string) ([]QueryLint, error) {
	prog, err := ParseProgram(input)
	if err != nil {
		normalized := NormalizeQuery(input)
		if normalized == input {
			return nil, err
		}
		prog, err = ParseProgram(normalized)
		if err != nil {
			return nil, err
		}
		return LintProgram(normalized, prog)
	}

	lints, err := LintProgram(input, prog)
	if err != nil {
		return nil, err
	}
	normalized := NormalizeQuery(input)
	if normalized == input {
		return lints, nil
	}
	normalizedProg, err := ParseProgram(normalized)
	if err != nil {
		return lints, nil
	}
	normalizedLints, err := LintProgram(normalized, normalizedProg)
	if err != nil {
		return lints, nil
	}
	return appendNewLintCodes(lints, normalizedLints), nil
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

	lints := lintDefaultSource(prog, tokens)
	lints = append(lints, lintLeadingWildcards(prog)...)
	lints = append(lints, lintRawExactCompare(prog)...)
	lints = append(lints, lintIndexRewrite(tokens)...)
	lints = append(lints, lintOptionAfterArg(tokens)...)
	lints = append(lints, lintDoubleQuotedNames(tokens)...)
	lints = append(lints, lintCountWithoutParens(tokens)...)
	lints = append(lints, lintDeprecatedSortSyntax(tokens)...)
	lints = append(lints, lintMixedSearchAndOr(input, tokens)...)
	lints = append(lints, lintDeepSearchNesting(prog)...)
	lints = append(lints, lintReservedFieldNames(tokens)...)
	lints = append(lints, lintDefaultMetricField(tokens)...)

	return lints, nil
}

func appendNewLintCodes(base, extra []QueryLint) []QueryLint {
	seen := make(map[string]bool, len(base))
	for _, lint := range base {
		seen[lint.Code] = true
	}
	for _, lint := range extra {
		if seen[lint.Code] {
			continue
		}
		base = append(base, lint)
		seen[lint.Code] = true
	}
	return base
}

func lintDefaultSource(prog *Program, tokens []Token) []QueryLint {
	if prog == nil || prog.Main == nil || prog.Main.Source != nil {
		return nil
	}

	pos := 0
	if len(tokens) > 0 {
		pos = tokens[0].Pos
	}

	return []QueryLint{{
		Code:     LintDefaultSource,
		Message:  "Default source `main` is used; add `FROM` for clarity",
		Position: pos,
	}}
}

func lintDoubleQuotedNames(tokens []Token) []QueryLint {
	var lints []QueryLint
	add := func(pos int) {
		lints = append(lints, QueryLint{
			Code:     LintDoubleQuotedName,
			Message:  "Canon: single quotes `'my-field'` for names with special characters",
			Position: pos,
		})
	}

	for i := 0; i < len(tokens); i++ {
		tok := tokens[i]
		switch tok.Type {
		case TokenFrom:
			if peekTokenType(tokens, i+1) == TokenString {
				add(tokens[i+1].Pos)
			}
		case TokenIndex:
			if peekTokenType(tokens, i+1) == TokenString {
				add(tokens[i+1].Pos)
			} else if peekTokenType(tokens, i+1) == TokenEq && peekTokenType(tokens, i+2) == TokenString {
				add(tokens[i+2].Pos)
			}
		case TokenAs:
			if peekTokenType(tokens, i+1) == TokenString {
				add(tokens[i+1].Pos)
			}
		case TokenFields:
			if peekTokenType(tokens, i+1) == TokenLParen {
				for j := i + 2; j < len(tokens) && tokens[j].Type != TokenRParen && tokens[j].Type != TokenEOF; j++ {
					if tokens[j].Type == TokenString {
						add(tokens[j].Pos)
					}
				}
			}
		case TokenIdent:
			if isFieldNameOption(tok.Literal) && peekTokenType(tokens, i+1) == TokenEq && peekTokenType(tokens, i+2) == TokenString {
				add(tokens[i+2].Pos)
			}
		}
	}

	return lints
}

func isFieldNameOption(name string) bool {
	switch strings.ToLower(name) {
	case "field", "source_field", "dest_field", "weight_field",
		"trace_id", "span_id", "parent_id":
		return true
	default:
		return false
	}
}

func lintOptionAfterArg(tokens []Token) []QueryLint {
	var lints []QueryLint

	for i := 0; i < len(tokens); i++ {
		if tokens[i].Type != TokenTransaction {
			continue
		}

		seenField := false
		for j := i + 1; j < len(tokens); j++ {
			switch tokens[j].Type {
			case TokenPipe, TokenRBracket, TokenSemicolon, TokenEOF:
				j = len(tokens)
				continue
			}

			if isTransactionOptionName(tokens[j]) && peekTokenType(tokens, j+1) == TokenEq {
				if seenField {
					lints = append(lints, QueryLint{
						Code:     LintOptionAfterArg,
						Message:  "Options (`key=value`) must precede positional arguments",
						Position: tokens[j].Pos,
					})
				}
				j++
				continue
			}

			if isIdentLike(tokens[j].Type) {
				seenField = true
			}
		}
	}

	return lints
}

func lintRawExactCompare(prog *Program) []QueryLint {
	if prog == nil {
		return nil
	}

	var lints []QueryLint
	for _, ds := range prog.Datasets {
		lints = append(lints, lintRawExactCompareInQuery(ds.Query)...)
	}
	lints = append(lints, lintRawExactCompareInQuery(prog.Main)...)

	return lints
}

func lintDefaultMetricField(tokens []Token) []QueryLint {
	var lints []QueryLint

	for i := 0; i < len(tokens); i++ {
		if tokens[i].Type != TokenSlowest {
			continue
		}
		hasBy := false
		for j := i + 1; j < len(tokens); j++ {
			switch tokens[j].Type {
			case TokenPipe, TokenRBracket, TokenSemicolon, TokenEOF:
				j = len(tokens)
			case TokenBy:
				hasBy = true
			}
		}
		if !hasBy {
			lints = append(lints, QueryLint{
				Code:     LintDefaultMetricField,
				Message:  "Default field `duration_ms` used; specify it explicitly for clarity",
				Position: tokens[i].Pos,
			})
		}
	}

	return lints
}

func lintDeprecatedSortSyntax(tokens []Token) []QueryLint {
	var lints []QueryLint

	for i := 0; i < len(tokens); i++ {
		if tokens[i].Type != TokenSort {
			continue
		}
		if peekTokenType(tokens, i+1) == TokenBy {
			continue
		}

		for j := i + 1; j < len(tokens); j++ {
			switch tokens[j].Type {
			case TokenPipe, TokenRBracket, TokenSemicolon, TokenEOF:
				j = len(tokens)
				continue
			case TokenComma:
				continue
			case TokenMinus, TokenPlus:
				if isIdentLike(peekTokenType(tokens, j+1)) && isSortDirection(peekTokenType(tokens, j+2)) {
					lints = append(lints, deprecatedSortLint(tokens[j+2].Pos))
				}
				j++
			default:
				if isIdentLike(tokens[j].Type) && isSortDirection(peekTokenType(tokens, j+1)) {
					lints = append(lints, deprecatedSortLint(tokens[j+1].Pos))
					j++
				}
			}
		}
	}

	return lints
}

func lintReservedFieldNames(tokens []Token) []QueryLint {
	var lints []QueryLint

	for i := 0; i < len(tokens); i++ {
		switch tokens[i].Type {
		case TokenBy:
			allowDirections := i > 0 && (tokens[i-1].Type == TokenSort || tokens[i-1].Type == TokenOrder)
			lints = append(lints, lintReservedFieldList(tokens, i+1, allowDirections)...)
		case TokenFields, TokenTable, TokenDedup, TokenKeep, TokenOmit, TokenSelect:
			lints = append(lints, lintReservedFieldList(tokens, i+1, false)...)
		case TokenSort:
			if peekTokenType(tokens, i+1) != TokenBy {
				lints = append(lints, lintReservedSortFields(tokens, i+1)...)
			}
		}
	}

	return lints
}

func lintReservedFieldList(tokens []Token, start int, allowDirections bool) []QueryLint {
	var lints []QueryLint

	for i := start; i < len(tokens); i++ {
		tok := tokens[i]
		switch tok.Type {
		case TokenPipe, TokenRBracket, TokenSemicolon, TokenEOF:
			return lints
		case TokenComma, TokenPlus, TokenMinus:
			continue
		case TokenNumber:
			continue
		}
		if allowDirections && isSortDirection(tok.Type) {
			continue
		}
		if isReservedFieldNameToken(tok) {
			lints = append(lints, reservedFieldNameLint(tok))
		}
		if !isIdentLike(tok.Type) && tok.Type != TokenGlob {
			return lints
		}
		if peekTokenType(tokens, i+1) != TokenComma && !(allowDirections && isSortDirection(peekTokenType(tokens, i+1))) {
			return lints
		}
	}

	return lints
}

func lintReservedSortFields(tokens []Token, start int) []QueryLint {
	var lints []QueryLint

	for i := start; i < len(tokens); i++ {
		tok := tokens[i]
		switch tok.Type {
		case TokenPipe, TokenRBracket, TokenSemicolon, TokenEOF:
			return lints
		case TokenComma:
			continue
		case TokenMinus, TokenPlus:
			if isReservedFieldNameToken(tokens[i+1]) {
				lints = append(lints, reservedFieldNameLint(tokens[i+1]))
			}
			i++
			continue
		}
		if isReservedFieldNameToken(tok) {
			lints = append(lints, reservedFieldNameLint(tok))
		}
		if isSortDirection(peekTokenType(tokens, i+1)) {
			i++
		}
	}

	return lints
}

func isReservedFieldNameToken(tok Token) bool {
	return tok.Type != TokenIdent && tok.Type != TokenGlob && isIdentLike(tok.Type)
}

func reservedFieldNameLint(tok Token) QueryLint {
	return QueryLint{
		Code:     LintReservedFieldName,
		Message:  "Use single quotes for reserved-word field names",
		Position: tok.Pos,
	}
}

func isSortDirection(t TokenType) bool {
	return t == TokenAsc || t == TokenDesc
}

func deprecatedSortLint(pos int) QueryLint {
	return QueryLint{
		Code:     LintDeprecatedSort,
		Message:  "Canon: use prefix sort form such as `sort -field`",
		Position: pos,
	}
}

func lintDeepSearchNesting(prog *Program) []QueryLint {
	if prog == nil {
		return nil
	}

	var lints []QueryLint
	for _, ds := range prog.Datasets {
		lints = append(lints, lintDeepSearchNestingInQuery(ds.Query)...)
	}
	lints = append(lints, lintDeepSearchNestingInQuery(prog.Main)...)

	return lints
}

func lintDeepSearchNestingInQuery(q *Query) []QueryLint {
	if q == nil {
		return nil
	}

	var lints []QueryLint
	for _, cmd := range q.Commands {
		switch c := cmd.(type) {
		case *SearchCommand:
			if c.Expression != nil && searchBooleanDepth(c.Expression) > 5 {
				lints = append(lints, QueryLint{
					Code:     LintDeepSearchNesting,
					Message:  "Deep nesting is hard to read; consider CTEs or split into stages",
					Position: 0,
				})
			}
		case *JoinCommand:
			lints = append(lints, lintDeepSearchNestingInQuery(c.Subquery)...)
		case *AppendCommand:
			lints = append(lints, lintDeepSearchNestingInQuery(c.Subquery)...)
		case *MultisearchCommand:
			for _, sub := range c.Searches {
				lints = append(lints, lintDeepSearchNestingInQuery(sub)...)
			}
		}
	}

	return lints
}

func searchBooleanDepth(expr SearchExpr) int {
	switch e := expr.(type) {
	case *SearchAndExpr:
		return 1 + maxInt(searchBooleanDepth(e.Left), searchBooleanDepth(e.Right))
	case *SearchOrExpr:
		return 1 + maxInt(searchBooleanDepth(e.Left), searchBooleanDepth(e.Right))
	case *SearchNotExpr:
		return 1 + searchBooleanDepth(e.Operand)
	default:
		return 0
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}

	return b
}

func lintRawExactCompareInQuery(q *Query) []QueryLint {
	if q == nil {
		return nil
	}

	var lints []QueryLint
	for _, cmd := range q.Commands {
		switch c := cmd.(type) {
		case *SearchCommand:
			if c.Expression != nil {
				lints = append(lints, lintRawExactCompareInSearch(c.Expression)...)
			}
		case *WhereCommand:
			lints = append(lints, lintRawExactCompareInExpr(c.Expr)...)
		case *JoinCommand:
			lints = append(lints, lintRawExactCompareInQuery(c.Subquery)...)
		case *AppendCommand:
			lints = append(lints, lintRawExactCompareInQuery(c.Subquery)...)
		case *MultisearchCommand:
			for _, sub := range c.Searches {
				lints = append(lints, lintRawExactCompareInQuery(sub)...)
			}
		}
	}

	return lints
}

func lintRawExactCompareInSearch(expr SearchExpr) []QueryLint {
	switch e := expr.(type) {
	case *SearchAndExpr:
		lints := lintRawExactCompareInSearch(e.Left)
		return append(lints, lintRawExactCompareInSearch(e.Right)...)
	case *SearchOrExpr:
		lints := lintRawExactCompareInSearch(e.Left)
		return append(lints, lintRawExactCompareInSearch(e.Right)...)
	case *SearchNotExpr:
		return lintRawExactCompareInSearch(e.Operand)
	case *SearchCompareExpr:
		if e.Field == "_raw" && e.Op == OpEq && e.Value != "" {
			return []QueryLint{rawExactCompareLint()}
		}
	}

	return nil
}

func lintRawExactCompareInExpr(expr Expr) []QueryLint {
	switch e := expr.(type) {
	case *BinaryExpr:
		lints := lintRawExactCompareInExpr(e.Left)
		return append(lints, lintRawExactCompareInExpr(e.Right)...)
	case *NotExpr:
		return lintRawExactCompareInExpr(e.Expr)
	case *CompareExpr:
		var lints []QueryLint
		lints = append(lints, lintRawExactCompareInExpr(e.Left)...)
		lints = append(lints, lintRawExactCompareInExpr(e.Right)...)
		if e.Op == "=" && isRawFieldExpr(e.Left) {
			lints = append(lints, rawExactCompareLint())
		}
		return lints
	case *InExpr:
		var lints []QueryLint
		lints = append(lints, lintRawExactCompareInExpr(e.Field)...)
		for _, value := range e.Values {
			lints = append(lints, lintRawExactCompareInExpr(value)...)
		}
		return lints
	case *ArithExpr:
		lints := lintRawExactCompareInExpr(e.Left)
		return append(lints, lintRawExactCompareInExpr(e.Right)...)
	case *FuncCallExpr:
		var lints []QueryLint
		for _, arg := range e.Args {
			lints = append(lints, lintRawExactCompareInExpr(arg)...)
		}
		return lints
	}

	return nil
}

func isRawFieldExpr(expr Expr) bool {
	field, ok := expr.(*FieldExpr)
	return ok && field.Name == "_raw"
}

func rawExactCompareLint() QueryLint {
	return QueryLint{
		Code:     LintRawExactCompare,
		Message:  "For substring search use `_raw LIKE \"%x%\"` or `search \"x\"`",
		Position: 0,
	}
}

func lintIndexRewrite(tokens []Token) []QueryLint {
	for i := 0; i+1 < len(tokens); i++ {
		if tokens[i].Type == TokenIndex && tokens[i+1].Type == TokenEq {
			return []QueryLint{{
				Code:     LintIndexRewrite,
				Message:  "`index=X` -> `FROM X`; explicit form recommended",
				Position: tokens[i].Pos,
			}}
		}
	}

	return nil
}

func lintLeadingWildcards(prog *Program) []QueryLint {
	if prog == nil {
		return nil
	}

	var lints []QueryLint
	for _, ds := range prog.Datasets {
		lints = append(lints, lintLeadingWildcardsInQuery(ds.Query)...)
	}
	lints = append(lints, lintLeadingWildcardsInQuery(prog.Main)...)

	return lints
}

func lintLeadingWildcardsInQuery(q *Query) []QueryLint {
	if q == nil {
		return nil
	}

	var lints []QueryLint
	for _, cmd := range q.Commands {
		switch c := cmd.(type) {
		case *SearchCommand:
			if c.Expression != nil {
				lints = append(lints, lintLeadingWildcardsInSearch(c.Expression)...)
			}
		case *WhereCommand:
			lints = append(lints, lintLeadingWildcardsInExpr(c.Expr)...)
		case *JoinCommand:
			lints = append(lints, lintLeadingWildcardsInQuery(c.Subquery)...)
		case *AppendCommand:
			lints = append(lints, lintLeadingWildcardsInQuery(c.Subquery)...)
		case *MultisearchCommand:
			for _, sub := range c.Searches {
				lints = append(lints, lintLeadingWildcardsInQuery(sub)...)
			}
		}
	}

	return lints
}

func lintLeadingWildcardsInSearch(expr SearchExpr) []QueryLint {
	switch e := expr.(type) {
	case *SearchAndExpr:
		lints := lintLeadingWildcardsInSearch(e.Left)
		return append(lints, lintLeadingWildcardsInSearch(e.Right)...)
	case *SearchOrExpr:
		lints := lintLeadingWildcardsInSearch(e.Left)
		return append(lints, lintLeadingWildcardsInSearch(e.Right)...)
	case *SearchNotExpr:
		return lintLeadingWildcardsInSearch(e.Operand)
	case *SearchKeywordExpr:
		if strings.HasPrefix(e.Value, "*") {
			return []QueryLint{leadingWildcardLint()}
		}
	case *SearchCompareExpr:
		if strings.HasPrefix(e.Value, "*") {
			return []QueryLint{leadingWildcardLint()}
		}
	case *SearchInExpr:
		var lints []QueryLint
		for _, value := range e.Values {
			if strings.HasPrefix(value.Value, "*") {
				lints = append(lints, leadingWildcardLint())
			}
		}
		return lints
	}

	return nil
}

func lintLeadingWildcardsInExpr(expr Expr) []QueryLint {
	switch e := expr.(type) {
	case *BinaryExpr:
		lints := lintLeadingWildcardsInExpr(e.Left)
		return append(lints, lintLeadingWildcardsInExpr(e.Right)...)
	case *NotExpr:
		return lintLeadingWildcardsInExpr(e.Expr)
	case *CompareExpr:
		var lints []QueryLint
		lints = append(lints, lintLeadingWildcardsInExpr(e.Left)...)
		lints = append(lints, lintLeadingWildcardsInExpr(e.Right)...)
		if strings.EqualFold(e.Op, "like") && exprHasLeadingWildcard(e.Right) {
			lints = append(lints, leadingWildcardLint())
		}
		return lints
	case *InExpr:
		var lints []QueryLint
		lints = append(lints, lintLeadingWildcardsInExpr(e.Field)...)
		for _, value := range e.Values {
			lints = append(lints, lintLeadingWildcardsInExpr(value)...)
		}
		return lints
	case *ArithExpr:
		lints := lintLeadingWildcardsInExpr(e.Left)
		return append(lints, lintLeadingWildcardsInExpr(e.Right)...)
	case *FuncCallExpr:
		var lints []QueryLint
		for _, arg := range e.Args {
			lints = append(lints, lintLeadingWildcardsInExpr(arg)...)
		}
		return lints
	}

	return nil
}

func exprHasLeadingWildcard(expr Expr) bool {
	switch e := expr.(type) {
	case *LiteralExpr:
		return strings.HasPrefix(e.Value, "*")
	case *GlobExpr:
		return strings.HasPrefix(e.Pattern, "*")
	default:
		return false
	}
}

func leadingWildcardLint() QueryLint {
	return QueryLint{
		Code:     LintLeadingWildcard,
		Message:  "Leading wildcard slows the query; consider an anchor",
		Position: 0,
	}
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
