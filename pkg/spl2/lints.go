package spl2

import (
	"fmt"
	"sort"
	"strings"
)

// QueryLint is a post-parse warning for syntactically valid queries.
type QueryLint struct {
	Code     string `json:"code"`
	Message  string `json:"message"`
	Reason   string `json:"reason,omitempty"`
	Severity string `json:"severity,omitempty"`
	Position int    `json:"position"`
}

// QuerySuggestion is an advisory edit or template suggestion derived from
// parser/lint context. Applying it is always an explicit user action.
type QuerySuggestion struct {
	Text       string `json:"text"`
	Reason     string `json:"reason"`
	SourceCode string `json:"source_code,omitempty"`
	Message    string `json:"message,omitempty"`
}

const (
	LintLeadingWildcard      = "L001"
	LintDefaultSource        = "L002"
	LintIndexRewrite         = "L003"
	LintStatsCountWide       = "L004"
	LintRawExactCompare      = "L005"
	LintOptionAfterArg       = "L010"
	LintAmbiguousDedupArgs   = "L011"
	LintDoubleQuotedName     = "L012"
	LintCountWithoutParens   = "L013"
	LintShortcutAvailable    = "L020"
	LintUnsupportedCommand   = "L021"
	LintDeprecatedSort       = "L022"
	LintMixedSearchAndOr     = "L030"
	LintDeepSearchNesting    = "L031"
	LintAllSourcesHigh       = "L032"
	LintUnquotedOpValue      = "L033"
	LintReservedFieldName    = "L034"
	LintTautologicalSearch   = "L035"
	LintDefaultMetricField   = "L036"
	LintBroadSearch          = "L037"
	LintNoExtractablePattern = "L038"
	LintPCRE2RegexFeature    = "L039"
)

const (
	LintSeverityWarning = "warning"
	LintSeverityNotice  = "notice"
)

// PrepareQueryLints enriches lints for API/UX presentation and returns them in
// the RFC display order: more severe lints first, then earlier query positions.
func PrepareQueryLints(lints []QueryLint) []QueryLint {
	if len(lints) == 0 {
		return lints
	}

	out := append([]QueryLint(nil), lints...)
	for i := range out {
		if out[i].Reason == "" {
			out[i].Reason = lintReason(out[i].Code)
		}
		if out[i].Severity == "" {
			out[i].Severity = lintSeverity(out[i].Code)
		}
	}

	sort.SliceStable(out, func(i, j int) bool {
		leftRank := lintSeverityRank(out[i].Severity)
		rightRank := lintSeverityRank(out[j].Severity)
		if leftRank != rightRank {
			return leftRank < rightRank
		}
		if out[i].Position != out[j].Position {
			return lintPositionRank(out[i].Position) < lintPositionRank(out[j].Position)
		}
		if out[i].Code != out[j].Code {
			return out[i].Code < out[j].Code
		}
		return out[i].Message < out[j].Message
	})

	return out
}

// SuggestionsFromLints exposes edit suggestions as a separate RFC meta block.
func SuggestionsFromLints(lints []QueryLint) []QuerySuggestion {
	var suggestions []QuerySuggestion
	for _, lint := range lints {
		if lint.Code != LintShortcutAvailable {
			continue
		}
		text := equivalentText(lint.Message)
		if text == "" {
			continue
		}
		reason := lint.Reason
		if reason == "" {
			reason = lintReason(lint.Code)
		}
		suggestions = append(suggestions, QuerySuggestion{
			Text:       text,
			Reason:     reason,
			SourceCode: lint.Code,
			Message:    lint.Message,
		})
	}
	return suggestions
}

func equivalentText(message string) string {
	const prefix = "Equivalent: `"
	start := strings.Index(message, prefix)
	if start < 0 {
		return ""
	}
	start += len(prefix)
	end := strings.Index(message[start:], "`")
	if end < 0 {
		return ""
	}
	return message[start : start+end]
}

func lintReason(code string) string {
	switch code {
	case LintLeadingWildcard, LintAllSourcesHigh, LintTautologicalSearch, LintBroadSearch, LintNoExtractablePattern, LintPCRE2RegexFeature:
		return "slow"
	case LintDefaultSource, LintIndexRewrite, LintUnsupportedCommand, LintMixedSearchAndOr, LintDefaultMetricField:
		return "compat"
	case LintShortcutAvailable:
		return "shortcut"
	case LintStatsCountWide:
		return "schema"
	case LintOptionAfterArg, LintAmbiguousDedupArgs, LintDoubleQuotedName, LintCountWithoutParens, LintDeprecatedSort, LintUnquotedOpValue, LintReservedFieldName, LintRawExactCompare:
		return "canon"
	default:
		return "canon"
	}
}

func lintSeverity(code string) string {
	switch code {
	case LintLeadingWildcard, LintStatsCountWide, LintRawExactCompare, LintMixedSearchAndOr, LintDeepSearchNesting, LintAllSourcesHigh, LintTautologicalSearch, LintBroadSearch, LintNoExtractablePattern, LintPCRE2RegexFeature:
		return LintSeverityWarning
	default:
		return LintSeverityNotice
	}
}

func lintSeverityRank(severity string) int {
	switch severity {
	case LintSeverityWarning:
		return 0
	case LintSeverityNotice:
		return 1
	default:
		return 2
	}
}

func lintPositionRank(position int) int {
	if position < 0 {
		return int(^uint(0) >> 1)
	}
	return position
}

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
		lints := lintOriginalSourceRewrites(input)
		normalizedLints, err := LintProgram(normalized, prog)
		if err != nil {
			return lints, nil
		}
		return appendNewLintCodes(lints, normalizedLints), nil
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

func lintOriginalSourceRewrites(input string) []QueryLint {
	lexer := NewLexer(input)
	tokens, err := lexer.Tokenize()
	if err != nil {
		return nil
	}

	return lintIndexRewrite(tokens)
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
	lints = append(lints, lintStatsCountWideRange(prog)...)
	lints = append(lints, lintIndexRewrite(tokens)...)
	lints = append(lints, lintOptionAfterArg(tokens)...)
	lints = append(lints, lintAmbiguousDedupArgs(tokens)...)
	lints = append(lints, lintDoubleQuotedNames(tokens)...)
	lints = append(lints, lintCountWithoutParens(tokens)...)
	lints = append(lints, lintLynxFlowShortcuts(prog, tokens)...)
	lints = append(lints, lintDeprecatedSortSyntax(tokens)...)
	lints = append(lints, lintMixedSearchAndOr(input, tokens)...)
	lints = append(lints, lintDeepSearchNesting(prog)...)
	lints = append(lints, lintUnquotedOperatorValues(input, tokens)...)
	lints = append(lints, lintReservedFieldNames(tokens)...)
	lints = append(lints, lintTautologicalSearchWideRange(prog)...)
	lints = append(lints, lintDefaultMetricField(tokens)...)
	lints = append(lints, lintNoExtractablePatterns(prog)...)
	lints = append(lints, lintPCRE2RegexFeatures(prog)...)

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
	seen := make(map[int]bool)
	add := func(pos int) {
		if seen[pos] {
			return
		}
		seen[pos] = true
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
			cmdType := segmentCommandType(tokens, i)
			if cmdType == TokenFrom {
				for j := i + 1; j < len(tokens) && !isSegmentBoundary(tokens[j].Type); j++ {
					if tokens[j].Type == TokenString {
						add(tokens[j].Pos)
					}
				}
			} else if isUnpackCommandType(cmdType) && peekTokenType(tokens, i+1) == TokenString {
				add(tokens[i+1].Pos)
			}
		case TokenIndex:
			if peekTokenType(tokens, i+1) == TokenString {
				add(tokens[i+1].Pos)
			} else if peekTokenType(tokens, i+1) == TokenEq && peekTokenType(tokens, i+2) == TokenString {
				add(tokens[i+2].Pos)
			}
		case TokenChart:
			for j := i + 1; j < len(tokens) && !isSegmentBoundary(tokens[j].Type); j++ {
				if (tokens[j].Type == TokenOver || tokens[j].Type == TokenBy) && peekTokenType(tokens, j+1) == TokenString {
					add(tokens[j+1].Pos)
				}
			}
		case TokenTable, TokenDedup, TokenSelect, TokenKeep, TokenOmit:
			for j := i + 1; j < len(tokens) && !isSegmentBoundary(tokens[j].Type); j++ {
				if tokens[j].Type == TokenString {
					add(tokens[j].Pos)
				}
			}
		case TokenFieldformat:
			if peekTokenType(tokens, i+1) == TokenString {
				add(tokens[i+1].Pos)
			}
		case TokenRename:
			for j := i + 1; j < len(tokens) && !isSegmentBoundary(tokens[j].Type); j++ {
				if tokens[j].Type == TokenString {
					add(tokens[j].Pos)
				}
			}
		case TokenBin:
			if peekTokenType(tokens, i+1) == TokenString {
				add(tokens[i+1].Pos)
			}
		case TokenJoin:
			for j := i + 1; j < len(tokens) && tokens[j].Type != TokenLBracket && !isSegmentBoundary(tokens[j].Type); j++ {
				if tokens[j].Type == TokenString {
					add(tokens[j].Pos)
				}
			}
		case TokenLookup:
			for j := i + 1; j < len(tokens) && !isSegmentBoundary(tokens[j].Type); j++ {
				if tokens[j].Type == TokenString {
					add(tokens[j].Pos)
				}
			}
		case TokenCorrelate:
			for j := i + 1; j < len(tokens) && !isSegmentBoundary(tokens[j].Type); j++ {
				if tokens[j].Type == TokenString {
					add(tokens[j].Pos)
				}
			}
		case TokenSort, TokenOrder:
			for j := i + 1; j < len(tokens) && !isSegmentBoundary(tokens[j].Type); j++ {
				if tokens[j].Type == TokenString {
					add(tokens[j].Pos)
				}
			}
		case TokenXyseries, TokenUntable:
			for j := i + 1; j < len(tokens) && !isSegmentBoundary(tokens[j].Type); j++ {
				if tokens[j].Type == TokenString {
					add(tokens[j].Pos)
				}
			}
		case TokenTop, TokenRare:
			for j := i + 1; j < len(tokens) && !isSegmentBoundary(tokens[j].Type); j++ {
				if tokens[j].Type == TokenString {
					add(tokens[j].Pos)
				}
			}
		case TokenRank, TokenTopby, TokenBottomby:
			for j := i + 1; j < len(tokens) && !isSegmentBoundary(tokens[j].Type); j++ {
				if tokens[j].Type == TokenString {
					add(tokens[j].Pos)
				}
			}
		case TokenLatency, TokenPercentiles, TokenBaseline, TokenChanges:
			for j := i + 1; j < len(tokens) && !isSegmentBoundary(tokens[j].Type); j++ {
				if tokens[j].Type == TokenString {
					add(tokens[j].Pos)
				}
			}
		case TokenTransaction:
			for j := i + 1; j < len(tokens) && !isSegmentBoundary(tokens[j].Type); j++ {
				if tokens[j].Type == TokenString && peekTokenType(tokens, j-1) != TokenEq {
					add(tokens[j].Pos)
				}
			}
		case TokenEval, TokenLet:
			for j := i + 1; j < len(tokens) && !isSegmentBoundary(tokens[j].Type); j++ {
				if tokens[j].Type == TokenString && peekTokenType(tokens, j+1) == TokenEq {
					add(tokens[j].Pos)
				}
			}
		case TokenAs:
			if peekTokenType(tokens, i+1) == TokenString {
				add(tokens[i+1].Pos)
			}
		case TokenFields:
			cmdType := segmentCommandType(tokens, i)
			if cmdType == TokenFields {
				for j := i + 1; j < len(tokens) && !isSegmentBoundary(tokens[j].Type); j++ {
					if tokens[j].Type == TokenString {
						add(tokens[j].Pos)
					}
				}
			} else if peekTokenType(tokens, i+1) == TokenLParen {
				for j := i + 2; j < len(tokens) && tokens[j].Type != TokenRParen && tokens[j].Type != TokenEOF; j++ {
					if tokens[j].Type == TokenString {
						add(tokens[j].Pos)
					}
				}
			}
		case TokenBy:
			if isGroupByCommandType(segmentCommandType(tokens, i)) {
				for j := i + 1; j < len(tokens) && !isSegmentBoundary(tokens[j].Type); j++ {
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

func segmentCommandType(tokens []Token, pos int) TokenType {
	start := 0
	for i := pos - 1; i >= 0; i-- {
		if tokens[i].Type == TokenPipe || tokens[i].Type == TokenSemicolon || tokens[i].Type == TokenLBracket {
			start = i + 1
			break
		}
	}
	for i := start; i < len(tokens) && i <= pos; i++ {
		if tokens[i].Type != TokenEOF {
			return tokens[i].Type
		}
	}

	return TokenEOF
}

func isSegmentBoundary(t TokenType) bool {
	return t == TokenPipe || t == TokenSemicolon || t == TokenRBracket || t == TokenEOF
}

func isUnpackCommandType(t TokenType) bool {
	switch t {
	case TokenUnpackJSON, TokenUnpackLogfmt, TokenUnpackSyslog, TokenUnpackCombined,
		TokenUnpackCLF, TokenUnpackNginxError, TokenUnpackCEF, TokenUnpackKV,
		TokenUnpackDocker, TokenUnpackRedis, TokenUnpackApacheError, TokenUnpackPostgres,
		TokenUnpackMySQLSlow, TokenUnpackHAProxy, TokenUnpackLEEF, TokenUnpackW3C,
		TokenUnpackPattern:
		return true
	default:
		return false
	}
}

func isGroupByCommandType(t TokenType) bool {
	switch t {
	case TokenStats, TokenTimechart, TokenStreamstats, TokenEventstats,
		TokenRare, TokenSessionize, TokenTrace, TokenGroup, TokenEvery,
		TokenRunning, TokenEnrich, TokenRollup, TokenLatency, TokenErrors,
		TokenRate, TokenProportion, TokenPercentiles, TokenImpact,
		TokenBaseline, TokenChanges, TokenExemplars:
		return true
	default:
		return false
	}
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

func lintAmbiguousDedupArgs(tokens []Token) []QueryLint {
	var lints []QueryLint

	for i, tok := range tokens {
		if tok.Type != TokenDedup {
			continue
		}

		sawField := false
		prevField := false
		for j := i + 1; j < len(tokens); j++ {
			t := tokens[j]
			switch t.Type {
			case TokenPipe, TokenRBracket, TokenSemicolon, TokenEOF:
				goto nextDedup
			case TokenComma:
				prevField = false
				continue
			case TokenNumber:
				if sawField {
					lints = append(lints, ambiguousDedupLint(t.Pos))
					goto nextDedup
				}
				prevField = false
				continue
			}

			if isIdentLike(t.Type) || t.Type == TokenGlob {
				if prevField {
					lints = append(lints, ambiguousDedupLint(t.Pos))
					goto nextDedup
				}
				sawField = true
				prevField = true
				continue
			}

			prevField = false
		}
	nextDedup:
	}

	return lints
}

func ambiguousDedupLint(pos int) QueryLint {
	return QueryLint{
		Code:     LintAmbiguousDedupArgs,
		Message:  "Canon: `dedup [N] <field>[, <field>...]`",
		Position: pos,
	}
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

func lintStatsCountWideRange(prog *Program) []QueryLint {
	if prog == nil {
		return nil
	}

	var lints []QueryLint
	for _, ds := range prog.Datasets {
		lints = append(lints, lintStatsCountWideRangeInQuery(ds.Query)...)
	}
	lints = append(lints, lintStatsCountWideRangeInQuery(prog.Main)...)

	return lints
}

func lintStatsCountWideRangeInQuery(q *Query) []QueryLint {
	if q == nil {
		return nil
	}

	hasTimeBounds := q.Source != nil && q.Source.TimeRange != nil
	var lints []QueryLint
	for _, cmd := range q.Commands {
		switch c := cmd.(type) {
		case *SearchCommand:
			if searchExprHasField(c.Expression, "_time") {
				hasTimeBounds = true
			}
		case *WhereCommand:
			if exprHasField(c.Expr, "_time") {
				hasTimeBounds = true
			}
		case *StatsCommand:
			if !hasTimeBounds && len(c.GroupBy) == 0 && statsHasCount(c) {
				lints = append(lints, QueryLint{
					Code:     LintStatsCountWide,
					Message:  "Without `BY` returns a single value; maybe you want `BY <field>`",
					Position: 0,
				})
			}
		case *JoinCommand:
			lints = append(lints, lintStatsCountWideRangeInQuery(c.Subquery)...)
		case *AppendCommand:
			lints = append(lints, lintStatsCountWideRangeInQuery(c.Subquery)...)
		case *AppendcolsCommand:
			lints = append(lints, lintStatsCountWideRangeInQuery(c.Subquery)...)
		case *AppendpipeCommand:
			lints = append(lints, lintStatsCountWideRangeInQuery(c.Subquery)...)
		case *MultisearchCommand:
			for _, sub := range c.Searches {
				lints = append(lints, lintStatsCountWideRangeInQuery(sub)...)
			}
		case *UnionCommand:
			for _, sub := range c.Branches {
				lints = append(lints, lintStatsCountWideRangeInQuery(sub)...)
			}
		}
	}

	return lints
}

func statsHasCount(cmd *StatsCommand) bool {
	if cmd == nil {
		return false
	}
	for _, agg := range cmd.Aggregations {
		if strings.EqualFold(agg.Func, "count") {
			return true
		}
	}

	return false
}

func exprHasField(expr Expr, field string) bool {
	switch e := expr.(type) {
	case *FieldExpr:
		return strings.EqualFold(e.Name, field)
	case *CompareExpr:
		return exprHasField(e.Left, field) || exprHasField(e.Right, field)
	case *BinaryExpr:
		return exprHasField(e.Left, field) || exprHasField(e.Right, field)
	case *ArithExpr:
		return exprHasField(e.Left, field) || exprHasField(e.Right, field)
	case *NotExpr:
		return exprHasField(e.Expr, field)
	case *FuncCallExpr:
		for _, arg := range e.Args {
			if exprHasField(arg, field) {
				return true
			}
		}
	case *InExpr:
		if exprHasField(e.Field, field) {
			return true
		}
		for _, value := range e.Values {
			if exprHasField(value, field) {
				return true
			}
		}
	case *FStringExpr:
		for _, part := range e.Parts {
			if exprHasField(part.ParsedExpr, field) {
				return true
			}
		}
	}

	return false
}

func searchExprHasField(expr SearchExpr, field string) bool {
	switch e := expr.(type) {
	case *SearchAndExpr:
		return searchExprHasField(e.Left, field) || searchExprHasField(e.Right, field)
	case *SearchOrExpr:
		return searchExprHasField(e.Left, field) || searchExprHasField(e.Right, field)
	case *SearchNotExpr:
		return searchExprHasField(e.Operand, field)
	case *SearchCompareExpr:
		return strings.EqualFold(e.Field, field)
	case *SearchInExpr:
		return strings.EqualFold(e.Field, field)
	}

	return false
}

func lintTautologicalSearchWideRange(prog *Program) []QueryLint {
	if prog == nil {
		return nil
	}

	var lints []QueryLint
	for _, ds := range prog.Datasets {
		lints = append(lints, lintTautologicalSearchWideRangeInQuery(ds.Query)...)
	}
	lints = append(lints, lintTautologicalSearchWideRangeInQuery(prog.Main)...)

	return lints
}

func lintTautologicalSearchWideRangeInQuery(q *Query) []QueryLint {
	if q == nil {
		return nil
	}

	hasTimeBounds := q.Source != nil && q.Source.TimeRange != nil
	var lints []QueryLint
	for _, cmd := range q.Commands {
		switch c := cmd.(type) {
		case *SearchCommand:
			if !hasTimeBounds && searchExprIsTautology(c.Expression) {
				lints = append(lints, QueryLint{
					Code:     LintTautologicalSearch,
					Message:  "This scans everything; add a time range, source, or predicate",
					Position: 0,
				})
			}
			if searchExprHasField(c.Expression, "_time") {
				hasTimeBounds = true
			}
		case *WhereCommand:
			if exprHasField(c.Expr, "_time") {
				hasTimeBounds = true
			}
		case *JoinCommand:
			lints = append(lints, lintTautologicalSearchWideRangeInQuery(c.Subquery)...)
		case *AppendCommand:
			lints = append(lints, lintTautologicalSearchWideRangeInQuery(c.Subquery)...)
		case *AppendcolsCommand:
			lints = append(lints, lintTautologicalSearchWideRangeInQuery(c.Subquery)...)
		case *AppendpipeCommand:
			lints = append(lints, lintTautologicalSearchWideRangeInQuery(c.Subquery)...)
		case *MultisearchCommand:
			for _, sub := range c.Searches {
				lints = append(lints, lintTautologicalSearchWideRangeInQuery(sub)...)
			}
		case *UnionCommand:
			for _, sub := range c.Branches {
				lints = append(lints, lintTautologicalSearchWideRangeInQuery(sub)...)
			}
		}
	}

	return lints
}

func searchExprIsTautology(expr SearchExpr) bool {
	kw, ok := expr.(*SearchKeywordExpr)
	return ok && kw.Value == "*"
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

func lintUnquotedOperatorValues(input string, tokens []Token) []QueryLint {
	var lints []QueryLint

	for i := 0; i < len(tokens); i++ {
		if tokens[i].Type != TokenSearch {
			continue
		}
		if i+1 >= len(tokens) || tokens[i+1].Type == TokenPipe || tokens[i+1].Type == TokenEOF {
			continue
		}

		start := tokens[i+1].Pos
		end := len(input)
		for j := i + 1; j < len(tokens); j++ {
			switch tokens[j].Type {
			case TokenPipe, TokenRBracket, TokenSemicolon, TokenEOF, TokenAt:
				end = tokens[j].Pos
				j = len(tokens)
			}
		}
		if start < 0 || start >= len(input) || end < start {
			continue
		}

		lints = append(lints, lintUnquotedOperatorValuesInSearch(input[start:end], start)...)
	}

	return lints
}

func lintUnquotedOperatorValuesInSearch(expr string, basePos int) []QueryLint {
	searchTokens, err := NewSearchLexer(expr).Tokenize()
	if err != nil {
		return nil
	}

	var lints []QueryLint
	for i := 0; i < len(searchTokens); i++ {
		tok := searchTokens[i]
		if tok.Type != STokWord {
			continue
		}

		next := peekSearchTokenType(searchTokens, i+1)
		if isSearchComparisonOp(next) {
			val := peekSearchToken(searchTokens, i+2)
			if shouldLintUnquotedOperatorValue(expr, val) {
				lints = append(lints, unquotedOperatorValueLint(basePos+val.Pos))
			}
			continue
		}

		if next == STokIN && peekSearchTokenType(searchTokens, i+2) == STokLParen {
			for j := i + 3; j < len(searchTokens); j++ {
				switch searchTokens[j].Type {
				case STokRParen, STokEOF:
					i = j
					goto nextSearchToken
				case STokWord:
					if shouldLintUnquotedOperatorValue(expr, searchTokens[j]) {
						lints = append(lints, unquotedOperatorValueLint(basePos+searchTokens[j].Pos))
					}
				}
			}
		}
	nextSearchToken:
	}

	return lints
}

func peekSearchToken(tokens []SearchToken, idx int) SearchToken {
	if idx >= 0 && idx < len(tokens) {
		return tokens[idx]
	}
	return SearchToken{Type: STokEOF}
}

func peekSearchTokenType(tokens []SearchToken, idx int) SearchTokenType {
	return peekSearchToken(tokens, idx).Type
}

func shouldLintUnquotedOperatorValue(expr string, tok SearchToken) bool {
	if tok.Type != STokWord || tok.Literal == "" {
		return false
	}
	if tok.Pos >= 0 && tok.Pos < len(expr) && expr[tok.Pos] == '\'' {
		return false
	}
	return containsValueOperatorChar(tok.Literal)
}

func containsValueOperatorChar(value string) bool {
	return strings.ContainsAny(value, "/+%?&=()<>!,")
}

func unquotedOperatorValueLint(pos int) QueryLint {
	return QueryLint{
		Code:     LintUnquotedOpValue,
		Message:  "Use double quotes for literal values containing spaces or operators",
		Position: pos,
	}
}

func lintLynxFlowShortcuts(prog *Program, tokens []Token) []QueryLint {
	if prog == nil {
		return nil
	}
	skipErrors := hasCommandToken(tokens, TokenErrors)
	skipRate := hasCommandToken(tokens, TokenRate)
	skipEvery := hasCommandToken(tokens, TokenEvery)
	skipLatency := hasCommandToken(tokens, TokenLatency)

	var lints []QueryLint
	for _, ds := range prog.Datasets {
		lints = append(lints, lintLynxFlowShortcutsInQuery(ds.Query, skipErrors, skipRate, skipEvery, skipLatency)...)
	}
	lints = append(lints, lintLynxFlowShortcutsInQuery(prog.Main, skipErrors, skipRate, skipEvery, skipLatency)...)

	return lints
}

func hasCommandToken(tokens []Token, typ TokenType) bool {
	for i, tok := range tokens {
		if tok.Type == typ {
			return i == 0 || tokens[i-1].Type == TokenPipe || tokens[i-1].Type == TokenSemicolon
		}
	}
	return false
}

func lintLynxFlowShortcutsInQuery(q *Query, skipErrors bool, skipRate bool, skipEvery bool, skipLatency bool) []QueryLint {
	if q == nil {
		return nil
	}

	var lints []QueryLint
	if !skipErrors {
		for i := 0; i+1 < len(q.Commands); i++ {
			where, ok := q.Commands[i].(*WhereCommand)
			if !ok || !isErrorsWhereExpr(where.Expr) {
				continue
			}
			stats, ok := q.Commands[i+1].(*StatsCommand)
			if !ok || !isDefaultCountStats(stats) {
				continue
			}
			lints = append(lints, errorsShortcutLint(stats.GroupBy))
		}
	}
	if !skipRate {
		for _, cmd := range q.Commands {
			timechart, ok := cmd.(*TimechartCommand)
			if ok && isRateTimechart(timechart) {
				lints = append(lints, rateShortcutLint(timechart))
			}
		}
	}
	if !skipEvery {
		for i := 0; i+1 < len(q.Commands); i++ {
			bin, ok := q.Commands[i].(*BinCommand)
			if !ok || !isTimeBin(bin) {
				continue
			}
			stats, ok := q.Commands[i+1].(*StatsCommand)
			if ok && isEveryCountStats(stats) {
				lints = append(lints, everyShortcutLint(bin, stats))
			}
		}
	}
	if !skipLatency {
		for _, cmd := range q.Commands {
			timechart, ok := cmd.(*TimechartCommand)
			if !ok {
				continue
			}
			if field, ok := latencyDefaultField(timechart); ok {
				lints = append(lints, latencyShortcutLint(timechart, field))
			}
		}
	}

	for _, cmd := range q.Commands {
		switch c := cmd.(type) {
		case *JoinCommand:
			lints = append(lints, lintLynxFlowShortcutsInQuery(c.Subquery, skipErrors, skipRate, skipEvery, skipLatency)...)
		case *AppendCommand:
			lints = append(lints, lintLynxFlowShortcutsInQuery(c.Subquery, skipErrors, skipRate, skipEvery, skipLatency)...)
		case *MultisearchCommand:
			for _, sub := range c.Searches {
				lints = append(lints, lintLynxFlowShortcutsInQuery(sub, skipErrors, skipRate, skipEvery, skipLatency)...)
			}
		}
	}

	return lints
}

func isErrorsWhereExpr(expr Expr) bool {
	in, ok := expr.(*InExpr)
	if !ok || in.Negated || len(in.Values) != 2 || !isErrorsLevelField(in.Field) {
		return false
	}

	values := map[string]bool{}
	for _, value := range in.Values {
		lit, ok := value.(*LiteralExpr)
		if !ok {
			return false
		}
		values[strings.ToLower(lit.Value)] = true
	}

	return values["error"] && values["fatal"]
}

func isErrorsLevelField(expr Expr) bool {
	switch e := expr.(type) {
	case *FieldExpr:
		return strings.EqualFold(e.Name, "level")
	case *FuncCallExpr:
		if !strings.EqualFold(e.Name, "lower") || len(e.Args) != 1 {
			return false
		}
		field, ok := e.Args[0].(*FieldExpr)
		return ok && strings.EqualFold(field.Name, "level")
	default:
		return false
	}
}

func isDefaultCountStats(stats *StatsCommand) bool {
	if len(stats.Aggregations) != 1 {
		return false
	}
	agg := stats.Aggregations[0]
	return strings.EqualFold(agg.Func, "count") && len(agg.Args) == 0 && agg.Alias == ""
}

func errorsShortcutLint(groupBy []string) QueryLint {
	form := "errors"
	if len(groupBy) > 0 {
		form += " by " + strings.Join(groupBy, ", ")
	}

	savings := 7
	if len(groupBy) > 0 {
		savings += len(groupBy)
	}

	return QueryLint{
		Code:     LintShortcutAvailable,
		Message:  fmt.Sprintf("Equivalent: `%s` (shorter by %d tokens)", form, savings),
		Position: 0,
	}
}

func isRateTimechart(cmd *TimechartCommand) bool {
	if cmd == nil || len(cmd.Aggregations) != 1 {
		return false
	}
	agg := cmd.Aggregations[0]
	return strings.EqualFold(agg.Func, "count") && len(agg.Args) == 0 && strings.EqualFold(agg.Alias, "rate")
}

func rateShortcutLint(cmd *TimechartCommand) QueryLint {
	form := "rate"
	if cmd.Span != "" && cmd.Span != "1m" {
		form += " per " + cmd.Span
	}
	if len(cmd.GroupBy) > 0 {
		form += " by " + strings.Join(cmd.GroupBy, ", ")
	}

	savings := 4
	if cmd.Span != "" && cmd.Span != "1m" {
		savings += 1
	}
	if len(cmd.GroupBy) > 0 {
		savings += len(cmd.GroupBy)
	}

	return QueryLint{
		Code:     LintShortcutAvailable,
		Message:  fmt.Sprintf("Equivalent: `%s` (shorter by %d tokens)", form, savings),
		Position: 0,
	}
}

func isTimeBin(cmd *BinCommand) bool {
	return cmd != nil && strings.EqualFold(cmd.Field, "_time") && cmd.Span != "" && cmd.Alias == ""
}

func isEveryCountStats(stats *StatsCommand) bool {
	if !isDefaultCountStats(stats) || len(stats.GroupBy) == 0 {
		return false
	}
	return strings.EqualFold(stats.GroupBy[len(stats.GroupBy)-1], "_time")
}

func everyShortcutLint(bin *BinCommand, stats *StatsCommand) QueryLint {
	groupBy := stats.GroupBy[:len(stats.GroupBy)-1]

	form := "every " + bin.Span
	if len(groupBy) > 0 {
		form += " by " + strings.Join(groupBy, ", ")
	}
	form += " compute count()"

	savings := 4
	if len(groupBy) > 0 {
		savings += len(groupBy)
	}

	return QueryLint{
		Code:     LintShortcutAvailable,
		Message:  fmt.Sprintf("Equivalent: `%s` (shorter by %d tokens)", form, savings),
		Position: 0,
	}
}

func latencyDefaultField(cmd *TimechartCommand) (string, bool) {
	if cmd == nil || cmd.Span == "" || len(cmd.Aggregations) != 4 {
		return "", false
	}

	expected := []struct {
		fn    string
		alias string
	}{
		{fn: "perc50", alias: "p50"},
		{fn: "perc95", alias: "p95"},
		{fn: "perc99", alias: "p99"},
		{fn: "count", alias: "count"},
	}

	var field string
	for i, want := range expected {
		agg := cmd.Aggregations[i]
		if !strings.EqualFold(agg.Func, want.fn) || !strings.EqualFold(agg.Alias, want.alias) {
			return "", false
		}
		if want.fn == "count" {
			if len(agg.Args) != 0 {
				return "", false
			}
			continue
		}
		if len(agg.Args) != 1 {
			return "", false
		}
		argField, ok := agg.Args[0].(*FieldExpr)
		if !ok || argField.Name == "" {
			return "", false
		}
		if field == "" {
			field = argField.Name
		} else if field != argField.Name {
			return "", false
		}
	}

	return field, field != ""
}

func latencyShortcutLint(cmd *TimechartCommand, field string) QueryLint {
	form := "latency " + field + " every " + cmd.Span
	if len(cmd.GroupBy) > 0 {
		form += " by " + strings.Join(cmd.GroupBy, ", ")
	}

	savings := 8
	if len(cmd.GroupBy) > 0 {
		savings += len(cmd.GroupBy)
	}

	return QueryLint{
		Code:     LintShortcutAvailable,
		Message:  fmt.Sprintf("Equivalent: `%s` (shorter by %d tokens)", form, savings),
		Position: 0,
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

func lintNoExtractablePatterns(prog *Program) []QueryLint {
	if prog == nil {
		return nil
	}

	var lints []QueryLint
	for _, ds := range prog.Datasets {
		lints = append(lints, lintNoExtractablePatternsInQuery(ds.Query)...)
	}
	lints = append(lints, lintNoExtractablePatternsInQuery(prog.Main)...)

	return lints
}

func lintNoExtractablePatternsInQuery(q *Query) []QueryLint {
	if q == nil {
		return nil
	}

	var lints []QueryLint
	for _, cmd := range q.Commands {
		switch c := cmd.(type) {
		case *SearchCommand:
			if c.Expression != nil {
				lints = append(lints, lintNoExtractablePatternsInSearch(c.Expression)...)
			}
		case *WhereCommand:
			lints = append(lints, lintNoExtractablePatternsInExpr(c.Expr)...)
		case *JoinCommand:
			lints = append(lints, lintNoExtractablePatternsInQuery(c.Subquery)...)
		case *AppendCommand:
			lints = append(lints, lintNoExtractablePatternsInQuery(c.Subquery)...)
		case *MultisearchCommand:
			for _, sub := range c.Searches {
				lints = append(lints, lintNoExtractablePatternsInQuery(sub)...)
			}
		}
	}

	return lints
}

func lintNoExtractablePatternsInSearch(expr SearchExpr) []QueryLint {
	switch e := expr.(type) {
	case *SearchAndExpr:
		lints := lintNoExtractablePatternsInSearch(e.Left)
		return append(lints, lintNoExtractablePatternsInSearch(e.Right)...)
	case *SearchOrExpr:
		lints := lintNoExtractablePatternsInSearch(e.Left)
		return append(lints, lintNoExtractablePatternsInSearch(e.Right)...)
	case *SearchNotExpr:
		return lintNoExtractablePatternsInSearch(e.Operand)
	case *SearchKeywordExpr:
		if e.Value != "*" && e.HasWildcard && !globHasExtractableLiteral(e.Value) {
			return []QueryLint{noExtractablePatternLint()}
		}
	case *SearchCompareExpr:
		if e.Value != "*" && e.Field == "_raw" && e.HasWildcard && !globHasExtractableLiteral(e.Value) {
			return []QueryLint{noExtractablePatternLint()}
		}
	case *SearchInExpr:
		if e.Field != "_raw" {
			return nil
		}
		var lints []QueryLint
		for _, value := range e.Values {
			if value.Value != "*" && value.HasWildcard && !globHasExtractableLiteral(value.Value) {
				lints = append(lints, noExtractablePatternLint())
			}
		}
		return lints
	}

	return nil
}

func lintNoExtractablePatternsInExpr(expr Expr) []QueryLint {
	switch e := expr.(type) {
	case *BinaryExpr:
		lints := lintNoExtractablePatternsInExpr(e.Left)
		return append(lints, lintNoExtractablePatternsInExpr(e.Right)...)
	case *NotExpr:
		return lintNoExtractablePatternsInExpr(e.Expr)
	case *CompareExpr:
		var lints []QueryLint
		lints = append(lints, lintNoExtractablePatternsInExpr(e.Left)...)
		lints = append(lints, lintNoExtractablePatternsInExpr(e.Right)...)
		if !isRawFieldExpr(e.Left) {
			return lints
		}
		switch strings.ToLower(e.Op) {
		case "=~", "!~":
			if pattern, ok := literalExprValue(e.Right); ok && !regexHasExtractableLiteral(pattern) {
				lints = append(lints, noExtractablePatternLint())
			}
		case "like", "not like":
			if pattern, ok := literalExprValue(e.Right); ok && !likeHasExtractableLiteral(pattern) {
				lints = append(lints, noExtractablePatternLint())
			}
		case "=", "!=":
			if pattern, ok := globExprPattern(e.Right); ok && pattern != "*" && !globHasExtractableLiteral(pattern) {
				lints = append(lints, noExtractablePatternLint())
			}
		}
		return lints
	case *InExpr:
		var lints []QueryLint
		lints = append(lints, lintNoExtractablePatternsInExpr(e.Field)...)
		for _, value := range e.Values {
			lints = append(lints, lintNoExtractablePatternsInExpr(value)...)
		}
		return lints
	case *ArithExpr:
		lints := lintNoExtractablePatternsInExpr(e.Left)
		return append(lints, lintNoExtractablePatternsInExpr(e.Right)...)
	case *FuncCallExpr:
		var lints []QueryLint
		for _, arg := range e.Args {
			lints = append(lints, lintNoExtractablePatternsInExpr(arg)...)
		}
		return lints
	}

	return nil
}

func literalExprValue(expr Expr) (string, bool) {
	lit, ok := expr.(*LiteralExpr)
	if !ok {
		return "", false
	}
	return lit.Value, true
}

func globExprPattern(expr Expr) (string, bool) {
	glob, ok := expr.(*GlobExpr)
	if !ok {
		return "", false
	}
	return glob.Pattern, true
}

func regexHasExtractableLiteral(pattern string) bool {
	return hasExtractableLiteralRun(pattern, func(ch byte, escaped bool, inClass bool) (bool, bool) {
		if inClass {
			return false, ch == ']'
		}
		if escaped {
			switch ch {
			case 'b', 'B', 'd', 'D', 's', 'S', 'w', 'W', 'A', 'z', 'Z':
				return false, false
			default:
				return isLiteralByte(ch), false
			}
		}
		if ch == '[' {
			return false, true
		}
		switch ch {
		case '.', '^', '$', '*', '+', '?', '(', ')', '{', '}', '|':
			return false, false
		default:
			return isLiteralByte(ch), false
		}
	})
}

func globHasExtractableLiteral(pattern string) bool {
	return hasExtractableLiteralRun(pattern, func(ch byte, escaped bool, inClass bool) (bool, bool) {
		if escaped {
			return isLiteralByte(ch), false
		}
		if inClass {
			return false, ch == ']'
		}
		if ch == '[' {
			return false, true
		}
		switch ch {
		case '*', '?', '{', '}', ',', '!':
			return false, false
		default:
			return isLiteralByte(ch), false
		}
	})
}

func likeHasExtractableLiteral(pattern string) bool {
	return hasExtractableLiteralRun(pattern, func(ch byte, escaped bool, inClass bool) (bool, bool) {
		if escaped {
			return isLiteralByte(ch), false
		}
		switch ch {
		case '%', '_':
			return false, false
		default:
			return isLiteralByte(ch), false
		}
	})
}

func hasExtractableLiteralRun(pattern string, classify func(ch byte, escaped bool, inClass bool) (literal bool, closeClass bool)) bool {
	const minExtractableNgram = 3

	run := 0
	escaped := false
	inClass := false
	for i := 0; i < len(pattern); i++ {
		ch := pattern[i]
		if !escaped && ch == '\\' {
			escaped = true
			run = 0
			continue
		}
		if !escaped && !inClass && ch == '[' {
			inClass = true
			run = 0
			continue
		}

		literal, closeClass := classify(ch, escaped, inClass)
		escaped = false
		if closeClass {
			inClass = false
		}

		if literal {
			run++
			if run >= minExtractableNgram {
				return true
			}
			continue
		}
		run = 0
	}

	return false
}

func isLiteralByte(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') ||
		(ch >= 'A' && ch <= 'Z') ||
		(ch >= '0' && ch <= '9') ||
		ch == '_'
}

func noExtractablePatternLint() QueryLint {
	return QueryLint{
		Code:     LintNoExtractablePattern,
		Message:  "Pattern cannot be prefiltered efficiently; add a literal anchor if possible",
		Position: 0,
	}
}

func lintPCRE2RegexFeatures(prog *Program) []QueryLint {
	if prog == nil {
		return nil
	}

	var lints []QueryLint
	for _, ds := range prog.Datasets {
		lints = append(lints, lintPCRE2RegexFeaturesInQuery(ds.Query)...)
	}
	lints = append(lints, lintPCRE2RegexFeaturesInQuery(prog.Main)...)

	return lints
}

func lintPCRE2RegexFeaturesInQuery(q *Query) []QueryLint {
	if q == nil {
		return nil
	}

	var lints []QueryLint
	for _, cmd := range q.Commands {
		switch c := cmd.(type) {
		case *RegexCommand:
			if regexRequiresPCRE2(c.Pattern) {
				lints = append(lints, pcre2RegexFeatureLint())
			}
		case *RexCommand:
			if regexRequiresPCRE2(c.Pattern) {
				lints = append(lints, pcre2RegexFeatureLint())
			}
		case *WhereCommand:
			lints = append(lints, lintPCRE2RegexFeaturesInExpr(c.Expr)...)
		case *JoinCommand:
			lints = append(lints, lintPCRE2RegexFeaturesInQuery(c.Subquery)...)
		case *AppendCommand:
			lints = append(lints, lintPCRE2RegexFeaturesInQuery(c.Subquery)...)
		case *MultisearchCommand:
			for _, sub := range c.Searches {
				lints = append(lints, lintPCRE2RegexFeaturesInQuery(sub)...)
			}
		}
	}

	return lints
}

func lintPCRE2RegexFeaturesInExpr(expr Expr) []QueryLint {
	switch e := expr.(type) {
	case *BinaryExpr:
		lints := lintPCRE2RegexFeaturesInExpr(e.Left)
		return append(lints, lintPCRE2RegexFeaturesInExpr(e.Right)...)
	case *NotExpr:
		return lintPCRE2RegexFeaturesInExpr(e.Expr)
	case *CompareExpr:
		var lints []QueryLint
		lints = append(lints, lintPCRE2RegexFeaturesInExpr(e.Left)...)
		lints = append(lints, lintPCRE2RegexFeaturesInExpr(e.Right)...)
		if strings.EqualFold(e.Op, "=~") || strings.EqualFold(e.Op, "!~") {
			if pattern, ok := literalExprValue(e.Right); ok && regexRequiresPCRE2(pattern) {
				lints = append(lints, pcre2RegexFeatureLint())
			}
		}
		return lints
	case *InExpr:
		var lints []QueryLint
		lints = append(lints, lintPCRE2RegexFeaturesInExpr(e.Field)...)
		for _, value := range e.Values {
			lints = append(lints, lintPCRE2RegexFeaturesInExpr(value)...)
		}
		return lints
	case *ArithExpr:
		lints := lintPCRE2RegexFeaturesInExpr(e.Left)
		return append(lints, lintPCRE2RegexFeaturesInExpr(e.Right)...)
	case *FuncCallExpr:
		var lints []QueryLint
		for _, arg := range e.Args {
			lints = append(lints, lintPCRE2RegexFeaturesInExpr(arg)...)
		}
		return lints
	}

	return nil
}

func regexRequiresPCRE2(pattern string) bool {
	inClass := false
	escaped := false
	for i := 0; i < len(pattern); i++ {
		ch := pattern[i]
		if escaped {
			switch ch {
			case '1', '2', '3', '4', '5', '6', '7', '8', '9', 'K', 'R':
				return true
			case 'k':
				if i+1 < len(pattern) && pattern[i+1] == '<' {
					return true
				}
			}
			escaped = false
			continue
		}
		if ch == '\\' {
			escaped = true
			continue
		}
		if ch == '[' {
			inClass = true
			continue
		}
		if ch == ']' {
			inClass = false
			continue
		}
		if inClass {
			continue
		}
		if ch == '(' && i+2 < len(pattern) && pattern[i+1] == '?' {
			switch pattern[i+2] {
			case '=', '!', '>', '(':
				return true
			case '<':
				if i+3 < len(pattern) && (pattern[i+3] == '=' || pattern[i+3] == '!') {
					return true
				}
			}
		}
		if i+1 < len(pattern) && pattern[i+1] == '+' {
			switch ch {
			case '*', '+', '?', '}':
				return true
			}
		}
	}

	return false
}

func pcre2RegexFeatureLint() QueryLint {
	return QueryLint{
		Code:     LintPCRE2RegexFeature,
		Message:  "This requires `--regex-engine=pcre2` and may be slower",
		Position: 0,
	}
}

func lintIndexRewrite(tokens []Token) []QueryLint {
	for i, tok := range tokens {
		if tok.Type == TokenEOF {
			return nil
		}
		if tok.Type != TokenIndex {
			continue
		}
		if !isIndexSourceShortcut(tokens, i) {
			return nil
		}
		return []QueryLint{{
			Code:     LintIndexRewrite,
			Message:  "`index=X` -> `FROM X`; explicit form recommended",
			Position: tok.Pos,
		}}
	}

	return nil
}

func isIndexSourceShortcut(tokens []Token, i int) bool {
	if i != 0 && tokens[i-1].Type != TokenSemicolon {
		return false
	}
	next := peekTokenType(tokens, i+1)
	switch next {
	case TokenEq, TokenNeq, TokenIn, TokenNot, TokenIdent, TokenString, TokenGlob, TokenStar, TokenNumber:
		return true
	default:
		return false
	}
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
		if e.Value != "*" && strings.HasPrefix(e.Value, "*") {
			return []QueryLint{leadingWildcardLint()}
		}
	case *SearchCompareExpr:
		if searchCompareHasLeadingWildcard(e) {
			return []QueryLint{leadingWildcardLint()}
		}
	case *SearchInExpr:
		var lints []QueryLint
		for _, value := range e.Values {
			if value.Value != "*" && strings.HasPrefix(value.Value, "*") {
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
		return strings.HasPrefix(e.Value, "*") || strings.HasPrefix(e.Value, "%")
	case *GlobExpr:
		return strings.HasPrefix(e.Pattern, "*")
	default:
		return false
	}
}

func searchCompareHasLeadingWildcard(e *SearchCompareExpr) bool {
	if e.Value == "*" || e.Value == "%" {
		return false
	}
	if e.Op == OpLike {
		return strings.HasPrefix(e.Value, "%")
	}
	return strings.HasPrefix(e.Value, "*")
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

		if strings.EqualFold(tok.Literal, "count") && peekTokenType(tokens, i-1) != TokenAs && peekTokenType(tokens, i+1) != TokenLParen {
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
