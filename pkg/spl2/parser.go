package spl2

import (
	"fmt"
	"strconv"
	"strings"
)

// maxExprDepth is the maximum recursion depth for expression parsing.
// This prevents stack overflow from deeply nested queries like NOT NOT NOT...
const maxExprDepth = 128

// ErrQueryTooComplex is returned when a query exceeds the maximum expression nesting depth.
var ErrQueryTooComplex = fmt.Errorf("spl2: query too complex: expression nesting exceeds %d levels", maxExprDepth)

// Parser is a recursive descent parser for SPL2 queries.
type Parser struct {
	tokens []Token
	pos    int
	input  string // original input for search sub-parsing
	depth  int    // current expression parsing recursion depth
}

// Parse parses a single SPL2 query string and returns the AST.
func Parse(input string) (*Query, error) {
	lexer := NewLexer(input)
	tokens, err := lexer.Tokenize()
	if err != nil {
		return nil, fmt.Errorf("spl2: lexer error: %w", err)
	}

	p := &Parser{tokens: tokens, input: input}

	return p.parseQuery()
}

// ParseProgram parses a full SPL2 program with optional CTEs.
func ParseProgram(input string) (*Program, error) {
	lexer := NewLexer(input)
	tokens, err := lexer.Tokenize()
	if err != nil {
		return nil, fmt.Errorf("spl2: lexer error: %w", err)
	}

	p := &Parser{tokens: tokens, input: input}

	return p.parseProgram()
}

func (p *Parser) peek() Token {
	if p.pos >= len(p.tokens) {
		return Token{Type: TokenEOF}
	}

	return p.tokens[p.pos]
}

func (p *Parser) peekAt(offset int) Token {
	idx := p.pos + offset
	if idx >= len(p.tokens) {
		return Token{Type: TokenEOF}
	}

	return p.tokens[idx]
}

func (p *Parser) advance() Token {
	tok := p.peek()
	if tok.Type != TokenEOF {
		p.pos++
	}

	return tok
}

func (p *Parser) expect(typ TokenType) (Token, error) {
	tok := p.peek()
	if tok.Type != typ {
		return tok, fmt.Errorf("spl2: expected %s, got %s %q at position %d", typ, tok.Type, tok.Literal, tok.Pos)
	}

	return p.advance(), nil
}

func (p *Parser) parseProgram() (*Program, error) {
	prog := &Program{}

	// Parse CTEs: $name = query ;
	for p.peek().Type == TokenDollar {
		ds, err := p.parseDatasetDef()
		if err != nil {
			return nil, err
		}
		prog.Datasets = append(prog.Datasets, ds)
	}

	// Parse main query.
	main, err := p.parseQuery()
	if err != nil {
		return nil, err
	}
	prog.Main = main

	return prog, nil
}

func (p *Parser) parseDatasetDef() (DatasetDef, error) {
	if _, err := p.expect(TokenDollar); err != nil {
		return DatasetDef{}, err
	}
	name, err := p.expect(TokenIdent)
	if err != nil {
		return DatasetDef{}, err
	}
	if _, err := p.expect(TokenEq); err != nil {
		return DatasetDef{}, err
	}

	q, err := p.parseQuery()
	if err != nil {
		return DatasetDef{}, err
	}

	// Consume optional semicolon.
	if p.peek().Type == TokenSemicolon {
		p.advance()
	}

	return DatasetDef{Name: name.Literal, Query: q}, nil
}

func (p *Parser) parseQuery() (*Query, error) {
	q := &Query{}

	// Parse optional source clause: FROM or INDEX (aliases).
	switch p.peek().Type {
	case TokenFrom:
		p.advance()
		// Catch from="name" — invalid syntax, provide helpful error.
		if p.peek().Type == TokenEq {
			return nil, fmt.Errorf("spl2: unexpected '=' after 'from' at position %d (use 'from nginx' without '=' or 'index=\"nginx\"')", p.peek().Pos)
		}
		// Could be FROM $variable or FROM index_name or FROM a, b, c or FROM logs*
		if p.peek().Type == TokenDollar {
			p.advance()
			tok, err := p.expect(TokenIdent)
			if err != nil {
				return nil, err
			}
			q.Source = &SourceClause{Index: tok.Literal, IsVariable: true}
		} else {
			sc, err := p.parseSourceClause()
			if err != nil {
				return nil, err
			}
			q.Source = sc
		}

	case TokenIndex:
		// INDEX is an alias for FROM as a source command.
		// Supports: index nginx, index="nginx", index=nginx
		sc, searchCmd, err := p.parseIndexSource()
		if err != nil {
			return nil, err
		}
		q.Source = sc
		// index="nginx" status>=500 desugars to: from nginx | search status>=500
		if searchCmd != nil {
			q.Commands = append(q.Commands, searchCmd)
		}
	}

	// Parse optional WHERE clause directly after FROM (without pipe).
	if q.Source != nil && len(q.Commands) == 0 && p.peek().Type == TokenWhere {
		cmd, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		q.Commands = append(q.Commands, cmd)
	}

	// Parse pipeline commands separated by pipes.
	for {
		tok := p.peek()
		if tok.Type == TokenEOF || tok.Type == TokenSemicolon ||
			tok.Type == TokenRBracket {
			break
		}

		if tok.Type == TokenPipe {
			p.advance()
		} else if len(q.Commands) > 0 {
			// If we already have commands and no pipe, we're done.
			break
		} else if q.Source != nil {
			// After FROM + optional WHERE, need a pipe for more commands.
			break
		}

		cmds, err := p.parseCommand()
		if err != nil {
			return nil, err
		}
		q.Commands = append(q.Commands, cmds...)
	}

	return q, nil
}

// parseIndexSource handles the INDEX keyword as a source command.
// It supports three forms:
//   - index nginx           → same as: from nginx
//   - index="nginx"         → same as: from nginx (SPL1 compat)
//   - index=nginx           → same as: from nginx (SPL1 compat)
//
// When the index= form is followed by additional tokens before a pipe,
// those tokens are desugared into a search command:
//
//	index="nginx" status>=500  →  from nginx | search status>=500
func (p *Parser) parseIndexSource() (*SourceClause, *SearchCommand, error) {
	p.advance() // consume "index"

	// index=<name> or index="name" (SPL1 compat: equals syntax)
	if p.peek().Type == TokenEq {
		p.advance() // consume "="
		name, err := p.parseSourceName()
		if err != nil {
			return nil, nil, fmt.Errorf("spl2: index= requires a source name: %w", err)
		}

		sc := &SourceClause{Index: name}
		if strings.Contains(name, "*") || strings.Contains(name, "?") {
			sc.IsGlob = true
		}

		// Check for trailing search predicates: index="nginx" status>=500 ...
		// These are desugared into a SearchCommand.
		searchCmd, err := p.parseImplicitSearch()
		if err != nil {
			return nil, nil, err
		}

		return sc, searchCmd, nil
	}

	// index <source> (bare form, same as: from <source>)
	if p.peek().Type == TokenDollar {
		p.advance()
		tok, err := p.expect(TokenIdent)
		if err != nil {
			return nil, nil, err
		}

		return &SourceClause{Index: tok.Literal, IsVariable: true}, nil, nil
	}

	sc, err := p.parseSourceClause()
	if err != nil {
		return nil, nil, err
	}

	return sc, nil, nil
}

// parseImplicitSearch checks for trailing tokens after an index=<name> form
// and, if present, parses them as an implicit search expression. Returns nil
// if there are no trailing search tokens (i.e., the next token is pipe, EOF,
// semicolon, or bracket).
func (p *Parser) parseImplicitSearch() (*SearchCommand, error) {
	// No trailing tokens — nothing to desugar.
	next := p.peek()
	if next.Type == TokenPipe || next.Type == TokenEOF ||
		next.Type == TokenSemicolon || next.Type == TokenRBracket {
		return nil, nil
	}

	// Extract raw text from current position to next pipe/EOF/bracket.
	startPos := next.Pos
	endPos := len(p.input)
	for p.peek().Type != TokenPipe && p.peek().Type != TokenEOF &&
		p.peek().Type != TokenRBracket && p.peek().Type != TokenSemicolon {
		p.advance()
	}
	if p.peek().Type != TokenEOF {
		endPos = p.peek().Pos
	}

	rawExpr := strings.TrimSpace(p.input[startPos:endPos])
	if rawExpr == "" {
		return nil, nil
	}

	expr, err := ParseSearchExpression(rawExpr)
	if err != nil {
		return nil, fmt.Errorf("spl2: implicit search after index=: %w", err)
	}

	cmd := &SearchCommand{Expression: expr}
	if kw, ok := expr.(*SearchKeywordExpr); ok {
		cmd.Term = kw.Value
	}

	return cmd, nil
}

// singleCmd wraps a single Command result into []Command for parseCommand return.
func singleCmd(cmd Command, err error) ([]Command, error) {
	if err != nil {
		return nil, err
	}

	return []Command{cmd}, nil
}

func (p *Parser) parseCommand() ([]Command, error) {
	tok := p.peek()

	switch tok.Type {
	case TokenSearch:
		return singleCmd(p.parseSearch())
	case TokenWhere:
		return singleCmd(p.parseWhere())
	case TokenStats:
		return singleCmd(p.parseStats())
	case TokenEval:
		return singleCmd(p.parseEval())
	case TokenSort:
		return singleCmd(p.parseSort())
	case TokenHead:
		return singleCmd(p.parseHead())
	case TokenTail:
		return singleCmd(p.parseTail())
	case TokenTimechart:
		return singleCmd(p.parseTimechart())
	case TokenRex:
		return singleCmd(p.parseRex())
	case TokenFields:
		return singleCmd(p.parseFields())
	case TokenTable:
		return singleCmd(p.parseTable())
	case TokenDedup:
		return singleCmd(p.parseDedup())
	case TokenRename:
		return singleCmd(p.parseRename())
	case TokenBin:
		return singleCmd(p.parseBin())
	case TokenStreamstats:
		return singleCmd(p.parseStreamstats())
	case TokenEventstats:
		return singleCmd(p.parseEventstats())
	case TokenJoin:
		return singleCmd(p.parseJoin())
	case TokenAppend:
		return singleCmd(p.parseAppend())
	case TokenMultisearch:
		return singleCmd(p.parseMultisearch())
	case TokenTransaction:
		return singleCmd(p.parseTransaction())
	case TokenXyseries:
		return singleCmd(p.parseXYSeries())
	case TokenTop:
		return p.parseTopOrTopby()
	case TokenRare:
		return singleCmd(p.parseRare())
	case TokenFillnull:
		return singleCmd(p.parseFillnull())
	case TokenMaterialize:
		return singleCmd(p.parseMaterialize())
	case TokenFrom:
		return singleCmd(p.parseFromCommand())
	case TokenViews:
		return singleCmd(p.parseViews())
	case TokenDropview:
		return singleCmd(p.parseDropview())
	case TokenUnpackJSON:
		return singleCmd(p.parseUnpack("json"))
	case TokenUnpackLogfmt:
		return singleCmd(p.parseUnpack("logfmt"))
	case TokenUnpackSyslog:
		return singleCmd(p.parseUnpack("syslog"))
	case TokenUnpackCombined:
		return singleCmd(p.parseUnpack("combined"))
	case TokenUnpackCLF:
		return singleCmd(p.parseUnpack("clf"))
	case TokenUnpackNginxError:
		return singleCmd(p.parseUnpack("nginx_error"))
	case TokenUnpackCEF:
		return singleCmd(p.parseUnpack("cef"))
	case TokenUnpackKV:
		return singleCmd(p.parseUnpack("kv"))
	case TokenUnpackDocker:
		return singleCmd(p.parseUnpack("docker"))
	case TokenUnpackRedis:
		return singleCmd(p.parseUnpack("redis"))
	case TokenUnpackApacheError:
		return singleCmd(p.parseUnpack("apache_error"))
	case TokenUnpackPostgres:
		return singleCmd(p.parseUnpack("postgres"))
	case TokenUnpackMySQLSlow:
		return singleCmd(p.parseUnpack("mysql_slow"))
	case TokenUnpackHAProxy:
		return singleCmd(p.parseUnpack("haproxy"))
	case TokenUnpackLEEF:
		return singleCmd(p.parseUnpack("leef"))
	case TokenUnpackW3C:
		return singleCmd(p.parseUnpack("w3c"))
	case TokenUnpackPattern:
		return singleCmd(p.parseUnpackPattern())
	case TokenJson:
		return singleCmd(p.parseJsonCmd())
	case TokenUnroll:
		return singleCmd(p.parseUnroll())
	case TokenPackJson:
		return singleCmd(p.parsePackJson())

	// Lynx Flow commands.
	case TokenLet:
		return singleCmd(p.parseLet())
	case TokenKeep:
		return singleCmd(p.parseKeep())
	case TokenOmit:
		return singleCmd(p.parseOmit())
	case TokenSelect:
		return singleCmd(p.parseSelectCmd())
	case TokenGroup:
		return singleCmd(p.parseGroup())
	case TokenEvery:
		return singleCmd(p.parseEvery())
	case TokenBucket:
		return singleCmd(p.parseBucketCmd())
	case TokenOrder:
		return singleCmd(p.parseOrderBy())
	case TokenTake:
		return singleCmd(p.parseTake())
	case TokenRank:
		return p.parseRank()
	case TokenTopby:
		return p.parseTopbyCmd()
	case TokenBottomby:
		return p.parseBottombyCmd()
	case TokenBottom:
		return p.parseBottomCmd()
	case TokenRunning:
		return singleCmd(p.parseRunning())
	case TokenEnrich:
		return singleCmd(p.parseEnrichCmd())
	case TokenParse:
		return singleCmd(p.parseLynxParse())
	case TokenExplode:
		return singleCmd(p.parseExplode())
	case TokenPack:
		return singleCmd(p.parseLynxPack())
	case TokenLookup:
		return singleCmd(p.parseLookupCmd())
	case TokenLatency:
		return singleCmd(p.parseLatency())
	case TokenErrors:
		return p.parseErrorsCmd()
	case TokenRate:
		return singleCmd(p.parseRateCmd())
	case TokenPercentiles:
		return singleCmd(p.parsePercentilesCmd())
	case TokenSlowest:
		return p.parseSlowestCmd()

	case TokenEOF:
		return nil, nil
	default:
		// Lynx Flow: bare expression as implicit where.
		// After a pipe, if the token starts a boolean expression, treat the
		// segment as: | where <expr>. This is an "Accepted alias (REPL shorthand)"
		// from the spec: `from nginx | status >= 500` === `from nginx | where status >= 500`.
		//
		// For TokenIdent, we require the NEXT token to be a comparison/logical
		// operator to avoid misinterpreting misspelled command names (like "stauts")
		// as field references. For NOT and (, the intent is unambiguous.
		switch tok.Type {
		case TokenNot, TokenLParen:
			expr, err := p.parseExpr()
			if err != nil {
				return nil, fmt.Errorf("spl2: unexpected token %s %q at position %d", tok.Type, tok.Literal, tok.Pos)
			}

			return []Command{&WhereCommand{Expr: expr}}, nil
		case TokenIdent:
			if isBareExprContinuation(p.peekAt(1).Type) || p.peekAt(1).Type == TokenLParen {
				expr, err := p.parseExpr()
				if err != nil {
					return nil, fmt.Errorf("spl2: unexpected token %s %q at position %d", tok.Type, tok.Literal, tok.Pos)
				}

				return []Command{&WhereCommand{Expr: expr}}, nil
			}
		}

		return nil, fmt.Errorf("spl2: unexpected command %s %q at position %d", tok.Type, tok.Literal, tok.Pos)
	}
}

func (p *Parser) parseSearch() (Command, error) {
	p.advance() // consume "search"

	// Check for SEARCH index=<idx> syntax.
	// TokenIndex is now a keyword, so check both TokenIndex and TokenIdent
	// for backward compatibility with any edge cases.
	if (p.peek().Type == TokenIndex || (p.peek().Type == TokenIdent && strings.ToLower(p.peek().Literal) == "index")) &&
		p.peekAt(1).Type == TokenEq {
		p.advance() // consume "index"
		p.advance() // consume "="
		idxName, err := p.parseSourceName()
		if err != nil {
			return nil, err
		}
		cmd := &SearchCommand{Index: idxName}
		// Parse additional predicates.
		for p.peek().Type != TokenPipe && p.peek().Type != TokenEOF &&
			p.peek().Type != TokenRBracket {
			pred, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			cmd.Predicates = append(cmd.Predicates, pred)
		}

		return cmd, nil
	}

	// Extract raw search expression text from current position to next pipe/EOF.
	startTok := p.peek()
	if startTok.Type == TokenEOF || startTok.Type == TokenPipe {
		return nil, fmt.Errorf("spl2: search requires an expression at position %d", startTok.Pos)
	}

	startPos := startTok.Pos

	// Find end position by scanning to next pipe, EOF, or bracket.
	endPos := len(p.input)
	savedPos := p.pos
	for p.peek().Type != TokenPipe && p.peek().Type != TokenEOF &&
		p.peek().Type != TokenRBracket && p.peek().Type != TokenSemicolon {
		p.advance()
	}
	if p.peek().Type != TokenEOF {
		endPos = p.peek().Pos
	}
	// p.pos is now past the search expression tokens — leave it there.

	rawExpr := strings.TrimSpace(p.input[startPos:endPos])

	expr, err := ParseSearchExpression(rawExpr)
	if err != nil {
		// Fall back to legacy single-term behavior for backward compatibility.
		p.pos = savedPos
		tok := p.peek()
		switch tok.Type {
		case TokenString:
			p.advance()

			return &SearchCommand{Term: tok.Literal}, nil
		case TokenIdent, TokenGlob:
			p.advance()

			return &SearchCommand{Term: tok.Literal}, nil
		default:
			return nil, fmt.Errorf("spl2: search: %w", err)
		}
	}

	cmd := &SearchCommand{Expression: expr}
	// Also set Term for backward compatibility when the expression is a simple keyword/glob.
	if kw, ok := expr.(*SearchKeywordExpr); ok {
		cmd.Term = kw.Value
	}

	return cmd, nil
}

func (p *Parser) parseWhere() (*WhereCommand, error) {
	p.advance() // consume "where"

	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	return &WhereCommand{Expr: expr}, nil
}

func (p *Parser) parseStats() (*StatsCommand, error) {
	p.advance() // consume "stats"

	aggs, err := p.parseAggList()
	if err != nil {
		return nil, err
	}

	var groupBy []string
	if p.peek().Type == TokenBy {
		p.advance()
		groupBy, err = p.parseIdentList()
		if err != nil {
			return nil, err
		}
	}

	return &StatsCommand{Aggregations: aggs, GroupBy: groupBy}, nil
}

func (p *Parser) parseEval() (*EvalCommand, error) {
	p.advance() // consume "eval"

	return p.parseEvalBody()
}

func (p *Parser) parseSort() (*SortCommand, error) {
	p.advance() // consume "sort"

	// Lynx Flow: "sort by <field> [asc|desc], ..." → order-by style parsing.
	if p.peek().Type == TokenBy {
		p.advance()

		return p.parseOrderByFields()
	}

	var fields []SortField
	for {
		tok := p.peek()
		if tok.Type == TokenPipe || tok.Type == TokenEOF || tok.Type == TokenRBracket {
			break
		}

		if tok.Type == TokenMinus {
			p.advance()
			name := p.peek()
			if !isIdentLike(name.Type) {
				return nil, fmt.Errorf("spl2: expected field name after - in sort, got %s", name.Type)
			}
			p.advance()
			fields = append(fields, SortField{Name: name.Literal, Desc: true})
		} else if tok.Type == TokenPlus {
			p.advance()
			name := p.peek()
			if !isIdentLike(name.Type) {
				return nil, fmt.Errorf("spl2: expected field name after + in sort, got %s", name.Type)
			}
			p.advance()
			fields = append(fields, SortField{Name: name.Literal})
		} else if isIdentLike(tok.Type) {
			p.advance()
			name := tok.Literal
			if strings.HasPrefix(name, "-") {
				fields = append(fields, SortField{Name: name[1:], Desc: true})
			} else {
				fields = append(fields, SortField{Name: name})
			}
		} else if tok.Type == TokenNumber && strings.HasPrefix(tok.Literal, "-") {
			// Handle sort -count where lexer reads it as negative number.
			p.advance()
			fields = append(fields, SortField{Name: tok.Literal[1:], Desc: true})
		} else {
			break
		}

		if p.peek().Type == TokenComma {
			p.advance()
		}
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("spl2: sort requires at least one field")
	}

	return &SortCommand{Fields: fields}, nil
}

func (p *Parser) parseHead() (*HeadCommand, error) {
	p.advance() // consume "head"

	count := 10 // default
	if p.peek().Type == TokenNumber {
		tok := p.advance()
		n, err := strconv.Atoi(tok.Literal)
		if err != nil {
			return nil, fmt.Errorf("spl2: invalid head count %q", tok.Literal)
		}
		count = n
	}

	return &HeadCommand{Count: count}, nil
}

func (p *Parser) parseTail() (*TailCommand, error) {
	p.advance() // consume "tail"

	count := 10 // default
	if p.peek().Type == TokenNumber {
		tok := p.advance()
		n, err := strconv.Atoi(tok.Literal)
		if err != nil {
			return nil, fmt.Errorf("spl2: invalid tail count %q", tok.Literal)
		}
		count = n
	}

	return &TailCommand{Count: count}, nil
}

func (p *Parser) parseTimechart() (*TimechartCommand, error) {
	p.advance() // consume "timechart"
	cmd := &TimechartCommand{}

	if p.peek().Type == TokenSpan || (p.peek().Type == TokenIdent && strings.ToLower(p.peek().Literal) == "span") {
		p.advance()
		if _, err := p.expect(TokenEq); err != nil {
			return nil, err
		}
		cmd.Span = p.readSpanValue()
	}

	aggs, err := p.parseAggList()
	if err != nil {
		return nil, err
	}
	cmd.Aggregations = aggs

	if p.peek().Type == TokenBy {
		p.advance()
		groupBy, err := p.parseIdentList()
		if err != nil {
			return nil, err
		}
		cmd.GroupBy = groupBy
	}

	return cmd, nil
}

func (p *Parser) parseRex() (*RexCommand, error) {
	p.advance()                       // consume "rex"
	cmd := &RexCommand{Field: "_raw"} // default field

	// Parse optional field=<name>.
	if isIdentLike(p.peek().Type) && strings.ToLower(p.peek().Literal) == "field" &&
		p.peekAt(1).Type == TokenEq {
		p.advance() // consume "field"
		p.advance() // consume "="
		tok, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		cmd.Field = tok.Literal
	}

	// Parse regex pattern.
	tok, err := p.expect(TokenString)
	if err != nil {
		return nil, err
	}
	cmd.Pattern = tok.Literal

	return cmd, nil
}

func (p *Parser) parseFields() (*FieldsCommand, error) {
	p.advance() // consume "fields"
	cmd := &FieldsCommand{}

	// Check for removal mode: fields - field1, field2.
	if p.peek().Type == TokenMinus {
		p.advance()
		cmd.Remove = true
	} else if p.peek().Type == TokenPlus {
		p.advance()
	}

	// Handle "fields *" (all fields) — same as "table *".
	if p.peek().Type == TokenStar {
		p.advance()
		cmd.Fields = []string{"*"}
		return cmd, nil
	}

	fields, err := p.parseIdentList()
	if err != nil {
		return nil, err
	}
	cmd.Fields = fields

	return cmd, nil
}

func (p *Parser) parseTable() (*TableCommand, error) {
	p.advance() // consume "table"

	// Handle "table *" (all fields).
	if p.peek().Type == TokenStar {
		p.advance()

		return &TableCommand{Fields: []string{"*"}}, nil
	}

	fields, err := p.parseIdentList()
	if err != nil {
		return nil, err
	}

	return &TableCommand{Fields: fields}, nil
}

func (p *Parser) parseDedup() (*DedupCommand, error) {
	p.advance() // consume "dedup"

	// Check for optional limit (e.g. DEDUP 3 field1, field2).
	limit := 0
	if p.peek().Type == TokenNumber {
		tok := p.advance()
		n, err := strconv.Atoi(tok.Literal)
		if err != nil {
			return nil, fmt.Errorf("invalid dedup limit: %s", tok.Literal)
		}
		limit = n
	}

	fields, err := p.parseIdentList()
	if err != nil {
		return nil, err
	}

	return &DedupCommand{Fields: fields, Limit: limit}, nil
}

func (p *Parser) parseRename() (*RenameCommand, error) {
	p.advance() // consume "rename"
	cmd := &RenameCommand{}

	for {
		old, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenAs); err != nil {
			return nil, err
		}
		newName, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		cmd.Renames = append(cmd.Renames, RenamePair{Old: old.Literal, New: newName.Literal})

		if p.peek().Type == TokenComma {
			p.advance()
		} else {
			break
		}
	}

	return cmd, nil
}

func (p *Parser) parseBin() (*BinCommand, error) {
	p.advance() // consume "bin"
	cmd := &BinCommand{}

	// Parse field name.
	field, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	cmd.Field = field.Literal

	// Parse span=<duration>.
	if p.peek().Type == TokenSpan || (p.peek().Type == TokenIdent && strings.ToLower(p.peek().Literal) == "span") {
		p.advance()
		if _, err := p.expect(TokenEq); err != nil {
			return nil, err
		}
		cmd.Span = p.readSpanValue()
	}

	// Parse optional AS <alias>.
	if p.peek().Type == TokenAs {
		p.advance()
		alias, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		cmd.Alias = alias.Literal
	}

	return cmd, nil
}

func (p *Parser) parseStreamstats() (*StreamstatsCommand, error) {
	p.advance() // consume "streamstats"
	cmd := &StreamstatsCommand{Current: true, Window: 0}

	// Parse optional current=true/false and window=N (before aggregations).
	p.parseStreamstatsOptions(cmd)

	aggs, err := p.parseAggList()
	if err != nil {
		return nil, err
	}
	cmd.Aggregations = aggs

	if p.peek().Type == TokenBy {
		p.advance()
		groupBy, err := p.parseIdentList()
		if err != nil {
			return nil, err
		}
		cmd.GroupBy = groupBy
	}

	// Parse trailing options (current=, window= may appear after aggregations).
	p.parseStreamstatsOptions(cmd)

	return cmd, nil
}

func (p *Parser) parseStreamstatsOptions(cmd *StreamstatsCommand) {
	for p.peek().Type == TokenIdent || p.peek().Type == TokenCurrent || p.peek().Type == TokenWindow {
		name := strings.ToLower(p.peek().Literal)
		if name == "current" && p.peekAt(1).Type == TokenEq {
			p.advance()
			p.advance() // consume =
			val := p.advance()
			cmd.Current = strings.EqualFold(val.Literal, "true")
		} else if name == "window" && p.peekAt(1).Type == TokenEq {
			p.advance()
			p.advance() // consume =
			val := p.advance()
			n, _ := strconv.Atoi(val.Literal)
			cmd.Window = n
		} else {
			break
		}
	}
}

func (p *Parser) parseEventstats() (*EventstatsCommand, error) {
	p.advance() // consume "eventstats"

	aggs, err := p.parseAggList()
	if err != nil {
		return nil, err
	}

	var groupBy []string
	if p.peek().Type == TokenBy {
		p.advance()
		groupBy, err = p.parseIdentList()
		if err != nil {
			return nil, err
		}
	}

	return &EventstatsCommand{Aggregations: aggs, GroupBy: groupBy}, nil
}

func (p *Parser) parseJoin() (*JoinCommand, error) {
	p.advance()                            // consume "join"
	cmd := &JoinCommand{JoinType: "inner"} // default

	// Parse optional type=inner/left.
	if p.peek().Type == TokenIdent && strings.ToLower(p.peek().Literal) == "type" &&
		p.peekAt(1).Type == TokenEq {
		p.advance()
		p.advance() // consume =
		jt := p.advance()
		cmd.JoinType = strings.ToLower(jt.Literal)
	}

	// Parse join field.
	field, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	cmd.Field = field.Literal

	// Parse subsearch in brackets.
	sub, err := p.parseSubsearch()
	if err != nil {
		return nil, err
	}
	cmd.Subquery = sub

	return cmd, nil
}

func (p *Parser) parseAppend() (*AppendCommand, error) {
	p.advance() // consume "append"

	sub, err := p.parseSubsearch()
	if err != nil {
		return nil, err
	}

	return &AppendCommand{Subquery: sub}, nil
}

func (p *Parser) parseMultisearch() (*MultisearchCommand, error) {
	p.advance() // consume "multisearch"
	cmd := &MultisearchCommand{}

	for p.peek().Type == TokenLBracket {
		sub, err := p.parseSubsearch()
		if err != nil {
			return nil, err
		}
		cmd.Searches = append(cmd.Searches, sub)
	}

	return cmd, nil
}

func (p *Parser) parseTransaction() (*TransactionCommand, error) {
	p.advance() // consume "transaction"
	cmd := &TransactionCommand{}

	// Parse field.
	field, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	cmd.Field = field.Literal

	// Parse optional maxspan, startswith, endswith.
	for p.peek().Type == TokenIdent {
		name := strings.ToLower(p.peek().Literal)
		if name == "maxspan" && p.peekAt(1).Type == TokenEq {
			p.advance()
			p.advance()
			cmd.MaxSpan = p.readSpanValue()
		} else if name == "startswith" && p.peekAt(1).Type == TokenEq {
			p.advance()
			p.advance()
			tok := p.advance()
			cmd.StartsWith = tok.Literal
		} else if name == "endswith" && p.peekAt(1).Type == TokenEq {
			p.advance()
			p.advance()
			tok := p.advance()
			cmd.EndsWith = tok.Literal
		} else {
			break
		}
	}

	return cmd, nil
}

func (p *Parser) parseXYSeries() (*XYSeriesCommand, error) {
	p.advance() // consume "xyseries"

	x, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	y, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	v, err := p.expectIdent()
	if err != nil {
		return nil, err
	}

	return &XYSeriesCommand{XField: x.Literal, YField: y.Literal, ValueField: v.Literal}, nil
}

// parseTop parses: top [N] <field> [by <field>].
func (p *Parser) parseTop() (*TopCommand, error) {
	p.advance() // consume "top"

	return p.parseTopRareBody(10)
}

// parseRare parses: rare [N] <field> [by <field>].
func (p *Parser) parseRare() (*RareCommand, error) {
	p.advance() // consume "rare"
	cmd, err := p.parseTopRareBody(10)
	if err != nil {
		return nil, err
	}

	return &RareCommand{N: cmd.N, Field: cmd.Field, ByField: cmd.ByField}, nil
}

// parseTopRareBody parses the shared body of top/rare commands.
func (p *Parser) parseTopRareBody(defaultN int) (*TopCommand, error) {
	n := defaultN

	// Check if next token is a number (the N value).
	if p.peek().Type == TokenNumber {
		tok := p.advance()
		parsed, err := strconv.Atoi(tok.Literal)
		if err != nil {
			return nil, fmt.Errorf("spl2: invalid top/rare count %q", tok.Literal)
		}
		n = parsed
	}

	// Field name.
	field, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("spl2: top/rare requires a field name")
	}

	cmd := &TopCommand{N: n, Field: field.Literal}

	// Optional: by <field>
	if p.peek().Type == TokenBy {
		p.advance() // consume "by"
		byField, err := p.expectIdent()
		if err != nil {
			return nil, fmt.Errorf("spl2: expected field name after 'by'")
		}
		cmd.ByField = byField.Literal
	}

	return cmd, nil
}

// parseFillnull parses: fillnull [value=<val>] [<field-list>].
func (p *Parser) parseFillnull() (*FillnullCommand, error) {
	p.advance() // consume "fillnull"

	cmd := &FillnullCommand{Value: ""}

	// Check for value=<val>
	if p.peek().Type == TokenIdent && p.peek().Literal == "value" {
		p.advance() // consume "value"
		if p.peek().Type == TokenEq {
			p.advance() // consume "="
			tok := p.advance()
			cmd.Value = tok.Literal
		}
	}

	// Collect field list.
	for p.peek().Type == TokenIdent {
		cmd.Fields = append(cmd.Fields, p.advance().Literal)
		if p.peek().Type == TokenComma {
			p.advance() // consume ","
		}
	}

	return cmd, nil
}

func (p *Parser) parseMaterialize() (*MaterializeCommand, error) {
	p.advance() // consume "materialize"

	tok, err := p.expect(TokenString)
	if err != nil {
		return nil, fmt.Errorf("spl2: materialize requires a quoted name")
	}
	cmd := &MaterializeCommand{Name: tok.Literal}

	// Parse optional retention=X and partition_by=X,Y
	for p.peek().Type == TokenIdent {
		key := strings.ToLower(p.peek().Literal)
		if key == "retention" && p.peekAt(1).Type == TokenEq {
			p.advance() // consume "retention"
			p.advance() // consume "="
			cmd.Retention = p.readOptionValue()
		} else if key == "partition_by" && p.peekAt(1).Type == TokenEq {
			p.advance() // consume "partition_by"
			p.advance() // consume "="
			// Read comma-separated field list.
			val := p.advance()
			fields := []string{val.Literal}
			for p.peek().Type == TokenComma {
				p.advance() // consume comma
				f := p.advance()
				fields = append(fields, f.Literal)
			}
			cmd.PartitionBy = fields
		} else {
			break
		}
	}

	return cmd, nil
}

func (p *Parser) parseFromCommand() (*FromCommand, error) {
	p.advance() // consume "from"

	name, err := p.parseSourceName()
	if err != nil {
		return nil, fmt.Errorf("spl2: from requires a view name: %w", err)
	}

	return &FromCommand{ViewName: name}, nil
}

// parseSourceClause parses the FROM target which can be:
//   - FROM name             (single source)
//   - FROM *                (all sources)
//   - FROM logs*            (glob pattern)
//   - FROM a, b, c          (comma-separated list)
//   - FROM "my-logs", web   (quoted names in list)
func (p *Parser) parseSourceClause() (*SourceClause, error) {
	// Handle FROM * (all sources).
	if p.peek().Type == TokenStar {
		p.advance()

		return &SourceClause{Index: "*", IsGlob: true}, nil
	}

	// Handle FROM glob_pattern (e.g., logs*).
	if p.peek().Type == TokenGlob {
		tok := p.advance()

		return &SourceClause{Index: tok.Literal, IsGlob: true}, nil
	}

	// Parse first source name.
	name, err := p.parseSourceName()
	if err != nil {
		return nil, err
	}

	// Check if the name contains wildcard characters (from a quoted string like "logs*").
	if strings.Contains(name, "*") || strings.Contains(name, "?") {
		return &SourceClause{Index: name, IsGlob: true}, nil
	}

	// Check for comma-separated list: FROM a, b, c
	if p.peek().Type == TokenComma {
		indices := []string{name}
		for p.peek().Type == TokenComma {
			p.advance() // consume comma

			// Handle glob or star tokens in comma list.
			if p.peek().Type == TokenGlob || p.peek().Type == TokenStar {
				tok := p.advance()
				indices = append(indices, tok.Literal)

				continue
			}

			nextName, err := p.parseSourceName()
			if err != nil {
				return nil, fmt.Errorf("spl2: expected source name after comma in FROM: %w", err)
			}
			indices = append(indices, nextName)
		}

		return &SourceClause{Index: indices[0], Indices: indices}, nil
	}

	// Single source name.
	return &SourceClause{Index: name}, nil
}

// parseSourceName parses a source/index/view name which may start with a digit
// (e.g., "2xlog"). The lexer tokenizes "2xlog" as NUMBER("2") + IDENT("xlog"),
// so we merge adjacent number+ident tokens into a single name. Also accepts
// quoted strings for names containing special characters.
func (p *Parser) parseSourceName() (string, error) {
	tok := p.peek()

	switch {
	case isIdentLike(tok.Type):
		p.advance()

		return tok.Literal, nil

	case tok.Type == TokenString:
		p.advance()

		return tok.Literal, nil

	case tok.Type == TokenGlob:
		p.advance()

		return tok.Literal, nil

	case tok.Type == TokenStar:
		p.advance()

		return "*", nil

	case tok.Type == TokenNumber:
		p.advance()
		name := tok.Literal

		// If an IDENT follows immediately (no whitespace gap), merge.
		// e.g., "2xlog" → NUMBER("2") @ pos 5 + IDENT("xlog") @ pos 6
		next := p.peek()
		if isIdentLike(next.Type) && next.Pos == tok.Pos+len(tok.Literal) {
			p.advance()
			name += next.Literal
		}

		return name, nil

	default:
		return "", fmt.Errorf("spl2: expected source name, got %s %q at position %d", tok.Type, tok.Literal, tok.Pos)
	}
}

func (p *Parser) parseViews() (*ViewsCommand, error) {
	p.advance() // consume "views"
	cmd := &ViewsCommand{}

	// Optional quoted view name.
	if p.peek().Type == TokenString {
		cmd.Name = p.advance().Literal
	}

	// Optional retention=X.
	if p.peek().Type == TokenIdent && strings.ToLower(p.peek().Literal) == "retention" &&
		p.peekAt(1).Type == TokenEq {
		p.advance() // consume "retention"
		p.advance() // consume "="
		cmd.Retention = p.readOptionValue()
	}

	return cmd, nil
}

func (p *Parser) parseDropview() (*DropviewCommand, error) {
	p.advance() // consume "dropview"

	tok, err := p.expect(TokenString)
	if err != nil {
		return nil, fmt.Errorf("spl2: dropview requires a quoted name")
	}

	return &DropviewCommand{Name: tok.Literal}, nil
}

// readOptionValue reads a single option value token (could be ident, number, or combined like "30d").
func (p *Parser) readOptionValue() string {
	tok := p.peek()
	if tok.Type == TokenNumber {
		p.advance()
		val := tok.Literal
		// Check for unit suffix (e.g., "30" + "d").
		if p.peek().Type == TokenIdent {
			val += p.advance().Literal
		}

		return val
	}
	if tok.Type == TokenIdent || tok.Type == TokenString {
		p.advance()

		return tok.Literal
	}

	return ""
}

func (p *Parser) parseSubsearch() (*Query, error) {
	if p.peek().Type == TokenLBracket {
		p.advance() // consume [

		// Check if starts with $ (dataset ref)
		if p.peek().Type == TokenDollar {
			p.advance()
			name, err := p.expect(TokenIdent)
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(TokenRBracket); err != nil {
				return nil, err
			}

			return &Query{Source: &SourceClause{Index: name.Literal, IsVariable: true}}, nil
		}

		q, err := p.parseQuery()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenRBracket); err != nil {
			return nil, err
		}

		return q, nil
	}

	return nil, fmt.Errorf("spl2: expected [subsearch], got %s at position %d", p.peek().Type, p.peek().Pos)
}

// readSpanValue reads a span value like "2m", "15m", "1h", "30s".
func (p *Parser) readSpanValue() string {
	tok := p.peek()
	switch tok.Type {
	case TokenNumber:
		p.advance()
		span := tok.Literal
		// Check for unit suffix.
		if p.peek().Type == TokenIdent {
			unit := p.advance()
			span += unit.Literal
		}

		return span
	case TokenIdent:
		p.advance()

		return tok.Literal
	}

	return ""
}

// parseUnpack parses: unpack_<format> [from <field>] [fields (<f1>, <f2>, ...)] [prefix "<p>"] [keep_original].
// All formats share identical option grammar.
func (p *Parser) parseUnpack(format string) (*UnpackCommand, error) {
	p.advance() // consume the keyword (unpack_json, unpack_logfmt, etc.)
	cmd := &UnpackCommand{Format: format, SourceField: "_raw"}

	// Parse options in any order.
	for {
		tok := p.peek()

		// Handle "from" which is TokenFrom (keyword), not TokenIdent.
		if tok.Type == TokenFrom {
			p.advance() // consume "from"
			fieldTok := p.peek()
			if fieldTok.Type == TokenIdent || fieldTok.Type == TokenString {
				p.advance()
				cmd.SourceField = fieldTok.Literal
			} else {
				return nil, fmt.Errorf("spl2: unpack_%s: expected field name after 'from', got %s", format, fieldTok.Type)
			}

			continue
		}

		// Handle "fields" which is TokenFields (keyword), not TokenIdent.
		if tok.Type == TokenFields {
			p.advance() // consume "fields"
			// Expect ( field1, field2, ... )
			if _, err := p.expect(TokenLParen); err != nil {
				return nil, fmt.Errorf("spl2: unpack_%s: expected '(' after 'fields'", format)
			}
			var fieldList []string
			for p.peek().Type != TokenRParen && p.peek().Type != TokenEOF {
				if len(fieldList) > 0 {
					if p.peek().Type == TokenComma {
						p.advance()
					}
				}
				f := p.peek()
				if f.Type != TokenIdent && f.Type != TokenString {
					return nil, fmt.Errorf("spl2: unpack_%s: expected field name in fields(), got %s", format, f.Type)
				}
				p.advance()
				fieldList = append(fieldList, f.Literal)
			}
			if _, err := p.expect(TokenRParen); err != nil {
				return nil, fmt.Errorf("spl2: unpack_%s: expected ')' after fields list", format)
			}
			cmd.Fields = fieldList

			continue
		}

		if tok.Type != TokenIdent {
			break
		}

		name := strings.ToLower(tok.Literal)
		switch name {
		case "prefix":
			p.advance() // consume "prefix"
			prefixTok := p.peek()
			if prefixTok.Type == TokenString {
				p.advance()
				cmd.Prefix = prefixTok.Literal
			} else if prefixTok.Type == TokenIdent {
				p.advance()
				cmd.Prefix = prefixTok.Literal
			} else {
				return nil, fmt.Errorf("spl2: unpack_%s: expected prefix value, got %s", format, prefixTok.Type)
			}

		case "keep_original":
			p.advance()
			cmd.KeepOriginal = true

		case "delim":
			p.advance() // consume "delim"
			// Expect = then value
			if p.peek().Type == TokenEq {
				p.advance() // consume "="
			}
			valTok := p.peek()
			if valTok.Type == TokenString || valTok.Type == TokenIdent {
				p.advance()
				cmd.Delim = valTok.Literal
			} else {
				return nil, fmt.Errorf("spl2: unpack_%s: expected delim value, got %s", format, valTok.Type)
			}

		case "assign":
			p.advance()
			if p.peek().Type == TokenEq {
				p.advance()
			}
			valTok := p.peek()
			if valTok.Type == TokenString || valTok.Type == TokenIdent {
				p.advance()
				cmd.Assign = valTok.Literal
			} else {
				return nil, fmt.Errorf("spl2: unpack_%s: expected assign value, got %s", format, valTok.Type)
			}

		case "quote":
			p.advance()
			if p.peek().Type == TokenEq {
				p.advance()
			}
			valTok := p.peek()
			if valTok.Type == TokenString || valTok.Type == TokenIdent {
				p.advance()
				cmd.Quote = valTok.Literal
			} else {
				return nil, fmt.Errorf("spl2: unpack_%s: expected quote value, got %s", format, valTok.Type)
			}

		case "header":
			p.advance() // consume "header"
			if p.peek().Type == TokenEq {
				p.advance() // consume "="
			}
			valTok := p.peek()
			if valTok.Type == TokenString {
				p.advance()
				cmd.Header = valTok.Literal
			} else {
				return nil, fmt.Errorf("spl2: unpack_%s: expected header string, got %s", format, valTok.Type)
			}

		default:
			// Unknown option — stop parsing options.
			goto done
		}
	}
done:

	return cmd, nil
}

// parseUnpackPattern parses: unpack_pattern "<pattern>" [from <field>] [fields (...)] [prefix "<p>"] [keep_original].
// The positional pattern string argument is required and stored in cmd.Pattern.
func (p *Parser) parseUnpackPattern() (*UnpackCommand, error) {
	p.advance() // consume "unpack_pattern"

	// Expect a positional string argument as the pattern.
	patternTok := p.peek()
	if patternTok.Type != TokenString {
		return nil, fmt.Errorf("spl2: unpack_pattern: expected pattern string, got %s at position %d", patternTok.Type, patternTok.Pos)
	}
	p.advance()

	cmd, err := p.parseUnpackOptions("pattern")
	if err != nil {
		return nil, err
	}
	cmd.Pattern = patternTok.Literal

	return cmd, nil
}

// parseUnpackOptions parses the shared option grammar for unpack commands
// (from, fields, prefix, keep_original, etc.) without consuming the keyword.
// Used by parseUnpackPattern to parse options after the positional pattern argument.
func (p *Parser) parseUnpackOptions(format string) (*UnpackCommand, error) {
	cmd := &UnpackCommand{Format: format, SourceField: "_raw"}

	for {
		tok := p.peek()

		if tok.Type == TokenFrom {
			p.advance()
			fieldTok := p.peek()
			if fieldTok.Type == TokenIdent || fieldTok.Type == TokenString {
				p.advance()
				cmd.SourceField = fieldTok.Literal
			} else {
				return nil, fmt.Errorf("spl2: unpack_%s: expected field name after 'from', got %s", format, fieldTok.Type)
			}

			continue
		}

		if tok.Type == TokenFields {
			p.advance()
			if _, err := p.expect(TokenLParen); err != nil {
				return nil, fmt.Errorf("spl2: unpack_%s: expected '(' after 'fields'", format)
			}
			var fieldList []string
			for p.peek().Type != TokenRParen && p.peek().Type != TokenEOF {
				if len(fieldList) > 0 {
					if p.peek().Type == TokenComma {
						p.advance()
					}
				}
				f := p.peek()
				if f.Type != TokenIdent && f.Type != TokenString {
					return nil, fmt.Errorf("spl2: unpack_%s: expected field name in fields(), got %s", format, f.Type)
				}
				p.advance()
				fieldList = append(fieldList, f.Literal)
			}
			if _, err := p.expect(TokenRParen); err != nil {
				return nil, fmt.Errorf("spl2: unpack_%s: expected ')' after fields list", format)
			}
			cmd.Fields = fieldList

			continue
		}

		if tok.Type != TokenIdent {
			break
		}

		name := strings.ToLower(tok.Literal)
		switch name {
		case "prefix":
			p.advance()
			prefixTok := p.peek()
			if prefixTok.Type == TokenString || prefixTok.Type == TokenIdent {
				p.advance()
				cmd.Prefix = prefixTok.Literal
			} else {
				return nil, fmt.Errorf("spl2: unpack_%s: expected prefix value, got %s", format, prefixTok.Type)
			}

		case "keep_original":
			p.advance()
			cmd.KeepOriginal = true

		default:
			return cmd, nil
		}
	}

	return cmd, nil
}

// parseUnroll parses: unroll field=<field>.
func (p *Parser) parseUnroll() (*UnrollCommand, error) {
	p.advance() // consume "unroll"

	cmd := &UnrollCommand{}

	// Expect field=<ident>
	tok := p.peek()
	if tok.Type == TokenIdent && strings.ToLower(tok.Literal) == "field" {
		p.advance() // consume "field"
		if p.peek().Type == TokenEq {
			p.advance() // consume "="
		}
		fieldTok := p.peek()
		if fieldTok.Type == TokenIdent || fieldTok.Type == TokenString {
			p.advance()
			cmd.Field = fieldTok.Literal
		} else {
			return nil, fmt.Errorf("spl2: unroll: expected field name after 'field=', got %s", fieldTok.Type)
		}
	} else if tok.Type == TokenIdent {
		// Allow bare field name: unroll items
		p.advance()
		cmd.Field = tok.Literal
	} else {
		return nil, fmt.Errorf("spl2: unroll: expected field name or field=<name>, got %s", tok.Type)
	}

	return cmd, nil
}

// parseJsonCmd parses: json [field=<field>] [paths="<p1>, <p2>"].
func (p *Parser) parseJsonCmd() (*JsonCommand, error) {
	p.advance() // consume "json"
	cmd := &JsonCommand{SourceField: "_raw"}

	// Parse options.
	for p.peek().Type == TokenIdent {
		name := strings.ToLower(p.peek().Literal)
		switch name {
		case "field":
			if p.peekAt(1).Type != TokenEq {
				goto done
			}
			p.advance() // consume "field"
			p.advance() // consume "="
			tok := p.peek()
			if tok.Type != TokenIdent && tok.Type != TokenString {
				return nil, fmt.Errorf("spl2: json: expected field name after 'field=', got %s", tok.Type)
			}
			p.advance()
			cmd.SourceField = tok.Literal

		case "paths":
			if p.peekAt(1).Type != TokenEq {
				goto done
			}
			p.advance() // consume "paths"
			p.advance() // consume "="
			tok, err := p.expect(TokenString)
			if err != nil {
				return nil, fmt.Errorf("spl2: json: expected quoted paths list after 'paths='")
			}
			// Parse comma-separated paths from the quoted string.
			// Each part may contain " AS alias" (case-insensitive).
			for _, part := range strings.Split(tok.Literal, ",") {
				part = strings.TrimSpace(part)
				if part == "" {
					continue
				}
				jp := JsonPath{}
				if asIdx := indexCaseInsensitive(part, " as "); asIdx >= 0 {
					jp.Path = strings.TrimSpace(part[:asIdx])
					jp.Alias = strings.TrimSpace(part[asIdx+4:])
				} else {
					jp.Path = part
				}
				cmd.Paths = append(cmd.Paths, jp)
			}

		case "path":
			if p.peekAt(1).Type != TokenEq {
				goto done
			}
			p.advance() // consume "path"
			p.advance() // consume "="
			tok, err := p.expect(TokenString)
			if err != nil {
				return nil, fmt.Errorf("spl2: json: expected quoted path after 'path='")
			}
			jp := JsonPath{Path: strings.TrimSpace(tok.Literal)}
			// Check for trailing AS <alias> outside the quoted string.
			if p.peek().Type == TokenAs {
				p.advance() // consume AS
				aliasTok := p.peek()
				if aliasTok.Type != TokenIdent && aliasTok.Type != TokenString {
					return nil, fmt.Errorf("spl2: json: expected alias name after AS")
				}
				p.advance()
				jp.Alias = aliasTok.Literal
			}
			cmd.Paths = append(cmd.Paths, jp)

		default:
			goto done
		}
	}
done:
	// Bare path syntax: | json items[*].price AS price, user.name
	// When no keyword args matched and the next token is an ident,
	// parse comma-separated dot-bracket paths with optional AS aliases.
	for p.peek().Type == TokenIdent {
		path := p.parseBareDotPath()
		if path == "" {
			break
		}
		jp := JsonPath{Path: path}
		if p.peek().Type == TokenAs {
			p.advance()
			aliasTok := p.peek()
			if aliasTok.Type != TokenIdent && aliasTok.Type != TokenString {
				return nil, fmt.Errorf("spl2: json: expected alias after AS")
			}
			p.advance()
			jp.Alias = aliasTok.Literal
		}
		cmd.Paths = append(cmd.Paths, jp)
		if p.peek().Type == TokenComma {
			p.advance()
			continue
		}
		break
	}

	return cmd, nil
}

// parseBareDotPath consumes tokens to build a dotted-bracket path like
// "items[*].price" or "user.name". Returns "" if the current token is
// not an identifier. The lexer handles simple dot-paths as single ident
// tokens (e.g., "user.name"); this helper stitches bracket-separated
// segments back together (e.g., items + [*] + .price → "items[*].price").
func (p *Parser) parseBareDotPath() string {
	if p.peek().Type != TokenIdent {
		return ""
	}
	tok := p.advance()
	var b strings.Builder
	b.WriteString(tok.Literal)

	for {
		switch p.peek().Type {
		case TokenLBracket:
			p.advance() // consume [
			b.WriteByte('[')
			for p.peek().Type != TokenRBracket && p.peek().Type != TokenEOF {
				b.WriteString(p.advance().Literal)
			}
			if p.peek().Type == TokenRBracket {
				p.advance()
				b.WriteByte(']')
			}
		case TokenIdent:
			// Continuation like ".price" (ident starting with dot, produced by lexer)
			lit := p.peek().Literal
			if len(lit) > 0 && lit[0] == '.' {
				p.advance()
				b.WriteString(lit)
			} else {
				return b.String()
			}
		default:
			return b.String()
		}
	}
}

// parsePackJson parses: pack_json [<f1>, <f2>, ...] into <target>.
func (p *Parser) parsePackJson() (*PackJsonCommand, error) {
	p.advance() // consume "pack_json"
	cmd := &PackJsonCommand{}

	// Check if the next token is "into" (no field list → pack all).
	tok := p.peek()
	if tok.Type == TokenInto {
		// No fields specified — pack all.
		p.advance() // consume "into"
		targetTok := p.peek()
		if !isIdentLike(targetTok.Type) && targetTok.Type != TokenString {
			return nil, fmt.Errorf("spl2: pack_json: expected target field name after 'into', got %s", targetTok.Type)
		}
		p.advance()
		cmd.Target = targetTok.Literal

		return cmd, nil
	}

	// Parse comma-separated field list until "into".
	for {
		tok := p.peek()
		if tok.Type == TokenInto {
			break
		}
		if !isIdentLike(tok.Type) && tok.Type != TokenString {
			return nil, fmt.Errorf("spl2: pack_json: expected field name or 'into', got %s %q", tok.Type, tok.Literal)
		}
		p.advance()
		cmd.Fields = append(cmd.Fields, tok.Literal)

		// Consume optional comma.
		if p.peek().Type == TokenComma {
			p.advance()
		}
	}

	// Consume "into".
	if _, err := p.expect(TokenInto); err != nil {
		return nil, fmt.Errorf("spl2: pack_json: expected 'into'")
	}

	// Consume target field.
	targetTok := p.peek()
	if !isIdentLike(targetTok.Type) && targetTok.Type != TokenString {
		return nil, fmt.Errorf("spl2: pack_json: expected target field name after 'into', got %s", targetTok.Type)
	}
	p.advance()
	cmd.Target = targetTok.Literal

	return cmd, nil
}

// parseExpr parses a boolean expression with ?? (null coalesce) as lowest precedence.
func (p *Parser) parseExpr() (Expr, error) {
	p.depth++
	if p.depth > maxExprDepth {
		return nil, ErrQueryTooComplex
	}
	defer func() { p.depth-- }()

	return p.parseNullCoalesce()
}

// parseNullCoalesce parses: expr ?? expr ?? expr ...
// Desugars to nested coalesce() calls. Lower precedence than OR.
func (p *Parser) parseNullCoalesce() (Expr, error) {
	left, err := p.parseOr()
	if err != nil {
		return nil, err
	}

	for p.peek().Type == TokenDoubleQuestion {
		p.advance()
		right, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		left = &FuncCallExpr{Name: "coalesce", Args: []Expr{left, right}}
	}

	return left, nil
}

func (p *Parser) parseOr() (Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.peek().Type == TokenOr {
		p.advance()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: "or", Right: right}
	}

	return left, nil
}

func (p *Parser) parseAnd() (Expr, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}

	for p.peek().Type == TokenAnd {
		p.advance()
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: "and", Right: right}
	}

	return left, nil
}

func (p *Parser) parseNot() (Expr, error) {
	if p.peek().Type == TokenNot {
		p.advance()
		expr, err := p.parseNot()
		if err != nil {
			return nil, err
		}

		return &NotExpr{Expr: expr}, nil
	}

	return p.parseComparison()
}

func (p *Parser) parseComparison() (Expr, error) {
	left, err := p.parseAddSub()
	if err != nil {
		return nil, err
	}

	tok := p.peek()
	switch tok.Type {
	case TokenEq, TokenNeq, TokenLt, TokenLte, TokenGt, TokenGte:
		p.advance()
		right, err := p.parseAddSub()
		if err != nil {
			return nil, err
		}

		return &CompareExpr{Left: left, Op: tok.Literal, Right: right}, nil
	case TokenLike:
		p.advance()
		right, err := p.parseAddSub()
		if err != nil {
			return nil, err
		}

		return &CompareExpr{Left: left, Op: "like", Right: right}, nil
	case TokenIn:
		return p.parseInExpr(left, false)
	case TokenRegexMatch:
		p.advance()
		right, err := p.parseAddSub()
		if err != nil {
			return nil, err
		}

		return &CompareExpr{Left: left, Op: "=~", Right: right}, nil
	case TokenRegexNotMatch:
		p.advance()
		right, err := p.parseAddSub()
		if err != nil {
			return nil, err
		}

		return &CompareExpr{Left: left, Op: "!~", Right: right}, nil
	case TokenBetween:
		// BETWEEN low AND high → desugars to (left >= low AND left <= high)
		p.advance() // consume BETWEEN
		low, err := p.parseAddSub()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenAnd); err != nil {
			return nil, fmt.Errorf("spl2: BETWEEN requires AND between bounds")
		}
		high, err := p.parseAddSub()
		if err != nil {
			return nil, err
		}

		return &BinaryExpr{
			Left:  &CompareExpr{Left: left, Op: ">=", Right: low},
			Op:    "and",
			Right: &CompareExpr{Left: left, Op: "<=", Right: high},
		}, nil
	case TokenIs:
		// IS NULL or IS NOT NULL
		p.advance() // consume IS
		if p.peek().Type == TokenNull {
			p.advance() // consume NULL

			return &FuncCallExpr{Name: "isnull", Args: []Expr{left}}, nil
		}
		if p.peek().Type == TokenNot {
			p.advance() // consume NOT
			if _, err := p.expect(TokenNull); err != nil {
				return nil, fmt.Errorf("spl2: expected NULL after IS NOT")
			}

			return &FuncCallExpr{Name: "isnotnull", Args: []Expr{left}}, nil
		}

		return nil, fmt.Errorf("spl2: expected NULL or NOT NULL after IS at position %d", p.peek().Pos)
	case TokenNot:
		// NOT IN (...) or NOT LIKE "pattern"
		if p.peekAt(1).Type == TokenIn {
			p.advance() // consume NOT
			return p.parseInExpr(left, true)
		}
		if p.peekAt(1).Type == TokenLike {
			p.advance() // consume NOT
			p.advance() // consume LIKE
			right, err := p.parseAddSub()
			if err != nil {
				return nil, err
			}

			return &CompareExpr{Left: left, Op: "not like", Right: right}, nil
		}
		// NOT is part of a boolean expression; let the caller handle it.
	}

	return left, nil
}

// parseInExpr parses the IN (val1, val2, ...) portion of an IN or NOT IN expression.
func (p *Parser) parseInExpr(left Expr, negated bool) (Expr, error) {
	p.advance() // consume IN
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, err
	}
	var values []Expr
	for p.peek().Type != TokenRParen {
		if len(values) > 0 {
			if _, err := p.expect(TokenComma); err != nil {
				return nil, err
			}
		}
		val, err := p.parseAddSub()
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	p.advance() // consume )

	return &InExpr{Field: left, Values: values, Negated: negated}, nil
}

func (p *Parser) parseAddSub() (Expr, error) {
	left, err := p.parseMulDiv()
	if err != nil {
		return nil, err
	}

	for p.peek().Type == TokenPlus || p.peek().Type == TokenMinus {
		op := p.advance()
		right, err := p.parseMulDiv()
		if err != nil {
			return nil, err
		}
		left = &ArithExpr{Left: left, Op: op.Literal, Right: right}
	}

	return left, nil
}

func (p *Parser) parseMulDiv() (Expr, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	for p.peek().Type == TokenStar || p.peek().Type == TokenSlash || p.peek().Type == TokenPercent {
		op := p.advance()
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left = &ArithExpr{Left: left, Op: op.Literal, Right: right}
	}

	return left, nil
}

func (p *Parser) parseUnary() (Expr, error) {
	if p.peek().Type == TokenMinus {
		p.advance()
		expr, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}

		return &ArithExpr{Left: &LiteralExpr{Value: "0"}, Op: "-", Right: expr}, nil
	}

	return p.parsePrimary()
}

func (p *Parser) parsePrimary() (Expr, error) {
	tok := p.peek()

	switch tok.Type {
	case TokenLParen:
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, err
		}

		return expr, nil

	case TokenString:
		p.advance()

		return &LiteralExpr{Value: tok.Literal}, nil

	case TokenNumber:
		p.advance()

		return &LiteralExpr{Value: tok.Literal}, nil

	case TokenTrue:
		p.advance()

		return &LiteralExpr{Value: "true"}, nil

	case TokenFalse:
		p.advance()

		return &LiteralExpr{Value: "false"}, nil

	case TokenGlob:
		p.advance()

		return &GlobExpr{Pattern: tok.Literal}, nil

	case TokenStar:
		p.advance()

		return &GlobExpr{Pattern: "*"}, nil

	case TokenIdent:
		// Could be a field reference or a function call.
		if p.peekAt(1).Type == TokenLParen {
			return p.parseFuncCall()
		}
		p.advance()
		expr := &FieldExpr{Name: tok.Literal}
		// Lynx Flow: field? → isnotnull(field).
		if p.peek().Type == TokenQuestionMark {
			p.advance()

			return &FuncCallExpr{Name: "isnotnull", Args: []Expr{expr}}, nil
		}

		return expr, nil

	default:
		// Handle keywords that can also be field/function names in expression context.
		if isIdentLike(tok.Type) {
			if p.peekAt(1).Type == TokenLParen {
				return p.parseFuncCall()
			}
			p.advance()
			expr := &FieldExpr{Name: tok.Literal}
			if p.peek().Type == TokenQuestionMark {
				p.advance()

				return &FuncCallExpr{Name: "isnotnull", Args: []Expr{expr}}, nil
			}

			return expr, nil
		}
		// Also handle SPL2 keywords in expression context.
		switch tok.Type {
		case TokenEval, TokenStats, TokenSearch, TokenIn:
			if p.peekAt(1).Type == TokenLParen {
				return p.parseFuncCall()
			}
			p.advance()

			return &FieldExpr{Name: tok.Literal}, nil
		}
		return nil, fmt.Errorf("spl2: unexpected token %s %q at position %d in expression", tok.Type, tok.Literal, tok.Pos)
	}
}

func (p *Parser) parseFuncCall() (Expr, error) {
	name := p.advance() // function name (could be ident or keyword)
	p.advance()         // consume (

	var args []Expr
	for p.peek().Type != TokenRParen {
		if len(args) > 0 {
			if _, err := p.expect(TokenComma); err != nil {
				return nil, err
			}
		}
		arg, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	p.advance() // consume )

	return &FuncCallExpr{Name: name.Literal, Args: args}, nil
}

func (p *Parser) parseAggList() ([]AggExpr, error) {
	var aggs []AggExpr

	for {
		tok := p.peek()
		if !isIdentLike(tok.Type) {
			break
		}

		funcName := tok.Literal
		hasParens := p.peekAt(1).Type == TokenLParen

		if !hasParens {
			// Check if this looks like an agg without parens (e.g., "count AS alias").
			// It's an agg if the next token is AS or BY or comma or pipe/EOF.
			next := p.peekAt(1)
			isAgg := next.Type == TokenAs || next.Type == TokenComma ||
				next.Type == TokenPipe || next.Type == TokenEOF ||
				next.Type == TokenBy || next.Type == TokenRBracket
			if !isAgg {
				break
			}
			// No-arg aggregation without parentheses (e.g., "count AS total").
			p.advance()
			agg := AggExpr{Func: funcName}
			if p.peek().Type == TokenAs {
				p.advance()
				alias, err := p.expectIdent()
				if err != nil {
					return nil, err
				}
				agg.Alias = alias.Literal
			}
			aggs = append(aggs, agg)
			if p.peek().Type == TokenComma {
				p.advance()
			}

			continue
		}

		p.advance() // consume function name
		p.advance() // consume (

		var args []Expr
		for p.peek().Type != TokenRParen {
			if len(args) > 0 {
				if _, err := p.expect(TokenComma); err != nil {
					return nil, err
				}
			}
			arg, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			args = append(args, arg)
		}
		p.advance() // consume )

		// Normalize shorthand percentile aliases: p50 → perc50, etc.
		normalizedFunc := funcName
		switch strings.ToLower(funcName) {
		case "p50":
			normalizedFunc = "perc50"
		case "p75":
			normalizedFunc = "perc75"
		case "p90":
			normalizedFunc = "perc90"
		case "p95":
			normalizedFunc = "perc95"
		case "p99":
			normalizedFunc = "perc99"
		}
		agg := AggExpr{Func: normalizedFunc, Args: args}

		// Optional "as alias".
		if p.peek().Type == TokenAs {
			p.advance()
			alias, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			agg.Alias = alias.Literal
		}

		aggs = append(aggs, agg)

		if p.peek().Type == TokenComma {
			p.advance()
		}
	}

	return aggs, nil
}

func (p *Parser) parseIdentList() ([]string, error) {
	var names []string

	for {
		tok := p.peek()
		if !isIdentLike(tok.Type) && tok.Type != TokenGlob {
			break
		}
		p.advance()
		names = append(names, tok.Literal)

		if p.peek().Type == TokenComma {
			p.advance()
		} else {
			break
		}
	}

	if len(names) == 0 {
		return nil, fmt.Errorf("spl2: expected at least one identifier at position %d", p.peek().Pos)
	}

	return names, nil
}

// isBareExprContinuation returns true if the given token type can follow a
// field identifier in a bare expression context (implicit where). This prevents
// misinterpreting misspelled command names as field references — only identifiers
// followed by comparison, logical, or null-check operators trigger the fallback.
func isBareExprContinuation(t TokenType) bool {
	switch t {
	case TokenEq, TokenNeq, TokenLt, TokenLte, TokenGt, TokenGte,
		TokenIn, TokenLike, TokenBetween, TokenIs,
		TokenRegexMatch, TokenRegexNotMatch,
		TokenQuestionMark, TokenDoubleQuestion,
		TokenNot, // for "field NOT IN (...)" or "field NOT LIKE ..."
		TokenPlus, TokenMinus, TokenStar, TokenSlash, TokenPercent,
		TokenAnd, TokenOr:
		return true
	}

	return false
}

// isIdentLike returns true if the token type can serve as an identifier in
// field-name or argument positions. Lynx Flow keywords like "group", "order",
// "select" etc. may also be valid field names, so the parser must accept them
// wherever a plain TokenIdent is expected.
func isIdentLike(t TokenType) bool {
	switch t {
	case TokenIdent,
		// Lynx Flow command keywords that can also be field names.
		TokenLet, TokenKeep, TokenOmit, TokenSelect, TokenGroup, TokenCompute,
		TokenEvery, TokenBucket, TokenOrder, TokenTake, TokenRank, TokenTopby,
		TokenBottomby, TokenBottom, TokenRunning, TokenEnrich, TokenParse,
		TokenExplode, TokenPack, TokenLookup, TokenIndex,
		// Lynx Flow clause keywords.
		TokenUsing, TokenExtract, TokenIfMissing, TokenPer, TokenOn,
		TokenInto, TokenAsc, TokenDesc,
		// Lynx Flow domain sugar keywords.
		TokenLatency, TokenErrors, TokenRate, TokenPercentiles, TokenSlowest,
		// SPL2 keywords that can be field names in expression context.
		TokenTypeKeyword, TokenCurrent, TokenWindow, TokenMaxspan,
		TokenStartswith, TokenEndswith:
		return true
	}

	return false
}

// isFormatKeyword returns true for SPL2 command keyword tokens that are also
// valid format names in "parse <format>(<field>)". For example, "json" lexes
// as TokenJson but is a valid format name for the parse command.
func isFormatKeyword(t TokenType) bool {
	switch t {
	case TokenJson, TokenIndex:
		return true
	}
	return false
}

// expectIdent consumes and returns the next token if it is an identifier
// or an ident-like keyword token. Returns an error otherwise.
func (p *Parser) expectIdent() (Token, error) {
	tok := p.peek()
	if isIdentLike(tok.Type) {
		return p.advance(), nil
	}

	return tok, fmt.Errorf("spl2: expected identifier, got %s %q at position %d", tok.Type, tok.Literal, tok.Pos)
}

// indexCaseInsensitive returns the index of the first case-insensitive match
// of substr in s, or -1 if not found.
func indexCaseInsensitive(s, substr string) int {
	lower := strings.ToLower(s)
	return strings.Index(lower, strings.ToLower(substr))
}

// ---------------------------------------------------------------------------
// Lynx Flow parse methods
// ---------------------------------------------------------------------------

// parseLet parses: let <field> = <expr> [, <field> = <expr> ...].
// Desugars to EvalCommand.
func (p *Parser) parseLet() (*EvalCommand, error) {
	p.advance() // consume "let"

	return p.parseEvalBody()
}

// parseEvalBody parses the shared body of eval/let: field = expr [, field = expr ...].
func (p *Parser) parseEvalBody() (*EvalCommand, error) {
	field, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(TokenEq); err != nil {
		return nil, err
	}
	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	cmd := &EvalCommand{
		Field:       field.Literal,
		Expr:        expr,
		Assignments: []EvalAssignment{{Field: field.Literal, Expr: expr}},
	}

	for p.peek().Type == TokenComma {
		p.advance()
		f, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenEq); err != nil {
			return nil, err
		}
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		cmd.Assignments = append(cmd.Assignments, EvalAssignment{Field: f.Literal, Expr: e})
	}

	return cmd, nil
}

// parseKeep parses: keep <f1>, <f2>, ...
// Desugars to FieldsCommand{Remove: false}.
func (p *Parser) parseKeep() (*FieldsCommand, error) {
	p.advance() // consume "keep"

	fields, err := p.parseIdentListLF()
	if err != nil {
		return nil, err
	}

	return &FieldsCommand{Fields: fields, Remove: false}, nil
}

// parseOmit parses: omit <f1>, <f2>, ...
// Desugars to FieldsCommand{Remove: true}.
func (p *Parser) parseOmit() (*FieldsCommand, error) {
	p.advance() // consume "omit"

	fields, err := p.parseIdentListLF()
	if err != nil {
		return nil, err
	}

	return &FieldsCommand{Fields: fields, Remove: true}, nil
}

// parseSelectCmd parses: select <field> [as <alias>], ...
func (p *Parser) parseSelectCmd() (*SelectCommand, error) {
	p.advance() // consume "select"
	cmd := &SelectCommand{}

	for {
		tok := p.peek()
		// Allow * for "select *"
		if tok.Type == TokenStar {
			p.advance()
			cmd.Columns = append(cmd.Columns, SelectColumn{Name: "*"})
		} else if isIdentLike(tok.Type) {
			p.advance()
			col := SelectColumn{Name: tok.Literal}
			if p.peek().Type == TokenAs {
				p.advance()
				alias, err := p.expectIdent()
				if err != nil {
					return nil, err
				}
				col.Alias = alias.Literal
			}
			cmd.Columns = append(cmd.Columns, col)
		} else {
			break
		}

		if p.peek().Type == TokenComma {
			p.advance()
		} else {
			break
		}
	}

	if len(cmd.Columns) == 0 {
		return nil, fmt.Errorf("spl2: select requires at least one column at position %d", p.peek().Pos)
	}

	return cmd, nil
}

// parseGroup parses: group [by <keys>] compute <aggs>.
// Note: "by" comes BEFORE "compute" (reverse of SPL2 stats).
// Desugars to StatsCommand.
func (p *Parser) parseGroup() (*StatsCommand, error) {
	p.advance() // consume "group"

	var groupBy []string

	// Optional "by <keys>".
	if p.peek().Type == TokenBy {
		p.advance()
		var err error
		groupBy, err = p.parseIdentListLF()
		if err != nil {
			return nil, err
		}
	}

	// Expect "compute".
	if p.peek().Type != TokenCompute {
		return nil, fmt.Errorf("spl2: group: expected 'compute' at position %d, got %s %q", p.peek().Pos, p.peek().Type, p.peek().Literal)
	}
	p.advance() // consume "compute"

	aggs, err := p.parseAggList()
	if err != nil {
		return nil, err
	}

	return &StatsCommand{Aggregations: aggs, GroupBy: groupBy}, nil
}

// parseEvery parses: every <span> [by <field>] compute <aggs>.
// Desugars to TimechartCommand.
func (p *Parser) parseEvery() (*TimechartCommand, error) {
	p.advance() // consume "every"
	cmd := &TimechartCommand{}

	cmd.Span = p.readSpanValue()
	if cmd.Span == "" {
		return nil, fmt.Errorf("spl2: every: expected time span at position %d", p.peek().Pos)
	}

	// Optional "by <fields>".
	if p.peek().Type == TokenBy {
		p.advance()
		groupBy, err := p.parseIdentListLF()
		if err != nil {
			return nil, err
		}
		cmd.GroupBy = groupBy
	}

	// Expect "compute".
	if p.peek().Type != TokenCompute {
		return nil, fmt.Errorf("spl2: every: expected 'compute' at position %d", p.peek().Pos)
	}
	p.advance()

	aggs, err := p.parseAggList()
	if err != nil {
		return nil, err
	}
	cmd.Aggregations = aggs

	return cmd, nil
}

// parseBucketCmd parses Lynx Flow: bucket <field> span=<dur> [as <alias>].
// Desugars to BinCommand.
func (p *Parser) parseBucketCmd() (*BinCommand, error) {
	// "bucket" is TokenBucket — reuse parseBin logic.
	p.advance() // consume "bucket"
	cmd := &BinCommand{}

	field, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	cmd.Field = field.Literal

	if p.peek().Type == TokenSpan || (isIdentLike(p.peek().Type) && strings.ToLower(p.peek().Literal) == "span") {
		p.advance()
		if _, err := p.expect(TokenEq); err != nil {
			return nil, err
		}
		cmd.Span = p.readSpanValue()
	}

	if p.peek().Type == TokenAs {
		p.advance()
		alias, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		cmd.Alias = alias.Literal
	}

	return cmd, nil
}

// parseOrderBy parses: order by <field> [asc|desc] [, <field> [asc|desc] ...].
// Desugars to SortCommand.
func (p *Parser) parseOrderBy() (*SortCommand, error) {
	p.advance() // consume "order"

	if p.peek().Type != TokenBy {
		return nil, fmt.Errorf("spl2: expected 'by' after 'order' at position %d", p.peek().Pos)
	}
	p.advance() // consume "by"

	return p.parseOrderByFields()
}

// parseOrderByFields parses: <field> [asc|desc] [, <field> [asc|desc] ...].
func (p *Parser) parseOrderByFields() (*SortCommand, error) {
	var fields []SortField

	for {
		tok := p.peek()
		if !isIdentLike(tok.Type) {
			break
		}
		p.advance()
		sf := SortField{Name: tok.Literal}

		// Check for asc/desc.
		if p.peek().Type == TokenDesc {
			p.advance()
			sf.Desc = true
		} else if p.peek().Type == TokenAsc {
			p.advance()
			// sf.Desc = false (default)
		}

		fields = append(fields, sf)

		if p.peek().Type == TokenComma {
			p.advance()
		} else {
			break
		}
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("spl2: order by requires at least one field")
	}

	return &SortCommand{Fields: fields}, nil
}

// parseTake parses: take <N>. Desugars to HeadCommand.
func (p *Parser) parseTake() (*HeadCommand, error) {
	p.advance() // consume "take"

	count := 10
	if p.peek().Type == TokenNumber {
		tok := p.advance()
		n, err := strconv.Atoi(tok.Literal)
		if err != nil {
			return nil, fmt.Errorf("spl2: invalid take count %q", tok.Literal)
		}
		count = n
	}

	return &HeadCommand{Count: count}, nil
}

// parseRank parses: rank top/bottom <N> by <expr>.
// Desugars to [SortCommand, HeadCommand].
func (p *Parser) parseRank() ([]Command, error) {
	p.advance() // consume "rank"

	tok := p.peek()
	var desc bool
	switch {
	case isIdentLike(tok.Type) && strings.ToLower(tok.Literal) == "top":
		desc = true
		p.advance()
	case isIdentLike(tok.Type) && strings.ToLower(tok.Literal) == "bottom":
		desc = false
		p.advance()
	case tok.Type == TokenTop:
		desc = true
		p.advance()
	case tok.Type == TokenBottom:
		desc = false
		p.advance()
	default:
		return nil, fmt.Errorf("spl2: rank: expected 'top' or 'bottom' at position %d", tok.Pos)
	}

	// Expect N.
	nTok, err := p.expect(TokenNumber)
	if err != nil {
		return nil, fmt.Errorf("spl2: rank: expected count after top/bottom")
	}
	n, err := strconv.Atoi(nTok.Literal)
	if err != nil {
		return nil, fmt.Errorf("spl2: rank: invalid count %q", nTok.Literal)
	}

	// Expect "by".
	if p.peek().Type != TokenBy {
		return nil, fmt.Errorf("spl2: rank: expected 'by' at position %d", p.peek().Pos)
	}
	p.advance()

	// Field name.
	field, err := p.expectIdent()
	if err != nil {
		return nil, err
	}

	sortCmd := &SortCommand{Fields: []SortField{{Name: field.Literal, Desc: desc}}}
	headCmd := &HeadCommand{Count: n}

	return []Command{sortCmd, headCmd}, nil
}

// parseTopOrTopby handles the overloaded "top" command:
//   - top N field           → frequency (TopCommand)
//   - top N field by field  → frequency within groups (TopCommand)
//   - top N field by agg()  → metric ranking (desugars to stats+sort+head like topby)
func (p *Parser) parseTopOrTopby() ([]Command, error) {
	p.advance() // consume "top"

	n := 10
	if p.peek().Type == TokenNumber {
		tok := p.advance()
		parsed, err := strconv.Atoi(tok.Literal)
		if err != nil {
			return nil, fmt.Errorf("spl2: invalid top count %q", tok.Literal)
		}
		n = parsed
	}

	field, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("spl2: top requires a field name")
	}

	if p.peek().Type != TokenBy {
		return []Command{&TopCommand{N: n, Field: field.Literal}}, nil
	}

	p.advance() // consume "by"

	// Disambiguate: if next is ident followed by '(' → metric ranking (topby).
	// Otherwise → frequency within groups.
	if isIdentLike(p.peek().Type) && p.peekAt(1).Type == TokenLParen {
		// Metric ranking: top N field by agg() [compute extra_aggs]
		return p.parseTopbyBody(n, field.Literal, true)
	}

	// Frequency within groups.
	byField, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("spl2: expected field name after 'by'")
	}

	return []Command{&TopCommand{N: n, Field: field.Literal, ByField: byField.Literal}}, nil
}

// parseTopbyCmd parses: topby <N> <field> using <agg> [compute <extra_aggs>].
func (p *Parser) parseTopbyCmd() ([]Command, error) {
	p.advance() // consume "topby"

	nTok, err := p.expect(TokenNumber)
	if err != nil {
		return nil, fmt.Errorf("spl2: topby requires a count")
	}
	n, _ := strconv.Atoi(nTok.Literal)

	field, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("spl2: topby requires a field name")
	}

	if p.peek().Type != TokenUsing {
		return nil, fmt.Errorf("spl2: topby: expected 'using' at position %d", p.peek().Pos)
	}
	p.advance() // consume "using"

	return p.parseTopbyBody(n, field.Literal, true)
}

// parseBottombyCmd parses: bottomby <N> <field> using <agg> [compute <extra_aggs>].
func (p *Parser) parseBottombyCmd() ([]Command, error) {
	p.advance() // consume "bottomby"

	nTok, err := p.expect(TokenNumber)
	if err != nil {
		return nil, fmt.Errorf("spl2: bottomby requires a count")
	}
	n, _ := strconv.Atoi(nTok.Literal)

	field, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("spl2: bottomby requires a field name")
	}

	if p.peek().Type != TokenUsing {
		return nil, fmt.Errorf("spl2: bottomby: expected 'using' at position %d", p.peek().Pos)
	}
	p.advance() // consume "using"

	return p.parseTopbyBody(n, field.Literal, false)
}

// parseTopbyBody parses the shared body after "using" (or after "by agg()"):
// <ranking_agg> [compute <extra_aggs>]
// Desugars to [StatsCommand, SortCommand, HeadCommand].
func (p *Parser) parseTopbyBody(n int, groupField string, descSort bool) ([]Command, error) {
	// Parse the ranking aggregation (single agg).
	rankAgg, err := p.parseSingleAgg()
	if err != nil {
		return nil, err
	}

	// Auto-generate alias if missing.
	if rankAgg.Alias == "" {
		if len(rankAgg.Args) > 0 {
			rankAgg.Alias = rankAgg.Func + "_" + rankAgg.Args[0].String()
		} else {
			rankAgg.Alias = rankAgg.Func
		}
	}

	allAggs := []AggExpr{rankAgg}

	// Optional "compute" extra aggregations.
	if p.peek().Type == TokenCompute {
		p.advance()
		extras, err := p.parseAggList()
		if err != nil {
			return nil, err
		}
		allAggs = append(allAggs, extras...)
	}

	statsCmd := &StatsCommand{Aggregations: allAggs, GroupBy: []string{groupField}}
	sortCmd := &SortCommand{Fields: []SortField{{Name: rankAgg.Alias, Desc: descSort}}}
	headCmd := &HeadCommand{Count: n}

	return []Command{statsCmd, sortCmd, headCmd}, nil
}

// parseSingleAgg parses a single aggregation: func([args]) [as alias].
func (p *Parser) parseSingleAgg() (AggExpr, error) {
	funcTok, err := p.expectIdent()
	if err != nil {
		return AggExpr{}, fmt.Errorf("spl2: expected aggregation function name")
	}

	if p.peek().Type != TokenLParen {
		// No-arg agg without parens.
		agg := AggExpr{Func: funcTok.Literal}
		if p.peek().Type == TokenAs {
			p.advance()
			alias, err := p.expectIdent()
			if err != nil {
				return AggExpr{}, err
			}
			agg.Alias = alias.Literal
		}

		return agg, nil
	}

	p.advance() // consume (
	var args []Expr
	for p.peek().Type != TokenRParen {
		if len(args) > 0 {
			if _, err := p.expect(TokenComma); err != nil {
				return AggExpr{}, err
			}
		}
		arg, err := p.parseExpr()
		if err != nil {
			return AggExpr{}, err
		}
		args = append(args, arg)
	}
	p.advance() // consume )

	agg := AggExpr{Func: funcTok.Literal, Args: args}
	if p.peek().Type == TokenAs {
		p.advance()
		alias, err := p.expectIdent()
		if err != nil {
			return AggExpr{}, err
		}
		agg.Alias = alias.Literal
	}

	return agg, nil
}

// parseBottomCmd parses: bottom <N> <field> [by <agg>/<field>].
// Frequency mode → RareCommand. Metric mode → bottomby desugaring.
func (p *Parser) parseBottomCmd() ([]Command, error) {
	p.advance() // consume "bottom"

	n := 10
	if p.peek().Type == TokenNumber {
		tok := p.advance()
		parsed, _ := strconv.Atoi(tok.Literal)
		n = parsed
	}

	field, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("spl2: bottom requires a field name")
	}

	if p.peek().Type != TokenBy {
		return []Command{&RareCommand{N: n, Field: field.Literal}}, nil
	}
	p.advance() // consume "by"

	// Disambiguate: agg() vs bare field.
	if isIdentLike(p.peek().Type) && p.peekAt(1).Type == TokenLParen {
		return p.parseTopbyBody(n, field.Literal, false)
	}

	byField, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("spl2: expected field name after 'by'")
	}

	return []Command{&RareCommand{N: n, Field: field.Literal, ByField: byField.Literal}}, nil
}

// parseRunning parses: running [window=N] [current=true|false] <aggs> [by <fields>].
// Desugars to StreamstatsCommand.
func (p *Parser) parseRunning() (*StreamstatsCommand, error) {
	p.advance() // consume "running"
	cmd := &StreamstatsCommand{Current: true, Window: 0}

	// Parse optional options before aggregations.
	p.parseStreamstatsOptions(cmd)

	aggs, err := p.parseAggList()
	if err != nil {
		return nil, err
	}
	cmd.Aggregations = aggs

	if p.peek().Type == TokenBy {
		p.advance()
		groupBy, err := p.parseIdentListLF()
		if err != nil {
			return nil, err
		}
		cmd.GroupBy = groupBy
	}

	// Trailing options.
	p.parseStreamstatsOptions(cmd)

	return cmd, nil
}

// parseEnrichCmd parses: enrich <aggs> [by <fields>].
// Desugars to EventstatsCommand.
func (p *Parser) parseEnrichCmd() (*EventstatsCommand, error) {
	p.advance() // consume "enrich"

	aggs, err := p.parseAggList()
	if err != nil {
		return nil, err
	}

	var groupBy []string
	if p.peek().Type == TokenBy {
		p.advance()
		groupBy, err = p.parseIdentListLF()
		if err != nil {
			return nil, err
		}
	}

	return &EventstatsCommand{Aggregations: aggs, GroupBy: groupBy}, nil
}

// parseLynxParse parses: parse <format>(<field>[, <pattern>]) [as <ns>] [extract (<f1>,<f2>)] [if_missing].
func (p *Parser) parseLynxParse() (Command, error) {
	p.advance() // consume "parse"

	// Read format name. Accept ident-like tokens AND SPL2 command keywords
	// like "json" (TokenJson) that are also valid format names.
	formatTok := p.peek()
	if !isIdentLike(formatTok.Type) && !isFormatKeyword(formatTok.Type) {
		return nil, fmt.Errorf("spl2: parse: expected format name at position %d", formatTok.Pos)
	}
	format := strings.ToLower(formatTok.Literal)
	p.advance()

	// Expect '('.
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, fmt.Errorf("spl2: parse: expected '(' after format name")
	}

	// Read source field.
	fieldTok, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("spl2: parse: expected field name inside parentheses")
	}
	sourceField := fieldTok.Literal

	// For "regex" and "pattern" formats, expect comma + pattern string.
	var pattern string
	if format == "regex" || format == "pattern" {
		if _, err := p.expect(TokenComma); err != nil {
			return nil, fmt.Errorf("spl2: parse %s: expected comma and pattern string", format)
		}
		patTok, err := p.expect(TokenString)
		if err != nil {
			return nil, fmt.Errorf("spl2: parse %s: expected pattern string", format)
		}
		pattern = patTok.Literal
	}

	// Expect ')'.
	if _, err := p.expect(TokenRParen); err != nil {
		return nil, fmt.Errorf("spl2: parse: expected ')' after arguments")
	}

	// For regex: return RexCommand.
	if format == "regex" {
		return &RexCommand{Field: sourceField, Pattern: pattern}, nil
	}

	// For all other formats: build UnpackCommand, then parse orthogonal modifiers.
	cmd := &UnpackCommand{Format: format, SourceField: sourceField}
	if format == "pattern" {
		cmd.Pattern = pattern
	}

	// Parse modifiers in any order: "as <ns>", "extract (<f1>, <f2>)", "if_missing".
	for {
		tok := p.peek()
		if tok.Type == TokenAs {
			// as <namespace> → prefix = namespace + "."
			p.advance()
			ns, err := p.expectIdent()
			if err != nil {
				return nil, fmt.Errorf("spl2: parse: expected namespace after 'as'")
			}
			cmd.Prefix = ns.Literal + "."
		} else if tok.Type == TokenExtract {
			// extract (<f1>, <f2>, ...)
			p.advance()
			if _, err := p.expect(TokenLParen); err != nil {
				return nil, fmt.Errorf("spl2: parse: expected '(' after 'extract'")
			}
			var fieldList []string
			for p.peek().Type != TokenRParen && p.peek().Type != TokenEOF {
				if len(fieldList) > 0 && p.peek().Type == TokenComma {
					p.advance()
				}
				f, err := p.expectIdent()
				if err != nil {
					return nil, fmt.Errorf("spl2: parse: expected field name in extract()")
				}
				fieldList = append(fieldList, f.Literal)
			}
			if _, err := p.expect(TokenRParen); err != nil {
				return nil, fmt.Errorf("spl2: parse: expected ')' after extract fields")
			}
			cmd.Fields = fieldList
		} else if tok.Type == TokenIfMissing {
			p.advance()
			cmd.KeepOriginal = true
		} else {
			break
		}
	}

	return cmd, nil
}

// parseExplode parses: explode <field>[, <field2>, ...] [as <alias>].
// Multi-field form zip-expands parallel arrays. Alias only allowed for single-field.
// Desugars to UnrollCommand.
func (p *Parser) parseExplode() (*UnrollCommand, error) {
	p.advance() // consume "explode"

	field, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("spl2: explode requires a field name")
	}

	cmd := &UnrollCommand{Field: field.Literal}

	for p.peek().Type == TokenComma {
		p.advance() // consume comma
		extra, err := p.expectIdent()
		if err != nil {
			return nil, fmt.Errorf("spl2: explode: expected field name after comma")
		}
		cmd.ExtraFields = append(cmd.ExtraFields, extra.Literal)
	}

	// Alias only allowed for single-field explode.
	if len(cmd.ExtraFields) == 0 && p.peek().Type == TokenAs {
		p.advance()
		alias, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		cmd.Alias = alias.Literal
	}

	return cmd, nil
}

// parseLynxPack parses: pack <f1>, <f2> into <target> OR pack into <target>.
// Desugars to PackJsonCommand.
func (p *Parser) parseLynxPack() (*PackJsonCommand, error) {
	p.advance() // consume "pack"
	cmd := &PackJsonCommand{}

	// Check for immediate "into" (no field list).
	if p.peek().Type == TokenInto {
		p.advance()
		target, err := p.expectIdent()
		if err != nil {
			return nil, fmt.Errorf("spl2: pack: expected target field name after 'into'")
		}
		cmd.Target = target.Literal

		return cmd, nil
	}

	// Parse comma-separated field list until "into".
	for {
		tok := p.peek()
		if tok.Type == TokenInto {
			break
		}
		if !isIdentLike(tok.Type) {
			return nil, fmt.Errorf("spl2: pack: expected field name or 'into', got %s %q", tok.Type, tok.Literal)
		}
		p.advance()
		cmd.Fields = append(cmd.Fields, tok.Literal)

		if p.peek().Type == TokenComma {
			p.advance()
		}
	}

	if _, err := p.expect(TokenInto); err != nil {
		return nil, fmt.Errorf("spl2: pack: expected 'into'")
	}

	target, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("spl2: pack: expected target field name after 'into'")
	}
	cmd.Target = target.Literal

	return cmd, nil
}

// parseLookupCmd parses: lookup <dataset> on <field>.
// Desugars to JoinCommand{JoinType: "left"}.
func (p *Parser) parseLookupCmd() (*JoinCommand, error) {
	p.advance() // consume "lookup"

	dataset, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("spl2: lookup requires a dataset name")
	}

	if p.peek().Type != TokenOn {
		return nil, fmt.Errorf("spl2: lookup: expected 'on' at position %d", p.peek().Pos)
	}
	p.advance() // consume "on"

	field, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("spl2: lookup: expected field name after 'on'")
	}

	return &JoinCommand{
		JoinType: "left",
		Field:    field.Literal,
		Subquery: &Query{Source: &SourceClause{Index: dataset.Literal}},
	}, nil
}

// parseLatency parses: latency <field> every <span> [by <group>] [compute <percentiles>].
// Desugars to TimechartCommand.
func (p *Parser) parseLatency() (*TimechartCommand, error) {
	p.advance() // consume "latency"

	field, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("spl2: latency requires a duration field name")
	}

	// Expect "every".
	if p.peek().Type != TokenEvery {
		return nil, fmt.Errorf("spl2: latency: expected 'every' at position %d", p.peek().Pos)
	}
	p.advance()

	span := p.readSpanValue()
	if span == "" {
		return nil, fmt.Errorf("spl2: latency: expected time span after 'every'")
	}

	cmd := &TimechartCommand{Span: span}

	// Optional "by <group>".
	if p.peek().Type == TokenBy {
		p.advance()
		groupBy, err := p.parseIdentListLF()
		if err != nil {
			return nil, err
		}
		cmd.GroupBy = groupBy
	}

	// Optional "compute <percentiles>". Default: p50, p95, p99, count.
	if p.peek().Type == TokenCompute {
		p.advance()
		// Parse custom agg list — these are shorthand names like p50, p95 etc.
		aggs, err := p.parseLatencyAggs(field.Literal)
		if err != nil {
			return nil, err
		}
		cmd.Aggregations = aggs
	} else {
		// Default percentiles.
		f := &FieldExpr{Name: field.Literal}
		cmd.Aggregations = []AggExpr{
			{Func: "perc50", Args: []Expr{f}, Alias: "p50"},
			{Func: "perc95", Args: []Expr{f}, Alias: "p95"},
			{Func: "perc99", Args: []Expr{f}, Alias: "p99"},
			{Func: "count", Alias: "count"},
		}
	}

	return cmd, nil
}

// parseLatencyAggs parses shorthand agg names for latency: p50, p75, p90, p95, p99, avg, max, count, etc.
func (p *Parser) parseLatencyAggs(durField string) ([]AggExpr, error) {
	var aggs []AggExpr
	f := &FieldExpr{Name: durField}

	for {
		tok := p.peek()
		if !isIdentLike(tok.Type) {
			break
		}
		name := strings.ToLower(tok.Literal)

		// Check if this is a function call with parens (standard agg).
		if p.peekAt(1).Type == TokenLParen {
			agg, err := p.parseSingleAgg()
			if err != nil {
				return nil, err
			}
			aggs = append(aggs, agg)
		} else {
			// Shorthand: p50, p95, avg, max, count, etc.
			p.advance()
			alias := name
			funcName := name

			// Map shorthand to full function name.
			switch name {
			case "p50":
				funcName = "perc50"
			case "p75":
				funcName = "perc75"
			case "p90":
				funcName = "perc90"
			case "p95":
				funcName = "perc95"
			case "p99":
				funcName = "perc99"
			case "count":
				aggs = append(aggs, AggExpr{Func: "count", Alias: alias})
				if p.peek().Type == TokenComma {
					p.advance()
				}

				continue
			}

			aggs = append(aggs, AggExpr{Func: funcName, Args: []Expr{f}, Alias: alias})
		}

		if p.peek().Type == TokenComma {
			p.advance()
		} else {
			break
		}
	}

	return aggs, nil
}

// parseErrorsCmd parses: errors [by <field>] [compute <aggs>].
// Desugars to [WhereCommand, StatsCommand].
func (p *Parser) parseErrorsCmd() ([]Command, error) {
	p.advance() // consume "errors"

	var groupBy []string
	if p.peek().Type == TokenBy {
		p.advance()
		var err error
		groupBy, err = p.parseIdentListLF()
		if err != nil {
			return nil, err
		}
	}

	var aggs []AggExpr
	if p.peek().Type == TokenCompute {
		p.advance()
		var err error
		aggs, err = p.parseAggList()
		if err != nil {
			return nil, err
		}
	} else {
		// Default: count()
		aggs = []AggExpr{{Func: "count"}}
	}

	// where level in ("error", "fatal")
	whereCmd := &WhereCommand{
		Expr: &InExpr{
			Field:  &FieldExpr{Name: "level"},
			Values: []Expr{&LiteralExpr{Value: "error"}, &LiteralExpr{Value: "fatal"}},
		},
	}
	statsCmd := &StatsCommand{Aggregations: aggs, GroupBy: groupBy}

	return []Command{whereCmd, statsCmd}, nil
}

// parseRateCmd parses: rate [per <span>] [by <field>].
// Desugars to TimechartCommand.
func (p *Parser) parseRateCmd() (*TimechartCommand, error) {
	p.advance() // consume "rate"

	span := "1m" // default
	if p.peek().Type == TokenPer {
		p.advance()
		span = p.readSpanValue()
		if span == "" {
			return nil, fmt.Errorf("spl2: rate: expected time span after 'per'")
		}
	}

	cmd := &TimechartCommand{
		Span:         span,
		Aggregations: []AggExpr{{Func: "count", Alias: "rate"}},
	}

	if p.peek().Type == TokenBy {
		p.advance()
		groupBy, err := p.parseIdentListLF()
		if err != nil {
			return nil, err
		}
		cmd.GroupBy = groupBy
	}

	return cmd, nil
}

// parsePercentilesCmd parses: percentiles <field> [by <group>].
// Desugars to StatsCommand with perc50/75/90/95/99.
func (p *Parser) parsePercentilesCmd() (*StatsCommand, error) {
	p.advance() // consume "percentiles"

	field, err := p.expectIdent()
	if err != nil {
		return nil, fmt.Errorf("spl2: percentiles requires a field name")
	}

	f := &FieldExpr{Name: field.Literal}
	aggs := []AggExpr{
		{Func: "perc50", Args: []Expr{f}, Alias: "p50"},
		{Func: "perc75", Args: []Expr{f}, Alias: "p75"},
		{Func: "perc90", Args: []Expr{f}, Alias: "p90"},
		{Func: "perc95", Args: []Expr{f}, Alias: "p95"},
		{Func: "perc99", Args: []Expr{f}, Alias: "p99"},
	}

	var groupBy []string
	if p.peek().Type == TokenBy {
		p.advance()
		groupBy, err = p.parseIdentListLF()
		if err != nil {
			return nil, err
		}
	}

	return &StatsCommand{Aggregations: aggs, GroupBy: groupBy}, nil
}

// parseSlowestCmd parses: slowest <N> [<group_field>] [by <dur_field>].
// Row mode (no group field): [SortCommand desc, HeadCommand].
// Group mode (with group field): desugars like topby using max(dur).
func (p *Parser) parseSlowestCmd() ([]Command, error) {
	p.advance() // consume "slowest"

	n := 10
	if p.peek().Type == TokenNumber {
		tok := p.advance()
		parsed, _ := strconv.Atoi(tok.Literal)
		n = parsed
	}

	// Check if we have a group field before "by".
	var groupField string
	durField := "duration_ms" // default

	// Peek: if next token is an ident and it's NOT "by", it's the group field.
	if isIdentLike(p.peek().Type) && p.peek().Type != TokenBy {
		groupField = p.advance().Literal
	}

	// Optional "by <dur_field>".
	if p.peek().Type == TokenBy {
		p.advance()
		dur, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		durField = dur.Literal
	}

	if groupField == "" {
		// Row mode: sort desc + head.
		sortCmd := &SortCommand{Fields: []SortField{{Name: durField, Desc: true}}}
		headCmd := &HeadCommand{Count: n}

		return []Command{sortCmd, headCmd}, nil
	}

	// Group mode: topby using max(dur).
	f := &FieldExpr{Name: durField}
	maxAlias := "max_" + durField
	statsCmd := &StatsCommand{
		Aggregations: []AggExpr{{Func: "max", Args: []Expr{f}, Alias: maxAlias}},
		GroupBy:      []string{groupField},
	}
	sortCmd := &SortCommand{Fields: []SortField{{Name: maxAlias, Desc: true}}}
	headCmd := &HeadCommand{Count: n}

	return []Command{statsCmd, sortCmd, headCmd}, nil
}

// parseIdentListLF parses a comma-separated list of identifiers, accepting
// ident-like keyword tokens (Lynx Flow compatible).
func (p *Parser) parseIdentListLF() ([]string, error) {
	var names []string

	for {
		tok := p.peek()
		if !isIdentLike(tok.Type) && tok.Type != TokenGlob {
			break
		}
		p.advance()
		names = append(names, tok.Literal)

		if p.peek().Type == TokenComma {
			p.advance()
		} else {
			break
		}
	}

	if len(names) == 0 {
		return nil, fmt.Errorf("spl2: expected at least one identifier at position %d", p.peek().Pos)
	}

	return names, nil
}
