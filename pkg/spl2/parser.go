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

	// Parse optional FROM clause.
	if p.peek().Type == TokenFrom {
		p.advance()
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

		cmd, err := p.parseCommand()
		if err != nil {
			return nil, err
		}
		if cmd != nil {
			q.Commands = append(q.Commands, cmd)
		}
	}

	return q, nil
}

func (p *Parser) parseCommand() (Command, error) {
	tok := p.peek()

	switch tok.Type {
	case TokenSearch:
		return p.parseSearch()
	case TokenWhere:
		return p.parseWhere()
	case TokenStats:
		return p.parseStats()
	case TokenEval:
		return p.parseEval()
	case TokenSort:
		return p.parseSort()
	case TokenHead:
		return p.parseHead()
	case TokenTail:
		return p.parseTail()
	case TokenTimechart:
		return p.parseTimechart()
	case TokenRex:
		return p.parseRex()
	case TokenFields:
		return p.parseFields()
	case TokenTable:
		return p.parseTable()
	case TokenDedup:
		return p.parseDedup()
	case TokenRename:
		return p.parseRename()
	case TokenBin:
		return p.parseBin()
	case TokenStreamstats:
		return p.parseStreamstats()
	case TokenEventstats:
		return p.parseEventstats()
	case TokenJoin:
		return p.parseJoin()
	case TokenAppend:
		return p.parseAppend()
	case TokenMultisearch:
		return p.parseMultisearch()
	case TokenTransaction:
		return p.parseTransaction()
	case TokenXyseries:
		return p.parseXYSeries()
	case TokenTop:
		return p.parseTop()
	case TokenRare:
		return p.parseRare()
	case TokenFillnull:
		return p.parseFillnull()
	case TokenMaterialize:
		return p.parseMaterialize()
	case TokenFrom:
		return p.parseFromCommand()
	case TokenViews:
		return p.parseViews()
	case TokenDropview:
		return p.parseDropview()
	case TokenEOF:
		return nil, nil
	default:
		return nil, fmt.Errorf("spl2: unexpected command %s %q at position %d", tok.Type, tok.Literal, tok.Pos)
	}
}

func (p *Parser) parseSearch() (Command, error) {
	p.advance() // consume "search"

	// Check for SEARCH index=<idx> syntax.
	if p.peek().Type == TokenIdent && strings.ToLower(p.peek().Literal) == "index" &&
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

	// Parse first assignment.
	field, err := p.expect(TokenIdent)
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

	// Parse additional comma-separated assignments: eval a=1, b=2
	for p.peek().Type == TokenComma {
		p.advance() // consume comma
		f, err := p.expect(TokenIdent)
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

func (p *Parser) parseSort() (*SortCommand, error) {
	p.advance() // consume "sort"

	var fields []SortField
	for {
		tok := p.peek()
		if tok.Type == TokenPipe || tok.Type == TokenEOF || tok.Type == TokenRBracket {
			break
		}

		if tok.Type == TokenMinus {
			p.advance()
			name := p.peek()
			if name.Type != TokenIdent {
				return nil, fmt.Errorf("spl2: expected field name after - in sort, got %s", name.Type)
			}
			p.advance()
			fields = append(fields, SortField{Name: name.Literal, Desc: true})
		} else if tok.Type == TokenPlus {
			p.advance()
			name := p.peek()
			if name.Type != TokenIdent {
				return nil, fmt.Errorf("spl2: expected field name after + in sort, got %s", name.Type)
			}
			p.advance()
			fields = append(fields, SortField{Name: name.Literal})
		} else if tok.Type == TokenIdent {
			// Could be a plain field or a field with - prefix baked in from old lexer.
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
	if p.peek().Type == TokenIdent && strings.ToLower(p.peek().Literal) == "field" &&
		p.peekAt(1).Type == TokenEq {
		p.advance() // consume "field"
		p.advance() // consume "="
		tok, err := p.expect(TokenIdent)
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

	fields, err := p.parseIdentList()
	if err != nil {
		return nil, err
	}
	cmd.Fields = fields

	return cmd, nil
}

func (p *Parser) parseTable() (*TableCommand, error) {
	p.advance() // consume "table"

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
		old, err := p.expect(TokenIdent)
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenAs); err != nil {
			return nil, err
		}
		newName, err := p.expect(TokenIdent)
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
	field, err := p.expect(TokenIdent)
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
		alias, err := p.expect(TokenIdent)
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
	field, err := p.expect(TokenIdent)
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
	field, err := p.expect(TokenIdent)
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

	x, err := p.expect(TokenIdent)
	if err != nil {
		return nil, err
	}
	y, err := p.expect(TokenIdent)
	if err != nil {
		return nil, err
	}
	v, err := p.expect(TokenIdent)
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
	field, err := p.expect(TokenIdent)
	if err != nil {
		return nil, fmt.Errorf("spl2: top/rare requires a field name")
	}

	cmd := &TopCommand{N: n, Field: field.Literal}

	// Optional: by <field>
	if p.peek().Type == TokenBy {
		p.advance() // consume "by"
		byField, err := p.expect(TokenIdent)
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

	switch tok.Type {
	case TokenIdent:
		p.advance()

		return tok.Literal, nil

	case TokenString:
		p.advance()

		return tok.Literal, nil

	case TokenGlob:
		p.advance()

		return tok.Literal, nil

	case TokenStar:
		p.advance()

		return "*", nil

	case TokenNumber:
		p.advance()
		name := tok.Literal

		// If an IDENT follows immediately (no whitespace gap), merge.
		// e.g., "2xlog" → NUMBER("2") @ pos 5 + IDENT("xlog") @ pos 6
		next := p.peek()
		if next.Type == TokenIdent && next.Pos == tok.Pos+len(tok.Literal) {
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

// parseExpr parses a boolean expression with OR as lowest precedence.
func (p *Parser) parseExpr() (Expr, error) {
	p.depth++
	if p.depth > maxExprDepth {
		return nil, ErrQueryTooComplex
	}
	defer func() { p.depth-- }()

	return p.parseOr()
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
		p.advance()
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

		return &InExpr{Field: left, Values: values}, nil
	}

	return left, nil
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

	for p.peek().Type == TokenStar || p.peek().Type == TokenSlash {
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

		return &FieldExpr{Name: tok.Literal}, nil

	// Handle keywords that can also be function names in expression context.
	case TokenEval, TokenStats, TokenSearch, TokenIn:
		if p.peekAt(1).Type == TokenLParen {
			return p.parseFuncCall()
		}
		p.advance()

		return &FieldExpr{Name: tok.Literal}, nil

	default:
		return nil, fmt.Errorf("spl2: unexpected token %s %q at position %d in expression", tok.Type, tok.Literal, tok.Pos)
	}
}

func (p *Parser) parseFuncCall() (Expr, error) {
	name := p.advance() // function name (could be ident or keyword like eval)
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
		if tok.Type != TokenIdent {
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
				alias, err := p.expect(TokenIdent)
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

		agg := AggExpr{Func: funcName, Args: args}

		// Optional "as alias".
		if p.peek().Type == TokenAs {
			p.advance()
			alias, err := p.expect(TokenIdent)
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
		if tok.Type != TokenIdent {
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
