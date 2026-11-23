package parser

import (
	"strings"

	"github.com/lynxbase/lynxdb/pkg/ppl/ast"
	"github.com/lynxbase/lynxdb/pkg/ppl/lexer"
)

// Search expressions bind NOT tightest, then OR, then AND; adjacent terms
// are joined by an implicit AND, as in the official grammar.

func (p *parser) search(def string) ast.Command {
	start := p.peek().Pos
	explicit := p.acceptKw("search")
	cmd := &ast.SearchCommand{}
	var termBuf [4]ast.SearchExpr
	terms := termBuf[:0]
	haveSource := false
	for !p.at(lexer.EOF) && !p.at(lexer.Pipe) && !p.at(lexer.RBracket) && !p.at(lexer.Semicolon) {
		if (p.atKw("source") || p.atKw("index")) && p.peekAt(1).Kind == lexer.Eq {
			if haveSource {
				p.fail(p.peek(), "a search has only one source clause")
			}
			haveSource = true
			p.next()
			p.next()
			p.sourceClause(cmd)
			continue
		}
		if len(terms) > 0 && p.atKw("and") {
			p.next()
			continue
		}
		terms = append(terms, p.searchOr())
	}
	if !haveSource {
		switch {
		case !explicit && len(terms) == 0:
			p.fail(p.peek(), "expected a query", "source=<index>", "search")
		case def == "":
			p.failAt(start, missingSource)
		}
		cmd.Sources = defaultSearch(start, def).Sources
	}
	for _, t := range terms {
		if cmd.Filter == nil {
			cmd.Filter = t
		} else {
			cmd.Filter = &ast.SearchAnd{Base: ast.Base{Span: ast.Span{Start: cmd.Filter.NodeSpan().Start, End: t.NodeSpan().End}}, L: cmd.Filter, R: t}
		}
	}
	cmd.Span = p.span(start)
	return cmd
}

// defaultSearch is the search of the default source def that a query naming
// no source starts with; it spans no text, at pos.
func defaultSearch(pos lexer.Pos, def string) *ast.SearchCommand {
	sp := ast.Base{Span: ast.Span{Start: pos, End: pos}}
	return &ast.SearchCommand{Base: sp, Sources: []ast.SourceRef{{Base: sp, Name: def}}}
}

func (p *parser) sourceClause(cmd *ast.SearchCommand) {
	if p.at(lexer.LBracket) {
		cmd.Sub = p.subSearch()
	} else {
		cmd.Sources = append(cmd.Sources, p.sourceRef())
		for p.at(lexer.Comma) {
			p.next()
			cmd.Sources = append(cmd.Sources, p.sourceRef())
		}
	}
	if p.atKw("as") {
		p.next()
		cmd.Alias = p.fieldName()
	}
}

// sourceRef reads an index name: the concatenation of adjacent name-like
// tokens, so that logs-2024.01.01, cluster:index and acc* need no quoting.
func (p *parser) sourceRef() ast.SourceRef {
	t := p.peek()
	start := t.Pos
	if t.Kind == lexer.QuotedIdent || t.Kind == lexer.String {
		p.next()
		return ast.SourceRef{Base: ast.Base{Span: p.span(start)}, Name: t.Value}
	}
	// While its tokens are adjacent and unquoted, the name is the source
	// text they span.
	var b strings.Builder
	copied := false
	last := start.Offset
	take := func(t *lexer.Token, text string) {
		if !copied && (t.Kind == lexer.QuotedIdent || t.Pos.Offset != last) {
			copied = true
			b.WriteString(p.src[start.Offset:last])
		}
		if copied {
			b.WriteString(text)
		}
		last = t.End.Offset
		p.next()
	}
	for first := true; ; first = false {
		t := p.peek()
		if !first && t.SpaceBefore {
			break
		}
		switch t.Kind {
		case lexer.Ident, lexer.NumericIdent, lexer.Integer, lexer.Decimal, lexer.Dot, lexer.Minus, lexer.Colon, lexer.Star, lexer.Span, lexer.Double, lexer.Float:
			take(t, t.Text)
			continue
		case lexer.QuotedIdent:
			if !first {
				take(t, t.Value)
				continue
			}
		}
		break
	}
	if last == start.Offset {
		p.fail(p.peek(), "expected an index name", "index name")
	}
	name := p.src[start.Offset:last]
	if copied {
		name = b.String()
	}
	return ast.SourceRef{Base: ast.Base{Span: p.span(start)}, Name: name}
}

func (p *parser) searchAnd() ast.SearchExpr {
	l := p.searchOr()
	for p.atKw("and") {
		p.next()
		r := p.searchOr()
		l = &ast.SearchAnd{Base: ast.Base{Span: ast.Span{Start: l.NodeSpan().Start, End: r.NodeSpan().End}}, L: l, R: r}
	}
	return l
}

func (p *parser) searchOr() ast.SearchExpr {
	l := p.searchNot()
	for p.atKw("or") {
		p.next()
		r := p.searchNot()
		l = &ast.SearchOr{Base: ast.Base{Span: ast.Span{Start: l.NodeSpan().Start, End: r.NodeSpan().End}}, L: l, R: r}
	}
	return l
}

func (p *parser) searchNot() ast.SearchExpr {
	if p.atKw("not") {
		start := p.next().Pos
		p.enter()
		x := p.searchNot()
		p.depth--
		return &ast.SearchNot{Base: ast.Base{Span: p.span(start)}, X: x}
	}
	return p.searchPrimary()
}

func (p *parser) searchPrimary() ast.SearchExpr {
	start := p.peek().Pos
	if p.accept(lexer.LParen) {
		p.enter()
		defer func() { p.depth-- }()
		e := p.searchAnd()
		for !p.at(lexer.RParen) {
			if p.at(lexer.EOF) || p.at(lexer.Pipe) {
				p.fail(p.peek(), "", "')'")
			}
			r := p.searchOr()
			e = &ast.SearchAnd{Base: ast.Base{Span: ast.Span{Start: e.NodeSpan().Start, End: r.NodeSpan().End}}, L: e, R: r}
			if p.atKw("and") {
				p.next()
			}
		}
		p.expect(lexer.RParen)
		return e
	}
	if (p.atKw("earliest") || p.atKw("latest")) && p.peekAt(1).Kind == lexer.Eq {
		latest := p.next().Is("latest")
		p.next()
		v, quoted := p.timeModifierValue()
		return &ast.TimeModifier{Base: ast.Base{Span: p.span(start)}, Latest: latest, Value: v, Quoted: quoted}
	}
	if p.isSearchFieldStart() {
		f := p.field()
		if op, ok := p.searchCompareOp(); ok {
			p.next()
			c := p.searchValue()
			c.Field, c.Op = f, op
			c.Span = p.span(start)
			return &c
		}
		p.expectKw("in")
		p.expect(lexer.LParen)
		in := &ast.SearchIn{Field: f}
		for {
			v := p.searchValue()
			v.Field, v.Op = f, "="
			in.Values = append(in.Values, v)
			if !p.accept(lexer.Comma) {
				break
			}
		}
		p.expect(lexer.RParen)
		in.Span = p.span(start)
		return in
	}
	v := p.searchValue()
	text := &ast.SearchText{Base: ast.Base{Span: p.span(start)}, Text: v.Value, Quoted: v.Quoted}
	return text
}

var searchCompareKinds = map[lexer.Kind]string{
	lexer.Eq: "=", lexer.EqEq: "=", lexer.NotEq: "!=", lexer.Lt: "<", lexer.Le: "<=", lexer.Gt: ">", lexer.Ge: ">=",
}

func (p *parser) searchCompareOp() (string, bool) {
	op, ok := searchCompareKinds[p.peek().Kind]
	return op, ok
}

// isSearchFieldStart reports whether the next tokens form "field <op>" or
// "field IN (".
func (p *parser) isSearchFieldStart() bool {
	save := p.i
	defer func() { p.i = save }()
	switch p.peek().Kind {
	case lexer.Ident, lexer.QuotedIdent:
	default:
		return false
	}
	ok := func() (ok bool) {
		defer func() {
			if recover() != nil {
				ok = false
			}
		}()
		p.field()
		return true
	}()
	if !ok {
		return false
	}
	if _, isOp := p.searchCompareOp(); isOp {
		return true
	}
	return p.atKw("in") && p.peekAt(1).Kind == lexer.LParen
}

// searchValue reads a search literal: a quoted string, or a run of adjacent
// term characters (letters, digits, '-', '_', '.', ':', '*', '?', '@', '/').
func (p *parser) searchValue() ast.SearchCompare {
	t := p.peek()
	var c ast.SearchCompare
	if t.Kind == lexer.String {
		p.next()
		c.Value, c.Quoted = t.Value, true
		return c
	}
	if t.Kind == lexer.QuotedIdent {
		p.next()
		c.Value = t.Value
		return c
	}
	var b strings.Builder
	kinds := 0
	for first := true; ; first = false {
		t := p.peek()
		if !first && t.SpaceBefore {
			break
		}
		switch t.Kind {
		case lexer.Ident, lexer.NumericIdent, lexer.Integer, lexer.Decimal, lexer.Double, lexer.Float, lexer.Span,
			lexer.Dot, lexer.Minus, lexer.Plus, lexer.Colon, lexer.Star, lexer.Question, lexer.Slash, lexer.TimeSnap:
			if first && (t.Is("and") || t.Is("or") || t.Is("not")) {
				break
			}
			b.WriteString(t.Text)
			p.next()
			kinds++
			continue
		}
		break
	}
	if b.Len() == 0 {
		p.fail(p.peek(), "expected a search term", "search term", "source=<index>")
	}
	c.Value = b.String()
	if kinds == 1 || (kinds == 2 && strings.HasPrefix(c.Value, "-")) {
		switch t.Kind {
		case lexer.Integer, lexer.Decimal, lexer.Double, lexer.Float, lexer.Minus:
			c.Numeric = isNumber(c.Value)
		case lexer.Ident:
			c.Bool = strings.EqualFold(c.Value, "true") || strings.EqualFold(c.Value, "false")
		}
	}
	return c
}

func isNumber(s string) bool {
	s = strings.TrimPrefix(s, "-")
	if s == "" {
		return false
	}
	dot := false
	for i := 0; i < len(s); i++ {
		switch {
		case s[i] >= '0' && s[i] <= '9':
		case s[i] == '.' && !dot:
			dot = true
		default:
			return false
		}
	}
	return true
}

// timeModifierValue reads the value of earliest= or latest=: now, now(), a
// number, a quoted string, a snap (@d), or a signed offset with an optional
// snap (-7d, +1d@d).
func (p *parser) timeModifierValue() (string, bool) {
	t := p.peek()
	switch t.Kind {
	case lexer.String:
		p.next()
		return t.Value, true
	case lexer.Integer, lexer.Decimal:
		p.next()
		return t.Text, false
	case lexer.TimeSnap:
		p.next()
		return t.Text, false
	case lexer.Ident:
		if t.Is("now") {
			p.next()
			if p.at(lexer.LParen) && p.peekAt(1).Kind == lexer.RParen {
				p.next()
				p.next()
			}
			return "now", false
		}
	case lexer.Plus, lexer.Minus:
		var b strings.Builder
		b.WriteString(p.next().Text)
		for !p.peek().SpaceBefore {
			nt := p.peek()
			if nt.Kind == lexer.Span || nt.Kind == lexer.TimeSnap || nt.Kind == lexer.Integer ||
				nt.Kind == lexer.Plus || nt.Kind == lexer.Minus || nt.Kind == lexer.Ident || nt.Kind == lexer.NumericIdent {
				b.WriteString(nt.Text)
				p.next()
				continue
			}
			break
		}
		return b.String(), false
	}
	p.fail(t, "expected a time modifier", "now", "-7d", "'2024-01-01 00:00:00'")
	return "", false
}
