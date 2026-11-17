package parser

import (
	"strings"

	"github.com/lynxbase/lynxdb/pkg/ppl/ast"
	"github.com/lynxbase/lynxdb/pkg/ppl/lexer"
)

// Operator precedence, lowest first: OR, XOR, AND, NOT, comparisons and
// predicates (= != < <= > >= LIKE ILIKE REGEXP CONTAINS, IN, BETWEEN, IS
// NULL), + -, * / %, unary minus.

func (p *parser) expr() ast.Expr {
	p.enter()
	e := p.orExpr()
	p.depth--
	return e
}

func (p *parser) binary(op string, l, r ast.Expr) ast.Expr {
	return &ast.Binary{Base: ast.Base{Span: ast.Span{Start: l.NodeSpan().Start, End: r.NodeSpan().End}}, Op: op, L: l, R: r}
}

func (p *parser) orExpr() ast.Expr {
	l := p.xorExpr()
	for p.atKw("or") {
		p.next()
		l = p.binary("or", l, p.xorExpr())
	}
	return l
}

func (p *parser) xorExpr() ast.Expr {
	l := p.andExpr()
	for p.atKw("xor") {
		p.next()
		l = p.binary("xor", l, p.andExpr())
	}
	return l
}

func (p *parser) andExpr() ast.Expr {
	l := p.notExpr()
	for p.atKw("and") {
		p.next()
		l = p.binary("and", l, p.notExpr())
	}
	return l
}

func (p *parser) notExpr() ast.Expr {
	if p.atKw("not") {
		start := p.next().Pos
		p.enter()
		x := p.notExpr()
		p.depth--
		return &ast.Unary{Base: ast.Base{Span: p.span(start)}, Op: "not", X: x}
	}
	return p.predicate()
}

var compareOps = map[lexer.Kind]string{
	lexer.Eq: "=", lexer.EqEq: "==", lexer.NotEq: "!=", lexer.Lt: "<", lexer.Le: "<=", lexer.Gt: ">", lexer.Ge: ">=",
}

var compareWords = []string{"like", "ilike", "regexp", "contains"}

func (p *parser) compareOp() (string, bool) {
	t := p.peek()
	if op, ok := compareOps[t.Kind]; ok {
		return op, true
	}
	for _, w := range compareWords {
		if t.Is(w) {
			return w, true
		}
	}
	return "", false
}

func (p *parser) predicate() ast.Expr {
	l := p.additive()
	for {
		start := l.NodeSpan().Start
		if op, ok := p.compareOp(); ok {
			p.next()
			l = p.binary(op, l, p.additive())
			continue
		}
		not := false
		if p.atKw("not") && (p.peekAt(1).Is("in") || p.peekAt(1).Is("between")) {
			p.next()
			not = true
		}
		switch {
		case p.atKw("in"):
			p.next()
			if p.at(lexer.LBracket) {
				sub := p.subSearch()
				l = &ast.Subquery{Base: ast.Base{Span: p.span(start)}, Kind: ast.SubqueryIn, Values: []ast.Expr{l}, Not: not, Pipeline: sub}
				continue
			}
			p.expect(lexer.LParen)
			var list []ast.Expr
			if !p.at(lexer.RParen) {
				list = append(list, p.expr())
				for p.accept(lexer.Comma) {
					list = append(list, p.expr())
				}
			}
			p.expect(lexer.RParen)
			l = &ast.In{Base: ast.Base{Span: p.span(start)}, X: l, List: list, Not: not}
			continue
		case p.atKw("between"):
			p.next()
			lo := p.additive()
			p.expectKw("and")
			hi := p.additive()
			l = &ast.Between{Base: ast.Base{Span: p.span(start)}, X: l, Lo: lo, Hi: hi, Not: not}
			continue
		case not:
			p.fail(p.peek(), "", "IN", "BETWEEN")
		case p.atKw("is"):
			p.next()
			isNot := p.acceptKw("not")
			p.expectKw("null")
			l = &ast.IsNull{Base: ast.Base{Span: p.span(start)}, X: l, Not: isNot}
			continue
		}
		return l
	}
}

func (p *parser) additive() ast.Expr {
	l := p.multiplicative()
	for p.at(lexer.Plus) || p.at(lexer.Minus) {
		op := p.next().Text
		l = p.binary(op, l, p.multiplicative())
	}
	return l
}

func (p *parser) multiplicative() ast.Expr {
	l := p.unary()
	for p.at(lexer.Star) || p.at(lexer.Slash) || p.at(lexer.Percent) {
		op := p.next().Text
		l = p.binary(op, l, p.unary())
	}
	return l
}

func (p *parser) unary() ast.Expr {
	if p.at(lexer.Minus) || p.at(lexer.Plus) {
		t := p.next()
		p.enter()
		x := p.unary()
		p.depth--
		if lit, ok := x.(*ast.Literal); ok && t.End.Offset == lit.Span.Start.Offset &&
			(lit.Kind == ast.LitInt || lit.Kind == ast.LitDecimal || lit.Kind == ast.LitDouble || lit.Kind == ast.LitFloat) {
			if t.Kind == lexer.Minus {
				lit.Text = "-" + lit.Text
			}
			lit.Span.Start = t.Pos
			return lit
		}
		if t.Kind == lexer.Plus {
			return x
		}
		return &ast.Unary{Base: ast.Base{Span: p.span(t.Pos)}, Op: "-", X: x}
	}
	return p.primary()
}

func (p *parser) literal(kind ast.LiteralKind, t *lexer.Token, text string) *ast.Literal {
	return &ast.Literal{Base: ast.Base{Span: ast.Span{Start: t.Pos, End: t.End}}, Kind: kind, Text: text}
}

var intervalUnits = map[string]bool{
	"microsecond": true, "millisecond": true, "second": true, "minute": true, "hour": true, "day": true,
	"week": true, "month": true, "quarter": true, "year": true, "second_microsecond": true,
	"minute_microsecond": true, "minute_second": true, "hour_microsecond": true, "hour_second": true,
	"hour_minute": true, "day_microsecond": true, "day_second": true, "day_minute": true, "day_hour": true,
	"year_month": true,
}

func (p *parser) primary() ast.Expr {
	t := p.peek()
	switch t.Kind {
	case lexer.String:
		p.next()
		return p.literal(ast.LitString, t, t.Value)
	case lexer.Integer:
		p.next()
		return p.literal(ast.LitInt, t, t.Value)
	case lexer.Decimal:
		p.next()
		return p.literal(ast.LitDecimal, t, t.Value)
	case lexer.Double:
		p.next()
		return p.literal(ast.LitDouble, t, t.Value)
	case lexer.Float:
		p.next()
		return p.literal(ast.LitFloat, t, t.Value)
	case lexer.Span:
		p.next()
		num, unit := splitSpan(t.Value)
		return &ast.Interval{Base: ast.Base{Span: ast.Span{Start: t.Pos, End: t.End}}, Value: p.literal(ast.LitInt, t, num), Unit: unit}
	case lexer.Star:
		p.next()
		return &ast.AllFields{Base: ast.Base{Span: ast.Span{Start: t.Pos, End: t.End}}}
	case lexer.LBracket:
		start := t.Pos
		sub := p.subSearch()
		return &ast.Subquery{Base: ast.Base{Span: p.span(start)}, Kind: ast.SubqueryScalar, Pipeline: sub}
	case lexer.LParen:
		start := p.next().Pos
		first := p.expr()
		if p.at(lexer.Comma) {
			values := []ast.Expr{first}
			for p.accept(lexer.Comma) {
				values = append(values, p.expr())
			}
			p.expect(lexer.RParen)
			not := p.acceptKw("not")
			p.expectKw("in")
			sub := p.subSearch()
			return &ast.Subquery{Base: ast.Base{Span: p.span(start)}, Kind: ast.SubqueryIn, Values: values, Not: not, Pipeline: sub}
		}
		p.expect(lexer.RParen)
		return first
	case lexer.Ident:
		switch {
		case t.Is("true") || t.Is("false"):
			p.next()
			return p.literal(ast.LitBool, t, strings.ToLower(t.Text))
		case t.Is("null"):
			p.next()
			return p.literal(ast.LitNull, t, "null")
		case t.Is("exists") && p.peekAt(1).Kind == lexer.LBracket:
			start := p.next().Pos
			sub := p.subSearch()
			return &ast.Subquery{Base: ast.Base{Span: p.span(start)}, Kind: ast.SubqueryExists, Pipeline: sub}
		case t.Is("interval") && p.peekAt(1).Kind != lexer.LParen && p.peekAt(1).Kind != lexer.Eq:
			start := p.next().Pos
			v := p.additive()
			ut := p.peek()
			unit := strings.ToLower(ut.Text)
			if ut.Kind != lexer.Ident || !intervalUnits[unit] {
				p.fail(ut, "expected an interval unit", "SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR")
			}
			p.next()
			return &ast.Interval{Base: ast.Base{Span: p.span(start)}, Value: v, Unit: unit}
		case (t.Is("date") || t.Is("time") || t.Is("timestamp")) && p.peekAt(1).Kind == lexer.String:
			p.next()
			s := p.next()
			kind := map[string]ast.LiteralKind{"date": ast.LitDate, "time": ast.LitTime, "timestamp": ast.LitTimestamp}[strings.ToLower(t.Text)]
			lit := p.literal(kind, t, s.Value)
			lit.Span.End = s.End
			return lit
		case p.peekAt(1).Kind == lexer.LParen:
			return p.call()
		}
		return p.field()
	case lexer.QuotedIdent, lexer.NumericIdent:
		return p.field()
	}
	p.fail(t, "expected an expression", "expression")
	return nil
}

func splitSpan(v string) (num, unit string) {
	i := 0
	for i < len(v) && (v[i] >= '0' && v[i] <= '9' || v[i] == '.') {
		i++
	}
	return v[:i], v[i:]
}

// identPart reads one segment of a qualified name.
func (p *parser) identPart() (string, *lexer.Token) {
	t := p.peek()
	switch t.Kind {
	case lexer.Ident, lexer.NumericIdent:
		p.next()
		return t.Text, t
	case lexer.QuotedIdent:
		p.next()
		return t.Value, t
	case lexer.Integer:
		p.next()
		return t.Text, t
	}
	p.fail(t, "expected a field name", "field name")
	return "", t
}

// fieldNode holds a field and its first name segment in one allocation.
type fieldNode struct {
	f    ast.Field
	part [1]string
}

// field parses a qualified name: segments separated by dots with no
// surrounding white space.
func (p *parser) field() *ast.Field {
	start := p.peek().Pos
	name, _ := p.identPart()
	n := &fieldNode{part: [1]string{name}}
	f := &n.f
	f.Parts = n.part[:]
	for p.at(lexer.Dot) && !p.peek().SpaceBefore && !p.peekAt(1).SpaceBefore {
		switch p.peekAt(1).Kind {
		case lexer.Ident, lexer.QuotedIdent, lexer.NumericIdent, lexer.Integer:
		default:
			f.Span = p.span(start)
			return f
		}
		p.next()
		part, _ := p.identPart()
		f.Parts = append(f.Parts, part)
	}
	f.Span = p.span(start)
	return f
}

// fieldName parses a qualified name and returns its dotted form.
func (p *parser) fieldName() string { return p.field().Name() }

func (p *parser) isLambdaStart() bool {
	if p.at(lexer.Ident) && p.peekAt(1).Kind == lexer.Arrow {
		return true
	}
	if !p.at(lexer.LParen) {
		return false
	}
	for n := 1; ; n += 2 {
		if p.peekAt(n).Kind != lexer.Ident {
			return false
		}
		switch p.peekAt(n + 1).Kind {
		case lexer.Comma:
			continue
		case lexer.RParen:
			return p.peekAt(n+2).Kind == lexer.Arrow
		default:
			return false
		}
	}
}

func (p *parser) lambda() ast.Expr {
	start := p.peek().Pos
	var params []string
	if p.accept(lexer.LParen) {
		params = append(params, p.expect(lexer.Ident).Text)
		for p.accept(lexer.Comma) {
			params = append(params, p.expect(lexer.Ident).Text)
		}
		p.expect(lexer.RParen)
	} else {
		params = append(params, p.expect(lexer.Ident).Text)
	}
	p.expect(lexer.Arrow)
	body := p.expr()
	return &ast.Lambda{Base: ast.Base{Span: p.span(start)}, Params: params, Body: body}
}

func (p *parser) funcArg() ast.Expr {
	if p.isLambdaStart() {
		return p.lambda()
	}
	return p.expr()
}

// callNode holds a call and room for its first arguments in one allocation.
type callNode struct {
	c    ast.Call
	args [3]ast.Expr
}

func (p *parser) call() ast.Expr {
	nameTok := p.next()
	start := nameTok.Pos
	name := strings.ToLower(nameTok.Text)
	p.expect(lexer.LParen)
	cn := &callNode{c: ast.Call{Name: name}}
	c := &cn.c
	c.Args = cn.args[:0]
	switch name {
	case "cast":
		x := p.expr()
		p.expectKw("as")
		tt := p.expect(lexer.Ident)
		p.expect(lexer.RParen)
		return &ast.Cast{Base: ast.Base{Span: p.span(start)}, X: x, Type: strings.ToLower(tt.Text)}
	case "case":
		cs := &ast.Case{}
		for {
			if p.acceptKw("else") {
				cs.Else = p.expr()
				break
			}
			cs.Conds = append(cs.Conds, p.expr())
			p.expect(lexer.Comma)
			cs.Vals = append(cs.Vals, p.expr())
			if !p.accept(lexer.Comma) {
				if p.atKw("else") {
					continue
				}
				break
			}
		}
		p.expect(lexer.RParen)
		cs.Span = p.span(start)
		return cs
	case "position":
		sub := p.additive()
		p.expectKw("in")
		str := p.additive()
		p.expect(lexer.RParen)
		c.Args = append(c.Args, sub, str)
		c.Span = p.span(start)
		return c
	case "extract":
		part := strings.ToLower(p.expect(lexer.Ident).Text)
		p.expectKw("from")
		x := p.expr()
		p.expect(lexer.RParen)
		return &ast.DatePart{Base: ast.Base{Span: p.span(start)}, Func: name, Part: part, Args: []ast.Expr{x}}
	case "timestampadd", "timestampdiff":
		part := strings.ToLower(p.expect(lexer.Ident).Text)
		p.expect(lexer.Comma)
		a := p.expr()
		p.expect(lexer.Comma)
		b := p.expr()
		p.expect(lexer.RParen)
		return &ast.DatePart{Base: ast.Base{Span: p.span(start)}, Func: name, Part: part, Args: []ast.Expr{a, b}}
	case "get_format":
		part := strings.ToLower(p.expect(lexer.Ident).Text)
		p.expect(lexer.Comma)
		a := p.expr()
		p.expect(lexer.RParen)
		return &ast.DatePart{Base: ast.Base{Span: p.span(start)}, Func: name, Part: part, Args: []ast.Expr{a}}
	}
	if isRelevanceFunc(name) && p.at(lexer.LBracket) {
		at := p.next().Pos
		list := &ast.Call{Name: "array"}
		for !p.at(lexer.RBracket) {
			t := p.next()
			if t.Kind == lexer.EOF {
				p.fail(t, "", "']'")
			}
			if t.Kind == lexer.String || t.Kind == lexer.Ident || t.Kind == lexer.QuotedIdent {
				v := t.Value
				if t.Kind == lexer.Ident {
					v = t.Text
				}
				list.Args = append(list.Args, p.literal(ast.LitString, t, v))
			}
		}
		p.next()
		list.Span = p.span(at)
		c.Args = append(c.Args, list)
		p.accept(lexer.Comma)
	}
	if !p.at(lexer.RParen) {
		for {
			if arg, ok := p.atArg(relevanceArgNames...); ok && isRelevanceFunc(name) {
				at := p.next().Pos
				p.next()
				c.NamedArgs = append(c.NamedArgs, ast.Arg{Name: arg, Value: p.expr(), Span: p.span(at)})
			} else {
				c.Args = append(c.Args, p.funcArg())
			}
			if !p.accept(lexer.Comma) {
				break
			}
		}
	}
	p.expect(lexer.RParen)
	if len(c.Args) == 0 {
		c.Args = nil
	}
	c.Span = p.span(start)
	return c
}

var relevanceFuncs = map[string]bool{
	"match": true, "match_phrase": true, "match_phrase_prefix": true, "match_bool_prefix": true,
	"simple_query_string": true, "multi_match": true, "query_string": true,
}

func isRelevanceFunc(name string) bool { return relevanceFuncs[name] }

var relevanceArgNames = []string{
	"allow_leading_wildcard", "analyzer", "analyze_wildcard", "auto_generate_synonyms_phrase_query", "boost",
	"cutoff_frequency", "default_field", "default_operator", "enable_position_increments", "escape", "fields",
	"flags", "fuzziness", "fuzzy_max_expansions", "fuzzy_prefix_length", "fuzzy_rewrite", "fuzzy_transpositions",
	"lenient", "low_freq_operator", "max_determinized_states", "max_expansions", "minimum_should_match",
	"operator", "phrase_slop", "prefix_length", "quote_analyzer", "quote_field_suffix", "rewrite", "slop",
	"tie_breaker", "time_zone", "type", "zero_terms_query",
}
