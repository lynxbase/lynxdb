package parser

import (
	"strconv"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/ppl/ast"
	"github.com/lynxbase/lynxdb/pkg/ppl/lexer"
)

func (p *parser) base(start lexer.Pos) ast.Base { return ast.Base{Span: p.span(start)} }

func (p *parser) intLit() int64 {
	t := p.peek()
	neg := false
	if t.Kind == lexer.Minus {
		p.next()
		neg = true
		t = p.peek()
	}
	if t.Kind != lexer.Integer {
		p.fail(t, "expected an integer", "integer")
	}
	p.next()
	n, err := strconv.ParseInt(t.Text, 10, 64)
	if err != nil {
		p.fail(t, "integer out of range")
	}
	if neg {
		n = -n
	}
	return n
}

func (p *parser) boolLit() bool {
	t := p.peek()
	if t.Is("true") || t.Is("t") {
		p.next()
		return true
	}
	if t.Is("false") || t.Is("f") {
		p.next()
		return false
	}
	p.fail(t, "expected true or false", "true", "false")
	return false
}

func (p *parser) stringLit() string {
	t := p.peek()
	if t.Kind != lexer.String {
		p.fail(t, "expected a quoted string", "string")
	}
	p.next()
	return t.Value
}

// argValue reads the value of a name=value command option: a literal, a
// span, a bare word (as a string), or a quoted identifier.
func (p *parser) argValue() ast.Expr {
	t := p.peek()
	switch t.Kind {
	case lexer.Ident:
		if t.Is("true") || t.Is("false") || t.Is("null") || p.peekAt(1).Kind == lexer.LParen {
			return p.primary()
		}
		f := p.field()
		return &ast.Literal{Base: ast.Base{Span: f.Span}, Kind: ast.LitString, Text: f.Name()}
	case lexer.QuotedIdent, lexer.NumericIdent:
		f := p.field()
		return &ast.Literal{Base: ast.Base{Span: f.Span}, Kind: ast.LitString, Text: f.Name()}
	case lexer.TimeSnap:
		p.next()
		return &ast.Literal{Base: ast.Base{Span: ast.Span{Start: t.Pos, End: t.End}}, Kind: ast.LitString, Text: t.Text}
	}
	return p.unary()
}

// args reads name=value options whose names are in names.
func (p *parser) args(names ...string) []ast.Arg {
	var out []ast.Arg
	for {
		name, ok := p.atArg(names...)
		if !ok {
			return out
		}
		start := p.next().Pos
		p.next()
		out = append(out, ast.Arg{Name: name, Value: p.argValue(), Span: p.span(start)})
	}
}

func (p *parser) fieldList() []*ast.Field {
	fs := []*ast.Field{p.field()}
	for {
		if p.accept(lexer.Comma) {
			fs = append(fs, p.field())
			continue
		}
		t := p.peek()
		if (t.Kind == lexer.Ident || t.Kind == lexer.QuotedIdent) && !p.isArgStart(1) && !isClauseKeyword(t) {
			fs = append(fs, p.field())
			continue
		}
		return fs
	}
}

var clauseKeywords = []string{"by", "as", "in", "with", "using", "from", "over", "keepempty", "consecutive", "replace", "append", "output", "on", "where", "and", "or"}

func isClauseKeyword(t lexer.Token) bool {
	for _, k := range clauseKeywords {
		if t.Is(k) {
			return true
		}
	}
	return false
}

// wcField reads a field name that may contain wildcards, or '*'.
func (p *parser) wcField() string {
	if p.at(lexer.Star) {
		p.next()
		return "*"
	}
	t := p.peek()
	if t.Kind == lexer.String {
		p.next()
		return t.Value
	}
	return p.fieldName()
}

func (p *parser) where() ast.Command {
	start := p.next().Pos
	return &ast.WhereCommand{Cond: p.expr(), Base: p.base(start)}
}

func (p *parser) fields(name string) ast.Command {
	start := p.next().Pos
	c := &ast.FieldsCommand{Command: name}
	if p.accept(lexer.Minus) {
		c.Exclude = true
	} else {
		p.accept(lexer.Plus)
	}
	c.Fields = append(c.Fields, p.wcField())
	for {
		if p.accept(lexer.Comma) {
			c.Fields = append(c.Fields, p.wcField())
			continue
		}
		t := p.peek()
		if t.Kind == lexer.Ident || t.Kind == lexer.QuotedIdent || t.Kind == lexer.Star {
			c.Fields = append(c.Fields, p.wcField())
			continue
		}
		break
	}
	c.Base = p.base(start)
	return c
}

func (p *parser) rename() ast.Command {
	start := p.next().Pos
	c := &ast.RenameCommand{}
	for {
		at := p.peek().Pos
		from := p.wcField()
		p.expectKw("as")
		to := p.wcField()
		c.Pairs = append(c.Pairs, ast.RenamePair{From: from, To: to, Span: p.span(at)})
		if !p.accept(lexer.Comma) && !(p.at(lexer.Ident) || p.at(lexer.QuotedIdent)) {
			break
		}
	}
	c.Base = p.base(start)
	return c
}

func (p *parser) replace() ast.Command {
	start := p.next().Pos
	c := &ast.ReplaceCommand{}
	for {
		c.Patterns = append(c.Patterns, p.stringLit())
		p.expectKw("with")
		c.Replacements = append(c.Replacements, p.stringLit())
		if !p.accept(lexer.Comma) {
			break
		}
	}
	p.expectKw("in")
	c.Fields = p.fieldList()
	c.Base = p.base(start)
	return c
}

func (p *parser) eval(name string) ast.Command {
	start := p.next().Pos
	c := &ast.EvalCommand{Command: name}
	for {
		at := p.peek().Pos
		f := p.field()
		p.expect(lexer.Eq)
		c.Assigns = append(c.Assigns, ast.EvalAssign{Field: f, Expr: p.expr(), Span: p.span(at)})
		if !p.accept(lexer.Comma) {
			break
		}
	}
	c.Base = p.base(start)
	return c
}

var statsArgNames = map[string][]string{
	"stats":       {"partitions", "allnum", "delim", "bucket_nullable"},
	"eventstats":  {"bucket_nullable"},
	"streamstats": {"current", "window", "global", "reset_before", "reset_after", "bucket_nullable", "time_window"},
}

func (p *parser) stats(name string) ast.Command {
	start := p.next().Pos
	c := &ast.StatsCommand{Command: name}
	c.Args = p.statsArgs(name)
	for {
		c.Aggs = append(c.Aggs, p.aggTerm())
		if !p.accept(lexer.Comma) {
			break
		}
	}
	if p.acceptKw("by") {
		c.By = p.byClause()
	}
	c.Args = append(c.Args, p.statsArgs(name)...)
	if name == "stats" {
		c.Args = append(c.Args, p.args("dedup_splitvalues")...)
	}
	c.Base = p.base(start)
	return c
}

func (p *parser) statsArgs(name string) []ast.Arg {
	var out []ast.Arg
	for {
		n, ok := p.atArg(statsArgNames[name]...)
		if !ok {
			return out
		}
		at := p.next().Pos
		p.next()
		var v ast.Expr
		if n == "reset_before" || n == "reset_after" {
			v = p.expr()
		} else {
			v = p.argValue()
		}
		out = append(out, ast.Arg{Name: n, Value: v, Span: p.span(at)})
	}
}

// aggTerm parses an aggregation call with an optional alias, normalizing
// the bare spellings count and c to count().
func (p *parser) aggTerm() ast.AggTerm {
	start := p.peek().Pos
	t := p.peek()
	if t.Kind != lexer.Ident {
		p.fail(t, "expected an aggregation function", "aggregation")
	}
	var call *ast.Call
	name := strings.ToLower(t.Text)
	if (name == "count" || name == "c") && p.peekAt(1).Kind != lexer.LParen {
		p.next()
		call = &ast.Call{Base: p.base(start), Name: "count"}
	} else {
		// A percentile shortcut such as p99.5 lexes as p99 . 5.
		if p.peekAt(1).Kind == lexer.Dot && p.peekAt(2).Kind == lexer.Integer && p.peekAt(3).Kind == lexer.LParen && isPercentileShortcut(name) {
			p.next()
			p.next()
			frac := p.next().Text
			p.toks[p.i-1] = lexer.Token{Kind: lexer.Ident, Text: t.Text + "." + frac, Pos: t.Pos, End: p.toks[p.i-1].End}
			p.i--
		}
		if p.peekAt(1).Kind != lexer.LParen {
			p.fail(p.peekAt(1), "expected '(' after aggregation "+t.Text, "'('")
		}
		e := p.call()
		c, ok := e.(*ast.Call)
		if !ok {
			p.failAt(start, "expected an aggregation function")
		}
		call = c
	}
	term := ast.AggTerm{Func: call, Text: strings.TrimSpace(p.src[start.Offset:p.prevEnd().Offset])}
	if p.acceptKw("as") {
		term.Alias = p.wcField()
	}
	term.Span = p.span(start)
	return term
}

func isPercentileShortcut(name string) bool {
	rest := ""
	switch {
	case strings.HasPrefix(name, "perc"):
		rest = name[4:]
	case strings.HasPrefix(name, "p"):
		rest = name[1:]
	default:
		return false
	}
	return rest != "" && isNumber(rest)
}

func (p *parser) spanExpr() *ast.SpanExpr {
	start := p.expectKw("span").Pos
	p.expect(lexer.LParen)
	s := &ast.SpanExpr{}
	if !(p.at(lexer.Span) || p.at(lexer.Integer) || p.at(lexer.Decimal) || p.at(lexer.Double)) || p.peekAt(1).Kind == lexer.Comma {
		s.Field = p.field()
		p.expect(lexer.Comma)
	}
	s.Value = p.spanLiteral()
	p.expect(lexer.RParen)
	if p.acceptKw("as") {
		s.Alias = p.fieldName()
	}
	s.Span = p.span(start)
	return s
}

func (p *parser) spanLiteral() string {
	t := p.peek()
	switch t.Kind {
	case lexer.Span, lexer.Integer, lexer.Decimal:
		p.next()
		return t.Value
	case lexer.Double:
		p.next()
		return t.Value + "d"
	case lexer.String:
		p.next()
		return t.Value
	}
	p.fail(t, "expected a span such as 5m or 10", "span")
	return ""
}

func (p *parser) byClause() *ast.ByClause {
	b := &ast.ByClause{}
	for {
		if p.atKw("span") && p.peekAt(1).Kind == lexer.LParen {
			if b.Span != nil {
				p.fail(p.peek(), "only one span expression is allowed in a by clause")
			}
			b.SpanFirst = len(b.Fields) == 0
			b.Span = p.spanExpr()
		} else {
			b.Fields = append(b.Fields, p.field())
		}
		if p.accept(lexer.Comma) {
			continue
		}
		t := p.peek()
		if (t.Kind == lexer.Ident || t.Kind == lexer.QuotedIdent) && !p.isArgStart(1) && !isClauseKeyword(t) {
			continue
		}
		return b
	}
}

func (p *parser) dedup() ast.Command {
	start := p.next().Pos
	c := &ast.DedupCommand{Count: 1}
	if p.at(lexer.Integer) {
		c.Count = p.intLit()
	}
	c.Fields = p.fieldList()
	for _, a := range p.args("keepempty", "consecutive") {
		b, ok := boolArg(a)
		if !ok {
			p.failAt(a.Span.Start, a.Name+" must be true or false")
		}
		if a.Name == "keepempty" {
			c.KeepEmpty = b
		} else {
			c.Consecutive = b
		}
	}
	c.Base = p.base(start)
	return c
}

func boolArg(a ast.Arg) (bool, bool) {
	lit, ok := a.Value.(*ast.Literal)
	if !ok {
		return false, false
	}
	switch strings.ToLower(lit.Text) {
	case "true", "t":
		return true, lit.Kind == ast.LitBool || lit.Kind == ast.LitString
	case "false", "f":
		return false, lit.Kind == ast.LitBool || lit.Kind == ast.LitString
	}
	return false, false
}

func (p *parser) sortField() ast.SortField {
	start := p.peek().Pos
	sf := ast.SortField{}
	prefixed := false
	if p.accept(lexer.Minus) {
		sf.Desc, prefixed = true, true
	} else if p.accept(lexer.Plus) {
		prefixed = true
	}
	t := p.peek()
	for _, cast := range []string{"auto", "str", "ip", "num"} {
		if t.Is(cast) && p.peekAt(1).Kind == lexer.LParen {
			p.next()
			p.next()
			sf.Cast = cast
			sf.Field = p.field()
			p.expect(lexer.RParen)
			break
		}
	}
	if sf.Field == nil {
		sf.Field = p.field()
	}
	for _, s := range []struct {
		kw   string
		desc bool
	}{{"asc", false}, {"a", false}, {"desc", true}, {"d", true}} {
		if p.atKw(s.kw) {
			if prefixed {
				p.fail(p.peek(), "a sort key takes either a +/- prefix or an asc/desc suffix, not both")
			}
			p.next()
			sf.Desc = s.desc
			break
		}
	}
	sf.Span = p.span(start)
	return sf
}

func (p *parser) sort() ast.Command {
	start := p.next().Pos
	c := &ast.SortCommand{}
	if p.at(lexer.Integer) {
		c.Count = p.intLit()
	}
	for {
		c.Keys = append(c.Keys, p.sortField())
		if !p.accept(lexer.Comma) {
			break
		}
	}
	c.Base = p.base(start)
	return c
}

func (p *parser) head() ast.Command {
	start := p.next().Pos
	c := &ast.HeadCommand{Count: 10}
	if _, ok := p.atArg("limit"); ok {
		p.next()
		p.next()
		c.Count = p.intLit()
	} else if p.at(lexer.Integer) {
		c.Count = p.intLit()
	}
	if p.acceptKw("from") {
		c.From = p.intLit()
	}
	c.Base = p.base(start)
	return c
}

func (p *parser) bin() ast.Command {
	start := p.next().Pos
	c := &ast.BinCommand{Field: p.field()}
	for {
		name, ok := p.atArg("span", "bins", "minspan", "aligntime", "start", "end")
		if !ok {
			break
		}
		at := p.next().Pos
		p.next()
		var v ast.Expr
		if name == "span" || name == "minspan" {
			t := p.peek()
			if (t.Kind == lexer.Ident || t.Kind == lexer.NumericIdent) && strings.Contains(strings.ToLower(t.Text), "log") {
				p.next()
				v = &ast.Literal{Base: ast.Base{Span: ast.Span{Start: t.Pos, End: t.End}}, Kind: ast.LitString, Text: strings.ToLower(t.Text)}
			} else {
				text := p.spanLiteral()
				v = &ast.Literal{Base: ast.Base{Span: p.span(t.Pos)}, Kind: ast.LitString, Text: text}
			}
		} else {
			v = p.argValue()
		}
		c.Args = append(c.Args, ast.Arg{Name: name, Value: v, Span: p.span(at)})
	}
	if p.acceptKw("as") {
		c.Alias = p.fieldName()
	}
	c.Base = p.base(start)
	return c
}

func (p *parser) top(name string) ast.Command {
	start := p.next().Pos
	c := &ast.TopCommand{Command: name, Count: 10}
	if _, ok := p.atArg("limit"); ok {
		p.next()
		p.next()
		c.Count = p.intLit()
	} else if p.at(lexer.Integer) {
		c.Count = p.intLit()
	}
	c.Args = p.args("countfield", "showcount", "percentfield", "showperc", "usenull")
	c.Fields = p.fieldList()
	if p.acceptKw("by") {
		c.By = p.fieldList()
	}
	c.Args = append(c.Args, p.args("countfield", "showcount", "percentfield", "showperc", "usenull")...)
	c.Base = p.base(start)
	return c
}

func (p *parser) parseCmd(name string) ast.Command {
	start := p.next().Pos
	src := p.additive()
	pattern := p.stringLit()
	return &ast.ParseCommand{Command: name, Source: src, Pattern: pattern, Base: p.base(start)}
}

func (p *parser) rex() ast.Command {
	start := p.next().Pos
	c := &ast.RexCommand{}
	opts := []string{"max_match", "mode", "offset_field"}
	if _, ok := p.atArg("field"); ok {
		p.next()
		p.next()
		c.Field = p.field()
	}
	c.Args = p.args(opts...)
	c.Pattern = p.stringLit()
	c.Args = append(c.Args, p.args(opts...)...)
	if c.Field == nil {
		if _, ok := p.atArg("field"); ok {
			p.next()
			p.next()
			c.Field = p.field()
		}
	}
	c.Base = p.base(start)
	return c
}

func (p *parser) regex() ast.Command {
	start := p.next().Pos
	c := &ast.RegexCommand{Field: p.field()}
	switch {
	case p.accept(lexer.Eq):
	case p.accept(lexer.NotEq):
		c.Negate = true
	default:
		p.fail(p.peek(), "", "'='", "'!='")
	}
	c.Pattern = p.stringLit()
	c.Base = p.base(start)
	return c
}

func (p *parser) spath() ast.Command {
	start := p.next().Pos
	c := &ast.SpathCommand{}
	for {
		switch name, ok := p.atArg("input", "output", "path"); {
		case ok && name == "input":
			p.next()
			p.next()
			c.Input = p.field()
		case ok && name == "output":
			p.next()
			p.next()
			c.Output = p.field()
		case ok && name == "path":
			p.next()
			p.next()
			c.Path = p.spathPath()
		case c.Path == "" && (p.at(lexer.Ident) || p.at(lexer.QuotedIdent) || p.at(lexer.String)):
			c.Path = p.spathPath()
		default:
			c.Base = p.base(start)
			return c
		}
	}
}

// spathPath reads a path such as a.b{0}.c or a quoted path.
func (p *parser) spathPath() string {
	if p.at(lexer.String) {
		return p.next().Value
	}
	var b strings.Builder
	for first := true; ; first = false {
		t := p.peek()
		if !first && t.SpaceBefore {
			break
		}
		switch t.Kind {
		case lexer.Ident, lexer.NumericIdent, lexer.Integer, lexer.Dot, lexer.LBrace, lexer.RBrace:
			b.WriteString(t.Text)
			p.next()
			continue
		case lexer.QuotedIdent:
			b.WriteString(t.Value)
			p.next()
			continue
		}
		break
	}
	return b.String()
}

func (p *parser) patterns() ast.Command {
	start := p.next().Pos
	c := &ast.PatternsCommand{}
	opts := []string{"method", "mode", "max_sample_count", "buffer_limit", "show_numbered_token", "pattern", "new_field", "variable_count_threshold", "frequency_threshold_percentage"}
	c.Args = p.args(opts...)
	c.Source = p.additive()
	if p.acceptKw("by") {
		c.By = p.fieldList()
	}
	c.Args = append(c.Args, p.args(opts...)...)
	c.Base = p.base(start)
	return c
}

func (p *parser) lookupPairs() []ast.LookupPair {
	var out []ast.LookupPair
	for {
		lp := ast.LookupPair{Field: p.fieldName()}
		if p.acceptKw("as") {
			lp.Alias = p.fieldName()
		}
		out = append(out, lp)
		if !p.accept(lexer.Comma) {
			return out
		}
	}
}

func (p *parser) lookup() ast.Command {
	start := p.next().Pos
	c := &ast.LookupCommand{Source: p.sourceRef()}
	c.Keys = p.lookupPairs()
	for _, m := range []string{"replace", "append", "output"} {
		if p.acceptKw(m) {
			c.Mode = m
			c.Outputs = p.lookupPairs()
			break
		}
	}
	c.Base = p.base(start)
	return c
}

func (p *parser) fillnull() ast.Command {
	start := p.next().Pos
	c := &ast.FillnullCommand{}
	switch {
	case p.acceptKw("with"):
		c.Value = p.additive()
		if p.acceptKw("in") {
			c.Fields = p.fieldList()
		}
	case p.acceptKw("using"):
		for {
			at := p.peek().Pos
			f := p.field()
			p.expect(lexer.Eq)
			c.Pairs = append(c.Pairs, ast.EvalAssign{Field: f, Expr: p.additive(), Span: p.span(at)})
			if !p.accept(lexer.Comma) {
				break
			}
		}
	default:
		if _, ok := p.atArg("value"); !ok {
			p.fail(p.peek(), "", "WITH", "USING", "value=")
		}
		p.next()
		p.next()
		c.Value = p.additive()
		if p.at(lexer.Ident) || p.at(lexer.QuotedIdent) {
			c.Fields = p.fieldList()
		}
	}
	c.Base = p.base(start)
	return c
}

func (p *parser) convert() ast.Command {
	start := p.next().Pos
	c := &ast.ConvertCommand{}
	if _, ok := p.atArg("timeformat"); ok {
		p.next()
		p.next()
		c.TimeFormat = p.stringLit()
	}
	for {
		fn := strings.ToLower(p.expect(lexer.Ident).Text)
		p.expect(lexer.LParen)
		cf := ast.ConvertFunc{Func: fn, Field: p.field()}
		p.expect(lexer.RParen)
		if p.acceptKw("as") {
			cf.Alias = p.fieldName()
		}
		c.Funcs = append(c.Funcs, cf)
		p.accept(lexer.Comma)
		if !(p.at(lexer.Ident) && p.peekAt(1).Kind == lexer.LParen) {
			break
		}
	}
	c.Base = p.base(start)
	return c
}

func (p *parser) trendline() ast.Command {
	start := p.next().Pos
	c := &ast.TrendlineCommand{}
	if p.acceptKw("sort") {
		sf := p.sortField()
		c.Sort = &sf
	}
	for p.atKw("sma") || p.atKw("wma") {
		kind := strings.ToLower(p.next().Text)
		p.expect(lexer.LParen)
		n := p.intLit()
		p.expect(lexer.Comma)
		term := ast.TrendlineTerm{Kind: kind, N: n, Field: p.field()}
		p.expect(lexer.RParen)
		if p.acceptKw("as") {
			term.Alias = p.fieldName()
		}
		c.Terms = append(c.Terms, term)
	}
	if len(c.Terms) == 0 {
		p.fail(p.peek(), "", "sma(n, field)", "wma(n, field)")
	}
	c.Base = p.base(start)
	return c
}

func (p *parser) fieldCommand(name string) ast.Command {
	start := p.next().Pos
	c := &ast.FieldCommand{Command: name, Field: p.field()}
	switch name {
	case "expand":
		if p.acceptKw("as") {
			c.Alias = p.fieldName()
		}
	case "flatten":
		if p.acceptKw("as") {
			paren := p.accept(lexer.LParen)
			for {
				c.Aliases = append(c.Aliases, p.fieldName())
				if !p.accept(lexer.Comma) {
					break
				}
			}
			if paren {
				p.expect(lexer.RParen)
			}
		}
	case "mvexpand":
		c.Args = p.args("limit")
	case "mvcombine":
		c.Args = p.args("delim")
	}
	c.Base = p.base(start)
	return c
}

func (p *parser) subPipelineCommand(name string) ast.Command {
	start := p.next().Pos
	c := &ast.SubPipelineCommand{Command: name}
	if name == "appendcol" {
		c.Args = p.args("override")
	}
	p.expect(lexer.LBracket)
	if name == "append" && ((p.atKw("source") || p.atKw("index")) && p.isArgStart(1) || p.atKw("search")) {
		c.Pipeline = p.pipeline(false)
	} else {
		c.Pipeline = p.subPipeline()
	}
	p.expect(lexer.RBracket)
	c.Base = p.base(start)
	return c
}

func (p *parser) join() ast.Command {
	start := p.peek().Pos
	c := &ast.JoinCommand{}
	sqlLike := false
	switch {
	case p.acceptKw("inner"):
		c.Kind, sqlLike = "inner", true
	case p.acceptKw("cross"):
		c.Kind, sqlLike = "cross", true
	case p.atKw("left") && !p.isArgStart(1):
		p.next()
		c.Kind, sqlLike = "left", true
		if p.acceptKw("semi") {
			c.Kind = "semi"
		} else if p.acceptKw("anti") {
			c.Kind = "anti"
		} else {
			p.acceptKw("outer")
		}
	case p.acceptKw("outer"):
		c.Kind, sqlLike = "left", true
	case p.atKw("right") && !p.isArgStart(1):
		p.next()
		p.acceptKw("outer")
		c.Kind, sqlLike = "right", true
	case p.acceptKw("full"):
		p.acceptKw("outer")
		c.Kind, sqlLike = "full", true
	case p.acceptKw("semi"):
		c.Kind, sqlLike = "semi", true
	case p.acceptKw("anti"):
		c.Kind, sqlLike = "anti", true
	}
	p.expectKw("join")
	c.Args = p.args("overwrite", "type", "max")
	if name, ok := p.atArg("left", "right"); ok {
		for ok {
			p.next()
			p.next()
			alias := p.fieldName()
			if name == "left" {
				c.LeftAlias = alias
			} else {
				c.RightAlias = alias
			}
			p.accept(lexer.Comma)
			name, ok = p.atArg("left", "right")
		}
		sqlLike = true
	}
	c.Args = append(c.Args, p.args("overwrite", "type", "max")...)
	if c.Kind == "" {
		c.Kind = "inner"
	}
	if p.atKw("on") || p.atKw("where") {
		p.next()
		c.Cond = p.expr()
	} else if !sqlLike {
		// The field-list form names the join fields and then the right
		// side; without a bracketed subsearch the last name is the index.
		var names []*ast.Field
		for (p.at(lexer.Ident) || p.at(lexer.QuotedIdent)) && !p.atKw("as") {
			names = append(names, p.field())
			p.accept(lexer.Comma)
		}
		if !p.at(lexer.LBracket) && len(names) > 0 {
			last := names[len(names)-1]
			c.Fields = names[:len(names)-1]
			src := ast.SourceRef{Base: ast.Base{Span: last.Span}, Name: last.Name()}
			c.Right = &ast.Pipeline{Span: last.Span, Commands: []ast.Command{&ast.SearchCommand{Base: ast.Base{Span: last.Span}, Sources: []ast.SourceRef{src}}}}
			if p.acceptKw("as") {
				c.RightAlias = p.fieldName()
			}
			c.Base = p.base(start)
			return c
		}
		c.Fields = names
	}
	c.Right = p.joinRight(c)
	c.Base = p.base(start)
	return c
}

// joinRight reads the right side: a bracketed subsearch or an index name,
// either optionally followed by "as alias".
func (p *parser) joinRight(c *ast.JoinCommand) *ast.Pipeline {
	start := p.peek().Pos
	var pl *ast.Pipeline
	if p.at(lexer.LBracket) {
		pl = p.subSearch()
	} else {
		src := p.sourceRef()
		pl = &ast.Pipeline{Commands: []ast.Command{&ast.SearchCommand{Base: ast.Base{Span: src.Span}, Sources: []ast.SourceRef{src}}}}
	}
	if p.acceptKw("as") {
		c.RightAlias = p.fieldName()
	}
	pl.Span = p.span(start)
	return pl
}

func (p *parser) totals(name string) ast.Command {
	start := p.next().Pos
	c := &ast.TotalsCommand{Command: name}
	opts := []string{"label", "labelfield", "fieldname", "row", "col"}
	if name == "addcoltotals" {
		opts = []string{"label", "labelfield"}
	}
	c.Args = p.args(opts...)
	if (p.at(lexer.Ident) || p.at(lexer.QuotedIdent)) && !p.isArgStart(1) {
		c.Fields = p.fieldList()
	}
	c.Args = append(c.Args, p.args(opts...)...)
	c.Base = p.base(start)
	return c
}

func (p *parser) chart(name string) ast.Command {
	start := p.next().Pos
	c := &ast.ChartCommand{Command: name}
	opts := []string{"limit", "useother", "otherstr", "usenull", "nullstr"}
	if name == "timechart" {
		opts = []string{"limit", "span", "useother", "usenull", "nullstr", "timefield", "bins", "minspan"}
	}
	c.Args = p.chartArgs(opts)
	c.Agg = p.aggTerm()
	binOpts := []string{"span", "bins", "minspan", "start", "end"}
	if name == "chart" && p.acceptKw("over") {
		c.Over = p.field()
		c.OverArgs = p.chartArgs(binOpts)
		if p.acceptKw("by") {
			c.By = p.field()
			c.ByArgs = p.chartArgs(binOpts)
		}
	} else if p.acceptKw("by") {
		c.By = p.field()
		if name == "chart" {
			c.ByArgs = p.chartArgs(binOpts)
			if p.accept(lexer.Comma) || (p.at(lexer.Ident) && !p.isArgStart(1)) {
				c.Over, c.OverArgs = c.By, c.ByArgs
				c.By = p.field()
				c.ByArgs = p.chartArgs(binOpts)
			}
		}
	}
	c.Args = append(c.Args, p.chartArgs(opts)...)
	c.Base = p.base(start)
	return c
}

func (p *parser) chartArgs(names []string) []ast.Arg {
	var out []ast.Arg
	for {
		name, ok := p.atArg(names...)
		if !ok {
			return out
		}
		at := p.next().Pos
		p.next()
		var v ast.Expr
		if name == "span" || name == "minspan" {
			t := p.peek()
			v = &ast.Literal{Base: ast.Base{Span: ast.Span{Start: t.Pos, End: t.End}}, Kind: ast.LitString, Text: p.spanLiteral()}
		} else {
			v = p.argValue()
		}
		out = append(out, ast.Arg{Name: name, Value: v, Span: p.span(at)})
	}
}

func (p *parser) transpose() ast.Command {
	start := p.next().Pos
	c := &ast.TransposeCommand{Rows: 5, ColumnName: "column"}
	for {
		if p.at(lexer.Integer) {
			c.Rows = p.intLit()
			continue
		}
		if _, ok := p.atArg("column_name"); ok {
			p.next()
			p.next()
			c.ColumnName = p.stringLit()
			continue
		}
		break
	}
	c.Base = p.base(start)
	return c
}

func (p *parser) xyseries() ast.Command {
	start := p.next().Pos
	c := &ast.XYSeriesCommand{}
	c.Args = p.args("sep", "format")
	c.X = p.field()
	c.Y = p.field()
	p.expectKw("in")
	p.expect(lexer.LParen)
	for {
		c.Values = append(c.Values, p.stringLit())
		if !p.accept(lexer.Comma) {
			break
		}
	}
	p.expect(lexer.RParen)
	c.Data = p.fieldList()
	c.Base = p.base(start)
	return c
}

func (p *parser) describe() ast.Command {
	start := p.next().Pos
	return &ast.DescribeCommand{Source: p.sourceRef(), Base: p.base(start)}
}

func (p *parser) makeResults() ast.Command {
	start := p.next().Pos
	c := &ast.MakeResultsCommand{Count: 1, HasArg: map[string]bool{}}
	for {
		name, ok := p.atArg("count", "format", "data")
		if !ok {
			break
		}
		p.next()
		p.next()
		c.HasArg[name] = true
		switch name {
		case "count":
			c.Count = p.intLit()
		case "format":
			c.Format = strings.ToLower(p.expect(lexer.Ident).Text)
		case "data":
			c.Data = p.stringLit()
		}
	}
	c.Base = p.base(start)
	return c
}

func (p *parser) multiSearch() ast.Command {
	start := p.next().Pos
	c := &ast.MultiSearchCommand{}
	for p.at(lexer.LBracket) {
		c.Searches = append(c.Searches, p.subSearch())
	}
	if len(c.Searches) < 2 {
		p.fail(p.peek(), "multisearch needs at least two subsearches", "'['")
	}
	c.Base = p.base(start)
	return c
}

func (p *parser) union() ast.Command {
	start := p.next().Pos
	c := &ast.UnionCommand{}
	if _, ok := p.atArg("maxout"); ok {
		p.next()
		p.next()
		c.MaxOut = p.intLit()
	}
	for {
		if p.at(lexer.LBracket) {
			c.Datasets = append(c.Datasets, p.subSearch())
		} else {
			src := p.sourceRef()
			c.Datasets = append(c.Datasets, &ast.Pipeline{Span: src.Span, Commands: []ast.Command{&ast.SearchCommand{Base: ast.Base{Span: src.Span}, Sources: []ast.SourceRef{src}}}})
		}
		p.accept(lexer.Comma)
		if !(p.at(lexer.LBracket) || p.at(lexer.Ident) || p.at(lexer.QuotedIdent)) {
			break
		}
	}
	c.Base = p.base(start)
	return c
}
