// Package parser builds PPL syntax trees from query text.
//
// The parser is a hand-written recursive-descent parser over the OpenSearch
// PPL grammar. Keywords are matched case-insensitively on identifier tokens,
// so keywords stay usable as field names wherever the grammar allows. The
// first syntax error stops parsing and is reported with its position and the
// tokens that would have been accepted.
package parser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/ppl/ast"
	"github.com/lynxbase/lynxdb/pkg/ppl/lexer"
)

// SyntaxError describes the first syntax error in a query.
type SyntaxError struct {
	Pos      lexer.Pos
	End      lexer.Pos
	Msg      string
	Expected []string
	Found    string
}

func (e *SyntaxError) Error() string {
	msg := fmt.Sprintf("syntax error at line %d, column %d: %s", e.Pos.Line, e.Pos.Col, e.Msg)
	if len(e.Expected) > 0 {
		msg += " (expected " + strings.Join(e.Expected, " or ") + ")"
	}
	return msg
}

type bailout struct{ err *SyntaxError }

type parser struct {
	src  string
	toks []lexer.Token
	i    int
}

// Parse parses one PPL statement.
func Parse(src string) (q *ast.Query, err error) {
	toks, lexErrs := lexer.Tokenize(src)
	if len(lexErrs) > 0 {
		e := lexErrs[0]
		return nil, &SyntaxError{Pos: e.Pos, End: e.Pos, Msg: e.Msg}
	}
	p := &parser{src: src, toks: toks}
	defer func() {
		if r := recover(); r != nil {
			b, ok := r.(bailout)
			if !ok {
				panic(r)
			}
			q, err = nil, b.err
		}
	}()
	q = p.query()
	return q, nil
}

// ParsePipeline parses a pipeline without the explain prefix; it is used for
// fragments such as saved searches.
func ParsePipeline(src string) (*ast.Pipeline, error) {
	q, err := Parse(src)
	if err != nil {
		return nil, err
	}
	if q.Explain {
		return nil, &SyntaxError{Pos: q.Span.Start, End: q.Span.End, Msg: "explain is not allowed here"}
	}
	return q.Pipeline, nil
}

func (p *parser) peek() lexer.Token { return p.toks[p.i] }

func (p *parser) peekAt(n int) lexer.Token {
	if p.i+n < len(p.toks) {
		return p.toks[p.i+n]
	}
	return p.toks[len(p.toks)-1]
}

func (p *parser) next() lexer.Token {
	t := p.toks[p.i]
	if t.Kind != lexer.EOF {
		p.i++
	}
	return t
}

func (p *parser) prevEnd() lexer.Pos {
	if p.i == 0 {
		return p.toks[0].Pos
	}
	return p.toks[p.i-1].End
}

func (p *parser) span(start lexer.Pos) ast.Span { return ast.Span{Start: start, End: p.prevEnd()} }

func (p *parser) fail(t lexer.Token, msg string, expected ...string) {
	found := describe(t)
	if msg == "" {
		msg = "unexpected " + found
	}
	panic(bailout{&SyntaxError{Pos: t.Pos, End: t.End, Msg: msg, Expected: expected, Found: found}})
}

func (p *parser) failAt(pos lexer.Pos, msg string) {
	panic(bailout{&SyntaxError{Pos: pos, End: pos, Msg: msg}})
}

func describe(t lexer.Token) string {
	switch t.Kind {
	case lexer.EOF:
		return "end of query"
	case lexer.String:
		return "string " + strconv.Quote(t.Value)
	default:
		return strconv.Quote(t.Text)
	}
}

func (p *parser) at(k lexer.Kind) bool { return p.peek().Kind == k }

func (p *parser) atKw(kw string) bool { return p.peek().Is(kw) }

func (p *parser) accept(k lexer.Kind) bool {
	if p.at(k) {
		p.next()
		return true
	}
	return false
}

func (p *parser) acceptKw(kw string) bool {
	if p.atKw(kw) {
		p.next()
		return true
	}
	return false
}

func (p *parser) expect(k lexer.Kind) lexer.Token {
	if !p.at(k) {
		p.fail(p.peek(), "", k.String())
	}
	return p.next()
}

func (p *parser) expectKw(kw string) lexer.Token {
	if !p.atKw(kw) {
		p.fail(p.peek(), "", strings.ToUpper(kw))
	}
	return p.next()
}

// atArg reports whether the next tokens are "<name> =" with name matched
// case-insensitively against names.
func (p *parser) atArg(names ...string) (string, bool) {
	t := p.peek()
	if t.Kind != lexer.Ident || p.peekAt(1).Kind != lexer.Eq {
		return "", false
	}
	for _, n := range names {
		if strings.EqualFold(t.Text, n) {
			return n, true
		}
	}
	return "", false
}

func (p *parser) query() *ast.Query {
	start := p.peek().Pos
	q := &ast.Query{}
	if p.atKw("explain") && p.peekAt(1).Kind != lexer.Eq {
		p.next()
		q.Explain = true
		for _, m := range []string{"simple", "standard", "cost", "extended"} {
			if p.acceptKw(m) {
				q.ExplainMode = m
				break
			}
		}
	}
	q.Pipeline = p.pipeline(true)
	p.accept(lexer.Semicolon)
	if !p.at(lexer.EOF) {
		p.fail(p.peek(), "", "'|'", "end of query")
	}
	q.Span = p.span(start)
	return q
}

// pipeline parses "[|] source-command (| command)*". When top is false the
// pipeline ends at a closing bracket.
func (p *parser) pipeline(top bool) *ast.Pipeline {
	start := p.peek().Pos
	pl := &ast.Pipeline{}
	p.accept(lexer.Pipe)
	pl.Commands = append(pl.Commands, p.sourceCommand())
	for p.accept(lexer.Pipe) {
		if p.at(lexer.EOF) || p.at(lexer.RBracket) || p.at(lexer.Pipe) || p.at(lexer.Semicolon) {
			continue
		}
		pl.Commands = append(pl.Commands, p.command())
	}
	if !top && !p.at(lexer.RBracket) {
		p.fail(p.peek(), "", "'|'", "']'")
	}
	pl.Span = p.span(start)
	return pl
}

// subPipeline parses the body of appendpipe and appendcol: commands with no
// source command.
func (p *parser) subPipeline() *ast.Pipeline {
	start := p.peek().Pos
	pl := &ast.Pipeline{}
	p.accept(lexer.Pipe)
	pl.Commands = append(pl.Commands, p.command())
	for p.accept(lexer.Pipe) {
		if p.at(lexer.RBracket) {
			break
		}
		pl.Commands = append(pl.Commands, p.command())
	}
	if !p.at(lexer.RBracket) {
		p.fail(p.peek(), "", "'|'", "']'")
	}
	pl.Span = p.span(start)
	return pl
}

func (p *parser) bracketed(f func() *ast.Pipeline) *ast.Pipeline {
	p.expect(lexer.LBracket)
	pl := f()
	p.expect(lexer.RBracket)
	return pl
}

func (p *parser) subSearch() *ast.Pipeline {
	return p.bracketed(func() *ast.Pipeline { return p.pipeline(false) })
}

func (p *parser) sourceCommand() ast.Command {
	t := p.peek()
	switch {
	case t.Is("describe"):
		return p.describe()
	case t.Is("makeresults"):
		return p.makeResults()
	case t.Is("multisearch"):
		return p.multiSearch()
	case t.Is("union") && !p.isArgStart(1):
		return p.union()
	case t.Is("show") && p.peekAt(1).Is("datasources"):
		start := p.next().Pos
		p.next()
		return &ast.UnsupportedCommand{Base: ast.Base{Span: p.span(start)}, Command: "show datasources"}
	case t.Is("rest") && p.peekAt(1).Kind == lexer.String:
		start := p.next().Pos
		p.skipToPipe()
		return &ast.UnsupportedCommand{Base: ast.Base{Span: p.span(start)}, Command: "rest"}
	case t.Is("graphlookup"):
		start := p.next().Pos
		p.skipToPipe()
		return &ast.UnsupportedCommand{Base: ast.Base{Span: p.span(start)}, Command: "graphlookup"}
	}
	return p.search()
}

func (p *parser) isArgStart(n int) bool { return p.peekAt(n).Kind == lexer.Eq }

// skipToPipe consumes tokens up to the next top-level pipe, honoring
// brackets; it is used for commands whose arguments are never compiled.
func (p *parser) skipToPipe() {
	depth := 0
	for {
		t := p.peek()
		switch t.Kind {
		case lexer.EOF:
			return
		case lexer.LParen, lexer.LBracket:
			depth++
		case lexer.RParen:
			depth--
		case lexer.RBracket:
			if depth == 0 {
				return
			}
			depth--
		case lexer.Pipe:
			if depth == 0 {
				return
			}
		}
		p.next()
	}
}

var unsupportedCommands = map[string]bool{
	"kmeans": true, "ad": true, "ml": true, "graphlookup": true, "foreach": true, "timewrap": true,
}

func (p *parser) command() ast.Command {
	t := p.peek()
	if t.Kind != lexer.Ident {
		p.fail(t, "expected a command name", "command")
	}
	name := strings.ToLower(t.Text)
	if unsupportedCommands[name] {
		start := p.next().Pos
		p.skipToPipe()
		return &ast.UnsupportedCommand{Base: ast.Base{Span: p.span(start)}, Command: name}
	}
	switch name {
	case "where":
		return p.where()
	case "fields", "table":
		return p.fields(name)
	case "rename":
		return p.rename()
	case "replace":
		return p.replace()
	case "eval", "fieldformat":
		return p.eval(name)
	case "stats", "eventstats", "streamstats":
		return p.stats(name)
	case "dedup":
		return p.dedup()
	case "sort":
		return p.sort()
	case "reverse":
		start := p.next().Pos
		return &ast.ReverseCommand{Base: ast.Base{Span: p.span(start)}}
	case "head":
		return p.head()
	case "bin":
		return p.bin()
	case "top", "rare":
		return p.top(name)
	case "parse", "grok":
		return p.parseCmd(name)
	case "rex":
		return p.rex()
	case "regex":
		return p.regex()
	case "spath":
		return p.spath()
	case "patterns":
		return p.patterns()
	case "lookup":
		return p.lookup()
	case "fillnull":
		return p.fillnull()
	case "convert":
		return p.convert()
	case "trendline":
		return p.trendline()
	case "expand", "mvexpand", "flatten", "nomv", "mvcombine":
		return p.fieldCommand(name)
	case "append", "appendcol", "appendpipe":
		return p.subPipelineCommand(name)
	case "join", "inner", "left", "right", "full", "cross", "outer", "semi", "anti":
		return p.join()
	case "addtotals", "addcoltotals":
		return p.totals(name)
	case "chart", "timechart":
		return p.chart(name)
	case "transpose":
		return p.transpose()
	case "xyseries":
		return p.xyseries()
	case "union":
		return p.union()
	case "search":
		p.fail(t, "search can only be the first command of a pipeline; use where to filter")
	case "describe", "makeresults", "multisearch":
		p.fail(t, name+" can only be the first command of a pipeline")
	}
	p.fail(t, "unknown command "+strconv.Quote(t.Text))
	return nil
}
