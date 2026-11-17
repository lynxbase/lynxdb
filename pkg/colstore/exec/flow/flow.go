// Package flow translates LynxFlow queries into plans of the columnar
// executor. It covers a source with time ranges and search terms, where,
// extend, keep, drop, rename, dedup, stats, sort and head, with the scalar and
// aggregate functions the executor has; everything else fails with
// ErrUnsupported and the name of what is missing.
//
// A field is read with every kind it holds. A condition that is not known to
// be true rejects the row, also under not: a negated comparison becomes the
// opposite comparison, and a negated test of a field holds only where the
// field has a value to test.
package flow

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/lynxflow/ast"
	"github.com/lynxbase/lynxdb/pkg/lynxflow/desugar"
	"github.com/lynxbase/lynxdb/pkg/lynxflow/format"
	"github.com/lynxbase/lynxdb/pkg/lynxflow/parser"
)

// ErrUnsupported reports a query that parses but that the executor cannot
// run.
var ErrUnsupported = errors.New("flow: not supported")

// rawField names the whole event in search functions.
const rawField = "_raw"

// Options sets how a query is translated.
type Options struct {
	// Now is the time that relative time ranges count from; the zero time
	// means the time of the call.
	Now time.Time
}

func unsupported(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrUnsupported, fmt.Sprintf(format, args...))
}

// Compile parses a query and returns the plan that computes it.
func Compile(text string, opts Options) (exec.Node, error) {
	q, diags := parser.Parse(text)
	for _, d := range diags {
		if d.Severity == parser.SeverityError {
			return nil, fmt.Errorf("flow: %s at offset %d", d.Message, d.Span.Start)
		}
	}
	if q.Pipeline.Source != nil {
		quoteTerms(q.Pipeline.Source.SugarTerms)
	}
	q, _ = desugar.Desugar(q, desugar.Options{})
	if len(q.Lets) != 0 {
		return nil, unsupported("let")
	}
	t := &translator{now: opts.Now, env: map[string]*exec.Expr{}}
	if t.now.IsZero() {
		t.now = time.Now()
	}
	if src := q.Pipeline.Source; src != nil {
		if err := t.source(src); err != nil {
			return nil, err
		}
	}
	for i := range q.Pipeline.Stages {
		if err := t.stage(&q.Pipeline.Stages[i]); err != nil {
			return nil, err
		}
	}

	return t.plan()
}

// quoteTerms makes the bare word that a search term compares a field with a
// string, which desugaring would leave looking like a field.
func quoteTerms(e ast.SearchExpr) {
	word := func(v ast.Expr) ast.Expr {
		if id, ok := v.(*ast.Ident); ok && !id.Quoted {
			return &ast.Literal{Kind: ast.LitString, Raw: fmt.Sprintf("%q", id.Name), Value: id.Name, Pos: id.Pos}
		}

		return v
	}
	switch e := e.(type) {
	case *ast.SearchBinary:
		quoteTerms(e.Left)
		quoteTerms(e.Right)
	case *ast.SearchNot:
		quoteTerms(e.Operand)
	case *ast.SearchParen:
		quoteTerms(e.Inner)
	case *ast.SearchKeyValue:
		e.Value = word(e.Value)
	case *ast.SearchIn:
		for i := range e.Values {
			e.Values[i] = word(e.Values[i])
		}
	}
}

// translator builds a plan from the stages of a pipeline in order.
type translator struct {
	now    time.Time
	filter []*exec.Expr
	// env maps the name of a computed or renamed field to its expression:
	// over the table's paths before an aggregate, over the aggregate's
	// columns after it.
	env map[string]*exec.Expr
	// cols lists the output columns once a stage has named them.
	cols   []exec.Named
	named  bool
	agg    *exec.Aggregate
	having []*exec.Expr
	sort   *exec.Sort
	// dedup holds the keys of a dedup stage, which keeps the first row of
	// each of their combinations.
	dedup []exec.Named
	// totals is set once a field stands for a total over the groups, which
	// a later filter of the groups would change.
	totals bool
}

func (t *translator) source(src *ast.FromStage) error {
	for _, s := range src.Sources {
		if s.Kind == ast.SourceInline || s.Kind == ast.SourceCTE {
			return unsupported("inline and bound sources")
		}
	}
	for _, r := range src.TimeRanges {
		if r.Snap != "" {
			return unsupported("time range snap %q", r.Snap)
		}
		for i, bound := range []ast.Expr{r.Start, r.End} {
			if bound == nil {
				continue
			}
			at, err := t.instant(bound)
			if err != nil {
				return err
			}
			if i == 0 {
				t.filter = append(t.filter, exec.Ge(exec.Time(), exec.Int(at)))
			} else {
				t.filter = append(t.filter, exec.Lt(exec.Time(), exec.Int(at)))
			}
		}
	}

	return nil
}

// instant returns the nanoseconds of a constant time: a timestamp, or a
// duration counted from now.
func (t *translator) instant(e ast.Expr) (int64, error) {
	switch e := e.(type) {
	case *ast.Paren:
		return t.instant(e.Inner)
	case *ast.Unary:
		if lit, ok := e.Operand.(*ast.Literal); ok && e.Op == ast.OpNeg && lit.Kind == ast.LitDuration {
			return t.now.Add(-lit.Value.(time.Duration)).UnixNano(), nil
		}
	case *ast.Literal:
		switch e.Kind {
		case ast.LitDuration:
			return t.now.Add(e.Value.(time.Duration)).UnixNano(), nil
		case ast.LitInt:
			return e.Value.(int64), nil
		case ast.LitString, ast.LitRawString:
			at, err := time.Parse(time.RFC3339Nano, e.Value.(string))
			if err != nil {
				return 0, fmt.Errorf("flow: %q is not a timestamp", e.Value)
			}

			return at.UnixNano(), nil
		}
	case *ast.Call:
		switch {
		case e.Callee == "now" && len(e.Args) == 0:
			return t.now.UnixNano(), nil
		case e.Callee == "timestamp" && len(e.Args) == 1:
			return t.instant(e.Args[0])
		}
	}

	return 0, unsupported("time %s", format.Expr(e))
}

func (t *translator) stage(s *ast.Stage) error {
	if s.HasError {
		return fmt.Errorf("flow: stage %s does not parse", s.Name)
	}
	if t.sort != nil && s.Head == nil {
		return unsupported("%s after sort or head", s.Name)
	}
	switch {
	case s.Dedup != nil:
		if s.Dedup.N > 1 || t.agg != nil || t.dedup != nil {
			return unsupported("dedup of more than one row per key, after stats or after dedup")
		}
		for _, k := range s.Dedup.Fields {
			e, err := t.expr(k)
			if err != nil {
				return err
			}
			t.dedup = append(t.dedup, exec.Named{Name: fmt.Sprintf("\x00key%d", len(t.dedup)), Expr: e})
		}
	case s.Eventstats != nil:
		if t.agg == nil || len(s.Eventstats.By) != 0 {
			return unsupported("eventstats before stats or by a field")
		}
		for _, a := range s.Eventstats.Aggs {
			call, ok := a.Func.(*ast.Call)
			if !ok || call.Callee != "sum" || len(call.Args) != 1 || a.WhereCond != nil || a.Alias == "" {
				return unsupported("eventstats %s; it gives the sum of a column of stats under a name", format.Expr(a.Func))
			}
			e, err := t.expr(call.Args[0])
			if err != nil {
				return err
			}
			if e.Op != exec.OpColumn {
				return unsupported("eventstats %s; it gives the sum of a column of stats under a name", format.Expr(a.Func))
			}
			t.set(a.Alias, exec.Total(e))
		}
		t.totals = true
	case s.Where != nil:
		if t.dedup != nil || t.totals {
			return unsupported("where after dedup or eventstats")
		}
		p, err := t.pred(s.Where.Expr, false)
		if err != nil {
			return err
		}
		if t.agg != nil {
			t.having = append(t.having, p)
		} else {
			t.filter = append(t.filter, p)
		}
	case s.Extend != nil:
		for _, a := range s.Extend.Assignments {
			e, err := t.expr(a.Value)
			if err != nil {
				return err
			}
			t.set(a.Name, e)
		}
	case s.Rename != nil:
		for _, r := range s.Rename.Renames {
			e, err := t.field(r.Old)
			if err != nil {
				return err
			}
			renamed := false
			for i := range t.cols {
				if t.cols[i].Name == r.Old {
					t.cols[i].Name, renamed = r.New, true
				}
			}
			if !renamed && t.named {
				return unsupported("rename of %s, which is not kept", r.Old)
			}
			delete(t.env, r.Old)
			t.env[r.New] = e
		}
	case s.Keep != nil:
		if s.Keep.StarExcept {
			return unsupported("keep * except")
		}
		var cols []exec.Named
		for _, p := range s.Keep.Patterns {
			if p.Glob {
				return unsupported("field pattern %s", p.Name)
			}
			e, err := t.field(p.Name)
			if err != nil {
				return err
			}
			cols = append(cols, exec.Named{Name: p.Name, Expr: e})
		}
		t.cols, t.named = cols, true
	case s.Drop != nil:
		if !t.named {
			return unsupported("drop before keep or stats")
		}
		for _, p := range s.Drop.Patterns {
			if p.Glob || s.Drop.StarExcept {
				return unsupported("field pattern %s", p.Name)
			}
			kept := t.cols[:0]
			for _, c := range t.cols {
				if c.Name != p.Name {
					kept = append(kept, c)
				}
			}
			t.cols = kept
		}
	case s.Stats != nil:
		if t.dedup != nil {
			return unsupported("stats after dedup")
		}

		return t.stats(s.Stats)
	case s.Sort != nil:
		t.sort = &exec.Sort{}
		for _, k := range s.Sort.Keys {
			name, ok := path(k.Field)
			if !ok {
				return unsupported("sort by %s; name it with extend first", format.Expr(k.Field))
			}
			t.sort.By = append(t.sort.By, exec.Order{Column: name, Desc: k.Desc})
		}
	case s.Head != nil:
		if s.Head.N <= 0 {
			return unsupported("head %d", s.Head.N)
		}
		if t.sort == nil {
			t.sort = &exec.Sort{}
		}
		if n := int(s.Head.N); t.sort.Limit == 0 || n < t.sort.Limit {
			t.sort.Limit = n
		}
	default:
		return unsupported("stage %s", s.Name)
	}

	return nil
}

// set defines the field name as e and adds it to the named output columns.
func (t *translator) set(name string, e *exec.Expr) {
	t.env[name] = e
	if !t.named {
		return
	}
	for i := range t.cols {
		if t.cols[i].Name == name {
			t.cols[i].Expr = e

			return
		}
	}
	t.cols = append(t.cols, exec.Named{Name: name, Expr: e})
}

var aggFuncs = map[string]exec.AggFunc{
	"count": exec.Count, "dc": exec.CountDistinct, "estdc": exec.ApproxDistinct, "sum": exec.Sum, "avg": exec.Avg,
	"min": exec.Min, "max": exec.Max, "stdev": exec.Stdev, "var": exec.Var,
	"first": exec.First, "last": exec.Last, "earliest": exec.Earliest, "latest": exec.Latest,
	"any_value": exec.AnyValue, "arg_min": exec.ArgMin, "arg_max": exec.ArgMax,
}

// percentiles maps the aggregates that are a percentile to its percentage.
var percentiles = map[string]float64{"p50": 50, "p75": 75, "p90": 90, "p95": 95, "p99": 99}

// aggregate translates one aggregate of a stats stage. A condition on it,
// f(x, where p), makes the argument absent in the rows where p is not true.
func (t *translator) aggregate(a *ast.AggExpr) (exec.Agg, error) {
	refused := unsupported("aggregate %s", format.Expr(a.Func))
	call, ok := a.Func.(*ast.Call)
	if !ok || call.Receiver != nil {
		return exec.Agg{}, refused
	}
	out := exec.Agg{Name: a.Alias}
	if out.Name == "" {
		out.Name = format.Expr(a.Func)
	}
	args, want := call.Args, 1
	fn, ok := aggFuncs[call.Callee]
	p, aliased := percentiles[call.Callee]
	switch {
	case aliased:
		fn, ok, out.P = exec.Percentile, true, p
	case call.Callee == "perc" && len(args) == 2:
		lit, isLit := args[1].(*ast.Literal)
		if !isLit || (lit.Kind != ast.LitInt && lit.Kind != ast.LitFloat) {
			return exec.Agg{}, refused
		}
		if out.P, ok = lit.Value.(float64); !ok {
			out.P = float64(lit.Value.(int64))
		}
		fn, ok, args = exec.Percentile, true, args[:1]
	case fn == exec.Count:
		want = min(len(args), 1)
	case fn == exec.ArgMin || fn == exec.ArgMax:
		want = 2
	}
	if !ok || len(args) != want {
		return exec.Agg{}, refused
	}
	out.Func = fn
	for i, arg := range args {
		e, err := t.expr(arg)
		if err != nil {
			return exec.Agg{}, err
		}
		if i == 0 {
			out.Arg = e
		} else {
			out.By = e
		}
	}
	if a.WhereCond != nil {
		cond, err := t.expr(a.WhereCond)
		if err != nil {
			return exec.Agg{}, err
		}
		// Rows are counted where a constant is present.
		if out.Arg == nil {
			out.Arg = exec.Bool(true)
		}
		out.Arg = exec.If(cond, out.Arg, &exec.Expr{Op: exec.OpLiteral})
	}

	return out, nil
}

func (t *translator) stats(s *ast.StatsPayload) error {
	if t.agg != nil {
		return unsupported("stats after stats")
	}
	agg := &exec.Aggregate{}
	env := map[string]*exec.Expr{}
	var cols []exec.Named
	add := func(name string) {
		env[name] = exec.Col(name, exec.KindAny)
		cols = append(cols, exec.Named{Name: name, Expr: env[name]})
	}
	for _, k := range s.By {
		e, err := t.expr(k)
		if err != nil {
			return err
		}
		name, ok := path(k)
		if !ok {
			name = format.Expr(k)
		}
		agg.Keys = append(agg.Keys, exec.Named{Name: name, Expr: e})
		add(name)
	}
	for i := range s.Aggs {
		out, err := t.aggregate(&s.Aggs[i])
		if err != nil {
			return err
		}
		agg.Aggs = append(agg.Aggs, out)
		add(out.Name)
	}
	t.agg, t.env, t.cols, t.named = agg, env, cols, true

	return nil
}

// plan assembles the nodes the stages asked for.
func (t *translator) plan() (exec.Node, error) {
	scan := &exec.Scan{}
	switch len(t.filter) {
	case 0:
	case 1:
		scan.Filter = t.filter[0]
	default:
		scan.Filter = exec.And(t.filter...)
	}
	var node exec.Node
	switch {
	case t.agg != nil:
		t.agg.Input = scan
		node = t.agg
		if len(t.having) != 0 {
			node = &exec.Filter{Input: node, Cond: exec.And(t.having...)}
		}
		// The aggregate's own columns in their order need no projection.
		same := len(t.cols) == len(t.agg.Keys)+len(t.agg.Aggs)
		for i := 0; same && i < len(t.cols); i++ {
			name := ""
			if i < len(t.agg.Keys) {
				name = t.agg.Keys[i].Name
			} else {
				name = t.agg.Aggs[i-len(t.agg.Keys)].Name
			}
			c := t.cols[i]
			same = c.Name == name && c.Expr.Op == exec.OpColumn && c.Expr.Path == name
		}
		if !same {
			node = &exec.Project{Input: node, Exprs: t.cols}
		}
	case !t.named || len(t.cols) == 0:
		return nil, unsupported("events without keep: name the fields to return")
	case t.dedup != nil:
		node = t.firstRows(scan)
	default:
		node = &exec.Project{Input: scan, Exprs: t.cols}
	}
	if t.sort != nil {
		t.sort.Input = node
		node = t.sort
	}

	return node, nil
}

// firstRows returns the output columns computed over the first row of each
// combination of the dedup keys: an aggregate that reads, from that row,
// every field the columns use.
func (t *translator) firstRows(scan *exec.Scan) exec.Node {
	agg := &exec.Aggregate{Input: scan, Keys: t.dedup}
	var over func(e *exec.Expr) *exec.Expr
	over = func(e *exec.Expr) *exec.Expr {
		if e.Op == exec.OpColumn {
			for i, a := range agg.Aggs {
				if a.Arg.Path == e.Path && a.Arg.Kind == e.Kind {
					return exec.Col(agg.Aggs[i].Name, exec.KindAny)
				}
			}
			name := fmt.Sprintf("\x00field%d", len(agg.Aggs))
			agg.Aggs = append(agg.Aggs, exec.Agg{Name: name, Func: exec.FirstRow, Arg: e})

			return exec.Col(name, exec.KindAny)
		}
		out := *e
		out.Args = make([]*exec.Expr, len(e.Args))
		for i, a := range e.Args {
			out.Args[i] = over(a)
		}

		return &out
	}
	cols := make([]exec.Named, len(t.cols))
	for i, c := range t.cols {
		cols[i] = exec.Named{Name: c.Name, Expr: over(c.Expr)}
	}

	return &exec.Project{Input: agg, Exprs: cols}
}

// path returns the dotted path of a field reference.
func path(e ast.Expr) (string, bool) {
	switch e := e.(type) {
	case *ast.Paren:
		return path(e.Inner)
	case *ast.Ident:
		return e.Name, true
	case *ast.Member:
		if obj, ok := path(e.Object); ok {
			return obj + "." + e.Field, true
		}
	}

	return "", false
}

// field returns what a field name stands for.
func (t *translator) field(name string) (*exec.Expr, error) {
	if e, ok := t.env[name]; ok {
		return e, nil
	}
	switch {
	case t.agg != nil:
		return nil, unsupported("field %s is not a column of the aggregate", name)
	case name == rawField:
		return nil, unsupported("%s outside search functions", rawField)
	case name == part.TimePath:
		return exec.Time(), nil
	}

	return exec.Col(name, exec.KindAny), nil
}

func literal(l *ast.Literal) (*exec.Expr, error) {
	switch l.Kind {
	case ast.LitString, ast.LitRawString:
		return exec.Str(l.Value.(string)), nil
	case ast.LitInt:
		return exec.Int(l.Value.(int64)), nil
	case ast.LitFloat:
		return exec.Float(l.Value.(float64)), nil
	case ast.LitBool:
		return exec.Bool(l.Value.(bool)), nil
	case ast.LitDuration:
		return exec.Int(int64(l.Value.(time.Duration))), nil
	case ast.LitNull:
		return &exec.Expr{Op: exec.OpLiteral}, nil
	}

	return nil, unsupported("literal %s", l.Raw)
}

var binaryOps = map[ast.BinaryOp]exec.Op{
	ast.OpEq: exec.OpEq, ast.OpNotEq: exec.OpNe, ast.OpLt: exec.OpLt, ast.OpLtEq: exec.OpLe, ast.OpGt: exec.OpGt, ast.OpGtEq: exec.OpGe,
	ast.OpAdd: exec.OpAdd, ast.OpSub: exec.OpSub, ast.OpMul: exec.OpMul, ast.OpDiv: exec.OpDiv, ast.OpMod: exec.OpMod,
	ast.OpAnd: exec.OpAnd, ast.OpOr: exec.OpOr, ast.OpCoalesce: exec.OpCoalesce,
}

// opposite maps a comparison to the one that holds where it does not, for
// values that compare at all.
var opposite = map[ast.BinaryOp]ast.BinaryOp{
	ast.OpEq: ast.OpNotEq, ast.OpNotEq: ast.OpEq, ast.OpLt: ast.OpGtEq, ast.OpGtEq: ast.OpLt, ast.OpGt: ast.OpLtEq, ast.OpLtEq: ast.OpGt,
}

// scalarCalls maps functions that take their arguments as they are.
var scalarCalls = map[string]exec.Op{
	"if": exec.OpIf, "case": exec.OpCase, "coalesce": exec.OpCoalesce, "nullif": exec.OpNullIf,
	"exists": exec.OpExists, "is_null": exec.OpIsNull, "is_missing": exec.OpIsMissing, "typeof": exec.OpTypeOf,
	"len": exec.OpLength, "lower": exec.OpLower, "upper": exec.OpUpper, "substr": exec.OpSubstr,
	"starts_with": exec.OpStartsWith, "ends_with": exec.OpEndsWith,
	"int": exec.OpToInt, "float": exec.OpToFloat, "string": exec.OpToString, "bool": exec.OpToBool,
	"abs": exec.OpAbs, "round": exec.OpRound, "floor": exec.OpFloor, "ceil": exec.OpCeil, "sqrt": exec.OpSqrt,
	"ln": exec.OpLn, "log": exec.OpLog, "exp": exec.OpExp, "pow": exec.OpPow,
	"trim": exec.OpTrim, "ltrim": exec.OpTrimLeft, "rtrim": exec.OpTrimRight, "index_of": exec.OpIndexOf,
}

// expr translates a value expression.
func (t *translator) expr(e ast.Expr) (*exec.Expr, error) {
	switch e := e.(type) {
	case *ast.Paren:
		return t.expr(e.Inner)
	case *ast.Literal:
		return literal(e)
	case *ast.Ident, *ast.Member:
		if name, ok := path(e); ok {
			return t.field(name)
		}
	case *ast.Unary:
		a, err := t.expr(e.Operand)
		if err != nil {
			return nil, err
		}
		if e.Op == ast.OpNot {
			return exec.Not(a), nil
		}

		return exec.Sub(exec.Int(0), a), nil
	case *ast.Binary:
		a, err := t.expr(e.Left)
		if err != nil {
			return nil, err
		}
		b, err := t.expr(e.Right)
		if err != nil {
			return nil, err
		}

		return &exec.Expr{Op: binaryOps[e.Op], Args: []*exec.Expr{a, b}}, nil
	case *ast.In, *ast.Between:
		return t.pred(e, false)
	case *ast.Call:
		return t.call(e)
	}

	return nil, unsupported("expression %s", format.Expr(e))
}

func (t *translator) args(c *ast.Call) ([]*exec.Expr, error) {
	out := make([]*exec.Expr, len(c.Args))
	for i, a := range c.Args {
		e, err := t.expr(a)
		if err != nil {
			return nil, err
		}
		out[i] = e
	}

	return out, nil
}

func text(e ast.Expr) (string, bool) {
	l, ok := e.(*ast.Literal)
	if !ok || (l.Kind != ast.LitString && l.Kind != ast.LitRawString) {
		return "", false
	}

	return l.Value.(string), true
}

// pattern returns what a search function looks for in its field; ok is
// false for a call that is not a search function with a constant term.
func pattern(c *ast.Call) (p tokenfilter.Pattern, ok bool, err error) {
	if len(c.Args) != 2 {
		return p, false, nil
	}
	term, ok := text(c.Args[1])
	if !ok || term == "" {
		return p, false, nil
	}
	switch c.Callee {
	case "has":
		return tokenfilter.Word(term).IgnoreCase(), true, nil
	case "contains", "contains_phrase":
		return tokenfilter.Substring(term).IgnoreCase(), true, nil
	case "contains_cs":
		return tokenfilter.Substring(term), true, nil
	case "has_glob":
		// A word, or the start of one followed by a star.
		prefix, star := strings.CutSuffix(term, "*")
		if strings.ContainsAny(prefix, `*?\`) || prefix == "" {
			return p, false, nil
		}
		if star {
			return tokenfilter.Prefix(prefix).IgnoreCase(), true, nil
		}

		return tokenfilter.Word(term).IgnoreCase(), true, nil
	case "matches":
		p, err := tokenfilter.Regexp(term)
		if err != nil {
			return p, false, fmt.Errorf("flow: %s: %w", format.Expr(c), err)
		}

		return p, true, nil
	}

	return p, false, nil
}

func (t *translator) call(c *ast.Call) (*exec.Expr, error) {
	if c.Receiver != nil || c.Bang {
		return nil, unsupported("call %s", format.Expr(c))
	}
	p, ok, err := pattern(c)
	if err != nil {
		return nil, err
	}
	if ok {
		if name, isPath := path(c.Args[0]); isPath && name == rawField && t.agg == nil {
			if _, shadowed := t.env[rawField]; !shadowed {
				return exec.MatchAny(p), nil
			}
		}
		field, err := t.expr(c.Args[0])
		if err != nil {
			return nil, err
		}

		return exec.Match(field, p), nil
	}
	args, err := t.args(c)
	if err != nil {
		return nil, err
	}
	if op, ok := scalarCalls[c.Callee]; ok {
		return &exec.Expr{Op: op, Args: args}, nil
	}
	switch {
	case c.Callee == "bin" && len(args) == 2:
		return exec.Bin(args[0], args[1]), nil
	case c.Callee == "timestamp" && len(c.Args) == 1:
		if at, err := t.instant(c.Args[0]); err == nil {
			return exec.Int(at), nil
		}
	case c.Callee == "now" && len(args) == 0:
		return exec.Int(t.now.UnixNano()), nil
	case c.Callee == "date_part" && len(args) == 2:
		if unit, ok := text(c.Args[1]); ok && unit == "hour" {
			return exec.HourOfDay(args[0]), nil
		}
	case c.Callee == "replace" && len(args) == 3:
		// A pattern without regular expression syntax is a plain string.
		if from, ok := text(c.Args[1]); ok && !strings.ContainsAny(from, `\.+*?()|[]{}^$`) {
			return exec.Replace(args[0], args[1], args[2]), nil
		}
	}

	return nil, unsupported("function %s", format.Expr(c))
}

// pred translates a condition; not asks for the rows where it is false.
func (t *translator) pred(e ast.Expr, not bool) (*exec.Expr, error) {
	switch e := e.(type) {
	case *ast.Paren:
		return t.pred(e.Inner, not)
	case *ast.Unary:
		if e.Op == ast.OpNot {
			return t.pred(e.Operand, !not)
		}
	case *ast.Binary:
		switch e.Op {
		case ast.OpAnd, ast.OpOr:
			a, err := t.pred(e.Left, not)
			if err != nil {
				return nil, err
			}
			b, err := t.pred(e.Right, not)
			if err != nil {
				return nil, err
			}
			if (e.Op == ast.OpAnd) != not {
				return exec.And(a, b), nil
			}

			return exec.Or(a, b), nil
		}
		if op, ok := opposite[e.Op]; ok && not {
			return t.expr(&ast.Binary{Op: op, Left: e.Left, Right: e.Right, Pos: e.Pos})
		}
		if _, ok := opposite[e.Op]; ok {
			return t.expr(e)
		}
	case *ast.In:
		list, ok := e.RHS.(*ast.Array)
		if !ok || len(list.Elems) == 0 {
			return nil, unsupported("in %s", format.Expr(e.RHS))
		}
		var any []*exec.Expr
		for _, v := range list.Elems {
			op := ast.OpEq
			if not {
				op = ast.OpNotEq
			}
			p, err := t.expr(&ast.Binary{Op: op, Left: e.LHS, Right: v, Pos: e.Pos})
			if err != nil {
				return nil, err
			}
			any = append(any, p)
		}
		if not {
			return exec.And(any...), nil
		}

		return exec.Or(any...), nil
	case *ast.Between:
		return t.pred(&ast.Binary{Op: ast.OpAnd,
			Left:  &ast.Binary{Op: ast.OpGtEq, Left: e.X, Right: e.Lo, Pos: e.Pos},
			Right: &ast.Binary{Op: ast.OpLtEq, Left: e.X, Right: e.Hi, Pos: e.Pos}, Pos: e.Pos}, not)
	case *ast.Literal:
		if e.Kind == ast.LitBool {
			return exec.Bool(e.Value.(bool) != not), nil
		}
	}
	p, err := t.expr(e)
	if err != nil || !not {
		return p, err
	}
	switch p.Op {
	case exec.OpMatchAny, exec.OpIsNull, exec.OpIsMissing, exec.OpExists:
		// These are true or false for every row.
		return exec.Not(p), nil
	}

	// False, as opposed to unknown.
	return exec.Eq(p, exec.Bool(false)), nil
}
