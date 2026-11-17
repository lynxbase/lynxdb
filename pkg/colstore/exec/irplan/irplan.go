// Package irplan plans queries in the canonical IR onto the columnar
// executor. It takes the plans that executor can run: a scan with filters,
// computed columns, one aggregate with a filter and computed columns above
// it, a choice of columns, a sort, a limit, and the first row of each key.
// Anything else is refused with ErrUnsupported and a name of what it met.
//
// Values keep the executor's rules where the IR's differ: a comparison of a
// number with a string is false and not a conversion.
package irplan

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/ir"
)

// ErrUnsupported reports a plan that is valid but that the executor cannot
// run.
var ErrUnsupported = errors.New("irplan: not supported")

// Options sets how a plan is translated.
type Options struct {
	// Now is the time that relative times count from; the zero time means
	// the time of the call.
	Now time.Time
}

func unsupported(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrUnsupported, fmt.Sprintf(format, args...))
}

// Plan returns the executor's plan for p.
func Plan(p *ir.Plan, opts Options) (exec.Node, error) {
	t := &translator{now: opts.Now, env: map[string]*exec.Expr{}}
	if t.now.IsZero() {
		t.now = time.Now()
	}
	// The operators from the scan upwards.
	var chain []ir.Node
	for n := p.Root; n != nil; {
		chain = append(chain, n)
		in := n.Inputs()
		if len(in) > 1 {
			return nil, unsupported("%s reads more than one input", n.Label())
		}
		if len(in) == 0 {
			break
		}
		n = in[0]
	}
	for i := len(chain) - 1; i >= 0; i-- {
		if err := t.node(chain[i]); err != nil {
			return nil, err
		}
	}

	return t.plan()
}

// translator builds a plan from the operators of a query from its scan
// upwards.
type translator struct {
	now    time.Time
	filter []*exec.Expr
	// env maps the name of a computed or renamed column to its expression:
	// over the table's paths below an aggregate, over the aggregate's
	// columns above it.
	env map[string]*exec.Expr
	// cols lists the output columns once an operator has named them.
	cols   []exec.Named
	named  bool
	agg    *exec.Aggregate
	having []*exec.Expr
	sort   *exec.Sort
	// dedup holds the keys whose every combination keeps its first row.
	dedup []exec.Named
	// totals is set once a column stands for a total over the groups, which
	// a later filter of the groups would change.
	totals bool
}

func (t *translator) node(n ir.Node) error {
	if t.sort != nil {
		return t.aboveSort(n)
	}
	switch n := n.(type) {
	case *ir.Scan:
		earliest, latest := n.Time.Resolve(t.now)
		if !earliest.IsZero() {
			t.filter = append(t.filter, exec.Ge(exec.Time(), exec.Int(earliest.UnixNano())))
		}
		if !latest.IsZero() {
			t.filter = append(t.filter, exec.Lt(exec.Time(), exec.Int(latest.UnixNano())))
		}
	case *ir.Filter:
		if t.dedup != nil || t.totals {
			return unsupported("a filter above the first rows of keys or above a total of the groups")
		}
		p, err := t.pred(n.Cond, false)
		if err != nil {
			return err
		}
		if t.agg != nil {
			t.having = append(t.having, p)
		} else {
			t.filter = append(t.filter, p)
		}
	case *ir.Compute:
		for _, a := range n.Assigns {
			e, err := t.expr(a.Expr)
			if err != nil {
				return err
			}
			t.set(a.Name, e)
		}
	case *ir.Project:
		return t.project(n)
	case *ir.RenameColumns:
		return t.rename(n)
	case *ir.Aggregate:
		return t.aggregate(n)
	case *ir.Window:
		return t.total(n)
	case *ir.Dedup:
		if n.Count != 1 || n.Consecutive || n.KeepNull || t.agg != nil || t.dedup != nil {
			return unsupported("%s", n.Label())
		}
		for _, k := range n.Keys {
			e, err := t.expr(k)
			if err != nil {
				return err
			}
			t.filter = append(t.filter, present(e))
			t.dedup = append(t.dedup, exec.Named{Name: fmt.Sprintf("\x00key%d", len(t.dedup)), Expr: e})
		}
	case *ir.Sort:
		return t.order(n.Keys, 0)
	case *ir.TopN:
		if n.Offset != 0 {
			return unsupported("%s", n.Label())
		}

		return t.order(n.Keys, n.Count)
	case *ir.Limit:
		t.sort = &exec.Sort{}

		return t.limit(n)
	default:
		return unsupported("%s", n.Label())
	}

	return nil
}

func (t *translator) limit(n *ir.Limit) error {
	if n.Offset != 0 || n.Count <= 0 {
		return unsupported("%s", n.Label())
	}
	if count := int(n.Count); t.sort.Limit == 0 || count < t.sort.Limit {
		t.sort.Limit = count
	}

	return nil
}

// aboveSort takes the operators that give the same rows below a sort and
// a limit as above them: a choice among the columns that keeps the sort's
// own, and columns computed under other names.
func (t *translator) aboveSort(n ir.Node) error {
	sorts := func(name string) bool {
		for _, by := range t.sort.By {
			if by.Column == name {
				return true
			}
		}

		return false
	}
	refused := unsupported("%s above a sort or a limit", n.Label())
	switch n := n.(type) {
	case *ir.Limit:
		return t.limit(n)
	case *ir.Project:
		if n.Drop {
			for _, name := range n.Names {
				if sorts(name) {
					return refused
				}
			}
		} else {
			for _, by := range t.sort.By {
				kept := false
				for _, name := range n.Names {
					kept = kept || name == by.Column
				}
				if !kept {
					return refused
				}
			}
		}

		return t.project(n)
	case *ir.Compute:
		for _, a := range n.Assigns {
			if sorts(a.Name) {
				return refused
			}
		}
		for _, a := range n.Assigns {
			e, err := t.expr(a.Expr)
			if err != nil {
				return err
			}
			t.set(a.Name, e)
		}

		return nil
	}

	return refused
}

// total translates the sum of a column of an aggregate over all of its
// groups, which every group then holds.
func (t *translator) total(n *ir.Window) error {
	if t.agg == nil || n.Frame.Kind != ir.FramePartition || len(n.PartitionBy) != 0 {
		return unsupported("%s; a window gives the sum of a column of an aggregate over all of its groups", n.Label())
	}
	for _, f := range n.Funcs {
		call := f.Agg
		refused := unsupported("the window function %s; a window gives the sum of a column of an aggregate", call)
		if call.Func.Name != "sum" || len(call.Args) != 1 || call.Distinct || call.Filter != nil {
			return refused
		}
		e, err := t.expr(call.Args[0])
		if err != nil {
			return err
		}
		if e.Op != exec.OpColumn {
			return refused
		}
		t.set(f.Name, exec.Total(e))
	}
	t.totals = true

	return nil
}

// present is true for the rows in which e has a value that is not null.
func present(e *exec.Expr) *exec.Expr {
	return exec.Not(exec.Or(exec.IsNull(e), exec.IsMissing(e)))
}

func (t *translator) order(keys []ir.SortKey, limit int64) error {
	t.sort = &exec.Sort{Limit: int(limit)}
	for _, k := range keys {
		ref, ok := k.Expr.(*ir.FieldRef)
		if !ok {
			return unsupported("a sort by %s; compute it as a column first", k.Expr)
		}
		t.sort.By = append(t.sort.By, exec.Order{Column: ref.Name, Desc: k.Desc, NullsFirst: k.NullsFirst})
	}

	return nil
}

func (t *translator) project(n *ir.Project) error {
	for _, name := range n.Names {
		if strings.ContainsAny(name, "*?") {
			return unsupported("the column pattern %s", name)
		}
	}
	if n.Drop {
		if !t.named {
			return unsupported("%s before the columns are named", n.Label())
		}
		for _, name := range n.Names {
			kept := t.cols[:0]
			for _, c := range t.cols {
				if c.Name != name {
					kept = append(kept, c)
				}
			}
			t.cols = kept
		}

		return nil
	}
	cols := make([]exec.Named, 0, len(n.Names))
	for _, name := range n.Names {
		e, err := t.field(name)
		if err != nil {
			return err
		}
		cols = append(cols, exec.Named{Name: name, Expr: e})
	}
	t.cols, t.named = cols, true

	return nil
}

func (t *translator) rename(n *ir.RenameColumns) error {
	for _, r := range n.Renames {
		if strings.Contains(r.From, "*") || strings.Contains(r.To, "*") {
			return unsupported("the rename pattern %s", r.From)
		}
		e, err := t.field(r.From)
		if err != nil {
			return err
		}
		renamed := false
		for i := range t.cols {
			if t.cols[i].Name == r.From {
				t.cols[i].Name, renamed = r.To, true
			}
		}
		if !renamed && t.named {
			return unsupported("the rename of %s, which is not among the columns", r.From)
		}
		delete(t.env, r.From)
		t.env[r.To] = e
	}

	return nil
}

// set defines the column name as e and adds it to the named columns.
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
	"count": exec.Count, "sum": exec.Sum, "avg": exec.Avg, "min": exec.Min, "max": exec.Max,
	"var_samp": exec.Var, "stddev_samp": exec.Stdev,
	"distinct_count": exec.CountDistinct, "distinct_count_approx": exec.ApproxDistinct,
	"percentile": exec.Percentile,
	"earliest":   exec.Earliest, "latest": exec.Latest, "first": exec.First, "last": exec.Last,
}

func (t *translator) aggregate(n *ir.Aggregate) error {
	if t.agg != nil || t.dedup != nil || n.Phase != ir.AggComplete {
		return unsupported("%s above an aggregate, above the first rows of keys, or split in phases", n.Label())
	}
	agg := &exec.Aggregate{}
	env := map[string]*exec.Expr{}
	var cols []exec.Named
	add := func(name string) {
		env[name] = exec.Col(name, exec.KindAny)
		cols = append(cols, exec.Named{Name: name, Expr: env[name]})
	}
	for _, g := range n.Groups {
		e, err := t.expr(g.Expr)
		if err != nil {
			return err
		}
		if n.DropNullGroups {
			t.filter = append(t.filter, present(e))
		}
		agg.Keys = append(agg.Keys, exec.Named{Name: g.Name, Expr: e})
		add(g.Name)
	}
	for _, a := range n.Aggs {
		out, err := t.aggCall(a)
		if err != nil {
			return err
		}
		agg.Aggs = append(agg.Aggs, out)
		add(out.Name)
	}
	t.agg, t.env, t.cols, t.named = agg, env, cols, true

	return nil
}

func (t *translator) aggCall(a ir.NamedAgg) (exec.Agg, error) {
	call := a.Agg
	refused := unsupported("the aggregate %s", call)
	fn, ok := aggFuncs[call.Func.Name]
	if !ok {
		return exec.Agg{}, refused
	}
	out := exec.Agg{Name: a.Name, Func: fn}
	args := call.Args
	switch {
	case call.Distinct && fn == exec.Count && len(args) == 1:
		out.Func = exec.CountDistinct
	case call.Distinct:
		return exec.Agg{}, refused
	case fn == exec.Count && len(args) > 1:
		return exec.Agg{}, refused
	case fn == exec.Percentile:
		if len(args) != 2 {
			return exec.Agg{}, refused
		}
		lit, isLit := args[1].(*ir.Literal)
		if !isLit {
			return exec.Agg{}, refused
		}
		switch v := lit.V.(type) {
		case int64:
			out.P = float64(v)
		case float64:
			out.P = v
		default:
			return exec.Agg{}, refused
		}
		args = args[:1]
	case (fn == exec.Earliest || fn == exec.Latest) && len(args) == 2:
		// The picks order by the time of the row and by nothing else.
		at, err := t.expr(args[1])
		if err != nil {
			return exec.Agg{}, err
		}
		if at.Op != exec.OpColumn || at.Path != part.TimePath {
			return exec.Agg{}, refused
		}
		args = args[:1]
	case fn != exec.Count && len(args) != 1:
		return exec.Agg{}, refused
	}
	if len(args) == 1 {
		e, err := t.expr(args[0])
		if err != nil {
			return exec.Agg{}, err
		}
		out.Arg = e
	}
	if call.Filter != nil {
		cond, err := t.expr(call.Filter)
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

// plan assembles the nodes the operators asked for.
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
		return nil, unsupported("rows without named columns: name the fields to return")
	case t.dedup != nil:
		node = t.firstRows(scan)
	default:
		node = &exec.Project{Input: scan, Exprs: t.cols}
	}
	if t.sort != nil {
		for _, by := range t.sort.By {
			known := false
			for _, c := range t.cols {
				known = known || c.Name == by.Column
			}
			if !known {
				return nil, unsupported("a sort by %s, which is not among the columns", by.Column)
			}
		}
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

// field returns what a column name stands for.
func (t *translator) field(name string) (*exec.Expr, error) {
	if e, ok := t.env[name]; ok {
		return e, nil
	}
	switch {
	case t.agg != nil:
		return nil, unsupported("the column %s, which the aggregate does not have", name)
	case name == part.TimePath:
		return exec.Time(), nil
	}

	return exec.Col(name, exec.KindAny), nil
}

func literal(l *ir.Literal) (*exec.Expr, error) {
	switch v := l.V.(type) {
	case nil:
		return &exec.Expr{Op: exec.OpLiteral}, nil
	case bool:
		return exec.Bool(v), nil
	case int64:
		return exec.Int(v), nil
	case float64:
		return exec.Float(v), nil
	case string:
		return exec.Str(v), nil
	case time.Time:
		return exec.Int(v.UnixNano()), nil
	case time.Duration:
		return exec.Int(int64(v)), nil
	}

	return nil, unsupported("the literal %s", l)
}

var binaryOps = map[ir.BinaryOp]exec.Op{
	ir.OpEq: exec.OpEq, ir.OpNe: exec.OpNe, ir.OpLt: exec.OpLt, ir.OpLe: exec.OpLe, ir.OpGt: exec.OpGt, ir.OpGe: exec.OpGe,
	ir.OpAdd: exec.OpAdd, ir.OpSub: exec.OpSub, ir.OpMul: exec.OpMul, ir.OpDiv: exec.OpDiv, ir.OpMod: exec.OpMod,
	ir.OpAnd: exec.OpAnd, ir.OpOr: exec.OpOr,
}

// opposite maps a comparison to the one that holds where it does not, for
// values that compare at all.
var opposite = map[ir.BinaryOp]ir.BinaryOp{
	ir.OpEq: ir.OpNe, ir.OpNe: ir.OpEq, ir.OpLt: ir.OpGe, ir.OpGe: ir.OpLt, ir.OpGt: ir.OpLe, ir.OpLe: ir.OpGt,
}

// scalarCalls maps the functions that take their arguments as they are.
var scalarCalls = map[string]exec.Op{
	"coalesce": exec.OpCoalesce, "typeof": exec.OpTypeOf,
	"length": exec.OpLength, "lower": exec.OpLower, "upper": exec.OpUpper, "concat": exec.OpConcat,
	"starts_with": exec.OpStartsWith, "ends_with": exec.OpEndsWith,
	"trim": exec.OpTrim, "ltrim": exec.OpTrimLeft, "rtrim": exec.OpTrimRight,
	"abs": exec.OpAbs, "round": exec.OpRound, "floor": exec.OpFloor, "ceil": exec.OpCeil, "sqrt": exec.OpSqrt,
	"ln": exec.OpLn, "exp": exec.OpExp, "pow": exec.OpPow, "mod": exec.OpMod,
	"num_bucket": exec.OpBin, "hour": exec.OpHourOfDay,
}

// expr translates a value expression.
func (t *translator) expr(e ir.Expr) (*exec.Expr, error) {
	switch e := e.(type) {
	case *ir.Literal:
		return literal(e)
	case *ir.FieldRef:
		return t.field(e.Name)
	case *ir.RelativeTime:
		return exec.Int(e.Bound.Resolve(t.now).UnixNano()), nil
	case *ir.Unary:
		a, err := t.expr(e.X)
		if err != nil {
			return nil, err
		}
		if e.Op == ir.OpNot {
			return exec.Not(a), nil
		}

		return exec.Sub(exec.Int(0), a), nil
	case *ir.Binary:
		op, ok := binaryOps[e.Op]
		if !ok {
			return nil, unsupported("the operator %s", e.Op)
		}
		a, err := t.expr(e.L)
		if err != nil {
			return nil, err
		}
		b, err := t.expr(e.R)
		if err != nil {
			return nil, err
		}

		return &exec.Expr{Op: op, Args: []*exec.Expr{a, b}}, nil
	case *ir.Case:
		var args []*exec.Expr
		for _, w := range e.Whens {
			cond, err := t.expr(w.Cond)
			if err != nil {
				return nil, err
			}
			then, err := t.expr(w.Then)
			if err != nil {
				return nil, err
			}
			args = append(args, cond, then)
		}
		if e.Else != nil {
			other, err := t.expr(e.Else)
			if err != nil {
				return nil, err
			}
			args = append(args, other)
		}

		return exec.Case(args...), nil
	case *ir.Cast:
		if e.X.Type().Kind == ir.KindTimestamp {
			// A time is held as a count of nanoseconds, which a cast is not.
			return nil, unsupported("a cast of the time %s", e.X)
		}
		a, err := t.expr(e.X)
		if err != nil {
			return nil, err
		}
		switch e.To.Kind {
		case ir.KindInt:
			return exec.ToInt(a), nil
		case ir.KindFloat:
			return exec.ToFloat(a), nil
		case ir.KindString:
			return exec.ToString(a), nil
		case ir.KindBool:
			return exec.ToBool(a), nil
		}

		return nil, unsupported("a cast to %s", e.To)
	case *ir.InList, *ir.IsMissing, *ir.Search:
		return t.pred(e, false)
	case *ir.Call:
		return t.call(e)
	}

	return nil, unsupported("the expression %s", e)
}

func text(e ir.Expr) (string, bool) {
	l, ok := e.(*ir.Literal)
	if !ok {
		return "", false
	}
	s, ok := l.V.(string)

	return s, ok
}

func (t *translator) call(c *ir.Call) (*exec.Expr, error) {
	refused := unsupported("the function %s", c)
	name := c.Func.Name
	switch {
	case name == "now" && len(c.Args) == 0:
		return exec.Int(t.now.UnixNano()), nil
	case name == "timestamp" && len(c.Args) == 1:
		if s, ok := text(c.Args[0]); ok {
			at, err := time.Parse(time.RFC3339Nano, s)
			if err != nil {
				return nil, fmt.Errorf("irplan: %q is not a timestamp", s)
			}

			return exec.Int(at.UnixNano()), nil
		}

		return nil, refused
	case name == "is_null" && len(c.Args) == 1:
		a, err := t.expr(c.Args[0])
		if err != nil {
			return nil, err
		}

		return exec.Or(exec.IsNull(a), exec.IsMissing(a)), nil
	case name == "like" || name == "regexp_match" || name == "contains":
		p, err := pattern(c)
		if err != nil {
			return nil, err
		}
		field, err := t.expr(c.Args[0])
		if err != nil {
			return nil, err
		}

		return exec.Match(field, p), nil
	case name == "time_bucket" && len(c.Args) == 3:
		width, ok := bucketWidth(c.Args[1], c.Args[2])
		if !ok {
			return nil, refused
		}
		a, err := t.expr(c.Args[0])
		if err != nil {
			return nil, err
		}

		return exec.Bin(a, exec.Int(int64(width))), nil
	case name == "substring":
		return t.substring(c)
	case name == "timestampdiff" && len(c.Args) == 3:
		unit, _ := text(c.Args[0])
		width, ok := diffUnits[unit]
		if !ok {
			return nil, refused
		}
		from, err := t.expr(c.Args[1])
		if err != nil {
			return nil, err
		}
		to, err := t.expr(c.Args[2])
		if err != nil {
			return nil, err
		}

		return exec.Div(exec.Sub(to, from), exec.Int(int64(width))), nil
	case name == "regexp_replace" && len(c.Args) == 3:
		// A pattern without regular expression syntax is a plain string.
		from, ok := text(c.Args[1])
		to, isText := text(c.Args[2])
		if !ok || !isText || from == "" || strings.ContainsAny(from, `\.+*?()|[]{}^$`) || strings.ContainsAny(to, `\$`) {
			return nil, refused
		}
		a, err := t.expr(c.Args[0])
		if err != nil {
			return nil, err
		}

		return exec.Replace(a, exec.Str(from), exec.Str(to)), nil
	case name == "locate" && len(c.Args) == 2:
		sub, err := t.expr(c.Args[0])
		if err != nil {
			return nil, err
		}
		a, err := t.expr(c.Args[1])
		if err != nil {
			return nil, err
		}
		// Counted from one, and zero where the string does not hold it.
		at := exec.Coalesce(exec.Add(exec.IndexOf(a, sub), exec.Int(1)), exec.Int(0))

		return exec.Case(present(a), at), nil
	case name == "tostring" && len(c.Args) == 1:
		a, err := t.expr(c.Args[0])
		if err != nil {
			return nil, err
		}

		return exec.ToString(a), nil
	case name == "log" && len(c.Args) == 1:
		a, err := t.expr(c.Args[0])
		if err != nil {
			return nil, err
		}

		return exec.Ln(a), nil
	}
	op, ok := scalarCalls[name]
	if !ok {
		return nil, refused
	}
	args := make([]*exec.Expr, len(c.Args))
	for i, a := range c.Args {
		e, err := t.expr(a)
		if err != nil {
			return nil, err
		}
		args[i] = e
	}

	return &exec.Expr{Op: op, Args: args}, nil
}

// substring translates a substring that starts at a constant place, which
// the IR counts from one and the executor from zero.
func (t *translator) substring(c *ir.Call) (*exec.Expr, error) {
	refused := unsupported("the function %s", c)
	if len(c.Args) < 2 || len(c.Args) > 3 {
		return nil, refused
	}
	lit, ok := c.Args[1].(*ir.Literal)
	if !ok {
		return nil, refused
	}
	start, ok := lit.V.(int64)
	if !ok || start == 0 {
		return nil, refused
	}
	if start > 0 {
		start--
	}
	s, err := t.expr(c.Args[0])
	if err != nil {
		return nil, err
	}
	if len(c.Args) == 2 {
		return exec.Substr(s, exec.Int(start)), nil
	}
	count, err := t.expr(c.Args[2])
	if err != nil {
		return nil, err
	}

	return exec.Substr(s, exec.Int(start), count), nil
}

// diffUnits holds the units of a difference of times that have one length.
var diffUnits = map[string]time.Duration{
	"microsecond": time.Microsecond, "millisecond": time.Millisecond, "second": time.Second,
	"minute": time.Minute, "hour": time.Hour, "day": 24 * time.Hour, "week": 7 * 24 * time.Hour,
}

// bucketWidth returns the nanoseconds of n units for the units whose
// buckets align to the epoch.
func bucketWidth(n, unit ir.Expr) (time.Duration, bool) {
	lit, ok := n.(*ir.Literal)
	if !ok {
		return 0, false
	}
	count, ok := lit.V.(int64)
	name, isText := text(unit)
	if !ok || !isText || count <= 0 {
		return 0, false
	}
	units := map[string]time.Duration{
		"ms": time.Millisecond, "s": time.Second, "m": time.Minute, "h": time.Hour, "d": 24 * time.Hour, "w": 7 * 24 * time.Hour,
	}
	width, ok := units[name]

	return time.Duration(count) * width, ok
}

// pattern returns what a function that tests a string looks for.
func pattern(c *ir.Call) (tokenfilter.Pattern, error) {
	refused := unsupported("the function %s with a pattern that is not a constant", c)
	if len(c.Args) < 2 {
		return tokenfilter.Pattern{}, refused
	}
	term, ok := text(c.Args[1])
	if !ok || term == "" {
		return tokenfilter.Pattern{}, refused
	}
	switch c.Func.Name {
	case "contains":
		return tokenfilter.Substring(term), nil
	case "regexp_match":
		p, err := tokenfilter.Regexp(term)
		if err != nil {
			return p, fmt.Errorf("irplan: %s: %w", c, err)
		}

		return p, nil
	}
	// like: any case unless its third argument is true.
	exact := false
	if len(c.Args) == 3 {
		lit, isLit := c.Args[2].(*ir.Literal)
		if !isLit {
			return tokenfilter.Pattern{}, refused
		}
		exact, _ = lit.V.(bool)
	}
	if inner, ok := likeSubstring(term); ok {
		p := tokenfilter.Substring(inner)
		if !exact {
			p = p.IgnoreCase()
		}

		return p, nil
	}
	expr := likeRegexp(term)
	if !exact {
		expr = "(?i)" + expr
	}
	p, err := tokenfilter.Regexp(expr)
	if err != nil {
		return p, fmt.Errorf("irplan: %s: %w", c, err)
	}

	return p, nil
}

// likeSubstring returns the text between the two percent signs of a LIKE
// pattern that has no other wildcard, which is a search for a substring.
func likeSubstring(pattern string) (string, bool) {
	if len(pattern) < 3 || pattern[0] != '%' || pattern[len(pattern)-1] != '%' {
		return "", false
	}
	inner := pattern[1 : len(pattern)-1]
	if strings.ContainsAny(inner, `%_\`) {
		return "", false
	}

	return inner, true
}

// likeRegexp returns the regular expression that matches the whole of what
// a LIKE pattern matches.
func likeRegexp(pattern string) string {
	var b strings.Builder
	b.WriteString("(?s)^")
	for i := 0; i < len(pattern); i++ {
		switch c := pattern[i]; {
		case c == '%':
			b.WriteString(".*")
		case c == '_':
			b.WriteByte('.')
		case c == '\\' && i+1 < len(pattern):
			i++
			b.WriteString(quoteMeta(pattern[i : i+1]))
		default:
			b.WriteString(quoteMeta(pattern[i : i+1]))
		}
	}
	b.WriteByte('$')

	return b.String()
}

func quoteMeta(s string) string {
	if strings.ContainsAny(s, `\.+*?()|[]{}^$`) {
		return `\` + s
	}

	return s
}

// search translates a search of text: of one column, or of every string of
// the row when it names none.
func (t *translator) search(s *ir.Search) (*exec.Expr, error) {
	fold := func(p tokenfilter.Pattern) tokenfilter.Pattern {
		if s.CaseSensitive {
			return p
		}

		return p.IgnoreCase()
	}
	var patterns []tokenfilter.Pattern
	switch s.Mode {
	case ir.SearchWord:
		// Every token of the pattern, as a whole token, wherever it is.
		for _, token := range strings.FieldsFunc(s.Pattern, func(r rune) bool { return !tokenfilter.IsTokenRune(r) }) {
			patterns = append(patterns, fold(tokenfilter.Word(token)))
		}
	case ir.SearchPrefix:
		patterns = append(patterns, fold(tokenfilter.Prefix(s.Pattern)))
	case ir.SearchPhrase:
		patterns = append(patterns, fold(tokenfilter.Word(s.Pattern)))
	case ir.SearchSubstring:
		patterns = append(patterns, fold(tokenfilter.Substring(s.Pattern)))
	case ir.SearchRegex:
		expr := s.Pattern
		if !s.CaseSensitive {
			expr = "(?i)" + expr
		}
		p, err := tokenfilter.Regexp(expr)
		if err != nil {
			return nil, fmt.Errorf("irplan: %s: %w", s, err)
		}
		patterns = append(patterns, p)
	case ir.SearchGlob:
		prefix, star := strings.CutSuffix(s.Pattern, "*")
		if !star || prefix == "" || strings.ContainsAny(prefix, "*?") {
			return nil, unsupported("%s; a wildcard is taken at the end of a word", s)
		}
		patterns = append(patterns, fold(tokenfilter.Prefix(prefix)))
	}
	if len(patterns) == 0 {
		return nil, unsupported("%s", s)
	}
	var field *exec.Expr
	if s.Field != "" {
		var err error
		if field, err = t.field(s.Field); err != nil {
			return nil, err
		}
	} else if t.agg != nil {
		return nil, unsupported("%s above an aggregate", s)
	}
	out := make([]*exec.Expr, len(patterns))
	for i, p := range patterns {
		if field == nil {
			out[i] = exec.MatchAny(p)
		} else {
			out[i] = exec.Match(field, p)
		}
	}
	if len(out) == 1 {
		return out[0], nil
	}

	return exec.And(out...), nil
}

// join returns the conjunction or disjunction of a and b as one node over
// the terms of both, which lets the executor order all of them by cost.
func join(op exec.Op, a, b *exec.Expr) *exec.Expr {
	out := &exec.Expr{Op: op}
	for _, e := range []*exec.Expr{a, b} {
		if e.Op == op {
			out.Args = append(out.Args, e.Args...)
		} else {
			out.Args = append(out.Args, e)
		}
	}

	return out
}

// pred translates a condition; not asks for the rows where it is false.
func (t *translator) pred(e ir.Expr, not bool) (*exec.Expr, error) {
	switch e := e.(type) {
	case *ir.Unary:
		if e.Op == ir.OpNot {
			return t.pred(e.X, !not)
		}
	case *ir.Binary:
		switch e.Op {
		case ir.OpAnd, ir.OpOr:
			a, err := t.pred(e.L, not)
			if err != nil {
				return nil, err
			}
			b, err := t.pred(e.R, not)
			if err != nil {
				return nil, err
			}
			if (e.Op == ir.OpAnd) != not {
				return join(exec.OpAnd, a, b), nil
			}

			return join(exec.OpOr, a, b), nil
		}
		if op, ok := opposite[e.Op]; ok && not {
			return t.expr(&ir.Binary{Op: op, L: e.L, R: e.R, T: e.T})
		}
		if _, ok := opposite[e.Op]; ok {
			return t.expr(e)
		}
	case *ir.InList:
		if len(e.List) == 0 {
			return nil, unsupported("%s", e)
		}
		negated := e.Not != not
		any := make([]*exec.Expr, 0, len(e.List))
		for _, v := range e.List {
			op := ir.OpEq
			if negated {
				op = ir.OpNe
			}
			p, err := t.expr(&ir.Binary{Op: op, L: e.X, R: v, T: ir.Bool})
			if err != nil {
				return nil, err
			}
			any = append(any, p)
		}
		if negated {
			return exec.And(any...), nil
		}

		return exec.Or(any...), nil
	case *ir.IsMissing:
		field, err := t.field(e.Field.Name)
		if err != nil {
			return nil, err
		}
		if not {
			return exec.Not(exec.IsMissing(field)), nil
		}

		return exec.IsMissing(field), nil
	case *ir.Search:
		p, err := t.search(e)
		if err != nil || !not {
			return p, err
		}
		if e.Field == "" {
			// A row either holds the text somewhere or does not.
			return exec.Not(p), nil
		}

		return exec.Eq(p, exec.Bool(false)), nil
	case *ir.Literal:
		if v, ok := e.V.(bool); ok {
			return exec.Bool(v != not), nil
		}
	case *ir.Call:
		// A condition that is false where it is unknown: the rows where it
		// is true, or the rest of them.
		if e.Func.Name == "coalesce" && len(e.Args) == 2 {
			if lit, ok := e.Args[1].(*ir.Literal); ok && lit.V == false {
				p, err := t.pred(e.Args[0], false)
				if err != nil || !not {
					return p, err
				}

				return exec.Not(p), nil
			}
		}
	}
	p, err := t.expr(e)
	if err != nil || !not {
		return p, err
	}
	switch p.Op {
	case exec.OpIsNull, exec.OpIsMissing, exec.OpExists:
		// These are true or false for every row.
		return exec.Not(p), nil
	}

	// False, as opposed to unknown.
	return exec.Eq(p, exec.Bool(false)), nil
}
