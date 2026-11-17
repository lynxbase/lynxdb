// Package irplan plans queries in the canonical IR onto the columnar
// executor. It takes the plans that executor can run: a scan with filters,
// computed columns, one aggregate with a filter and computed columns above
// it, a choice of columns or whole events, a sort, a limit, and the first
// row of each key.
// Anything else is refused with ErrUnsupported and a name of what it met.
//
// Values keep the executor's rules where the IR's differ: a comparison of a
// number with a string is false and not a conversion.
package irplan

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec/irfunc"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// ErrUnsupported reports a plan that is valid but that the executor cannot
// run.
var ErrUnsupported = errors.New("irplan: not supported")

// Options sets how a plan is translated.
type Options struct {
	// Now is the time that relative times count from; the zero time means
	// the time of the call.
	Now time.Time
	// Opaque, when set, is asked for a function that computes an expression
	// which the executor has no operations for: the function takes the
	// values of fields, in their order, in one row. It returns false for an
	// expression that it does not compute either.
	Opaque func(e ir.Expr, fields []*ir.FieldRef) (func() exec.Func, bool)
}

func unsupported(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrUnsupported, fmt.Sprintf(format, args...))
}

// Plan returns the executor's plan for p.
func Plan(p *ir.Plan, opts Options) (exec.Node, error) {
	t := &translator{now: opts.Now, opaque: opts.Opaque, env: map[string]*exec.Expr{}}
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

// Rows returns the plan that gives the events of a scan as rows: those in
// its time range that pass the conditions among its hints which the executor
// has operations for, with the columns it names or else whole, and no more
// of them than its limit. The conditions it leaves out are for the operators
// above the scan, which hold them all.
func Rows(scan *ir.Scan, opts Options) (exec.Node, error) {
	t := &translator{now: opts.Now, opaque: opts.Opaque, env: map[string]*exec.Expr{}}
	if t.now.IsZero() {
		t.now = time.Now()
	}
	if err := t.node(&ir.Scan{Time: scan.Time}); err != nil {
		return nil, err
	}
	for _, hint := range scan.Prune {
		if p, err := t.pred(hint, false); err == nil {
			t.filter = append(t.filter, p)
		}
	}
	if scan.Columns != nil {
		// The time orders the newest first, and stands for the row where
		// no column is asked for.
		names := scan.Columns
		if (scan.Descending || len(names) == 0) && !slices.Contains(names, ir.TimeColumn) {
			names = append(names[:len(names):len(names)], ir.TimeColumn)
		}
		if err := t.project(&ir.Project{Names: names}); err != nil {
			return nil, err
		}
	}
	switch {
	case scan.Descending:
		if err := t.order([]ir.SortKey{{Expr: ir.Field(ir.TimeColumn), Desc: true}}, scan.Limit); err != nil {
			return nil, err
		}
	case scan.Limit > 0:
		t.sort = &exec.Sort{Limit: int(scan.Limit)}
	}

	return t.plan()
}

// translator builds a plan from the operators of a query from its scan
// upwards.
type translator struct {
	now time.Time
	// opaque computes the expressions that the executor has no operations
	// for, when the caller gave a way to.
	opaque func(e ir.Expr, fields []*ir.FieldRef) (func() exec.Func, bool)
	filter []*exec.Expr
	// env maps the name of a computed or renamed column to its expression:
	// over the table's paths below an aggregate, over the aggregate's
	// columns above it.
	env map[string]*exec.Expr
	// cols lists the output columns once an operator has named them. Until
	// then the rows are whole events: every stored field less gone, with
	// the fields of assigned as env defines them.
	cols     []exec.Named
	named    bool
	assigned []string
	gone     []string
	// unseen holds the columns that order the rows and that an operator
	// above the sort left out of the output.
	unseen []exec.Named
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
		// What the sort reads keeps ordering the rows when the choice of
		// columns leaves it out.
		ordering := make([]exec.Named, 0, len(t.sort.By))
		for _, by := range t.sort.By {
			if slices.ContainsFunc(t.unseen, func(c exec.Named) bool { return c.Name == by.Column }) {
				continue
			}
			e, err := t.shown(by.Column)
			if err != nil {
				return err
			}
			ordering = append(ordering, exec.Named{Name: by.Column, Expr: e})
		}
		if err := t.project(n); err != nil {
			return err
		}
		for _, c := range ordering {
			if _, err := t.shown(c.Name); err != nil || (!t.named && slices.Contains(t.gone, c.Name)) {
				t.unseen = append(t.unseen, c)
			}
		}

		return nil
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

// shown returns what the output column name holds; it is an error for a
// name that the named columns do not have.
func (t *translator) shown(name string) (*exec.Expr, error) {
	if !t.named {
		return t.field(name)
	}
	for _, c := range t.cols {
		if c.Name == name {
			return c.Expr, nil
		}
	}

	return nil, unsupported("a sort by %s, which is not among the columns", name)
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
		for _, name := range n.Names {
			t.drop(name)
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
		if !t.named {
			t.drop(r.From)
			t.set(r.To, e)

			continue
		}
		renamed := false
		for i := range t.cols {
			if t.cols[i].Name == r.From {
				t.cols[i].Name, renamed = r.To, true
			}
		}
		if !renamed {
			return unsupported("the rename of %s, which is not among the columns", r.From)
		}
		delete(t.env, r.From)
		t.env[r.To] = e
	}

	return nil
}

// drop removes the column name from the output.
func (t *translator) drop(name string) {
	if t.named {
		t.cols = slices.DeleteFunc(t.cols, func(c exec.Named) bool { return c.Name == name })

		return
	}
	delete(t.env, name)
	t.assigned = slices.DeleteFunc(t.assigned, func(a string) bool { return a == name })
	if !slices.Contains(t.gone, name) {
		t.gone = append(t.gone, name)
	}
}

// set defines the column name as e and adds it to the named columns.
func (t *translator) set(name string, e *exec.Expr) {
	t.env[name] = e
	if !t.named {
		t.gone = slices.DeleteFunc(t.gone, func(g string) bool { return g == name })
		if !slices.Contains(t.assigned, name) {
			t.assigned = append(t.assigned, name)
		}

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
	// The columns of the plan: the output, then what only orders it.
	cols := append(t.cols[:len(t.cols):len(t.cols)], t.unseen...)
	var node exec.Node
	switch {
	case t.agg != nil:
		t.agg.Input = scan
		node = t.agg
		if len(t.having) != 0 {
			node = &exec.Filter{Input: node, Cond: exec.And(t.having...)}
		}
		// The aggregate's own columns in their order need no projection.
		same := len(cols) == len(t.agg.Keys)+len(t.agg.Aggs)
		for i := 0; same && i < len(cols); i++ {
			name := ""
			if i < len(t.agg.Keys) {
				name = t.agg.Keys[i].Name
			} else {
				name = t.agg.Aggs[i-len(t.agg.Keys)].Name
			}
			c := cols[i]
			same = c.Name == name && c.Expr.Op == exec.OpColumn && c.Expr.Path == name
		}
		if !same {
			node = &exec.Project{Input: node, Exprs: cols}
		}
	case t.named && len(cols) == 0:
		return nil, unsupported("rows without a column")
	case !t.named:
		// Whole events: what the sort reads and what the query computed,
		// then every other field.
		names := t.assigned
		if t.sort != nil {
			for _, by := range t.sort.By {
				if !slices.Contains(names, by.Column) && !slices.ContainsFunc(t.unseen, func(c exec.Named) bool { return c.Name == by.Column }) {
					names = append(names[:len(names):len(names)], by.Column)
				}
			}
		}
		cols = cols[:0:0]
		for _, name := range names {
			e, err := t.field(name)
			if err != nil {
				return nil, err
			}
			cols = append(cols, exec.Named{Name: name, Expr: e})
		}
		cols = append(cols, t.unseen...)
		if t.dedup != nil {
			node = t.firstRows(scan, cols, true)
		} else {
			node = &exec.Project{Input: scan, Exprs: cols, Rest: true, Except: t.gone}
		}
	case t.dedup != nil:
		node = t.firstRows(scan, cols, false)
	default:
		node = &exec.Project{Input: scan, Exprs: cols}
	}
	if t.sort != nil {
		for _, by := range t.sort.By {
			if !slices.ContainsFunc(cols, func(c exec.Named) bool { return c.Name == by.Column }) {
				return nil, unsupported("a sort by %s, which is not among the columns", by.Column)
			}
		}
		for _, c := range t.unseen {
			t.sort.Hide = append(t.sort.Hide, c.Name)
		}
		t.sort.Input = node
		node = t.sort
	}

	return node, nil
}

// firstRows returns cols computed over the first row of each combination of
// the dedup keys: an aggregate that reads, from that row, every field the
// columns use, and with whole every other field of the row after them.
func (t *translator) firstRows(scan *exec.Scan, cols []exec.Named, whole bool) exec.Node {
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
	first := &exec.Project{Input: agg, Exprs: make([]exec.Named, len(cols))}
	for i, c := range cols {
		first.Exprs[i] = exec.Named{Name: c.Name, Expr: over(c.Expr)}
	}
	if whole {
		// A pick of its own: its column stays the place of the row.
		const row = "\x00row"
		agg.Aggs = append(agg.Aggs, exec.Agg{Name: row, Func: exec.FirstRow, Arg: exec.Time()})
		first.Exprs = append(first.Exprs, exec.Named{Name: row, Expr: exec.Col(row, exec.KindAny)})
		first.Rest, first.Except = true, t.gone
	}

	return first
}

// field returns what a column name stands for.
func (t *translator) field(name string) (*exec.Expr, error) {
	if e, ok := t.env[name]; ok {
		return e, nil
	}
	switch {
	case t.agg != nil:
		return nil, unsupported("the column %s, which the aggregate does not have", name)
	case slices.Contains(t.gone, name):
		return &exec.Expr{Op: exec.OpLiteral}, nil
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
// Functions that the executor would compute value by value anyway run as
// their native implementations, which follow the IR's rules.
var scalarCalls = map[string]exec.Op{
	"coalesce": exec.OpCoalesce, "num_bucket": exec.OpBin,
}

// expr translates a value expression: into the operations of the executor,
// or else into a function of the fields that the expression reads.
func (t *translator) expr(e ir.Expr) (*exec.Expr, error) {
	out, err := t.operations(e)
	if err == nil || t.opaque == nil || !errors.Is(err, ErrUnsupported) {
		return out, err
	}
	var fields []*ir.FieldRef
	whole := true
	ir.WalkExpr(e, func(x ir.Expr) bool {
		switch x := x.(type) {
		case *ir.Subquery:
			whole = false
		case *ir.FieldRef:
			if !slices.ContainsFunc(fields, func(f *ir.FieldRef) bool { return f.Name == x.Name }) {
				fields = append(fields, x)
			}
		}

		return whole
	})
	if !whole {
		return nil, err
	}
	// A function takes an argument: a constant stands in for one that
	// reads no field.
	args := []*exec.Expr{exec.Bool(true)}
	if len(fields) != 0 {
		args = args[:0]
	}
	for _, f := range fields {
		arg, failed := t.field(f.Name)
		if failed != nil {
			return nil, failed
		}
		args = append(args, arg)
	}
	newFunc, ok := t.opaque(e, fields)
	if !ok {
		return nil, err
	}

	return exec.Call(newFunc, args...), nil
}

// operations translates a value expression into the operations of the
// executor.
func (t *translator) operations(e ir.Expr) (*exec.Expr, error) {
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
		// The executor's conversions differ from the IR's, which the native
		// cast follows.
		newFunc, err := irfunc.Cast(e)
		if err != nil {
			return nil, unsupported("a cast to %s", e.To)
		}

		return t.native(newFunc, []ir.Expr{e.X})
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

// call translates a call into the executor's operations, or else into the
// native implementation of its function.
func (t *translator) call(c *ir.Call) (*exec.Expr, error) {
	out, err := t.operationCall(c)
	if err == nil || !errors.Is(err, ErrUnsupported) {
		return out, err
	}
	b, ok := irfunc.Lookup(c.Func.Name)
	if !ok {
		return nil, err
	}
	newFunc, buildErr := b(c, irfunc.Env{Now: t.now})
	if buildErr != nil {
		return nil, err
	}

	return t.native(newFunc, c.Args)
}

// native translates the arguments of a native function.
func (t *translator) native(newFunc func() exec.Func, in []ir.Expr) (*exec.Expr, error) {
	args := make([]*exec.Expr, 0, max(len(in), 1))
	for _, a := range in {
		e, err := t.expr(a)
		if err != nil {
			return nil, err
		}
		args = append(args, e)
	}
	if len(args) == 0 {
		// A function takes an argument: a constant stands in for none.
		args = append(args, exec.Bool(true))
	}

	return exec.Call(newFunc, args...), nil
}

func (t *translator) operationCall(c *ir.Call) (*exec.Expr, error) {
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
	case name == "time_bucket" && len(c.Args) == 3 && c.Args[0].Type().Kind == ir.KindTimestamp:
		width, ok := bucketWidth(c.Args[1], c.Args[2])
		if !ok {
			return nil, refused
		}
		a, err := t.expr(c.Args[0])
		if err != nil {
			return nil, err
		}

		return exec.Bin(a, exec.Int(int64(width))), nil
	case name == "hour" && len(c.Args) == 1 && c.Args[0].Type().Kind == ir.KindTimestamp:
		a, err := t.expr(c.Args[0])
		if err != nil {
			return nil, err
		}

		return exec.HourOfDay(a), nil
	case name == "timestampdiff" && len(c.Args) == 3 &&
		c.Args[1].Type().Kind == ir.KindTimestamp && c.Args[2].Type().Kind == ir.KindTimestamp:
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

// compared translates a comparison that is a condition on rows. A field of
// the table compared with a number also passes where it holds the number as
// text, which the IR reads as the number; the executor compares a string
// with a number as nothing.
func (t *translator) compared(e *ir.Binary) (*exec.Expr, error) {
	p, err := t.expr(e)
	if err != nil || t.agg != nil || len(p.Args) != 2 {
		return p, err
	}
	col, lit := p.Args[0], p.Args[1]
	if col.Op == exec.OpLiteral {
		col, lit = lit, col
	}
	if col.Op != exec.OpColumn || col.Kind != exec.KindAny || lit.Op != exec.OpLiteral {
		return p, nil
	}
	number := lit
	switch lit.Value.Kind {
	case vec.KindInt:
		number = exec.Float(float64(lit.Value.Int))
	case vec.KindFloat:
	default:
		return p, nil
	}
	text := &exec.Expr{Op: p.Op, Args: []*exec.Expr{exec.ToFloat(exec.Col(col.Path, vec.KindString)), number}}
	if p.Args[0].Op == exec.OpLiteral {
		text.Args[0], text.Args[1] = text.Args[1], text.Args[0]
	}

	return exec.Or(p, text), nil
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
			return t.compared(&ir.Binary{Op: op, L: e.L, R: e.R, T: e.T})
		}
		if _, ok := opposite[e.Op]; ok {
			return t.compared(e)
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
