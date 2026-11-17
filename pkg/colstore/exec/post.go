package exec

import (
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// pexpr is a scalar expression over the columns of an aggregate.
type pexpr struct {
	op   Op
	col  int
	lit  Value
	args []*pexpr
	// slot numbers the buffer that holds a computed node's values.
	slot int
	pat  tokenfilter.Pattern
	// byValue is set for a function computed row by row from values.
	byValue bool
	// rows is set for the total of a count of rows that no filter thins out:
	// the number of rows aggregated, known before the groups are merged.
	rows bool
	// newFunc makes the function of an OpFunc node.
	newFunc func() Func
}

func (q *query) project(p *Project) error {
	in := q.names
	var names []string
	slots := 0
	for _, e := range p.Exprs {
		pe, err := postExpr(e.Expr, in, &slots)
		if err != nil {
			return err
		}
		q.post = append(q.post, pe)
		names = append(names, e.Name)
	}
	if len(q.post) == 0 {
		return unsupported("a projection without columns")
	}
	for _, pe := range q.post {
		q.totals(pe)
	}
	q.fields = make([]bool, len(q.post))
	if q.located != nil {
		q.final = make([]*sexpr, len(q.post))
		for i, pe := range q.post {
			if pe.op == OpColumn {
				q.final[i] = q.located[pe.col]
				q.fields[i] = q.final[i] != nil
			} else if pe.locates(q.located) {
				q.early = true
			}
		}
	}
	q.names = names
	if !p.Rest {
		return nil
	}
	last := len(q.post) - 1
	pick := q.post[last]
	if q.located == nil || pick.op != OpColumn || q.located[pick.col] == nil {
		return unsupported("the fields of a row above an aggregate follow a column that picks the row")
	}
	// The place of the row stays what the column holds.
	q.located[pick.col], q.final[last] = nil, nil
	q.rest = true
	q.hidden = append(append(q.hidden, names[:last]...), p.Except...)

	return nil
}

// totals decides how the totals in e become known: the total of a count of
// rows from the scan, any other once every group is merged.
func (q *query) totals(e *pexpr) {
	for _, a := range e.args {
		q.totals(a)
	}
	if e.op != OpTotal {
		return
	}
	if out := e.col - len(q.agg.keys); q.having == nil && out >= 0 && q.agg.outs[out].fn == Count && q.agg.outs[out].rows >= 0 {
		// Rows are counted as they are scanned, not in key order.
		e.rows, q.ordered = true, nil

		return
	}
	q.early = true
}

// total makes dst n rows of the sum of the numbers in col: an integer when
// all of them are, a null when there is none.
func total(dst, col *colVec, n int) {
	var ints int64
	var floats float64
	isFloat, any := false, false
	for i := 0; i < n; i++ {
		switch v := col.value(i); v.Kind {
		case vec.KindInt:
			ints, any = ints+v.Int, true
		case vec.KindFloat:
			floats, isFloat, any = floats+v.Float, true, true
		}
	}
	switch {
	case !any:
		dst.reset()
		for i := 0; i < n; i++ {
			dst.appendNull()
		}
	case isFloat:
		out := dst.bulkFloats(n)
		for i := range out {
			out[i] = floats + float64(ints)
		}
	default:
		out := dst.bulkInts(n)
		for i := range out {
			out[i] = ints
		}
	}
}

// locates reports whether the expression reads a column that locates a row.
func (e *pexpr) locates(located []*sexpr) bool {
	for _, a := range e.args {
		if a.locates(located) {
			return true
		}
	}

	return e.op == OpColumn && located[e.col] != nil
}

func postExpr(e *Expr, in []string, slots *int) (*pexpr, error) {
	if e == nil {
		return nil, unsupported("missing expression")
	}
	switch e.Op {
	case OpColumn:
		for i, name := range in {
			if name == e.Path {
				return &pexpr{op: OpColumn, col: i}, nil
			}
		}

		return nil, unsupported("unknown column %q", e.Path)
	case OpLiteral:
		*slots++

		return &pexpr{op: OpLiteral, lit: e.Value, slot: *slots - 1}, nil
	case OpTotal:
		if len(e.Args) != 1 || e.Args[0] == nil || e.Args[0].Op != OpColumn {
			return nil, unsupported("a total takes a column of the aggregate")
		}
		of, err := postExpr(e.Args[0], in, slots)
		if err != nil {
			return nil, err
		}
		*slots++

		return &pexpr{op: OpTotal, col: of.col, slot: *slots - 1}, nil
	case OpHourOfDay, OpAdd, OpSub, OpMul, OpDiv:
		want := 2
		if e.Op == OpHourOfDay {
			want = 1
		}
		if len(e.Args) != want {
			return nil, unsupported("wrong number of arguments")
		}
		*slots++
		pe := &pexpr{op: e.Op, slot: *slots - 1}
		for _, a := range e.Args {
			arg, err := postExpr(a, in, slots)
			if err != nil {
				return nil, err
			}
			pe.args = append(pe.args, arg)
		}

		return pe, nil
	}

	least, most := arity(e.Op)
	if e.Op == OpFunc && e.NewFunc != nil && len(e.Args) != 0 {
		least, most = 1, -1
	}
	if least == 0 || len(e.Args) < least || (most >= 0 && len(e.Args) > most) {
		return nil, unsupported("expression over an aggregate")
	}
	*slots++
	pe := &pexpr{op: e.Op, pat: e.Pattern, slot: *slots - 1, byValue: true, newFunc: e.NewFunc}
	for _, a := range e.Args {
		arg, err := postExpr(a, in, slots)
		if err != nil {
			return nil, err
		}
		pe.args = append(pe.args, arg)
	}

	return pe, nil
}

// postEval computes a projection over batches of aggregate rows.
type postEval struct {
	// fns holds the functions of the OpFunc nodes this evaluator computed.
	fns map[*pexpr]Func
	// rows is the number of rows the query aggregated.
	rows int64
	tmp  []colVec
	out  []colVec
	// args, vals and text serve functions computed from values.
	args []*colVec
	vals []Value
	text []byte
}

func (p *postEval) held() int {
	n := 0
	for i := range p.tmp {
		n += p.tmp[i].held()
	}

	return n
}

// run returns the projected columns of the n rows of in; they alias in and
// the evaluator's buffers until the next run.
func (p *postEval) run(q *query, in []colVec, n int) []colVec {
	p.out = fit(p.out, len(q.post))
	for i, e := range q.post {
		p.out[i] = *p.eval(e, in, n)
	}

	return p.out
}

func (p *postEval) buf(slot int) *colVec {
	if slot >= len(p.tmp) {
		p.tmp = fit(p.tmp, slot+1)
	}

	return &p.tmp[slot]
}

func (p *postEval) eval(e *pexpr, in []colVec, n int) *colVec {
	switch e.op {
	case OpTotal:
		out := p.buf(e.slot)
		if e.rows {
			all := out.bulkInts(n)
			for i := range all {
				all[i] = p.rows
			}

			return out
		}
		total(out, &in[e.col], n)

		return out
	case OpColumn:
		return &in[e.col]
	case OpLiteral:
		out := p.buf(e.slot)
		out.reset()
		for i := 0; i < n; i++ {
			out.appendValue(e.lit)
		}

		return out
	case OpHourOfDay:
		a := p.eval(e.args[0], in, n)
		out := p.buf(e.slot)
		if a.kind == vec.KindInt {
			vals := out.bulkInts(n)
			for i, t := range a.ints[:n] {
				vals[i] = hourOfDay(t)
			}

			return out
		}
		out.reset()
		for i := 0; i < n; i++ {
			if v := a.value(i); v.Kind == vec.KindInt {
				out.appendInt(vec.KindInt, hourOfDay(v.Int))
			} else {
				out.appendNull()
			}
		}

		return out
	}
	if e.byValue {
		return p.byValue(e, in, n)
	}
	a := p.eval(e.args[0], in, n)
	out := p.buf(e.slot)
	if lit := e.args[1]; lit.op == OpLiteral && lit.lit.Kind == vec.KindInt && a.kind == vec.KindInt && (e.op != OpDiv || lit.lit.Int != 0) {
		vals, x, y := out.bulkInts(n), a.ints[:n], lit.lit.Int
		switch {
		case e.op == OpAdd:
			for i := range vals {
				vals[i] = x[i] + y
			}
		case e.op == OpSub:
			for i := range vals {
				vals[i] = x[i] - y
			}
		case e.op == OpMul:
			for i := range vals {
				vals[i] = x[i] * y
			}
		case y == -1:
			for i := range vals {
				vals[i] = -x[i]
			}
		default:
			for i := range vals {
				vals[i] = x[i] / y
			}
		}

		return out
	}
	b := p.eval(e.args[1], in, n)
	if a.kind == vec.KindInt && b.kind == vec.KindInt && e.op != OpDiv {
		vals := out.bulkInts(n)
		x, y := a.ints[:n], b.ints[:n]
		switch e.op {
		case OpAdd:
			for i := range vals {
				vals[i] = x[i] + y[i]
			}
		case OpSub:
			for i := range vals {
				vals[i] = x[i] - y[i]
			}
		default:
			for i := range vals {
				vals[i] = x[i] * y[i]
			}
		}

		return out
	}
	out.reset()
	for i := 0; i < n; i++ {
		out.appendValue(arith(e.op, a.value(i), b.value(i)))
	}

	return out
}

// byValue computes a function row by row from the values of its arguments.
func (p *postEval) byValue(e *pexpr, in []colVec, n int) *colVec {
	base := len(p.args)
	for _, a := range e.args {
		p.args = append(p.args, p.eval(a, in, n))
	}
	args := p.args[base:]
	p.vals = fit(p.vals, len(args))
	out := p.buf(e.slot)
	out.reset()
	var fn Func
	if e.op == OpFunc {
		if fn = p.fns[e]; fn == nil {
			if p.fns == nil {
				p.fns = map[*pexpr]Func{}
			}
			fn = e.newFunc()
			p.fns[e] = fn
		}
	}
	for i := 0; i < n; i++ {
		for k, a := range args {
			// A column of an aggregate holds a value or a null.
			if p.vals[k] = a.value(i); p.vals[k].Kind == KindAny {
				p.vals[k].Kind = vec.KindNull
			}
		}
		var v Value
		if fn != nil {
			v, p.text = fn(p.vals, p.text[:0])
		} else {
			v, p.text = scalarValue(e.op, &e.pat, p.vals, p.text[:0])
		}
		out.appendValue(v)
	}
	p.args = p.args[:base]

	return out
}

// arith applies an arithmetic operation to two values: integers stay
// integers, a float makes the result a float, and anything else, like a
// division of integers by zero, gives null.
func arith(op Op, a, b Value) Value {
	if a.Kind == vec.KindInt && b.Kind == vec.KindInt {
		x, y := a.Int, b.Int
		switch op {
		case OpAdd:
			x += y
		case OpSub:
			x -= y
		case OpMul:
			x *= y
		default:
			switch y {
			case 0:
				return Value{}
			case -1:
				x = -x
			default:
				x /= y
			}
		}

		return Value{Kind: vec.KindInt, Int: x}
	}
	x, ok := number(a)
	y, ok2 := number(b)
	if !ok || !ok2 {
		return Value{}
	}
	switch op {
	case OpAdd:
		x += y
	case OpSub:
		x -= y
	case OpMul:
		x *= y
	default:
		if y == 0 {
			return Value{}
		}
		x /= y
	}

	return Value{Kind: vec.KindFloat, Float: x}
}

func number(v Value) (float64, bool) {
	switch v.Kind {
	case vec.KindInt:
		return float64(v.Int), true
	case vec.KindFloat:
		return v.Float, true
	}

	return 0, false
}
