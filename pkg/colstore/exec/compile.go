package exec

import (
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// ErrUnsupported reports a plan the executor cannot run.
var ErrUnsupported = errors.New("exec: unsupported plan")

func unsupported(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrUnsupported, fmt.Sprintf(format, args...))
}

// kindSlots is the number of column slots a path takes, one per kind.
const kindSlots = 6

// query is a plan bound to the snapshot it runs over.
type query struct {
	paths   []string
	pathIdx map[string]int
	filter  *pred
	opts    colstore.ScanOptions
	agg     *aggPlan
	// ordered is set when the aggregation can run in key order.
	ordered *orderedPlan
	// rowExprs are the columns of a plan without an aggregate.
	rowExprs []*sexpr
	// post computes the final columns from an aggregate's; nil keeps them.
	post  []*pexpr
	names []string
	order []orderSpec
	limit int
	// depth bounds the nesting of the filter and of scalar expressions.
	depth int
}

type orderSpec struct {
	col  int
	desc bool
}

// sexpr is a scalar expression over the columns of a block.
type sexpr struct {
	op   Op
	path int
	kind vec.Kind
	lit  Value
	args []*sexpr
}

func compile(snap *colstore.Snapshot, plan Node) (*query, error) {
	q := &query{pathIdx: map[string]int{}}
	n := plan
	srt, _ := n.(*Sort)
	if srt != nil {
		n = srt.Input
	}
	prj, _ := n.(*Project)
	if prj != nil {
		n = prj.Input
	}
	agg, _ := n.(*Aggregate)
	if agg != nil {
		n = agg.Input
	}
	scan, ok := n.(*Scan)
	if !ok || scan == nil {
		return nil, unsupported("a plan is a scan, then an optional aggregate, projection and sort")
	}
	if scan.Filter != nil {
		p, err := q.pred(scan.Filter, 0)
		if err != nil {
			return nil, err
		}
		q.filter = p
		q.opts = pushdown(snap, scan.Filter)
	}

	switch {
	case agg != nil:
		if err := q.aggregate(agg); err != nil {
			return nil, err
		}
		q.ordered = q.planOrdered(snap, scan.Filter)
		q.names = q.agg.names
		if prj != nil {
			if err := q.project(prj); err != nil {
				return nil, err
			}
		}
	case prj != nil:
		for _, e := range prj.Exprs {
			s, err := q.scanExpr(e.Expr, 0)
			if err != nil {
				return nil, err
			}
			q.rowExprs = append(q.rowExprs, s)
			q.names = append(q.names, e.Name)
		}
	default:
		return nil, unsupported("a plan without an aggregate needs a projection")
	}

	if srt != nil {
		if srt.Limit < 0 {
			return nil, unsupported("negative limit")
		}
		q.limit = srt.Limit
		for _, o := range srt.By {
			col := -1
			for i, name := range q.names {
				if name == o.Column {
					col = i
				}
			}
			if col < 0 {
				return nil, unsupported("sort by unknown column %q", o.Column)
			}
			q.order = append(q.order, orderSpec{col: col, desc: o.Desc})
		}
	}

	return q, nil
}

func (q *query) path(name string) int {
	if i, ok := q.pathIdx[name]; ok {
		return i
	}
	i := len(q.paths)
	q.paths = append(q.paths, name)
	q.pathIdx[name] = i

	return i
}

func (q *query) deeper(depth int) {
	if depth+1 > q.depth {
		q.depth = depth + 1
	}
}

// scanExpr binds a value expression to the table's paths.
func (q *query) scanExpr(e *Expr, depth int) (*sexpr, error) {
	if e == nil {
		return nil, unsupported("missing expression")
	}
	q.deeper(depth)
	switch e.Op {
	case OpColumn:
		if e.Kind > vec.KindVariant {
			return nil, unsupported("column %q of unknown kind", e.Path)
		}

		return &sexpr{op: OpColumn, path: q.path(e.Path), kind: e.Kind}, nil
	case OpLiteral:
		return &sexpr{op: OpLiteral, lit: e.Value}, nil
	case OpHourOfDay, OpAdd, OpSub, OpMul, OpDiv:
		want := 2
		if e.Op == OpHourOfDay {
			want = 1
		}
		if len(e.Args) != want {
			return nil, unsupported("wrong number of arguments")
		}
		s := &sexpr{op: e.Op}
		for i, a := range e.Args {
			arg, err := q.scanExpr(a, depth+1+i)
			if err != nil {
				return nil, err
			}
			if !arg.integer() {
				return nil, unsupported("scalar functions take integers")
			}
			s.args = append(s.args, arg)
		}

		return s, nil
	}

	return nil, unsupported("expression is not a value")
}

// integer reports whether e can be read as integers: a computed value, an
// integer literal, or a column, of which only the integers are read.
func (e *sexpr) integer() bool {
	switch e.op {
	case OpColumn:
		return e.kind == vec.KindInt || e.kind == KindAny
	case OpLiteral:
		return e.lit.Kind == vec.KindInt
	}

	return true
}

type predKind uint8

const (
	predConst predKind = iota
	predAnd
	predOr
	predNot
	// predStrEq compares a string column with a literal for equality.
	predStrEq
	// predStr applies any other string comparison or a text match.
	predStr
	predNum
	predBool
	// predInts compares two integer expressions row by row.
	predInts
)

// pred is a filter node bound to the table's paths.
type pred struct {
	kind predKind
	op   Op
	path int
	lit  Value
	pat  tokenfilter.Pattern
	args []*pred
	// onInt and onFloat say which numeric columns a predNum reads; lo, hi
	// and ne state the comparison for integers, and none that no integer
	// satisfies it.
	onInt, onFloat bool
	lo, hi         int64
	ne, none       bool
	f              float64
	left, right    *sexpr
	// cost orders the arguments of an And, cheapest first.
	cost int
}

func (q *query) pred(e *Expr, depth int) (*pred, error) {
	if e == nil {
		return nil, unsupported("missing filter expression")
	}
	q.deeper(depth)
	switch e.Op {
	case OpAnd, OpOr:
		kind := predAnd
		if e.Op == OpOr {
			kind = predOr
		}
		p := &pred{kind: kind}
		for _, a := range e.Args {
			arg, err := q.pred(a, depth+1)
			if err != nil {
				return nil, err
			}
			p.args = append(p.args, arg)
			p.cost += arg.cost
		}
		if kind == predAnd {
			p.args = mergeRanges(p.args)
			sort.SliceStable(p.args, func(i, j int) bool { return p.args[i].cost < p.args[j].cost })
		}

		return p, nil
	case OpNot:
		if len(e.Args) != 1 {
			return nil, unsupported("not takes one argument")
		}
		arg, err := q.pred(e.Args[0], depth)
		if err != nil {
			return nil, err
		}

		return &pred{kind: predNot, args: []*pred{arg}, cost: arg.cost}, nil
	case OpMatch:
		if len(e.Args) != 1 || e.Args[0] == nil || e.Args[0].Op != OpColumn {
			return nil, unsupported("match takes a column")
		}
		col := e.Args[0]
		if col.Kind != vec.KindString && col.Kind != KindAny {
			return &pred{kind: predConst}, nil
		}

		return &pred{kind: predStr, op: OpMatch, path: q.path(col.Path), pat: e.Pattern, cost: 16}, nil
	case OpEq, OpNe, OpLt, OpLe, OpGt, OpGe:
		if len(e.Args) != 2 || e.Args[0] == nil || e.Args[1] == nil {
			return nil, unsupported("a comparison takes two arguments")
		}

		return q.comparison(e.Op, e.Args[0], e.Args[1], depth)
	case OpColumn:
		if e.Kind != vec.KindBool && e.Kind != KindAny {
			return &pred{kind: predConst}, nil
		}

		return &pred{kind: predBool, op: OpEq, path: q.path(e.Path), lit: Value{Kind: vec.KindBool, Bool: true}, cost: 2}, nil
	case OpLiteral:
		if e.Value.Kind != vec.KindBool {
			return nil, unsupported("a filter literal must be a boolean")
		}

		return &pred{kind: predConst, lit: e.Value}, nil
	}

	return nil, unsupported("expression is not a filter")
}

// mergeRanges folds the integer range comparisons of one column among the
// arguments of an And into one, so that a block is read once for them.
func mergeRanges(args []*pred) []*pred {
	out := args[:0]
	for _, a := range args {
		ranged := a.kind == predNum && a.onInt && !a.onFloat && !a.ne
		merged := false
		for _, b := range out {
			if !ranged || b.kind != predNum || b.path != a.path || !b.onInt || b.onFloat || b.ne {
				continue
			}
			b.lo, b.hi, b.none = max(b.lo, a.lo), min(b.hi, a.hi), b.none || a.none
			b.none = b.none || b.lo > b.hi
			merged = true

			break
		}
		if !merged {
			out = append(out, a)
		}
	}

	return out
}

// flip returns the comparison that holds for (b, a) when op holds for (a, b).
func flip(op Op) Op {
	switch op {
	case OpLt:
		return OpGt
	case OpLe:
		return OpGe
	case OpGt:
		return OpLt
	case OpGe:
		return OpLe
	}

	return op
}

func (q *query) comparison(op Op, a, b *Expr, depth int) (*pred, error) {
	if a.Op == OpLiteral && b.Op == OpColumn {
		a, b, op = b, a, flip(op)
	}
	if a.Op != OpColumn || b.Op != OpLiteral {
		left, err := q.scanExpr(a, depth+1)
		if err != nil {
			return nil, err
		}
		right, err := q.scanExpr(b, depth+2)
		if err != nil {
			return nil, err
		}
		if !left.integer() || !right.integer() {
			return nil, unsupported("a comparison of two expressions takes integers")
		}

		return &pred{kind: predInts, op: op, left: left, right: right, cost: 8}, nil
	}
	any := a.Kind == KindAny
	switch b.Value.Kind {
	case vec.KindString:
		if a.Kind != vec.KindString && !any {
			return &pred{kind: predConst}, nil
		}
		p := &pred{kind: predStr, op: op, path: q.path(a.Path), lit: b.Value, cost: 4}
		if op == OpEq {
			p.kind, p.cost = predStrEq, 1
		}

		return p, nil
	case vec.KindInt, vec.KindFloat:
		p := &pred{kind: predNum, op: op, path: q.path(a.Path), cost: 3}
		p.onInt = a.Kind == vec.KindInt || any
		p.onFloat = a.Kind == vec.KindFloat || any
		if !p.onInt && !p.onFloat {
			return &pred{kind: predConst}, nil
		}
		if b.Value.Kind == vec.KindInt {
			p.f = float64(b.Value.Int)
			p.intRange(op, b.Value.Int)
		} else {
			p.f = b.Value.Float
			p.floatRange(op, b.Value.Float)
		}

		return p, nil
	case vec.KindBool:
		if op != OpEq && op != OpNe {
			return nil, unsupported("booleans compare for equality only")
		}
		if a.Kind != vec.KindBool && !any {
			return &pred{kind: predConst}, nil
		}

		return &pred{kind: predBool, op: op, path: q.path(a.Path), lit: b.Value, cost: 2}, nil
	}

	return nil, unsupported("comparison with a literal of kind %s", b.Value.Kind)
}

// intRange states x op v as lo <= x <= hi, negated when ne is set.
func (p *pred) intRange(op Op, v int64) {
	p.lo, p.hi = math.MinInt64, math.MaxInt64
	switch op {
	case OpEq:
		p.lo, p.hi = v, v
	case OpNe:
		p.lo, p.hi, p.ne = v, v, true
	case OpLt:
		if v == math.MinInt64 {
			p.none = true
		}
		p.hi = v - 1
	case OpLe:
		p.hi = v
	case OpGt:
		if v == math.MaxInt64 {
			p.none = true
		}
		p.lo = v + 1
	case OpGe:
		p.lo = v
	}
}

// floatRange states x op f for an integer x as an integer range.
func (p *pred) floatRange(op Op, f float64) {
	p.lo, p.hi = math.MinInt64, math.MaxInt64
	if math.IsNaN(f) {
		p.none = op != OpNe

		return
	}
	// Floats at or beyond these bounds are outside the integers.
	const limit = 9223372036854775808.0
	below, above := f < -limit, f >= limit
	whole := f == math.Floor(f) && !below && !above
	switch op {
	case OpEq:
		if !whole {
			p.none = true

			return
		}
		p.lo, p.hi = int64(f), int64(f)
	case OpNe:
		if whole {
			p.lo, p.hi, p.ne = int64(f), int64(f), true
		}
	case OpLt, OpLe:
		switch {
		case below:
			p.none = true
		case above:
		case whole && op == OpLt:
			p.intRange(OpLt, int64(f))
		default:
			p.hi = int64(math.Floor(f))
		}
	case OpGt, OpGe:
		switch {
		case above:
			p.none = true
		case below:
		case whole && op == OpGt:
			p.intRange(OpGt, int64(f))
		default:
			p.lo = int64(math.Ceil(f))
		}
	}
}

// conjuncts appends the arguments of the top-level And of e, which every
// matching row satisfies.
func conjuncts(e *Expr, out []*Expr) []*Expr {
	if e == nil {
		return out
	}
	if e.Op != OpAnd {
		return append(out, e)
	}
	for _, a := range e.Args {
		out = conjuncts(a, out)
	}

	return out
}

// columnLiteral splits a comparison of a column with a literal, in either
// order, into its parts.
func columnLiteral(e *Expr) (col *Expr, lit Value, op Op, ok bool) {
	if len(e.Args) != 2 || e.Args[0] == nil || e.Args[1] == nil {
		return nil, Value{}, 0, false
	}
	a, b, op := e.Args[0], e.Args[1], e.Op
	if a.Op == OpLiteral && b.Op == OpColumn {
		a, b, op = b, a, flip(op)
	}
	if a.Op != OpColumn || b.Op != OpLiteral {
		return nil, Value{}, 0, false
	}

	return a, b.Value, op, true
}

// pushdown derives what the scan can skip from the conjuncts of the filter:
// string equalities on the leading sort key paths of the snapshot's ordered
// parts become a key filter, string equalities and text matches contribute
// the tokens a block must hold, and comparisons of the row time bound it.
func pushdown(snap *colstore.Snapshot, filter *Expr) colstore.ScanOptions {
	var opts colstore.ScanOptions
	minT, maxT := int64(math.MinInt64), int64(math.MaxInt64)
	equal := map[string][]byte{}
	for _, c := range conjuncts(filter, nil) {
		switch c.Op {
		case OpMatch:
			if len(c.Args) == 1 && c.Args[0] != nil && c.Args[0].Op == OpColumn &&
				(c.Args[0].Kind == vec.KindString || c.Args[0].Kind == KindAny) {
				opts.Tokens = c.Pattern.Tokens(opts.Tokens)
			}
		case OpEq, OpLt, OpLe, OpGt, OpGe:
			col, lit, op, ok := columnLiteral(c)
			if !ok {
				continue
			}
			switch {
			case lit.Kind == vec.KindString && op == OpEq && (col.Kind == vec.KindString || col.Kind == KindAny):
				opts.Tokens = tokenfilter.AppendHashes(opts.Tokens, lit.Bytes)
				if _, dup := equal[col.Path]; !dup {
					equal[col.Path] = lit.Bytes
				}
			case lit.Kind == vec.KindInt && col.Path == part.TimePath && (col.Kind == vec.KindInt || col.Kind == KindAny):
				var p pred
				p.intRange(op, lit.Int)
				if p.none {
					p.lo, p.hi = math.MaxInt64, math.MinInt64
				}
				minT, maxT = max(minT, p.lo), min(maxT, p.hi)
			}
		}
	}
	if minT != math.MinInt64 || maxT != math.MaxInt64 {
		opts.MinTime, opts.MaxTime = minT, maxT
	}
	if len(equal) == 0 || snap == nil {
		return opts
	}
	// The longest sort key prefix that the equalities cover prunes the most.
	for _, h := range snap.Parts() {
		if !h.Ordered() {
			continue
		}
		key := h.SortKey()
		n := 0
		for n < len(key) {
			if _, ok := equal[key[n]]; !ok {
				break
			}
			n++
		}
		if n <= len(opts.Key.Paths) {
			continue
		}
		opts.Key = colstore.KeyFilter{Paths: key[:n]}
		for _, path := range key[:n] {
			opts.Key.Values = append(opts.Key.Values, equal[path])
		}
	}

	return opts
}
