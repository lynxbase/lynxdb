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
	// having filters the rows of the aggregate, with havingIDs comparisons.
	having    *hpred
	havingIDs int
	// post computes the final columns from an aggregate's; nil keeps them.
	post  []*pexpr
	names []string
	order []orderSpec
	limit int
	// depth bounds the nesting of the filter and of scalar expressions, and
	// slots counts the computed nodes of the scalar expressions.
	depth int
	slots int
	// located holds, per column of the aggregate, the column whose value in
	// a row that the aggregate picked it stands for, or nil; nil when none
	// does. The values are read for the rows of the result alone, which
	// final names by output column, unless a filter, a function or the order
	// above the aggregate reads one: early is then set, and they are read
	// for every group before those run.
	located []*sexpr
	final   []*sexpr
	early   bool
	// places is set when a plan without an aggregate records where its rows
	// are. rest is set when its last column is those places, which the
	// result replaces by the rows' fields other than hidden.
	places bool
	rest   bool
	hidden []string
}

type orderSpec struct {
	col        int
	desc       bool
	nullsFirst bool
}

// sexpr is a scalar expression over the columns of a block.
type sexpr struct {
	op   Op
	path int
	kind vec.Kind
	lit  Value
	pat  tokenfilter.Pattern
	args []*sexpr
	// kinds is the set of kinds the expression can give, a bit per kind.
	kinds uint8
	// ints is set for a tree of integer columns, integer literals and
	// integer functions, which evalInt computes at depth.
	ints  bool
	depth int
	// slot numbers the buffers of a computed node or a literal.
	slot int
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
	flt, _ := n.(*Filter)
	if flt != nil {
		n = flt.Input
	}
	agg, _ := n.(*Aggregate)
	if agg != nil {
		n = agg.Input
	}
	if flt != nil && agg == nil {
		return nil, unsupported("a filter node goes above an aggregate; a scan carries its own")
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
		q.names, q.final = q.agg.names, q.located
		q.early = flt != nil && ((q.located != nil && reads(flt.Cond, q.names, q.located)) || uses(flt.Cond, OpTotal))
		if flt != nil {
			if err := q.filterAbove(flt); err != nil {
				return nil, err
			}
		}
		if prj != nil {
			if err := q.project(prj); err != nil {
				return nil, err
			}
		}
	case prj != nil:
		if len(prj.Exprs) == 0 && !prj.Rest {
			return nil, unsupported("a projection of no columns")
		}
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
			q.early = q.early || (q.final != nil && q.final[col] != nil)
			q.order = append(q.order, orderSpec{col: col, desc: o.Desc, nullsFirst: o.NullsFirst})
		}
	}
	if agg == nil {
		q.lateRows(prj)
	}

	return q, nil
}

// placeName names the column of places, which no result shows.
const placeName = "\x00place"

// lateRows leaves what a plan of rows need not read for every row that
// passes the filter to be read for the rows of the result alone: under a
// sort with a limit, the columns the sort does not read, and every field
// that the projection does not name.
func (q *query) lateRows(prj *Project) {
	if q.limit > 0 && len(q.order) > 0 {
		for i, e := range q.rowExprs {
			sorts := false
			for _, o := range q.order {
				sorts = sorts || o.col == i
			}
			if e.op != OpColumn || sorts {
				continue
			}
			if q.final == nil {
				q.final = make([]*sexpr, len(q.rowExprs))
			}
			q.final[i], q.rowExprs[i], q.places = e, &sexpr{op: opPlace}, true
		}
	}
	if !prj.Rest {
		return
	}
	q.hidden = append(append(q.hidden, q.names...), prj.Except...)
	q.rowExprs, q.names = append(q.rowExprs, &sexpr{op: opPlace}), append(q.names, placeName)
	q.places, q.rest = true, true
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
		s := &sexpr{op: OpColumn, path: q.path(e.Path), kind: e.Kind, kinds: kindBit(e.Kind), ints: e.Kind == vec.KindInt, depth: depth}
		if e.Kind == KindAny {
			s.kinds = allKinds
		}

		return s, nil
	case OpLiteral:
		s := &sexpr{op: OpLiteral, lit: e.Value, ints: e.Value.Kind == vec.KindInt, depth: depth, slot: q.slots}
		if !e.Value.IsNull() {
			s.kinds = kindBit(e.Value.Kind)
		}
		q.slots++

		return s, nil
	}
	least, most := arity(e.Op)
	if least == 0 {
		return nil, unsupported("expression is not a value")
	}
	if len(e.Args) < least || (most >= 0 && len(e.Args) > most) {
		return nil, unsupported("wrong number of arguments")
	}
	s := &sexpr{op: e.Op, pat: e.Pattern, depth: depth, slot: q.slots}
	q.slots++
	switch e.Op {
	case OpHourOfDay, OpAdd, OpSub, OpMul, OpDiv, OpMod:
		s.ints = true
	}
	for i, a := range e.Args {
		arg, err := q.scanExpr(a, depth+1+i)
		if err != nil {
			return nil, err
		}
		s.ints = s.ints && arg.ints
		switch e.Op {
		case OpExists, OpIsNull, OpIsMissing, OpTypeOf:
			// A reference to every kind sees nulls apart from missing values.
			if arg.op == OpColumn && arg.kind == KindAny {
				arg.kinds |= nullBit
			}
		}
		s.args = append(s.args, arg)
	}
	s.kinds = resultKinds(e.Op, s.args)
	if s.ints {
		s.kinds = kindBit(vec.KindInt)
	}

	return s, nil
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
	// predExpr is true where a scalar expression gives true.
	predExpr
	// predHas is true where a path holds a value of one of the kinds of a
	// set, or where it holds none of them when ne is set.
	predHas
	// predAny is true where any string column of the block matches a
	// pattern.
	predAny
)

// pred is a filter node bound to the table's paths.
type pred struct {
	kind predKind
	op   Op
	path int
	lit  Value
	pat  tokenfilter.Pattern
	// tokens holds the hashes of the tokens that a string matching the
	// pattern of a predAny has.
	tokens []uint64
	args   []*pred
	// onInt and onFloat say which numeric columns a predNum reads; lo, hi
	// and ne state the comparison for integers, and none that no integer
	// satisfies it.
	onInt, onFloat bool
	lo, hi         int64
	ne, none       bool
	f              float64
	left, right    *sexpr
	// kinds is the set of kinds a predHas looks for.
	kinds uint8
	// cost orders the arguments of an And, cheapest first.
	cost int
}

// presence returns the filter node for Exists, IsNull or IsMissing of a
// column, which block statistics can settle.
func (q *query) presence(e *Expr) *pred {
	col := e.Args[0]
	if len(e.Args) != 1 || col == nil || col.Op != OpColumn || col.Kind > vec.KindVariant {
		return nil
	}
	p := &pred{kind: predHas, path: q.path(col.Path), kinds: kindBit(col.Kind), ne: e.Op == OpIsMissing, cost: 1}
	switch {
	case col.Kind == KindAny && e.Op == OpIsNull:
		p.kinds = nullBit
	case col.Kind == KindAny:
		p.kinds = allKinds | nullBit
	case e.Op == OpIsNull:
		return &pred{kind: predConst}
	}

	return p
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
		if len(e.Args) != 1 || e.Args[0] == nil {
			return nil, unsupported("match takes one argument")
		}
		if e.Args[0].Op != OpColumn {
			return q.predExpr(e, depth)
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
	case OpMatchAny:
		return &pred{kind: predAny, pat: e.Pattern, tokens: e.Pattern.Tokens(nil), cost: 64}, nil
	case OpExists, OpIsNull, OpIsMissing:
		if len(e.Args) == 1 {
			if p := q.presence(e); p != nil {
				return p, nil
			}
		}
	}
	if least, _ := arity(e.Op); least == 0 {
		return nil, unsupported("expression is not a filter")
	}

	return q.predExpr(e, depth)
}

// predExpr returns the filter node that is true where the scalar expression
// e gives true.
func (q *query) predExpr(e *Expr, depth int) (*pred, error) {
	s, err := q.scanExpr(e, depth+1)
	if err != nil {
		return nil, err
	}
	if s.kinds&kindBit(vec.KindBool) == 0 {
		return &pred{kind: predConst}, nil
	}

	return &pred{kind: predExpr, left: s, cost: 12}, nil
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
		if !left.ints || !right.ints {
			return q.predExpr(pair(op, a, b), depth)
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

// conjunctTokens appends the tokens that the block of a row satisfying the
// conjunct c holds in one column: those of a text match or of a string
// equality, in the column they read, or in any for a match of every column.
func conjunctTokens(c *Expr, dst []colstore.ColumnTokens) []colstore.ColumnTokens {
	var set colstore.ColumnTokens
	switch c.Op {
	case OpMatchAny:
		set.Hashes = c.Pattern.Tokens(nil)
	case OpMatch:
		if len(c.Args) == 1 && c.Args[0] != nil && c.Args[0].Op == OpColumn &&
			(c.Args[0].Kind == vec.KindString || c.Args[0].Kind == KindAny) {
			set.Path, set.Hashes = c.Args[0].Path, c.Pattern.Tokens(nil)
		}
	case OpEq:
		col, lit, op, ok := columnLiteral(c)
		if ok && op == OpEq && lit.Kind == vec.KindString && (col.Kind == vec.KindString || col.Kind == KindAny) {
			set.Path, set.Hashes = col.Path, tokenfilter.AppendHashesLongestFirst(nil, lit.Bytes)
		}
	}
	if len(set.Hashes) == 0 {
		return dst
	}

	return append(dst, set)
}

// pushdown derives what the scan can skip from the conjuncts of the filter:
// string equalities on the leading sort key paths of the snapshot's ordered
// parts become a key filter, string equalities and text matches contribute
// the tokens a column of a block must hold, and comparisons of the row time
// bound it.
func pushdown(snap *colstore.Snapshot, filter *Expr) colstore.ScanOptions {
	var opts colstore.ScanOptions
	minT, maxT := int64(math.MinInt64), int64(math.MaxInt64)
	equal := map[string][]byte{}
	for _, c := range conjuncts(filter, nil) {
		opts.ColumnTokens = conjunctTokens(c, opts.ColumnTokens)
		switch c.Op {
		case OpEq, OpLt, OpLe, OpGt, OpGe:
			col, lit, op, ok := columnLiteral(c)
			if !ok {
				continue
			}
			switch {
			case lit.Kind == vec.KindString && op == OpEq && (col.Kind == vec.KindString || col.Kind == KindAny):
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
