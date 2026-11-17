package ppl

import (
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/ppl/ast"
)

// ectx compiles expressions against one input schema.
type ectx struct {
	c      *compiler
	schema ir.Schema
	params []*ir.LambdaParam
}

func (c *compiler) ectx(s ir.Schema) *ectx { return &ectx{c: c, schema: s} }

func (x *ectx) expr(e ast.Expr) (ir.Expr, error) {
	switch n := e.(type) {
	case *ast.Literal:
		return literal(n)
	case *ast.Interval:
		d, unit, err := x.interval(n)
		if err != nil {
			return nil, err
		}
		if unit != "" {
			return nil, typeErr(n, "an interval of %s cannot be used as a value; use it with date_add or date_sub", unit)
		}
		return d, nil
	case *ast.Field:
		return x.field(n)
	case *ast.AllFields:
		return nil, semanticErr(n, "'*' is only valid in count(*) and field lists")
	case *ast.Unary:
		return x.unary(n)
	case *ast.Binary:
		return x.binary(n)
	case *ast.In:
		v, err := x.expr(n.X)
		if err != nil {
			return nil, err
		}
		list := make([]ir.Expr, len(n.List))
		for i, item := range n.List {
			if list[i], err = x.expr(item); err != nil {
				return nil, err
			}
			if list[i], err = x.coerceForCompare(item, v, list[i]); err != nil {
				return nil, err
			}
		}
		return &ir.InList{X: v, List: list, Not: n.Not}, nil
	case *ast.Between:
		v, err := x.expr(n.X)
		if err != nil {
			return nil, err
		}
		lo, err := x.compareOperand(n.Lo, v)
		if err != nil {
			return nil, err
		}
		hi, err := x.compareOperand(n.Hi, v)
		if err != nil {
			return nil, err
		}
		var b ir.Expr = ir.And(&ir.Binary{Op: ir.OpGe, L: v, R: lo, T: ir.Bool}, &ir.Binary{Op: ir.OpLe, L: v, R: hi, T: ir.Bool})
		if n.Not {
			b = &ir.Unary{Op: ir.OpNot, X: b, T: ir.Bool}
		}
		return b, nil
	case *ast.IsNull:
		v, err := x.expr(n.X)
		if err != nil {
			return nil, err
		}
		var r ir.Expr = callIR("is_null", v)
		if n.Not {
			r = &ir.Unary{Op: ir.OpNot, X: r, T: ir.Bool}
		}
		return r, nil
	case *ast.Call:
		return x.call(n)
	case *ast.Case:
		return x.caseExpr(n)
	case *ast.Cast:
		return x.cast(n)
	case *ast.Lambda:
		return nil, semanticErr(n, "a lambda is only valid as an argument of a collection function")
	case *ast.Subquery:
		return x.subquery(n)
	case *ast.DatePart:
		return x.datePart(n)
	}
	return nil, semanticErr(e, "unsupported expression")
}

func callIR(name string, args ...ir.Expr) *ir.Call {
	f := ir.MustFunc(name)
	types := make([]ir.Type, len(args))
	for i, a := range args {
		types[i] = a.Type()
	}
	t, err := f.Check(types)
	if err != nil {
		t = ir.Any
	}
	return &ir.Call{Func: f, Args: args, T: t}
}

func literal(n *ast.Literal) (ir.Expr, error) {
	switch n.Kind {
	case ast.LitNull:
		return ir.NullLit(), nil
	case ast.LitBool:
		return ir.BoolLit(n.Text == "true"), nil
	case ast.LitInt:
		v, err := strconv.ParseInt(n.Text, 10, 64)
		if err != nil {
			f, ferr := strconv.ParseFloat(n.Text, 64)
			if ferr != nil {
				return nil, typeErr(n, "invalid integer %s", n.Text)
			}
			return ir.FloatLit(f), nil
		}
		return ir.IntLit(v), nil
	case ast.LitDecimal, ast.LitDouble, ast.LitFloat:
		v, err := strconv.ParseFloat(n.Text, 64)
		if err != nil {
			return nil, typeErr(n, "invalid number %s", n.Text)
		}
		return ir.FloatLit(v), nil
	case ast.LitString:
		return ir.StringLit(n.Text), nil
	case ast.LitDate:
		t, ok := parseDate(n.Text)
		if !ok {
			return nil, typeErr(n, "invalid DATE literal %q; use 'yyyy-MM-dd'", n.Text)
		}
		return ir.TimestampLit(t), nil
	case ast.LitTimestamp:
		t, ok := parseTimestamp(n.Text)
		if !ok {
			return nil, typeErr(n, "invalid TIMESTAMP literal %q; use 'yyyy-MM-dd HH:mm:ss[.fraction]'", n.Text)
		}
		return ir.TimestampLit(t), nil
	case ast.LitTime:
		d, ok := parseTimeOfDay(n.Text)
		if !ok {
			return nil, typeErr(n, "invalid TIME literal %q; use 'HH:mm:ss[.fraction]'", n.Text)
		}
		return ir.DurationLit(d), nil
	}
	return nil, typeErr(n, "unsupported literal")
}

var timestampLayouts = []string{
	"2006-01-02 15:04:05.999999999", "2006-01-02T15:04:05.999999999Z07:00", "2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04", "2006-01-02T15:04", "2006-01-02", "01/02/2006:15:04:05",
}

func parseTimestamp(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	for _, l := range timestampLayouts {
		if t, err := time.Parse(l, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

func parseDate(s string) (time.Time, bool) {
	t, err := time.Parse("2006-01-02", strings.TrimSpace(s))
	return t, err == nil
}

func parseTimeOfDay(s string) (time.Duration, bool) {
	s = strings.TrimSpace(s)
	for _, l := range []string{"15:04:05.999999999", "15:04:05", "15:04"} {
		if t, err := time.Parse(l, s); err == nil {
			return time.Duration(t.Hour())*time.Hour + time.Duration(t.Minute())*time.Minute +
				time.Duration(t.Second())*time.Second + time.Duration(t.Nanosecond()), true
		}
	}
	return 0, false
}

var fixedUnits = map[string]time.Duration{
	"microsecond": time.Microsecond, "millisecond": time.Millisecond, "second": time.Second,
	"minute": time.Minute, "hour": time.Hour, "day": 24 * time.Hour, "week": 7 * 24 * time.Hour,
}

// interval compiles INTERVAL n unit: a fixed-length unit gives a duration
// expression and an empty unit; a calendar unit (month, quarter, year) and
// the compound units give the count expression and the unit name.
func (x *ectx) interval(n *ast.Interval) (ir.Expr, string, error) {
	v, err := x.expr(n.Value)
	if err != nil {
		return nil, "", err
	}
	if !ir.Float.Accepts(v.Type()) {
		return nil, "", typeErr(n, "an interval count must be numeric, got %s", v.Type())
	}
	unit := normalizeIntervalUnit(n.Unit)
	if d, ok := fixedUnits[unit]; ok {
		if lit, ok := v.(*ir.Literal); ok {
			switch val := lit.V.(type) {
			case int64:
				return ir.DurationLit(time.Duration(val) * d), "", nil
			case float64:
				return ir.DurationLit(time.Duration(val * float64(d))), "", nil
			}
		}
		return callIR("sec_to_time", &ir.Binary{Op: ir.OpMul, L: v, R: ir.FloatLit(d.Seconds()), T: ir.Float}), "", nil
	}
	return v, unit, nil
}

func normalizeIntervalUnit(u string) string {
	u = strings.ToLower(u)
	switch u {
	case "s", "sec", "secs", "seconds":
		return "second"
	case "m", "min", "mins", "minutes":
		return "minute"
	case "h", "hr", "hrs", "hours":
		return "hour"
	case "d", "days":
		return "day"
	case "w", "weeks":
		return "week"
	case "mon", "months":
		return "month"
	case "q", "qtr", "qtrs", "quarters":
		return "quarter"
	case "y", "yr", "yrs", "years":
		return "year"
	case "ms", "milliseconds":
		return "millisecond"
	case "us", "microseconds":
		return "microsecond"
	}
	return u
}

func (x *ectx) field(n *ast.Field) (ir.Expr, error) {
	name := n.Name()
	for i := len(x.params) - 1; i >= 0; i-- {
		if x.params[i].Name == n.Parts[0] {
			var e ir.Expr = x.params[i]
			for _, part := range n.Parts[1:] {
				e = &ir.Index{X: e, Key: ir.StringLit(part), T: ir.Any}
			}
			return e, nil
		}
	}
	if len(x.c.outer) > 0 && len(n.Parts) > 1 && x.c.outerQualifier(n.Parts[0]) {
		return nil, correlated(n, "%s of the outer query", name)
	}
	return resolve(&ast.Field{Base: n.Base, Parts: []string{name}}, x.schema)
}

// outerQualifier reports whether q names a source or alias of an outer query only.
func (c *compiler) outerQualifier(q string) bool {
	if qualifies(c.st, q) {
		return false
	}
	for _, p := range c.outer {
		if qualifies(p, q) {
			return true
		}
	}
	return false
}

func qualifies(p *pipe, q string) bool {
	if p == nil {
		return false
	}
	if p.aliases[q] {
		return true
	}
	for _, src := range sourceNames(p.origin) {
		if src == q {
			return true
		}
	}
	return false
}

func correlated(n ast.Node, format string, args ...any) *Error {
	return unsupportedFeature(n, "correlated subqueries are not supported: the subquery refers to "+format, args...)
}

func (x *ectx) unary(n *ast.Unary) (ir.Expr, error) {
	v, err := x.expr(n.X)
	if err != nil {
		return nil, err
	}
	if n.Op == "not" {
		if err := requireBool(n.X, v); err != nil {
			return nil, err
		}
		return &ir.Unary{Op: ir.OpNot, X: v, T: ir.Bool}, nil
	}
	switch v.Type().Kind {
	case ir.KindInt, ir.KindFloat, ir.KindDuration, ir.KindAny, ir.KindNull:
	default:
		return nil, typeErr(n, "cannot negate a %s", v.Type())
	}
	if lit, ok := v.(*ir.Literal); ok {
		switch val := lit.V.(type) {
		case int64:
			return ir.IntLit(-val), nil
		case float64:
			return ir.FloatLit(-val), nil
		}
	}
	return &ir.Unary{Op: ir.OpNeg, X: v, T: v.Type()}, nil
}

func requireBool(n ast.Node, e ir.Expr) error {
	switch e.Type().Kind {
	case ir.KindBool, ir.KindAny, ir.KindNull:
		return nil
	}
	return typeErr(n, "expected a boolean expression, got %s", e.Type())
}

var arithOps = map[string]ir.BinaryOp{"+": ir.OpAdd, "-": ir.OpSub, "*": ir.OpMul, "/": ir.OpDiv, "%": ir.OpMod}

var cmpOps = map[string]ir.BinaryOp{"=": ir.OpEq, "==": ir.OpEq, "!=": ir.OpNe, "<": ir.OpLt, "<=": ir.OpLe, ">": ir.OpGt, ">=": ir.OpGe}

func (x *ectx) binary(n *ast.Binary) (ir.Expr, error) {
	switch n.Op {
	case "and", "or", "xor":
		l, err := x.expr(n.L)
		if err != nil {
			return nil, err
		}
		r, err := x.expr(n.R)
		if err != nil {
			return nil, err
		}
		if err := requireBool(n.L, l); err != nil {
			return nil, err
		}
		if err := requireBool(n.R, r); err != nil {
			return nil, err
		}
		op := map[string]ir.BinaryOp{"and": ir.OpAnd, "or": ir.OpOr, "xor": ir.OpXor}[n.Op]
		return &ir.Binary{Op: op, L: l, R: r, T: ir.Bool}, nil
	case "like", "ilike":
		s, err := x.expr(n.L)
		if err != nil {
			return nil, err
		}
		p, err := x.expr(n.R)
		if err != nil {
			return nil, err
		}
		return x.likeCall(n, s, p, false)
	case "regexp":
		return x.callNamed(n, "regexp_match", []ast.Expr{n.L, n.R})
	case "contains":
		s, err := x.expr(n.L)
		if err != nil {
			return nil, err
		}
		sub, err := x.expr(n.R)
		if err != nil {
			return nil, err
		}
		return callIR("contains", callIR("lower", toStringArg(s)), callIR("lower", toStringArg(sub))), nil
	}
	if op, ok := cmpOps[n.Op]; ok {
		l, err := x.expr(n.L)
		if err != nil {
			return nil, err
		}
		r, err := x.expr(n.R)
		if err != nil {
			return nil, err
		}
		if r, err = x.coerceForCompare(n.R, l, r); err != nil {
			return nil, err
		}
		if l, err = x.coerceForCompare(n.L, r, l); err != nil {
			return nil, err
		}
		if err := comparable(n, l.Type(), r.Type()); err != nil {
			return nil, err
		}
		return &ir.Binary{Op: op, L: l, R: r, T: ir.Bool}, nil
	}
	op, ok := arithOps[n.Op]
	if !ok {
		return nil, semanticErr(n, "unknown operator %s", n.Op)
	}
	if iv, isInterval := n.R.(*ast.Interval); isInterval && (op == ir.OpAdd || op == ir.OpSub) {
		l, err := x.expr(n.L)
		if err != nil {
			return nil, err
		}
		return x.addInterval(n, l, iv, op == ir.OpSub)
	}
	l, err := x.expr(n.L)
	if err != nil {
		return nil, err
	}
	r, err := x.expr(n.R)
	if err != nil {
		return nil, err
	}
	t, err := arithType(n, op, l.Type(), r.Type())
	if err != nil {
		return nil, err
	}
	return &ir.Binary{Op: op, L: l, R: r, T: t}, nil
}

func toStringArg(e ir.Expr) ir.Expr {
	switch e.Type().Kind {
	case ir.KindString, ir.KindAny, ir.KindNull:
		return e
	}
	return &ir.Cast{X: e, To: ir.String}
}

// likeCall builds like(s, pattern, case_sensitive); PPL's default is
// case-insensitive matching.
func (x *ectx) likeCall(n ast.Node, s, p ir.Expr, caseSensitive bool) (ir.Expr, error) {
	if _, ok := p.(*ir.Literal); !ok {
		return nil, semanticErr(n, "the LIKE pattern must be a string literal")
	}
	if !ir.String.Accepts(p.Type()) {
		return nil, typeErr(n, "the LIKE pattern must be a string")
	}
	return callIR("like", toStringArg(s), p, ir.BoolLit(caseSensitive)), nil
}

func (x *ectx) addInterval(n ast.Node, l ir.Expr, iv *ast.Interval, negate bool) (ir.Expr, error) {
	v, unit, err := x.interval(iv)
	if err != nil {
		return nil, err
	}
	if unit == "" {
		op := ir.OpAdd
		if negate {
			op = ir.OpSub
		}
		t, err := arithType(n, op, l.Type(), v.Type())
		if err != nil {
			return nil, err
		}
		return &ir.Binary{Op: op, L: l, R: v, T: t}, nil
	}
	count, u, err := calendarUnit(iv, v, unit)
	if err != nil {
		return nil, err
	}
	if negate {
		count = negateExpr(count)
	}
	return callIR("date_add", l, count, ir.StringLit(u)), nil
}

func negateExpr(e ir.Expr) ir.Expr {
	if lit, ok := e.(*ir.Literal); ok {
		if v, ok := lit.V.(int64); ok {
			return ir.IntLit(-v)
		}
	}
	return &ir.Unary{Op: ir.OpNeg, X: e, T: e.Type()}
}

// calendarUnit converts a calendar or compound interval to a count of a
// unit date_add understands.
func calendarUnit(n ast.Node, v ir.Expr, unit string) (ir.Expr, string, error) {
	switch unit {
	case "month", "quarter", "year":
		if v.Type().Kind == ir.KindFloat {
			v = callIR("floor", v)
		}
		return v, unit, nil
	}
	return nil, "", unsupportedFeature(n, "interval unit %s is not supported; use a single unit such as HOUR or DAY", strings.ToUpper(unit))
}

func arithType(n ast.Node, op ir.BinaryOp, l, r ir.Type) (ir.Type, error) {
	lk, rk := l.Kind, r.Kind
	dyn := func(k ir.Kind) bool { return k == ir.KindAny || k == ir.KindNull }
	switch {
	case l.IsNumeric() && r.IsNumeric():
		if lk == ir.KindInt && rk == ir.KindInt {
			return ir.Int, nil
		}
		return ir.Float, nil
	case lk == ir.KindString && rk == ir.KindString && op == ir.OpAdd:
		return ir.String, nil
	case (lk == ir.KindString && r.IsNumeric()) || (l.IsNumeric() && rk == ir.KindString):
		return ir.Float, nil
	case lk == ir.KindTimestamp && rk == ir.KindDuration && (op == ir.OpAdd || op == ir.OpSub):
		return ir.Timestamp, nil
	case lk == ir.KindDuration && rk == ir.KindTimestamp && op == ir.OpAdd:
		return ir.Timestamp, nil
	case lk == ir.KindTimestamp && rk == ir.KindTimestamp && op == ir.OpSub:
		return ir.Duration, nil
	case lk == ir.KindDuration && rk == ir.KindDuration && (op == ir.OpAdd || op == ir.OpSub):
		return ir.Duration, nil
	case lk == ir.KindDuration && r.IsNumeric() && (op == ir.OpMul || op == ir.OpDiv):
		return ir.Duration, nil
	case l.IsNumeric() && rk == ir.KindDuration && op == ir.OpMul:
		return ir.Duration, nil
	case dyn(lk) || dyn(rk):
		for _, k := range []ir.Kind{lk, rk} {
			switch k {
			case ir.KindBool, ir.KindArray, ir.KindObject:
				return ir.Any, typeErr(n, "operator %s is not defined for %s operands", op, k)
			}
		}
		if lk == ir.KindTimestamp || rk == ir.KindTimestamp || lk == ir.KindDuration || rk == ir.KindDuration {
			return ir.Any, nil
		}
		if lk == ir.KindFloat || rk == ir.KindFloat {
			return ir.Float, nil
		}
		return ir.Any, nil
	}
	hint := ""
	if lk == ir.KindString || rk == ir.KindString {
		hint = "; use concat() to join strings"
	}
	return ir.Any, typeErr(n, "operator %s is not defined for %s and %s%s", op, l, r, hint)
}

func comparable(n ast.Node, l, r ir.Type) error {
	lk, rk := l.Kind, r.Kind
	if lk == ir.KindAny || rk == ir.KindAny || lk == ir.KindNull || rk == ir.KindNull {
		return nil
	}
	if l.IsNumeric() && r.IsNumeric() {
		return nil
	}
	if (l.IsNumeric() && rk == ir.KindString) || (lk == ir.KindString && r.IsNumeric()) {
		return nil
	}
	if lk == rk {
		return nil
	}
	return typeErr(n, "cannot compare %s with %s", l, r)
}

// compareOperand compiles e and converts a literal to the type of other
// when PPL compares them that way.
func (x *ectx) compareOperand(e ast.Expr, other ir.Expr) (ir.Expr, error) {
	v, err := x.expr(e)
	if err != nil {
		return nil, err
	}
	return x.coerceForCompare(e, other, v)
}

// coerceForCompare converts a string literal compared with a timestamp to a
// timestamp, and an epoch number compared with a timestamp to a timestamp.
func (x *ectx) coerceForCompare(n ast.Node, other, v ir.Expr) (ir.Expr, error) {
	lit, ok := v.(*ir.Literal)
	if !ok || other.Type().Kind != ir.KindTimestamp {
		return v, nil
	}
	switch val := lit.V.(type) {
	case string:
		t, ok := parseTimestamp(val)
		if !ok {
			return nil, typeErr(n, "cannot compare a timestamp with %q: not a timestamp", val)
		}
		return ir.TimestampLit(t), nil
	case int64:
		return ir.TimestampLit(time.Unix(val, 0)), nil
	case float64:
		sec, frac := math.Modf(val)
		return ir.TimestampLit(time.Unix(int64(sec), int64(frac*1e9))), nil
	}
	return v, nil
}

func (x *ectx) caseExpr(n *ast.Case) (ir.Expr, error) {
	out := &ir.Case{T: ir.Null}
	for i, cond := range n.Conds {
		cv, err := x.expr(cond)
		if err != nil {
			return nil, err
		}
		if err := requireBool(cond, cv); err != nil {
			return nil, err
		}
		vv, err := x.expr(n.Vals[i])
		if err != nil {
			return nil, err
		}
		out.Whens = append(out.Whens, ir.When{Cond: cv, Then: vv})
		out.T = ir.Unify(out.T, vv.Type())
	}
	if n.Else != nil {
		ev, err := x.expr(n.Else)
		if err != nil {
			return nil, err
		}
		out.Else = ev
		out.T = ir.Unify(out.T, ev.Type())
	}
	if out.T.Kind == ir.KindNull {
		out.T = ir.Any
	}
	return out, nil
}

var castTypes = map[string]ir.Type{
	"int": ir.Int, "integer": ir.Int, "long": ir.Int, "bigint": ir.Int, "short": ir.Int, "byte": ir.Int,
	"double": ir.Float, "float": ir.Float, "string": ir.String, "boolean": ir.Bool,
	"timestamp": ir.Timestamp, "ip": ir.String,
}

func (x *ectx) cast(n *ast.Cast) (ir.Expr, error) {
	v, err := x.expr(n.X)
	if err != nil {
		return nil, err
	}
	switch n.Type {
	case "date":
		return callIR("date", v), nil
	case "time":
		return callIR("time", v), nil
	case "json":
		return callIR("json", toStringArg(v)), nil
	}
	t, ok := castTypes[n.Type]
	if !ok {
		return nil, typeErr(n, "unknown cast target type %s", n.Type)
	}
	return &ir.Cast{X: v, To: t}, nil
}

func (x *ectx) subquery(n *ast.Subquery) (ir.Expr, error) {
	c := x.c
	c.outer = append(c.outer, c.st)
	plan, err := c.pipeline(n.Pipeline)
	c.outer = c.outer[:len(c.outer)-1]
	if err != nil {
		return nil, outerField(err, x.schema)
	}
	switch n.Kind {
	case ast.SubqueryExists:
		return &ir.Subquery{Mode: ir.SubqueryExists, Plan: plan, Not: n.Not, T: ir.Bool}, nil
	case ast.SubqueryIn:
		args := make([]ir.Expr, len(n.Values))
		for i, v := range n.Values {
			if args[i], err = x.expr(v); err != nil {
				return nil, err
			}
		}
		s := plan.Schema()
		if !s.Open && len(s.Columns) != len(args) {
			return nil, semanticErr(n, "the IN subquery returns %d columns for %d values", len(s.Columns), len(args))
		}
		return &ir.Subquery{Mode: ir.SubqueryIn, Plan: plan, Args: args, Not: n.Not, T: ir.Bool}, nil
	}
	s := plan.Schema()
	if s.Open || len(s.Columns) != 1 {
		return nil, semanticErr(n, "a scalar subquery must return exactly one column; end it with stats or fields")
	}
	return &ir.Subquery{Mode: ir.SubqueryScalar, Plan: plan, T: s.Columns[0].Type}, nil
}

// outerField reports a field the subquery lacks and outer has as correlated.
func outerField(err error, outer ir.Schema) error {
	pe, ok := err.(*Error)
	if !ok || pe.field == "" {
		return err
	}
	if _, known := outer.Lookup(pe.field); !known {
		return err
	}
	e := correlated(nil, "field %s of the outer query", ir.QuoteName(pe.field))
	e.Pos, e.End = pe.Pos, pe.End
	return e
}

func (x *ectx) datePart(n *ast.DatePart) (ir.Expr, error) {
	args := make([]ir.Expr, len(n.Args))
	for i, a := range n.Args {
		v, err := x.expr(a)
		if err != nil {
			return nil, err
		}
		args[i] = v
	}
	part := ir.StringLit(n.Part)
	switch n.Func {
	case "extract":
		return x.checkedCall(n, "extract", []ir.Expr{part, args[0]})
	case "timestampadd":
		return x.checkedCall(n, "timestampadd", []ir.Expr{part, args[0], args[1]})
	case "timestampdiff":
		return x.checkedCall(n, "timestampdiff", []ir.Expr{part, args[0], args[1]})
	case "get_format":
		return x.checkedCall(n, "get_format", []ir.Expr{part, args[0]})
	}
	return nil, semanticErr(n, "unknown function %s", n.Func)
}

// checkedCall builds a call of a registered IR function, reporting arity and
// type errors at n.
func (x *ectx) checkedCall(n ast.Node, name string, args []ir.Expr) (ir.Expr, error) {
	f := ir.MustFunc(name)
	types := make([]ir.Type, len(args))
	for i, a := range args {
		types[i] = a.Type()
		if prm, ok := f.ParamFor(i); ok && prm.Const {
			if _, lit := a.(*ir.Literal); !lit {
				return nil, semanticErr(n, "%s: argument %d must be a constant", name, i+1)
			}
		}
	}
	t, err := f.Check(types)
	if err != nil {
		return nil, typeErr(n, "%v", err)
	}
	return &ir.Call{Func: f, Args: args, T: t}, nil
}
