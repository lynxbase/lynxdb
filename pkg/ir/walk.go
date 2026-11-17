package ir

// ExprChildren returns the direct sub-expressions of e. Subquery plans are
// not expressions and are not returned.
func ExprChildren(e Expr) []Expr {
	switch x := e.(type) {
	case *Call:
		return x.Args
	case *ArrayExpr:
		return x.Elems
	case *ObjectExpr:
		return x.Values
	case *Subquery:
		return x.Args
	}
	return AppendExprChildren(nil, e)
}

// AppendExprChildren appends the direct sub-expressions of e to dst, in the
// order ExprChildren returns them.
func AppendExprChildren(dst []Expr, e Expr) []Expr {
	switch x := e.(type) {
	case *Unary:
		return append(dst, x.X)
	case *Binary:
		return append(dst, x.L, x.R)
	case *Call:
		return append(dst, x.Args...)
	case *Case:
		for _, w := range x.Whens {
			dst = append(dst, w.Cond, w.Then)
		}
		if x.Else != nil {
			dst = append(dst, x.Else)
		}
		return dst
	case *Cast:
		return append(dst, x.X)
	case *ArrayExpr:
		return append(dst, x.Elems...)
	case *ObjectExpr:
		return append(dst, x.Values...)
	case *Index:
		return append(dst, x.X, x.Key)
	case *Lambda:
		return append(dst, x.Body)
	case *InList:
		return append(append(dst, x.X), x.List...)
	case *IsMissing:
		return append(dst, x.Field)
	case *Subquery:
		return append(dst, x.Args...)
	case *AggCall:
		dst = append(dst, x.Args...)
		if x.Filter != nil {
			dst = append(dst, x.Filter)
		}
		return dst
	}
	return dst
}

// WalkExpr visits e and its sub-expressions in pre-order; when visit returns
// false the sub-expressions of that expression are skipped.
func WalkExpr(e Expr, visit func(Expr) bool) {
	if e == nil || !visit(e) {
		return
	}
	var buf [4]Expr
	for _, c := range AppendExprChildren(buf[:0], e) {
		WalkExpr(c, visit)
	}
}

// RewriteExpr rebuilds e bottom-up, replacing each expression by rewrite's
// result. Expressions whose children are unchanged are passed to rewrite
// as-is; changed ones are shallow-copied first, so shared trees are never
// mutated.
func RewriteExpr(e Expr, rewrite func(Expr) Expr) Expr {
	if e == nil {
		return nil
	}
	return rewrite(withChildren(e, func(c Expr) Expr { return RewriteExpr(c, rewrite) }))
}

func mapExprs(es []Expr, f func(Expr) Expr) ([]Expr, bool) {
	var out []Expr
	for i, e := range es {
		ne := f(e)
		if ne != e && out == nil {
			out = append(make([]Expr, 0, len(es)), es[:i]...)
		}
		if out != nil {
			out = append(out, ne)
		}
	}
	if out == nil {
		return es, false
	}
	return out, true
}

func withChildren(e Expr, f func(Expr) Expr) Expr {
	switch x := e.(type) {
	case *Unary:
		if nx := f(x.X); nx != x.X {
			c := *x
			c.X = nx
			return &c
		}
	case *Binary:
		nl, nr := f(x.L), f(x.R)
		if nl != x.L || nr != x.R {
			c := *x
			c.L, c.R = nl, nr
			return &c
		}
	case *Call:
		if args, ok := mapExprs(x.Args, f); ok {
			c := *x
			c.Args = args
			return &c
		}
	case *Case:
		changed := false
		whens := make([]When, len(x.Whens))
		for i, w := range x.Whens {
			whens[i] = When{Cond: f(w.Cond), Then: f(w.Then)}
			changed = changed || whens[i].Cond != w.Cond || whens[i].Then != w.Then
		}
		var els Expr
		if x.Else != nil {
			els = f(x.Else)
			changed = changed || els != x.Else
		}
		if changed {
			return &Case{Whens: whens, Else: els, T: x.T}
		}
	case *Cast:
		if nx := f(x.X); nx != x.X {
			return &Cast{X: nx, To: x.To}
		}
	case *ArrayExpr:
		if elems, ok := mapExprs(x.Elems, f); ok {
			return &ArrayExpr{Elems: elems, T: x.T}
		}
	case *ObjectExpr:
		if vals, ok := mapExprs(x.Values, f); ok {
			return &ObjectExpr{Keys: x.Keys, Values: vals}
		}
	case *Index:
		nx, nk := f(x.X), f(x.Key)
		if nx != x.X || nk != x.Key {
			return &Index{X: nx, Key: nk, T: x.T}
		}
	case *Lambda:
		if nb := f(x.Body); nb != x.Body {
			return &Lambda{Params: x.Params, Body: nb}
		}
	case *InList:
		nx := f(x.X)
		list, ok := mapExprs(x.List, f)
		if ok || nx != x.X {
			return &InList{X: nx, List: list, Not: x.Not}
		}
	case *Subquery:
		if args, ok := mapExprs(x.Args, f); ok {
			c := *x
			c.Args = args
			return &c
		}
	case *AggCall:
		args, ok := mapExprs(x.Args, f)
		var filter Expr
		if x.Filter != nil {
			filter = f(x.Filter)
		}
		if ok || filter != x.Filter {
			c := *x
			c.Args, c.Filter = args, filter
			return &c
		}
	}
	return e
}

// ColumnsRead returns the column names e reads, in first-use order. Columns
// read inside subqueries belong to the subquery and are not included.
func ColumnsRead(e Expr) []string {
	var out []string
	seen := map[string]bool{}
	add := func(n string) {
		if !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
	}
	WalkExpr(e, func(x Expr) bool {
		switch v := x.(type) {
		case *FieldRef:
			add(v.Name)
		case *Search:
			if v.Field == "" {
				add(RawColumn)
			} else {
				add(v.Field)
			}
		}
		return true
	})
	return out
}

// AppendConjuncts appends the top-level AND operands of e to dst.
func AppendConjuncts(dst []Expr, e Expr) []Expr {
	if b, ok := e.(*Binary); ok && b.Op == OpAnd {
		return AppendConjuncts(AppendConjuncts(dst, b.L), b.R)
	}
	return append(dst, e)
}

// And combines predicates with AND; it returns nil for an empty list.
func And(es ...Expr) Expr {
	var out Expr
	for _, e := range es {
		if e == nil {
			continue
		}
		if out == nil {
			out = e
			continue
		}
		out = &Binary{Op: OpAnd, L: out, R: e, T: Bool}
	}
	return out
}

// NodeExprs returns the expressions an operator evaluates, not including
// those of its inputs.
func NodeExprs(n Node) []Expr {
	switch x := n.(type) {
	case *Scan:
		return x.Prune
	case *Values:
		var out []Expr
		for _, r := range x.Rows {
			out = append(out, r...)
		}
		return out
	case *Filter:
		return []Expr{x.Cond}
	case *Compute:
		out := make([]Expr, len(x.Assigns))
		for i, a := range x.Assigns {
			out[i] = a.Expr
		}
		return out
	case *Aggregate:
		var out []Expr
		for _, g := range x.Groups {
			out = append(out, g.Expr)
		}
		for _, a := range x.Aggs {
			out = append(out, a.Agg)
		}
		for _, k := range x.OrderBy {
			out = append(out, k.Expr)
		}
		return out
	case *Window:
		var out []Expr
		for _, g := range x.PartitionBy {
			out = append(out, g.Expr)
		}
		for _, a := range x.Funcs {
			out = append(out, a.Agg)
		}
		for _, e := range []Expr{x.Frame.Span, x.Frame.ResetBefore, x.Frame.ResetAfter} {
			if e != nil {
				out = append(out, e)
			}
		}
		return out
	case *Sort:
		return sortKeyExprs(x.Keys)
	case *TopN:
		return sortKeyExprs(x.Keys)
	case *Dedup:
		return x.Keys
	case *Join:
		out := append(append([]Expr(nil), x.LeftKeys...), x.RightKeys...)
		if x.Cond != nil {
			out = append(out, x.Cond)
		}
		return out
	case *Extract:
		return []Expr{x.Source}
	case *Patterns:
		return append([]Expr{x.Source}, x.GroupBy...)
	case *FillNull:
		var out []Expr
		for _, a := range x.Fields {
			out = append(out, a.Expr)
		}
		if x.Default != nil {
			out = append(out, x.Default)
		}
		return out
	}
	return nil
}

func sortKeyExprs(keys []SortKey) []Expr {
	out := make([]Expr, len(keys))
	for i, k := range keys {
		out[i] = k.Expr
	}
	return out
}

// Transform rebuilds a plan bottom-up: inputs are transformed first, then
// rewrite is applied to the operator (re-created with the new inputs when
// any changed).
func Transform(n Node, rewrite func(Node) Node) Node {
	if u, ok := n.(unary); ok {
		in := u.input()
		if nin := Transform(in, rewrite); nin != in {
			n = n.WithInputs([]Node{nin})
		}
		return rewrite(n)
	}
	ins := n.Inputs()
	if len(ins) > 0 {
		changed := false
		nins := make([]Node, len(ins))
		for i, in := range ins {
			nins[i] = Transform(in, rewrite)
			changed = changed || nins[i] != in
		}
		if changed {
			n = n.WithInputs(nins)
		}
	}
	return rewrite(n)
}

// WalkPlan visits n and its inputs in pre-order, including the plans of
// subqueries in expressions.
func WalkPlan(n Node, visit func(Node) bool) {
	if n == nil || !visit(n) {
		return
	}
	for _, e := range NodeExprs(n) {
		WalkExpr(e, func(x Expr) bool {
			if sq, ok := x.(*Subquery); ok {
				WalkPlan(sq.Plan, visit)
			}
			return true
		})
	}
	if u, ok := n.(unary); ok {
		WalkPlan(u.input(), visit)
		return
	}
	for _, in := range n.Inputs() {
		WalkPlan(in, visit)
	}
}
