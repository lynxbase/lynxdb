package ppl

import (
	"strings"

	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/ppl/ast"
)

var joinKinds = map[string]ir.JoinKind{
	"inner": ir.JoinInner, "left": ir.JoinLeft, "outer": ir.JoinLeft, "right": ir.JoinRight, "full": ir.JoinFull,
	"semi": ir.JoinSemi, "anti": ir.JoinAnti, "cross": ir.JoinCross,
}

// joinSide names one input of a join for qualified-name resolution.
type joinSide struct {
	schema ir.Schema
	names  map[string]bool
}

func (s joinSide) qualified(f *ast.Field) (string, bool) {
	if len(f.Parts) > 1 && s.names[f.Parts[0]] {
		return strings.Join(f.Parts[1:], "."), true
	}
	return "", false
}

func (c *compiler) join(st *pipe, n *ast.JoinCommand) error {
	right, err := c.pipeline(n.Right)
	if err != nil {
		return err
	}
	j := &ir.Join{Left: st.node, Right: right, Kind: joinKinds[n.Kind], Collision: ir.CollisionRight}
	for _, a := range n.Args {
		switch a.Name {
		case "type":
			v, _ := stringArgValue(a)
			k, ok := joinKinds[strings.ToLower(v)]
			if !ok {
				return semanticErr(a.Value, "unknown join type %s", v)
			}
			j.Kind = k
		case "overwrite":
			v, err := boolArgValue(a)
			if err != nil {
				return err
			}
			if !v {
				j.Collision = ir.CollisionLeft
			}
		case "max":
			v, err := intArgValue(a)
			if err != nil || v < 0 {
				return semanticErr(a.Value, "max must be a non-negative integer")
			}
			j.MaxMatches = v
		}
	}
	left := joinSide{schema: st.schema(), names: map[string]bool{}}
	rightSide := joinSide{schema: right.Schema(), names: map[string]bool{}}
	for a := range st.aliases {
		left.names[a] = true
	}
	if n.LeftAlias != "" {
		left.names[n.LeftAlias] = true
		j.LeftQualifier = n.LeftAlias
	}
	if n.RightAlias != "" {
		rightSide.names[n.RightAlias] = true
		j.RightQualifier = n.RightAlias
	}
	for _, src := range sourceNames(st.origin) {
		left.names[src] = true
	}
	for _, src := range sourceNames(right) {
		rightSide.names[src] = true
	}
	switch {
	case len(n.Fields) > 0:
		for _, f := range n.Fields {
			lref, err := resolve(f, left.schema)
			if err != nil {
				return err
			}
			rref, err := resolve(f, rightSide.schema)
			if err != nil {
				return err
			}
			j.LeftKeys = append(j.LeftKeys, lref)
			j.RightKeys = append(j.RightKeys, rref)
		}
	case n.Cond != nil:
		if err := c.joinCondition(j, n.Cond, left, rightSide); err != nil {
			return err
		}
	case j.Kind != ir.JoinCross:
		return semanticErr(n, "%s join needs a condition: on <condition> or a field list", j.Kind)
	}
	st.node = j
	if st.aliases == nil {
		st.aliases = map[string]bool{}
	}
	for a := range left.names {
		st.aliases[a] = true
	}
	return nil
}

func sourceNames(n ir.Node) []string {
	var out []string
	if n == nil {
		return nil
	}
	ir.WalkPlan(n, func(x ir.Node) bool {
		if s, ok := x.(*ir.Scan); ok {
			for _, src := range s.Sources {
				out = append(out, src.Name)
			}
		}
		return true
	})
	return out
}

// joinCondition splits the condition into equality keys between the sides
// and a residual predicate over the joined row.
func (c *compiler) joinCondition(j *ir.Join, cond ast.Expr, left, right joinSide) error {
	var residual []ast.Expr
	var conj func(e ast.Expr)
	conj = func(e ast.Expr) {
		if b, ok := e.(*ast.Binary); ok && b.Op == "and" {
			conj(b.L)
			conj(b.R)
			return
		}
		residual = append(residual, e)
	}
	conj(cond)
	var rest []ast.Expr
	for _, e := range residual {
		if f, ok := e.(*ast.Field); ok {
			lref, lerr := c.sideRef(f, left, right)
			rref, rerr := c.sideRef(f, right, left)
			if lerr == nil && rerr == nil {
				j.LeftKeys = append(j.LeftKeys, lref)
				j.RightKeys = append(j.RightKeys, rref)
				continue
			}
			return semanticErr(f, "join field %s must exist on both sides", f.Name())
		}
		if b, ok := e.(*ast.Binary); ok && (b.Op == "=" || b.Op == "==") {
			lf, lok := b.L.(*ast.Field)
			rf, rok := b.R.(*ast.Field)
			if lok && rok {
				if l, err := c.sideRef(lf, left, right); err == nil {
					if r, err := c.sideRef(rf, right, left); err == nil {
						j.LeftKeys = append(j.LeftKeys, l)
						j.RightKeys = append(j.RightKeys, r)
						continue
					}
				}
				if l, err := c.sideRef(rf, left, right); err == nil {
					if r, err := c.sideRef(lf, right, left); err == nil {
						j.LeftKeys = append(j.LeftKeys, l)
						j.RightKeys = append(j.RightKeys, r)
						continue
					}
				}
			}
		}
		rest = append(rest, e)
	}
	if len(rest) == 0 {
		return nil
	}
	combined := ir.UnionSchemas(left.schema, right.schema)
	x := &ectx{c: c, schema: combined}
	var preds []ir.Expr
	for _, e := range rest {
		stripped := stripQualifiers(e, left, right)
		p, err := x.expr(stripped)
		if err != nil {
			return err
		}
		if err := requireBool(e, p); err != nil {
			return err
		}
		preds = append(preds, p)
	}
	j.Cond = ir.And(preds...)
	return nil
}

// sideRef resolves f on side, accepting a qualifier of that side and
// rejecting a qualifier of the other side.
func (c *compiler) sideRef(f *ast.Field, side, other joinSide) (*ir.FieldRef, error) {
	if name, ok := side.qualified(f); ok {
		return resolveName(f, name, side.schema)
	}
	if _, ok := other.qualified(f); ok {
		return nil, semanticErr(f, "field %s belongs to the other side", f.Name())
	}
	name := canonicalField(f.Name())
	if _, known := side.schema.Lookup(name); known {
		return &ir.FieldRef{Name: name, T: mustType(side.schema, name)}, nil
	}
	if _, knownOther := other.schema.Lookup(name); knownOther && !side.schema.Open {
		return nil, semanticErr(f, "field %s is not on this side", name)
	}
	return resolveName(f, name, side.schema)
}

func mustType(s ir.Schema, name string) ir.Type {
	t, _ := s.Resolve(name)
	return t
}

func stripQualifiers(e ast.Expr, sides ...joinSide) ast.Expr {
	switch n := e.(type) {
	case *ast.Field:
		for _, s := range sides {
			if name, ok := s.qualified(n); ok {
				return &ast.Field{Base: n.Base, Parts: strings.Split(name, ".")}
			}
		}
		return n
	case *ast.Binary:
		return &ast.Binary{Base: n.Base, Op: n.Op, L: stripQualifiers(n.L, sides...), R: stripQualifiers(n.R, sides...)}
	case *ast.Unary:
		return &ast.Unary{Base: n.Base, Op: n.Op, X: stripQualifiers(n.X, sides...)}
	case *ast.Call:
		args := make([]ast.Expr, len(n.Args))
		for i, a := range n.Args {
			args[i] = stripQualifiers(a, sides...)
		}
		return &ast.Call{Base: n.Base, Name: n.Name, Args: args}
	}
	return e
}
