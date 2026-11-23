package opt

import (
	"sort"
	"time"

	"github.com/lynxbase/lynxdb/pkg/ir"
)

func timestampBound(lit *ir.Literal) *ir.TimeBound {
	t, ok := lit.V.(time.Time)
	if !ok {
		return nil
	}
	return ir.AbsoluteBound(t)
}

// colSet is a set of required column names; nil means every column. A set
// travels down a chain of single-input operators and is updated in place:
// an operator no longer needs its own set once its input's is known.
type colSet map[string]bool

func (s colSet) all() bool { return s == nil }

// addCols adds the columns e reads and returns the set, which is every
// column for a search of free text: it reads every field.
func (s colSet) addCols(e ir.Expr) colSet {
	if s == nil || e == nil {
		return s
	}
	all := false
	ir.WalkExpr(e, func(x ir.Expr) bool {
		switch v := x.(type) {
		case *ir.FieldRef:
			s[v.Name] = true
		case *ir.Search:
			if v.Field == "" {
				all = true
			} else {
				s[v.Field] = true
			}
		}
		return !all
	})
	if all {
		return nil
	}
	return s
}

// columnPruning records on each scan the columns the plan above it reads,
// when they are known.
func columnPruning(root ir.Node) ir.Node {
	return prune(root, nil)
}

func prune(n ir.Node, req colSet) ir.Node {
	var in colSet
	switch x := n.(type) {
	case *ir.Scan:
		if req.all() {
			return n
		}
		req[ir.TimeColumn] = true
		cols := make([]string, 0, len(req))
		for c := range req {
			cols = append(cols, c)
		}
		sort.Strings(cols)
		cp := *x
		cp.Columns = cols
		return &cp
	case *ir.Project:
		switch {
		case x.Drop:
			in = req
		case hasPattern(x.Names):
		case req.all():
			in = make(colSet, len(x.Names))
			for _, name := range x.Names {
				in[name] = true
			}
		default:
			in = req
			for c := range in {
				if !containsName(x.Names, c) {
					delete(in, c)
				}
			}
		}
	case *ir.Aggregate:
		in = colSet{}
		for _, g := range x.Groups {
			in = in.addCols(g.Expr)
		}
		for _, a := range x.Aggs {
			in = in.addCols(a.Agg)
		}
	case *ir.Filter:
		in = req.addCols(x.Cond)
	case *ir.Compute:
		if !req.all() {
			for _, a := range x.Assigns {
				delete(req, a.Name)
			}
			for _, a := range x.Assigns {
				req = req.addCols(a.Expr)
			}
			in = req
		}
	case *ir.Sort:
		in = req
		for _, k := range x.Keys {
			in = in.addCols(k.Expr)
		}
	case *ir.TopN:
		in = req
		for _, k := range x.Keys {
			in = in.addCols(k.Expr)
		}
	case *ir.Limit, *ir.Reverse:
		in = req
	case *ir.Dedup:
		in = req
		for _, k := range x.Keys {
			in = in.addCols(k)
		}
	case *ir.RenameColumns:
		if !req.all() && !hasPatternRename(x.Renames) {
			in = req
			for _, rn := range x.Renames {
				if in[rn.To] {
					delete(in, rn.To)
					in[rn.From] = true
				}
			}
		}
	case *ir.Extract:
		if !req.all() {
			for _, c := range x.Outputs {
				delete(req, c.Name)
			}
			in = req.addCols(x.Source)
		}
	}
	var buf [2]ir.Node
	ins := ir.AppendInputs(buf[:0], n)
	switch len(ins) {
	case 0:
		return n
	case 1:
		if pruned := prune(ins[0], in); pruned != ins[0] {
			return n.WithInputs([]ir.Node{pruned})
		}
		return n
	}
	var nins []ir.Node
	for i, c := range ins {
		p := prune(c, nil)
		if p != c && nins == nil {
			nins = make([]ir.Node, len(ins))
			copy(nins, ins[:i])
		}
		if nins != nil {
			nins[i] = p
		}
	}
	if nins == nil {
		return n
	}
	return n.WithInputs(nins)
}

func hasPattern(names []string) bool {
	for _, n := range names {
		if ir.IsNamePattern(n) {
			return true
		}
	}
	return false
}

func containsName(names []string, name string) bool {
	for _, n := range names {
		if n == name {
			return true
		}
	}
	return false
}

func hasPatternRename(renames []ir.Rename) bool {
	for _, rn := range renames {
		if ir.IsNamePattern(rn.From) {
			return true
		}
	}
	return false
}
