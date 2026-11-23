package columnar

import (
	"fmt"
	"slices"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/ir"
)

// absentFields returns a warning for every field that the plan under n
// reads from a scan whose indexes hold events but none of which holds the
// field. A field that an operator above the scan computes, renames or
// takes out of text is that operator's, not the scan's.
func (r *run) absentFields(n ir.Node) []string {
	var warnings []string
	var walk func(n ir.Node, above []ir.Node)
	walk = func(n ir.Node, above []ir.Node) {
		if scan, ok := n.(*ir.Scan); ok {
			warnings = append(warnings, r.absentFrom(scan, readFrom(n, above))...)

			return
		}
		for _, in := range n.Inputs() {
			walk(in, append(above[:len(above):len(above)], n))
		}
	}
	walk(n, nil)

	return warnings
}

// readFrom returns the fields that the operators above, nearest last, read
// from the rows of in, in the order they first read them: up to an operator
// whose fields come from elsewhere or cannot be told.
func readFrom(in ir.Node, above []ir.Node) []string {
	var read []string
	made := map[string]bool{ir.TimeColumn: true}
	reads := func(e ir.Expr) {
		ir.WalkExpr(e, func(x ir.Expr) bool {
			name := ""
			switch x := x.(type) {
			case *ir.FieldRef:
				name = x.Name
			case *ir.Search:
				name = x.Field
			case *ir.Subquery:
				return false
			}
			if name != "" && !made[name] && !slices.Contains(read, name) {
				read = append(read, name)
			}

			return true
		})
	}
	for i := len(above) - 1; i >= 0; i-- {
		switch x := above[i].(type) {
		case *ir.Filter, *ir.Sort, *ir.TopN, *ir.Dedup, *ir.Extract, *ir.Patterns:
			for _, e := range ir.NodeExprs(x) {
				reads(e)
			}
			switch x := x.(type) {
			case *ir.Extract:
				for _, c := range x.Outputs {
					made[c.Name] = true
				}
			case *ir.Patterns:
				made[x.Output] = true
			}
		case *ir.Compute:
			for _, a := range x.Assigns {
				reads(a.Expr)
				made[a.Name] = true
			}
		case *ir.FillNull:
			// It makes the fields it fills.
			for _, a := range x.Fields {
				made[a.Name] = true
			}
		case *ir.Window:
			for _, e := range ir.NodeExprs(x) {
				reads(e)
			}
			for _, f := range x.Funcs {
				made[f.Name] = true
			}
		case *ir.Project:
			if !x.Drop {
				for _, name := range x.Names {
					if !ir.IsNamePattern(name) {
						reads(&ir.FieldRef{Name: name})
					}
				}
			}
		case *ir.RenameColumns:
			for _, rn := range x.Renames {
				if !ir.IsNamePattern(rn.From) {
					reads(&ir.FieldRef{Name: rn.From})
					made[rn.To] = true
				}
			}
		case *ir.Unnest:
			reads(&ir.FieldRef{Name: x.Field})
			made[x.As] = true
		case *ir.Join:
			// The side the rows of in reach the join by.
			from, keys := in, x.LeftKeys
			if i+1 < len(above) {
				from = above[i+1]
			}
			if from == x.Right {
				keys = x.RightKeys
			}
			for _, k := range keys {
				reads(k)
			}

			return read
		case *ir.Aggregate:
			for _, e := range ir.NodeExprs(x) {
				reads(e)
			}

			return read
		case *ir.Limit, *ir.Reverse:
		default:
			return read
		}
	}

	return read
}

// absentFrom returns the warnings for the fields of read that none of the
// indexes of a scan holds, when they hold events.
func (r *run) absentFrom(scan *ir.Scan, read []string) []string {
	if len(read) == 0 || (len(scan.Sources) == 1 && strings.HasPrefix(scan.Sources[0].Name, pushedSource)) {
		return nil
	}
	names, _ := r.sources(scan)
	held := make([]bool, len(read))
	events := false
	var indexes []string
	for _, name := range names {
		snap, ok := r.tables.Snapshot(name)
		if !ok {
			continue
		}
		indexes = append(indexes, name)
		for _, h := range snap.Parts() {
			events = events || h.Rows > 0
			for i, f := range read {
				if _, ok := h.PathID(f); ok || h.HasObject(f) {
					held[i] = true
				}
			}
		}
		snap.Release()
	}
	if !events {
		return nil
	}
	var out []string
	for i, f := range read {
		if held[i] {
			continue
		}
		which := "the index " + indexes[0]
		if len(indexes) > 1 {
			which = "the indexes " + strings.Join(indexes, ", ")
		}
		out = append(out, fmt.Sprintf("no event of %s holds the field %s", which, f))
	}

	return out
}
