package irplan

import (
	"fmt"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// oneField is a row of the field x.
type oneField exec.Value

func (r oneField) Field(path string) exec.Value {
	if path == "x" {
		return exec.Value(r)
	}

	return exec.Value{}
}

func (r oneField) Values(fn func(exec.Value) bool) { fn(exec.Value(r)) }

// TestListsAsSets requires a long list of constants to give what the
// comparisons with each of them give.
func TestListsAsSets(t *testing.T) {
	lits := map[string][]ir.Expr{
		"numbers": {ir.IntLit(1), ir.IntLit(2), ir.FloatLit(2.5), ir.IntLit(10)},
		"texts":   {ir.StringLit("10"), ir.StringLit(" 7"), ir.StringLit("abc"), ir.StringLit("2.5")},
		"mixed":   {ir.IntLit(1), ir.StringLit("x"), ir.BoolLit(true), ir.StringLit("10")},
		"nulls":   {ir.IntLit(1), ir.NullLit()},
		"bools":   {ir.BoolLit(false)},
	}
	values := []exec.Value{
		{}, {Kind: vec.KindNull}, {Kind: vec.KindInt, Int: 1}, {Kind: vec.KindInt, Int: 10}, {Kind: vec.KindInt, Int: 7},
		{Kind: vec.KindFloat, Float: 2.5}, {Kind: vec.KindFloat, Float: 10}, {Kind: vec.KindBool, Bool: true}, {Kind: vec.KindBool},
		{Kind: vec.KindString, Bytes: []byte("10")}, {Kind: vec.KindString, Bytes: []byte("10.0")}, {Kind: vec.KindString, Bytes: []byte("abc")},
		{Kind: vec.KindString, Bytes: []byte("x")}, {Kind: vec.KindString, Bytes: []byte("1")}, {Kind: vec.KindString, Bytes: []byte(" 2.5 ")},
		{Kind: vec.KindVariant, Bytes: []byte{0}},
	}
	x := &ir.FieldRef{Name: "x", T: ir.Any}
	for name, some := range lits {
		// Enough constants for a set, the same ones repeated.
		var list []ir.Expr
		for len(list) <= listedComparisons {
			list = append(list, some...)
		}
		for _, not := range []bool{false, true} {
			tr := newTranslator(Options{})
			set, err := tr.inList(x, list, not)
			if err != nil || set == nil {
				t.Fatalf("%s: %v", name, err)
			}
			op := ir.OpEq
			if not {
				op = ir.OpNe
			}
			var each []*exec.Expr
			for _, lit := range list {
				e, err := tr.operations(&ir.Binary{Op: op, L: x, R: lit, T: ir.Bool})
				if err != nil {
					t.Fatal(err)
				}
				each = append(each, e)
			}
			compared := exec.Or(each...)
			if not {
				compared = exec.And(each...)
			}
			setRow, compareRow := exec.NewRowExpr(set), exec.NewRowExpr(compared)
			for _, v := range values {
				got, want := setRow.Value(oneField(v)), compareRow.Value(oneField(v))
				if fmt.Sprint(got.Kind, got.Bool) != fmt.Sprint(want.Kind, want.Bool) {
					t.Errorf("%s, not %v, x = %v %q: set %v %v, comparisons %v %v", name, not, v.Kind, v.Bytes, got.Kind, got.Bool, want.Kind, want.Bool)
				}
			}
		}
	}
}
