package columnar

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/ppl"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

// operationValues are the values of field a, one row each; b holds a few
// values that rows take in turn.
var operationValues = []string{
	`0`, `1`, `-1`, `42`, `-7`, `9007199254740993`, `0.5`, `-2.5`, `2.675`, `1e21`, `3.0`,
	`""`, `" "`, `"abc"`, `"ABC def"`, `"  padded \t"`, `"42"`, `" 42 "`, `"3.5"`, `"-17"`, `"1e3"`, `"true"`,
	`"héllo wörld"`, `"İstanbul"`, `"ΣΑΣ"`, `"日本語"`, `"2024-02-29 23:59:59"`, `"12:34:56"`, `"a,b"`, `"x"`,
	`true`, `false`, `null`, `[1,"a"]`, `{"k":"v"}`,
}

var operationB = []string{`"b"`, `2`, `""`, `null`, `1.5`}

// operationExprs are expressions the planner maps to operations of the
// executor.
var operationExprs = []string{
	"tostring(a)", "lower(a)", "upper(a)", "length(a)", "concat(a, b)", "concat(a, '-')",
	"starts_with(a, 'a')", "ends_with(a, 'c')", "trim(a)", "ltrim(a)", "rtrim(a)",
	"abs(a)", "round(a)", "floor(a)", "ceil(a)", "sqrt(a)", "ln(a)", "exp(a)", "pow(a, 2)", "mod(a, 3)",
	"log(a)", "hour(a)", "coalesce(a, b)", "typeof(a)", "substring(a, 2)", "substring(a, 2, 2)", "substring(a, -2)",
	"locate('b', a)", "like(a, '%a%')", "contains(a, 'a')", "regexp_match(a, 'a.c')", "regexp_replace(a, 'a', 'X')",
	"is_null(a)",
}

const (
	notText    = "the executor gives no value for an argument that is not a string, the IR takes its text"
	notNumber  = "the executor gives no value for a numeric string or a boolean, the IR reads it as a number"
	roundsInts = notNumber + "; the executor rounds an integer to a float, the IR keeps it an integer"
	saturates  = notNumber + "; the executor gives no value for a float beyond int64, the IR converts it"
)

// knownDifferences records the expressions whose executor operations give
// what the IR's rules do not, and how.
var knownDifferences = map[string]string{
	"tostring(a)":    "the executor writes the float 3 as 3, the IR as 3.0, and gives no value for an array",
	"lower(a)":       notText,
	"upper(a)":       notText,
	"length(a)":      notText,
	"concat(a, b)":   notText,
	"concat(a, '-')": notText,
	"trim(a)":        notText,
	"ltrim(a)":       notText,
	"rtrim(a)":       notText,
	"abs(a)":         notNumber,
	"round(a)":       roundsInts,
	"floor(a)":       saturates,
	"ceil(a)":        saturates,
	"sqrt(a)":        notNumber,
	"ln(a)":          notNumber,
	"exp(a)":         notNumber,
	"pow(a, 2)":      notNumber,
	"mod(a, 3)":      "the executor gives no value for a numeric string",
	"log(a)":         notNumber,
	"hour(a)": "the executor reads an integer of no declared type as a time and gives no value for a time " +
		"string, the IR the reverse",
	"typeof(a)":                   "the executor names a stored null missing, the IR null",
	"substring(a, 2)":             notText,
	"substring(a, 2, 2)":          notText,
	"substring(a, -2)":            notText,
	"like(a, '%a%')":              notText,
	"regexp_match(a, 'a.c')":      notText,
	"regexp_replace(a, 'a', 'X')": notText,
}

// TestExecutorOperationsAgainstIR runs the expressions the planner maps to
// operations of the executor and compares every row with the row engine's
// value of the same expression for the same stored values.
func TestExecutorOperationsAgainstIR(t *testing.T) {
	docs := make([]string, 0, len(operationValues)+1)
	for i, a := range operationValues {
		docs = append(docs, fmt.Sprintf(`{"id":%d,"a":%s,"b":%s}`, i, a, operationB[i%len(operationB)]))
	}
	docs = append(docs, fmt.Sprintf(`{"id":%d,"b":"b"}`, len(operationValues)))
	tables := testTables{"t": tableOf(t, docs...)}
	for _, text := range operationExprs {
		query := "source=t | eval x = " + text + " | fields id, a, b, x"
		compiled, err := ppl.Compile(query)
		if err != nil {
			t.Logf("%s: %v", query, err)
			continue
		}
		expr := computed(compiled.Plan.Root, "x")
		if expr == nil {
			t.Fatalf("%s: no computed column", query)
		}
		prog, err := vm.CompileExpr(expr, vm.CompileOptions{Now: testNow})
		if err != nil {
			t.Fatalf("%s: %v", expr, err)
		}
		res, err := Run(context.Background(), compiled.Plan, tables, Options{Now: testNow})
		if err != nil || !res.Columnar {
			t.Fatalf("%s: %v (columnar %v)", query, err, res != nil && res.Columnar)
		}
		var machine vm.VM
		var diffs []string
		for _, got := range res.Rows {
			row := map[string]event.Value{}
			for _, name := range []string{"a", "b"} {
				if v, ok := got[name]; ok {
					row[name] = v
				}
			}
			want, err := machine.Execute(prog, row)
			if err != nil {
				want = event.NullValue()
			}
			have := got["x"]
			if have.Type() != want.Type() || have.String() != want.String() {
				diffs = append(diffs, fmt.Sprintf("a=%s b=%s: executor %s %q, IR %s %q",
					rowText(row["a"]), rowText(row["b"]), have.Type(), have.String(), want.Type(), want.String()))
			}
		}
		sort.Strings(diffs)
		if len(diffs) > 0 {
			t.Logf("%s differs in %d of %d rows: %s", text, len(diffs), len(res.Rows), strings.Join(diffs, "; "))
		}
		if _, known := knownDifferences[text]; known != (len(diffs) > 0) {
			t.Errorf("%s: %d differences, listed as known: %v", text, len(diffs), known)
		}
	}
}

// castDifferences records how the executor's conversions differ from the
// IR's casts, which the planner uses instead.
var castDifferences = map[string]string{
	"ToInt": "the executor gives no value for text with spaces around it and for a float beyond int64, " +
		"the IR trims the text and converts the float",
	"ToFloat":  "the executor gives no value for text with spaces around it, the IR trims it",
	"ToString": "the executor writes the float 3 as 3, the IR as 3.0, and gives no value for an array or object",
}

// TestExecutorConversionsAgainstIR compares the executor's ToInt, ToFloat,
// ToString and ToBool with the IR's casts over the same stored values.
func TestExecutorConversionsAgainstIR(t *testing.T) {
	docs := make([]string, 0, len(operationValues)+1)
	for i, a := range operationValues {
		docs = append(docs, fmt.Sprintf(`{"id":%d,"a":%s}`, i, a))
	}
	docs = append(docs, fmt.Sprintf(`{"id":%d}`, len(operationValues)))
	snap := tableOf(t, docs...).Snapshot()
	defer snap.Release()
	a := exec.Col("a", exec.KindAny)
	for _, c := range []struct {
		name string
		op   *exec.Expr
		to   ir.Type
	}{{"ToInt", exec.ToInt(a), ir.Int}, {"ToFloat", exec.ToFloat(a), ir.Float}, {"ToString", exec.ToString(a), ir.String}, {"ToBool", exec.ToBool(a), ir.Bool}} {
		plan := &exec.Project{Input: &exec.Scan{}, Exprs: []exec.Named{{Name: "a", Expr: a}, {Name: "x", Expr: c.op}}}
		res, err := exec.Run(context.Background(), snap, plan, exec.Options{Workers: 1})
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		prog, err := vm.CompileExpr(&ir.Cast{X: &ir.FieldRef{Name: "a", T: ir.Any}, To: c.to}, vm.CompileOptions{Now: testNow})
		if err != nil {
			t.Fatal(err)
		}
		var machine vm.VM
		var diffs []string
		for r := 0; r < res.Rows; r++ {
			row := map[string]event.Value{}
			if v := res.Value(r, 0); v.Kind != exec.KindAny {
				row["a"] = valueOf(v)
			}
			want, err := machine.Execute(prog, row)
			if err != nil {
				want = event.NullValue()
			}
			have := valueOf(res.Value(r, 1))
			if have.Type() != want.Type() || have.String() != want.String() {
				diffs = append(diffs, fmt.Sprintf("%s: executor %s %q, IR %s %q", rowText(row["a"]), have.Type(), have.String(), want.Type(), want.String()))
			}
		}
		sort.Strings(diffs)
		if len(diffs) > 0 {
			t.Logf("%s differs in %d of %d rows: %s", c.name, len(diffs), res.Rows, strings.Join(diffs, "; "))
		}
		if _, known := castDifferences[c.name]; known != (len(diffs) > 0) {
			t.Errorf("%s: %d differences, listed as known: %v", c.name, len(diffs), known)
		}
	}
}

func rowText(v event.Value) string {
	if v.IsNull() {
		return "null"
	}

	return v.Type().String() + " " + v.String()
}

// computed returns the expression a plan computes for a column.
func computed(n ir.Node, name string) ir.Expr {
	if c, ok := n.(*ir.Compute); ok {
		for _, a := range c.Assigns {
			if a.Name == name {
				return a.Expr
			}
		}
	}
	for _, in := range n.Inputs() {
		if e := computed(in, name); e != nil {
			return e
		}
	}

	return nil
}
