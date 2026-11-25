package exec_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec/irfunc"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// layout says how documents become a table.
type layout struct {
	name      string
	blockRows int
	parts     int
	// sortKey, when set, orders each part by these paths.
	sortKey []string
	// shared lets the writer give a part dictionaries and tells it of
	// every row of the part before its blocks, so that it does where they
	// pay.
	shared bool
}

var layouts = []layout{
	{name: "arrival", blockRows: 256, parts: 3},
	{name: "one block", blockRows: 1 << 16, parts: 1},
	{name: "ordered", blockRows: 128, parts: 2, sortKey: []string{"kind", "op", "coll", "user"}},
}

// buildTable shreds docs into the parts and blocks of a layout.
func buildTable(t testing.TB, docs [][]byte, timeField string, l layout) *colstore.Table {
	t.Helper()
	var handles []*colstore.Handle
	per := (len(docs) + l.parts - 1) / max(l.parts, 1)
	for from := 0; from < len(docs); from += per {
		opts := part.Options{Dictionary: l.shared}
		if l.sortKey != nil {
			opts.Ordered, opts.SortKey = true, l.sortKey
		}
		var buf bytes.Buffer
		w := part.NewWriter(&buf, opts)
		all := shred.NewBatch(shred.Options{TimeField: timeField})
		for _, doc := range docs[from:min(from+per, len(docs))] {
			if err := all.AddJSON(doc, 0); err != nil {
				t.Fatalf("%v in %s", err, doc)
			}
		}
		if l.sortKey != nil {
			all.SortBy(l.sortKey)
		}
		if l.shared {
			w.Share([]shred.Run{{Batch: all, From: 0, To: all.Rows}})
		}
		block := shred.NewBatch(shred.Options{})
		for row := 0; row < all.Rows; row += l.blockRows {
			block.Reset()
			all.CopyRows(block, row, min(row+l.blockRows, all.Rows))
			if err := w.WriteBatch(block); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		p, err := part.Open(buf.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		handles = append(handles, colstore.NewHandle(p, nil))
	}
	tbl := colstore.NewTable()
	if err := tbl.Apply(handles, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tbl.Close)

	return tbl
}

func render(v exec.Value) string {
	switch v.Kind {
	case vec.KindBool:
		return strconv.FormatBool(v.Bool)
	case vec.KindInt:
		return strconv.FormatInt(v.Int, 10)
	case vec.KindFloat:
		return "f" + strconv.FormatFloat(v.Float, 'g', 10, 64)
	case vec.KindString:
		return strconv.Quote(string(v.Bytes))
	case vec.KindVariant:
		return "variant"
	}

	return "null"
}

func renderRow(row []exec.Value) string {
	parts := make([]string, len(row))
	for i, v := range row {
		parts[i] = render(v)
	}

	return strings.Join(parts, " | ")
}

func resultRows(res *exec.Result) []string {
	out := make([]string, res.Rows)
	var row []exec.Value
	for i := range out {
		row = res.Row(i, row[:0])
		out[i] = renderRow(row)
	}

	return out
}

// check runs plan on snap and requires the rows of the reference evaluator,
// in order when ordered is set.
func check(t *testing.T, snap *colstore.Snapshot, rows []refRow, plan exec.Node, ordered bool, workers int) *exec.Result {
	t.Helper()

	return checkAgainst(t, snap, rows, plan, plan, ordered, workers)
}

// checkAgainst runs plan on snap and requires the rows that the reference
// evaluator gives for the plan like.
func checkAgainst(t *testing.T, snap *colstore.Snapshot, rows []refRow, plan, like exec.Node, ordered bool, workers int) *exec.Result {
	t.Helper()
	res, err := exec.Run(context.Background(), snap, plan, exec.Options{Workers: workers})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	wantRows, names := refRun(rows, like)
	if len(res.Columns) != len(names) {
		t.Fatalf("got %d columns, want %d", len(res.Columns), len(names))
	}
	for i, name := range names {
		if res.Columns[i].Name != name {
			t.Fatalf("column %d is %q, want %q", i, res.Columns[i].Name, name)
		}
	}
	got := resultRows(res)
	want := make([]string, len(wantRows))
	for i, row := range wantRows {
		want[i] = renderRow(row)
	}
	if !ordered {
		sort.Strings(got)
		sort.Strings(want)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d\n got  %s\n want %s", len(got), len(want), head(got), head(want))
	}
	for i := range got {
		if got[i] != want[i] && !sameRow(got[i], want[i]) {
			t.Fatalf("row %d of %d\n got  %s\n want %s", i, len(got), got[i], want[i])
		}
	}

	return res
}

// sameRow reports whether two rendered rows differ only in the last digits
// of their floats, where sums taken in another order round apart.
func sameRow(got, want string) bool {
	a, b := strings.Split(got, " | "), strings.Split(want, " | ")
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] == b[i] {
			continue
		}
		x, errX := strconv.ParseFloat(strings.TrimPrefix(a[i], "f"), 64)
		y, errY := strconv.ParseFloat(strings.TrimPrefix(b[i], "f"), 64)
		if errX != nil || errY != nil || a[i][0] != 'f' || b[i][0] != 'f' || math.Abs(x-y) > 1e-9*math.Max(math.Abs(x), math.Abs(y)) {
			return false
		}
	}

	return true
}

func head(rows []string) string {
	if len(rows) > 6 {
		rows = rows[:6]
	}

	return strings.Join(rows, " ;; ")
}

var (
	synthKinds = []string{"commit", "commit", "commit", "commit", "identity", "account"}
	synthOps   = []string{"create", "create", "create", "update", "delete"}
	synthColls = []string{"feed.like", "feed.like", "feed.like", "graph.follow", "graph.follow", "feed.post",
		"feed.repost", "graph.block", "actor.profile", "graph.list", "feed.gate"}
	synthWords = []string{"good", "morning", "love", "crypto", "cryptography", "software", "hardware", "the", "a",
		"Good", "warehouse", "beware", "sun", "moon", "rare"}
)

// synthDocs returns n documents shaped like an event stream, with paths of
// every kind, mixed kinds, missing values and nulls.
func synthDocs(n int, seed int64) [][]byte {
	rng := rand.New(rand.NewSource(seed))
	docs := make([][]byte, n)
	ts := int64(1_700_000_000_000_000)
	for i := range docs {
		var b strings.Builder
		ts += int64(rng.Intn(40_000_000))
		kind := synthKinds[rng.Intn(len(synthKinds))]
		fmt.Fprintf(&b, `{"id":%d,"ts":%d,"kind":%q,"user":"u%04d"`, i, ts, kind, int(rng.ExpFloat64()*400)%3000)
		if kind == "commit" {
			fmt.Fprintf(&b, `,"op":%q,"coll":%q`, synthOps[rng.Intn(len(synthOps))], synthColls[rng.Intn(len(synthColls))])
		}
		switch r := rng.Intn(20); {
		case r < 11:
			fmt.Fprintf(&b, `,"n":%d`, rng.Intn(100))
		case r < 13:
			fmt.Fprintf(&b, `,"n":%d.5`, rng.Intn(100))
		case r < 15:
			fmt.Fprintf(&b, `,"n":"s%d"`, rng.Intn(5))
		case r < 16:
			b.WriteString(`,"n":null`)
		case r < 17:
			fmt.Fprintf(&b, `,"n":%v`, rng.Intn(2) == 0)
		}
		if rng.Intn(4) != 0 {
			fmt.Fprintf(&b, `,"big":%d`, rng.Int63()-rng.Int63())
		}
		fmt.Fprintf(&b, `,"score":%d.25`, rng.Intn(1000))
		if rng.Intn(10) < 7 {
			words := make([]string, 2+rng.Intn(6))
			for w := range words {
				words[w] = synthWords[rng.Intn(len(synthWords))]
			}
			fmt.Fprintf(&b, `,"msg":%q`, strings.Join(words, " "))
		}
		if rng.Intn(5) != 0 {
			fmt.Fprintf(&b, `,"flag":%v`, rng.Intn(2) == 0)
		}
		fmt.Fprintf(&b, `,"nested":{"x":%d,"y":"y%d"}`, rng.Intn(7), rng.Intn(3))
		if rng.Intn(6) == 0 {
			b.WriteString(`,"tags":["a","b"]`)
		}
		b.WriteString("}")
		docs[i] = []byte(b.String())
	}

	return docs
}

func referenceRows(t testing.TB, docs [][]byte, timeField string) []refRow {
	t.Helper()
	rows := make([]refRow, len(docs))
	for i, doc := range docs {
		rows[i] = flatten(t, doc, timeField)
	}

	return rows
}

// fn calls the function of the IR of a name with args, which it reads as of
// no declared type, or as the constants that literals hold.
func fn(name string, args ...*exec.Expr) *exec.Expr {
	call := &ir.Call{Func: ir.MustFunc(name), T: ir.Any}
	for _, a := range args {
		var arg ir.Expr = &ir.FieldRef{Name: "x", T: ir.Any}
		if a.Op == exec.OpLiteral {
			switch v := a.Value; v.Kind {
			case vec.KindInt:
				arg = ir.IntLit(v.Int)
			case vec.KindFloat:
				arg = ir.FloatLit(v.Float)
			case vec.KindString:
				arg = ir.StringLit(string(v.Bytes))
			case vec.KindBool:
				arg = ir.BoolLit(v.Bool)
			}
		}
		call.Args = append(call.Args, arg)
	}
	build, ok := irfunc.Lookup(name)
	if !ok {
		panic("no function " + name)
	}
	newFunc, err := build(call, irfunc.Env{})
	if err != nil {
		panic(err)
	}

	return exec.Call(newFunc, args...)
}

// cast converts a to the IR type to.
func cast(to ir.Type, a *exec.Expr) *exec.Expr {
	newFunc, err := irfunc.Cast(&ir.Cast{X: &ir.FieldRef{Name: "x", T: ir.Any}, To: to})
	if err != nil {
		panic(err)
	}

	return exec.Call(newFunc, a)
}

// nullIf is a unless it equals b.
func nullIf(a, b *exec.Expr) *exec.Expr {
	return exec.Case(exec.Eq(a, b), &exec.Expr{Op: exec.OpLiteral}, a)
}

// exists is true where a holds a value or a null.
func exists(a *exec.Expr) *exec.Expr { return exec.Not(exec.IsMissing(a)) }
