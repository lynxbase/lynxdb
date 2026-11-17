package exec_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// layout says how documents become a table.
type layout struct {
	name      string
	blockRows int
	parts     int
	// sortKey, when set, orders each part by these paths.
	sortKey []string
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
		opts := part.Options{}
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
		if got[i] != want[i] {
			t.Fatalf("row %d of %d\n got  %s\n want %s", i, len(got), got[i], want[i])
		}
	}

	return res
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
