package columnar

import (
	"bufio"
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ingest/ndjson"
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/query"
)

// agreeEvents are the JSON events of the index events: values of every kind
// in one field, numbers as text, nulls beside missing fields, arrays and
// objects.
var agreeEvents = []string{
	`{"ts":1700000001000000,"s":"10","n":1,"f":1.5,"v":1,"k":1,"a":null,"host":"web-01","level":"error","msg":"disk fused on node","tags":["x","y"],"http":{"path":"/a","status":200},"ok":true,"ms":"10"}`,
	`{"ts":1700000002000000,"s":"9","n":2,"f":2.5,"v":1.0,"k":2,"host":"web-02","level":"info","msg":"request served","tags":["z"],"http":{"path":"/b","status":404},"ok":false,"ms":"9"}`,
	`{"ts":1700000003000000,"s":"100","n":3,"f":-0.5,"v":"1","k":3,"a":"x","host":"web-01","level":"warn","msg":"Fused bits","ok":true,"ms":"100"}`,
	`{"ts":1700000004000000,"s":"1z","n":-4,"f":0,"v":"x","k":1,"a":"x","host":"db-01","level":"error","msg":"connection refused","http":{"path":"/a"}}`,
	`{"ts":1700000005000000,"s":"-3","n":5,"v":true,"k":2,"a":null,"host":"db-01","level":"info","msg":"user=bob action=login"}`,
	`{"ts":1700000006000000,"s":"2.5","f":3.25,"v":null,"host":"web-02","level":"debug","msg":"cache miss 404","tags":[]}`,
	`{"ts":1700000007000000,"s":"abc","n":7,"f":1000,"k":3,"host":"WEB-01","level":"error","msg":"Disk Full","ok":false}`,
	`{"ts":1700000008000000,"s":" 7","n":8,"f":2.5,"v":2,"host":"web-03","level":"info","msg":"req took 12ms","ms":"12"}`,
	`{"ts":1700000009000000,"s":"1e3","n":1,"f":1.5,"v":"10","k":1,"host":"web-03","level":"error","msg":"disk fused again","tags":["x"],"http":{"path":"/c","status":500}}`,
}

// agreeText are the lines of the index text, which arrive as text.
var agreeText = []string{
	"req took 10ms from web-01",
	"req took 9ms from web-02",
	"req took 100ms from web-01",
	"error: fused relay on web-03",
	"connection refused by db-01 status=503",
	"user=alice action=logout took=7",
}

// agreeDB loads the indexes that the corpus reads.
func agreeDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	appendText := func(index string, r *os.File) {
		conv, err := ndjson.NewText("")
		if err != nil {
			t.Fatal(err)
		}
		if err := conv.Read(r, func(b []byte) error {
			_, err := appendTo(db, index, b)
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	for index, file := range map[string]string{"ssh": "OpenSSH_2k.log", "openstack": "OpenStack_2k.log"} {
		f, err := os.Open(filepath.Join("..", "..", "..", "testdata", "logs", file))
		if err != nil {
			t.Fatal(err)
		}
		appendText(index, f)
		_ = f.Close()
	}
	lines := filepath.Join(t.TempDir(), "text.log")
	if err := os.WriteFile(lines, []byte(strings.Join(agreeText, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(lines)
	if err != nil {
		t.Fatal(err)
	}
	appendText("text", f)
	_ = f.Close()
	if _, _, err := db.Create("events", Settings{TimeFields: []string{"ts"}}); err != nil {
		t.Fatal(err)
	}
	if rejected, err := appendTo(db, "events", []byte(strings.Join(agreeEvents, "\n")+"\n")); err != nil || rejected != 0 {
		t.Fatalf("append: %d rejected, %v", rejected, err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}

	return db
}

// agreeCorpus reads the queries of testdata/agree.ppl: one per line, with
// comments after #.
func agreeCorpus(t *testing.T) []string {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", "agree.ppl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		out = append(out, line)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}

	return out
}

// shownValue renders a value with its type; floats keep ten digits, so that
// sums taken in another order agree.
func shownValue(v event.Value) string {
	switch v.Type() {
	case event.FieldTypeFloat:
		return "float:" + strconv.FormatFloat(v.AsFloat(), 'g', 10, 64)
	case event.FieldTypeArray:
		elems := v.AsArray()
		out := make([]string, len(elems))
		for i, e := range elems {
			out[i] = shownValue(e)
		}

		return "array:[" + strings.Join(out, ",") + "]"
	}

	return v.Type().String() + ":" + v.String()
}

// shownRows renders rows, each with its fields in the order of their names.
func shownRows(rows []map[string]event.Value) []string {
	out := make([]string, len(rows))
	for i, row := range rows {
		names := make([]string, 0, len(row))
		for name := range row {
			names = append(names, name)
		}
		sort.Strings(names)
		fields := make([]string, len(names))
		for j, name := range names {
			fields[j] = name + "=" + shownValue(row[name])
		}
		out[i] = strings.Join(fields, " ")
	}

	return out
}

// ordered reports whether a plan fixes the order of its rows: a sort under
// the operators that keep the order of their input.
func ordered(n ir.Node) bool {
	for {
		switch x := n.(type) {
		case *ir.Sort, *ir.TopN:
			return true
		case *ir.Project, *ir.Limit, *ir.Compute, *ir.RenameColumns, *ir.FillNull:
			n = x.Inputs()[0]
		default:
			return false
		}
	}
}

// agree runs a query by the columnar executor and by the row operators and
// returns how the results differ, or "".
func agree(db *DB, text string) string {
	compiled, err := query.Compile(text)
	if err != nil {
		return ""
	}
	ctx := context.Background()
	opts := Options{Now: testNow, Workers: 1}
	got, err := Run(ctx, compiled.Plan, db, opts)
	opts.rows = true
	want, wantErr := Run(ctx, compiled.Plan, db, opts)
	switch {
	case err != nil || wantErr != nil:
		if (err == nil) != (wantErr == nil) {
			return fmt.Sprintf("columnar error %v, row operators error %v", err, wantErr)
		}

		return ""
	}
	a, b := shownRows(got.Maps()), shownRows(want.Maps())
	if !ordered(compiled.Plan.Root) {
		sort.Strings(a)
		sort.Strings(b)
	}
	if strings.Join(a, "\n") == strings.Join(b, "\n") {
		return ""
	}
	for i := 0; i < max(len(a), len(b)); i++ {
		var x, y string
		if i < len(a) {
			x = a[i]
		}
		if i < len(b) {
			y = b[i]
		}
		if x != y {
			return fmt.Sprintf("%d and %d rows; row %d:\n  columnar  %s\n  row ops   %s", len(a), len(b), i, x, y)
		}
	}

	return ""
}

// TestExecutorsAgree requires the columnar executor and the row operators to
// give every query of the corpus the same rows.
func TestExecutorsAgree(t *testing.T) {
	db := agreeDB(t)
	for _, text := range agreeCorpus(t) {
		if diff := agree(db, text); diff != "" {
			t.Errorf("%s\n%s", text, diff)
		}
	}
}

// TestExecutorsAgreeOnExpressions requires both ways of running a plan to
// compute random expressions over the events alike.
func TestExecutorsAgreeOnExpressions(t *testing.T) {
	db := agreeDB(t)
	r := rand.New(rand.NewPCG(3, 5))
	for i := 0; i < 600; i++ {
		e := randomExpr(r, 3)
		for _, text := range []string{
			"source=events | eval x = " + e + " | fields k, x",
			"source=events | where " + e + " | stats count() as c",
			"source=events | eval x = " + e + " | stats count() as c by x",
			"source=events | eval x = " + e + " | sort x, k | fields x",
		} {
			if diff := agree(db, text); diff != "" {
				t.Errorf("%s\n%s", text, diff)
			}
		}
	}
}

var (
	randomFields = []string{"s", "n", "f", "v", "k", "a", "host", "msg", "ok", "ms", "nothing", "http.status"}
	randomLits   = []string{"0", "1", "-2", "2.5", "'10'", "'9'", "'1z'", "'web-01'", "''", "true", "false", "null", "'2.5'"}
	randomBinary = []string{"+", "-", "*", "/", "%", "=", "!=", "<", "<=", ">", ">=", "AND", "OR"}
	randomCalls  = []struct {
		name  string
		arity int
	}{
		{"abs", 1}, {"round", 1}, {"floor", 1}, {"ceil", 1}, {"length", 1}, {"lower", 1}, {"upper", 1},
		{"trim", 1}, {"isnull", 1}, {"isnotnull", 1}, {"typeof", 1}, {"tostring", 1}, {"tonumber", 1},
		{"coalesce", 2}, {"concat", 2}, {"if", 3}, {"nullif", 2}, {"substring", 2}, {"locate", 2}, {"pow", 2},
		{"greatest", 2}, {"least", 2}, {"isempty", 1}, {"sqrt", 1}, {"ln", 1}, {"mod", 2},
	}
)

// randomExpr returns a random expression over the fields of events.
func randomExpr(r *rand.Rand, depth int) string {
	if depth == 0 || r.IntN(4) == 0 {
		if r.IntN(2) == 0 {
			return randomFields[r.IntN(len(randomFields))]
		}

		return randomLits[r.IntN(len(randomLits))]
	}
	switch r.IntN(6) {
	case 0, 1, 2:
		return "(" + randomExpr(r, depth-1) + " " + randomBinary[r.IntN(len(randomBinary))] + " " + randomExpr(r, depth-1) + ")"
	case 3:
		return "NOT (" + randomExpr(r, depth-1) + ")"
	case 4:
		casts := []string{"int", "double", "string", "boolean"}
		return "cast(" + randomExpr(r, depth-1) + " as " + casts[r.IntN(len(casts))] + ")"
	}
	c := randomCalls[r.IntN(len(randomCalls))]
	args := make([]string, c.arity)
	for i := range args {
		args[i] = randomExpr(r, depth-1)
	}

	return c.name + "(" + strings.Join(args, ", ") + ")"
}
