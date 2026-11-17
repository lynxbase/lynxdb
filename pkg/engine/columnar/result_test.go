package columnar

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/query"
)

// plain gives a value of a row as encoding/json decodes its JSON form.
func plain(v event.Value) any {
	switch v.Type() {
	case event.FieldTypeBool:
		return v.AsBool()
	case event.FieldTypeInt:
		return float64(v.AsInt())
	case event.FieldTypeFloat:
		return v.AsFloat()
	case event.FieldTypeString:
		return v.AsString()
	case event.FieldTypeTimestamp:
		return v.AsTimestamp().UTC().Format(time.RFC3339Nano)
	case event.FieldTypeDuration:
		return float64(v.AsDuration())
	case event.FieldTypeArray:
		out := make([]any, 0, len(v.AsArray()))
		for _, e := range v.AsArray() {
			out = append(out, plain(e))
		}

		return out
	case event.FieldTypeObject:
		out := make(map[string]any, len(v.AsObject()))
		for name, e := range v.AsObject() {
			out[name] = plain(e)
		}

		return out
	}

	return nil
}

func TestResultAsJSON(t *testing.T) {
	tables := testData(t)
	for _, c := range []struct{ text, want string }{
		{`source=logs | stats count() as n, max(status) as top by level | sort level | head 2`,
			`{"n":2,"top":503,"level":"error"} {"n":2,"top":200,"level":"info"}`},
		{`source=logs | where status = 404 | fields - ts`,
			`{"_time":"2023-11-14T21:30:00Z","dur":0.25,"host":"b","level":"warn","msg":"not found","status":404}`},
		{"source=logs | where status >= 500 | sort status | fields status, tags, `http.path`",
			`{"status":500,"tags":["x","y"]} {"status":503,"http.path":"/x"}`},
		{`source=logs | stats max(_time) as last, avg(dur) as mean by host | sort host | head 1`,
			`{"last":"2023-11-15T00:30:00Z","mean":1,"host":"a"}`},
		{`source=logs | where status = 500 | eval q = concat(msg, ' "', host, '"\n') | fields q`,
			`{"q":"disk full on node \"a\"\n"}`},
		{`source=logs | eventstats avg(dur) as mean by host | where dur > mean | fields host, dur, mean`,
			`{"host":"a","dur":1.5,"mean":1} {"host":"b","dur":2.5,"mean":1.375}`},
		{`source=logs | eval r = rand() | where r >= 0 and status = 500 | fields _time, tags, status`,
			`{"_time":"2023-11-15T00:30:00Z","tags":["x","y"],"status":500}`},
		{`source=nope | stats count() as n`, `{"n":0}`},
		{`source=logs | where status = 1`, ``},
	} {
		compiled, err := query.Compile(c.text)
		if err != nil {
			t.Fatalf("%s: %v", c.text, err)
		}
		res, err := Run(context.Background(), compiled.Plan, tables, Options{Now: testNow})
		if err != nil {
			t.Fatalf("%s: %v", c.text, err)
		}
		var buf []byte
		lines := make([]string, res.Len())
		for i := range lines {
			buf = res.AppendJSON(buf[:0], i)
			lines[i] = string(buf)
		}
		if got := strings.Join(lines, " "); got != c.want {
			t.Errorf("%s\n got  %s\n want %s", c.text, got, c.want)
		}
		rows := res.Maps()
		if len(rows) != len(lines) {
			t.Fatalf("%s: %d rows as maps, %d as JSON", c.text, len(rows), len(lines))
		}
		for i, line := range lines {
			var got map[string]any
			if err := json.Unmarshal([]byte(line), &got); err != nil {
				t.Fatalf("%s: row %d is no JSON object: %v: %s", c.text, i, err, line)
			}
			want := make(map[string]any, len(rows[i]))
			for name, v := range rows[i] {
				want[name] = plain(v)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("%s: row %d\n as JSON %v\n as map  %v", c.text, i, got, want)
			}
		}
	}
}

// TestRowsHoldOnlyTheirFields requires a row to show the fields its event
// holds and null only where the event holds null or the plan computed it,
// whichever engine ran the plan.
func TestRowsHoldOnlyTheirFields(t *testing.T) {
	db, err := Open(Config{Dir: t.TempDir(), Store: colstore.StoreOptions{TimeFields: []string{"ts"}, KeepText: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	began := time.Now()
	events := `{"ts":1700000000,"msg":"a","n":1}
{"ts":1700000001,"msg":"b","n":null}
{"ts":1700000002,"other":"c"}
plain text line
`
	if _, err := db.Append("t", []byte(events)); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	ask := func(text string, columnar bool) []string {
		t.Helper()
		compiled, err := query.Compile(text)
		if err != nil {
			t.Fatalf("%s: %v", text, err)
		}
		res, err := db.Query(context.Background(), compiled.Plan, Options{})
		if err != nil {
			t.Fatalf("%s: %v", text, err)
		}
		if res.Columnar != columnar {
			t.Fatalf("%s: columnar %v", text, res.Columnar)
		}
		rows := make([]string, res.Len())
		for i := range rows {
			rows[i] = string(res.AppendJSON(nil, i))
		}

		return rows
	}
	// The rows in the order of their text, since engines differ in the
	// order of the members.
	sorted := func(rows []string) string {
		for i, row := range rows {
			var fields map[string]any
			if err := json.Unmarshal([]byte(row), &fields); err != nil {
				t.Fatalf("%s: %v", row, err)
			}
			text, _ := json.Marshal(fields)
			rows[i] = string(text)
		}
		slices.Sort(rows)

		return strings.Join(rows, " ")
	}
	for _, c := range []struct{ rest, want string }{
		{`fields - ts, _time`, `{"_raw":"plain text line"} {"msg":"a","n":1} {"msg":"b","n":null} {"other":"c"}`},
		{`eval x = n + 1 | fields - ts, _time`,
			`{"_raw":"plain text line","x":null} {"msg":"a","n":1,"x":2} {"msg":"b","n":null,"x":null} {"other":"c","x":null}`},
		{`fields msg, n`, `{"msg":"a","n":1} {"msg":"b","n":null} {} {}`},
		{`where isnull(n) | fields - ts, _time`, `{"_raw":"plain text line"} {"msg":"b","n":null} {"other":"c"}`},
	} {
		if got := sorted(ask(`source=t | `+c.rest, true)); got != c.want {
			t.Errorf("%s in the executor\n got  %s\n want %s", c.rest, got, c.want)
		}
		if got := sorted(ask(`source=t | eval r = rand() | where r >= 0 | fields - r | `+c.rest, false)); got != c.want {
			t.Errorf("%s in the row operators\n got  %s\n want %s", c.rest, got, c.want)
		}
	}
	if got := ask(`source=t | where isnull(ts) | fields _time`, true); len(got) != 1 {
		t.Fatalf("events without a time: %v", got)
	} else {
		var row struct {
			Time time.Time `json:"_time"`
		}
		if err := json.Unmarshal([]byte(got[0]), &row); err != nil || row.Time.Before(began.Add(-time.Second)) || row.Time.After(time.Now()) {
			t.Fatalf("an event without a time is at %s (%v), appended after %s", got[0], err, began)
		}
	}
	if got := strings.Join(ask(`describe t`, false), " "); !strings.Contains(got, `"msg"`) || strings.Contains(got, `{}`) {
		t.Fatalf("describe: %s", got)
	}
}

// TestANumberIsOneValueWhateverItsKind requires the executor to group and
// count a number once, whether events hold it as an integer or as a float.
// The row operators still tell the two kinds apart in groups.
func TestANumberIsOneValueWhateverItsKind(t *testing.T) {
	db, err := Open(Config{Dir: t.TempDir(), Store: colstore.StoreOptions{TimeFields: []string{"ts"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Two appends, so that the kinds meet inside a block and across blocks.
	for _, events := range []string{
		`{"ts":1700000000,"n":1}` + "\n" + `{"ts":1700000001,"n":1.0}` + "\n" + `{"ts":1700000002,"n":1.5}` + "\n",
		`{"ts":1700000003,"n":2.0}` + "\n" + `{"ts":1700000004,"n":2}` + "\n" + `{"ts":1700000005,"n":1.0}` + "\n",
	} {
		if _, err := db.Append("t", []byte(events)); err != nil {
			t.Fatal(err)
		}
		if err := db.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	for text, want := range map[string]string{
		`stats count() as c by n | sort n`: `{"c":3,"n":1} {"c":1,"n":1.5} {"c":2,"n":2}`,
		`stats distinct_count(n) as d`:     `{"d":3}`,
		`dedup n | sort n | fields n`:      `{"n":1} {"n":1.5} {"n":2}`,
	} {
		compiled, err := query.Compile(`source=t | ` + text)
		if err != nil {
			t.Fatalf("%s: %v", text, err)
		}
		res, err := db.Query(context.Background(), compiled.Plan, Options{})
		if err != nil {
			t.Fatalf("%s: %v", text, err)
		}
		rows := make([]string, res.Len())
		for i := range rows {
			rows[i] = string(res.AppendJSON(nil, i))
		}
		if got := strings.Join(rows, " "); got != want || !res.Columnar {
			t.Errorf("%s\n got  %s (columnar %v)\n want %s", text, got, res.Columnar, want)
		}
	}
}

// TestTimesAsText requires the text of a time to be what the standard
// library writes for it.
func TestTimesAsText(t *testing.T) {
	times := []int64{0, 1, -1, 999_999_999, 1_000_000_000, -1_000_000_000, 1_700_000_000_500_000_000,
		951_782_400_000_000_000, 951_868_799_999_999_999, 4_107_542_400_000_000_000, -2_208_988_800_000_000_000,
		math.MaxInt64, math.MinInt64}
	rng := rand.New(rand.NewSource(1))
	for range 20000 {
		times = append(times, rng.Int63()-rng.Int63(), rng.Int63n(4e18)/1e6*1e6, rng.Int63n(4e18)/1e9*1e9)
	}
	for _, nanos := range times {
		want := `"` + time.Unix(0, nanos).UTC().Format(time.RFC3339Nano) + `"`
		if got := string(appendTime(nil, nanos)); got != want {
			t.Fatalf("%d: %s, want %s", nanos, got, want)
		}
	}
}

func BenchmarkTimesAsText(b *testing.B) {
	var buf []byte
	for i := 0; b.Loop(); i++ {
		buf = appendTime(buf[:0], 1_700_000_000_123_456_000+int64(i)*1_000_003)
	}
}

func BenchmarkResultAsJSON(b *testing.B) {
	docs := make([]string, 20000)
	for i := range docs {
		docs[i] = fmt.Sprintf(`{"ts":%d,"level":"info","status":%d,"dur":%d.5,"host":"host-%d","msg":"request %d served in time","tags":["x","y"]}`,
			testNow.Add(-time.Duration(i)*time.Second).UnixMicro(), 200+i%7, i%90, i%50, i)
	}
	tables := testTables{"logs": tableOf(b, docs...)}
	compiled, err := query.Compile(`source=logs | where status >= 200`)
	if err != nil {
		b.Fatal(err)
	}
	for _, form := range []string{"from columns", "from maps"} {
		b.Run(form, func(b *testing.B) {
			b.ReportAllocs()
			var buf []byte
			for b.Loop() {
				res, err := Run(context.Background(), compiled.Plan, tables, Options{Now: testNow})
				if err != nil || res.Len() != len(docs) || !res.Columnar {
					b.Fatal(err, res.Len())
				}
				if form == "from maps" {
					res.rows, res.table = rowsOf(res.table, res.schema), nil
				}
				for i := 0; i < res.Len(); i++ {
					buf = res.AppendJSON(buf[:0], i)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(len(docs)), "ns/row")
		})
	}
}
