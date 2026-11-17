package columnar

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

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
			`{"status":500,"tags":["x","y"],"http.path":null} {"status":503,"tags":null,"http.path":"/x"}`},
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
