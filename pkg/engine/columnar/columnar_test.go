package columnar

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/query"
)

var testNow = time.Unix(1_700_010_000, 0).UTC()

// testTables maps a table's name to its rows.
type testTables map[string]*colstore.Table

func (t testTables) Snapshot(name string) (*colstore.Snapshot, bool) {
	tbl, ok := t[name]
	if !ok {
		return nil, false
	}

	return tbl.Snapshot(), true
}

func (t testTables) Names() []string {
	names := make([]string, 0, len(t))
	for name := range t {
		names = append(names, name)
	}
	sort.Strings(names)

	return names
}

func tableOf(t testing.TB, docs ...string) *colstore.Table {
	t.Helper()
	batch := shred.NewBatch(shred.Options{TimeField: "ts"})
	for _, doc := range docs {
		if err := batch.AddJSON([]byte(doc), 0); err != nil {
			t.Fatal(err)
		}
	}
	var buf bytes.Buffer
	w := part.NewWriter(&buf, part.Options{})
	if err := w.WriteBatch(batch); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	p, err := part.Open(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	tbl := colstore.NewTable()
	if err := tbl.Apply([]*colstore.Handle{colstore.NewHandle(p, nil)}, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tbl.Close)

	return tbl
}

func testData(t testing.TB) testTables {
	at := func(ago int) int64 { return testNow.Add(-time.Duration(ago) * time.Minute).UnixMicro() }
	return testTables{
		"logs": tableOf(t,
			fmt.Sprintf(`{"ts":%d,"level":"error","status":500,"dur":1.5,"host":"a","msg":"disk full on node","tags":["x","y"]}`, at(30)),
			fmt.Sprintf(`{"ts":%d,"level":"info","status":200,"dur":0.5,"host":"a","msg":"request served"}`, at(90)),
			fmt.Sprintf(`{"ts":%d,"level":"error","status":503,"dur":2.5,"host":"b","msg":"Disk failure","http":{"path":"/x"}}`, at(150)),
			fmt.Sprintf(`{"ts":%d,"level":"warn","status":404,"dur":0.25,"host":"b","msg":"not found"}`, at(210)),
			fmt.Sprintf(`{"ts":%d,"level":"info","status":200,"dur":1.0,"host":"a","msg":"request served"}`, at(400))),
		"logs-old": tableOf(t,
			fmt.Sprintf(`{"ts":%d,"level":"error","status":500,"host":"c","msg":"old failure"}`, at(9000))),
		"hosts": tableOf(t,
			fmt.Sprintf(`{"ts":%d,"host":"a","dc":"east"}`, at(1)),
			fmt.Sprintf(`{"ts":%d,"host":"b","dc":"west"}`, at(2))),
	}
}

// render gives the rows as text: the fields of a row in the order of their
// names, rows in the order they came in.
func render(rows []map[string]event.Value, skip ...string) string {
	out := make([]string, len(rows))
	for i, row := range rows {
		var names []string
		for name := range row {
			if !strings.Contains(" "+strings.Join(skip, " ")+" ", " "+name+" ") {
				names = append(names, name)
			}
		}
		sort.Strings(names)
		fields := make([]string, len(names))
		for j, name := range names {
			fields[j] = name + "=" + row[name].String()
		}
		out[i] = strings.Join(fields, " ")
	}

	return strings.Join(out, "; ")
}

func TestRun(t *testing.T) {
	tables := testData(t)
	for _, c := range []struct {
		name, text, want string
		columnar         bool
	}{
		{"an aggregate in the columnar executor", `source=logs | stats count() as n, max(status) as top by level | sort level`,
			"level=error n=2 top=503; level=info n=2 top=200; level=warn n=1 top=404", true},
		{"whole events in the columnar executor", `source=logs | where status = 404 | fields - _time, ts`,
			"dur=0.25 host=b level=warn msg=not found status=404", true},
		{"a time in the columnar executor", `source=logs | stats max(_time) as last by host | sort host | head 1`,
			"host=a last=" + event.TimestampValue(testNow.Add(-30*time.Minute)).String(), true},
		{"a function without an operation above an aggregate", `source=logs | stats count() as n by host | eval h = md5(host) | sort host | fields n, h`,
			"h=0cc175b9c0f1b6a831c399e269772661 n=3; h=92eb5ffee6ae2fec3ad71c777531578f n=2", true},
		{"a function without an operation below an aggregate", `source=logs | eval h = md5(host) | stats count() as n by h | sort h`,
			"h=0cc175b9c0f1b6a831c399e269772661 n=3; h=92eb5ffee6ae2fec3ad71c777531578f n=2", true},
		{"a filter by a function without an operation", `source=logs | where reverse(host) = 'b' and length(sha1(msg)) = 40 | stats count() as n`, "n=2", true},
		{"a function of a time", `source=logs | eval day = date_format(_time, '%Y-%m-%d %H') | stats count() as n by day | sort day | head 2`,
			"day=2023-11-14 18 n=1; day=2023-11-14 21 n=1", true},
		{"a time from a function", `source=logs | where status = 404 | eval later = date_add(_time, INTERVAL 1 HOUR) | fields later`,
			"later=" + event.TimestampValue(testNow.Add(-150*time.Minute)).String(), true},
		{"an array of an event", `source=logs | where status = 500 | eval n = array_length(tags) | fields n, host`, "host=a n=2", true},
		{"an array from a function", `source=logs | where status = 404 | eval words = split(msg, ' ') | eval n = array_length(words) | fields words, n`, `n=2 words=["not","found"]`, true},
		{"a function with a function of its own", `source=logs | where status = 500 | eval loud = transform(tags, x -> upper(x)) | fields loud`, `loud=["X","Y"]`, true},
		{"a function that differs each time", `source=logs | eval r = rand() | where r >= 0 | stats count() as n`, "n=5", false},
		{"the members of an object", `source=logs | where status >= 500 | sort status | fields host, http`, "host=a; host=b http.path=/x", true},
		{"the members of an object in the row operators",
			`source=logs | eval r = rand() | where r >= 0 and status >= 500 | sort status | fields host, http`, "host=a; host=b http.path=/x", false},
		{"events without an object", `source=logs | where status = 503 | fields - http, ts, _time, tags`,
			"dur=2.5 host=b level=error msg=Disk failure status=503", true},
		{"events without an object in the row operators",
			`source=logs | eval r = rand() | where r >= 0 and status = 503 | fields - http, ts, _time, tags, r`,
			"dur=2.5 host=b level=error msg=Disk failure status=503", false},
		{"a percentile of few values", `source=logs | stats percentile(dur, 90) as p90, median(dur) as mid by host | sort host`,
			"host=a mid=1 p90=1.5; host=b mid=2.5 p90=2.5", true},
		{"a percentile of few values in the row operators",
			`source=logs | eval r = rand() | where r >= 0 | stats percentile(dur, 90) as p90, median(dur) as mid by host | sort host`,
			"host=a mid=1 p90=1.5; host=b mid=2.5 p90=2.5", false},
		{"tables of a pattern", "source=`logs*` | stats count() as n by host | sort host", "host=a n=3; host=b n=2; host=c n=1", true},
		{"two tables", "source=logs, hosts | stats count() as n", "n=7", true},
		{"events of two tables", "source=logs-old, hosts | eval h = md5(host) | stats count() as n by h | sort h | head 1", "h=0cc175b9c0f1b6a831c399e269772661 n=1", true},
		{"a number searched in every field", `source=logs 404 | fields msg`, "msg=not found", true},
		{"a number and a word searched in every field", `source=logs 200 served NOT 500 | stats count() as n`, "n=2", true},
		{"a table that is not there", `source=nope | stats count() as n`, "n=0", true},
		{"a join of two tables", `source=logs | join left=l right=r ON l.host = r.host hosts | stats count() as n by dc | sort dc`, "dc=east n=3; dc=west n=2", false},
		{"a lookup in another table", `source=logs | lookup hosts host OUTPUT dc | stats count() as n by dc | sort dc`, "dc=east n=3; dc=west n=2", false},
		{"a statistic of every event's group", `source=logs | eventstats avg(dur) as mean by host | where dur > mean | fields host, dur, mean`,
			"dur=1.5 host=a mean=1; dur=2.5 host=b mean=1.375", false},
		{"a field taken out of text", `source=logs | rex field=msg "(?<first>\w+)" | stats count() as n by first | sort - n, first | head 2`,
			"first=request n=2; first=Disk n=1", true},
		{"the fields of a table", `describe hosts | fields COLUMN_NAME, TYPE_NAME, ORDINAL_POSITION`,
			"COLUMN_NAME=_time ORDINAL_POSITION=1 TYPE_NAME=timestamp; COLUMN_NAME=dc ORDINAL_POSITION=2 TYPE_NAME=string; " +
				"COLUMN_NAME=host ORDINAL_POSITION=3 TYPE_NAME=string; COLUMN_NAME=ts ORDINAL_POSITION=4 TYPE_NAME=bigint", false},
		{"the types of a table's fields", `describe logs | where COLUMN_NAME in ('dur', 'tags', 'status') | fields COLUMN_NAME, TYPE_NAME, TABLE_NAME`,
			"COLUMN_NAME=dur TABLE_NAME=logs TYPE_NAME=double; COLUMN_NAME=status TABLE_NAME=logs TYPE_NAME=bigint; COLUMN_NAME=tags TABLE_NAME=logs TYPE_NAME=array", false},
		{"rows numbered as they come and cut short", `source=logs | where status < 500 | streamstats count() as i | where i <= 2 | stats count() as n`, "n=2", false},
		{"a field taken out of text and a limit", `source=logs | rex field=msg "(?<first>\\w+)" | where first = 'request' | head 1 | fields first, status`,
			"first=request status=200", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			compiled, err := query.Compile(c.text)
			if err != nil {
				t.Fatalf("%s: %v", c.text, err)
			}
			// The plan as the language states it, and as the optimizer
			// leaves it.
			for name, plan := range map[string]*ir.Plan{"stated": compiled.Logical, "optimized": compiled.Plan} {
				res, err := Run(context.Background(), plan, tables, Options{Now: testNow})
				if err != nil {
					t.Fatalf("%s, %s: %v", c.text, name, err)
				}
				if got := render(res.Maps()); got != c.want || res.Columnar != c.columnar {
					t.Fatalf("%s, %s\n got  %s (columnar %v)\n want %s (columnar %v)", c.text, name, got, res.Columnar, c.want, c.columnar)
				}
			}
		})
	}
}

// TestFieldsTakenOutOfText requires the executor to take fields out of text
// by a regular expression as the row operators do, whatever the text holds.
func TestFieldsTakenOutOfText(t *testing.T) {
	tables := testData(t)
	for _, extract := range []string{
		`rex field=msg "(?<first>\\w+) (?<second>\\w+)"`,
		`rex field=msg "(?<verb>full|served)( on (?<place>\\w+))?"`,
		`rex field=status "(?<hundreds>\\d)(?<rest>\\d+)"`,
		`rex field=dur "(?<whole>\\d+)"`,
		`rex field=nope "(?<any>.)"`,
		`rex field=level "(?<level>err)"`,
		`parse msg '(?<head>[a-z]+) (?<tail>full|found)'`,
		`parse host '(?<letter>[b-z])'`,
	} {
		run := func(text string, columnar bool) string {
			t.Helper()
			compiled, err := query.Compile(text)
			if err != nil {
				t.Fatalf("%s: %v", text, err)
			}
			res, err := Run(context.Background(), compiled.Plan, tables, Options{Now: testNow})
			if err != nil {
				t.Fatalf("%s: %v", text, err)
			}
			if res.Columnar != columnar {
				t.Fatalf("%s: columnar %v", text, res.Columnar)
			}

			return render(res.Maps(), "ts", "_time", "tags", "http.path")
		}
		got := run(`source=logs | `+extract+` | sort status, dur`, true)
		want := run(`source=logs | eval r = rand() | where r >= 0 | fields - r | `+extract+` | sort status, dur`, false)
		if got != want {
			t.Errorf("%s\n in the executor     %s\n in the row operators %s", extract, got, want)
		}
	}
}

// TestGroupsOfEventsWithoutTheKey requires both engines to give the events
// that do not hold a group key one group whose key is null.
func TestGroupsOfEventsWithoutTheKey(t *testing.T) {
	tables := testData(t)
	for prefix, columnar := range map[string]bool{`source=logs | `: true, `source=logs | eval r = rand() | where r >= 0 | `: false} {
		compiled, err := query.Compile(prefix + "stats count() as n by `http.path` | sort n")
		if err != nil {
			t.Fatal(err)
		}
		res, err := Run(context.Background(), compiled.Plan, tables, Options{Now: testNow})
		if err != nil {
			t.Fatal(err)
		}
		rows := make([]string, res.Len())
		for i := range rows {
			rows[i] = string(res.AppendJSON(nil, i))
		}
		if got, want := strings.Join(rows, " "), `{"n":1,"http.path":"/x"} {"n":4,"http.path":null}`; got != want || res.Columnar != columnar {
			t.Errorf("columnar %v: %s, want %s", res.Columnar, got, want)
		}
	}
}

// TestNullsFilledInNamedColumns requires the executor to fill the nulls of
// named columns as the row operators do: where an event holds a null and
// where it does not hold the field.
func TestNullsFilledInNamedColumns(t *testing.T) {
	tables := testData(t)
	for _, fill := range []string{
		`fillnull with 0 in dur, nope`,
		`fillnull with 'none' in host, http.path`,
		`fillnull using dur = 9, level = 'unknown', missing = false`,
		`fillnull with 0 in dur | eval twice = dur * 2`,
	} {
		run := func(text string, columnar bool) string {
			t.Helper()
			compiled, err := query.Compile(text)
			if err != nil {
				t.Fatalf("%s: %v", text, err)
			}
			res, err := Run(context.Background(), compiled.Plan, tables, Options{Now: testNow})
			if err != nil {
				t.Fatalf("%s: %v", text, err)
			}
			if res.Columnar != columnar {
				t.Fatalf("%s: columnar %v", text, res.Columnar)
			}

			return render(res.Maps(), "ts", "_time", "tags")
		}
		got := run("source=logs, logs-old | "+fill+" | sort host, status, dur", true)
		want := run("source=logs, logs-old | eval r = rand() | where r >= 0 | fields - r | "+fill+" | sort host, status, dur", false)
		if got != want {
			t.Errorf("%s\n in the executor      %s\n in the row operators %s", fill, got, want)
		}
	}
}
