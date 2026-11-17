package irplan

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/ppl"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

var testNow = time.Unix(1_700_010_000, 0)

// testEvents are five events, each so many minutes before testNow.
var testEvents = []struct {
	ago    int
	level  string
	status int
	dur    float64
	host   string
	msg    string
}{
	{30, "error", 500, 1.5, "a", "disk full on node"},
	{90, "info", 200, 0.5, "a", "request served"},
	{150, "error", 503, 2.5, "b", "Disk failure"},
	{210, "warn", 404, 0.25, "b", "not found"},
	{400, "info", 200, 1.0, "a", "request served"},
}

func testSnapshot(t testing.TB) *colstore.Snapshot {
	t.Helper()
	batch := shred.NewBatch(shred.Options{TimeField: "ts"})
	for _, e := range testEvents {
		at := testNow.Add(-time.Duration(e.ago) * time.Minute).UnixMicro()
		doc := fmt.Sprintf(`{"ts":%d,"level":%q,"status":%d,"dur":%g,"host":%q,"msg":%q}`, at, e.level, e.status, e.dur, e.host, e.msg)
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
	snap := tbl.Snapshot()
	t.Cleanup(snap.Release)

	return snap
}

func compile(text string) (exec.Node, error) {
	res, err := ppl.Compile(text)
	if err != nil {
		return nil, err
	}

	return Plan(res.Plan, Options{Now: testNow})
}

// answer runs a query and renders its rows, values joined by spaces and rows
// by semicolons.
func answer(t *testing.T, snap *colstore.Snapshot, text string) string {
	t.Helper()
	plan, err := compile(text)
	if err != nil {
		t.Fatalf("%s: %v", text, err)
	}
	res, err := exec.Run(context.Background(), snap, plan, exec.Options{Workers: 2})
	if err != nil {
		t.Fatalf("%s: %v", text, err)
	}
	var rows []string
	for i := 0; i < res.Rows; i++ {
		var vals []string
		for _, v := range res.Row(i, nil) {
			switch v.Kind {
			case vec.KindString:
				vals = append(vals, string(v.Bytes))
			case vec.KindInt:
				vals = append(vals, fmt.Sprint(v.Int))
			case vec.KindFloat:
				vals = append(vals, fmt.Sprint(v.Float))
			case vec.KindBool:
				vals = append(vals, fmt.Sprint(v.Bool))
			default:
				vals = append(vals, "null")
			}
		}
		rows = append(rows, strings.Join(vals, " "))
	}

	return strings.Join(rows, "; ")
}

func TestQueries(t *testing.T) {
	snap := testSnapshot(t)
	for _, c := range []struct{ name, text, want string }{
		{"search word", `source=logs disk | stats count()`, "2"},
		{"search phrase", `source=logs "disk full" | stats count()`, "1"},
		{"search prefix", `source=logs fail* | stats count()`, "1"},
		{"field terms", `source=logs level=info NOT host=b | stats count()`, "2"},
		{"a term that is not there", `source=logs NOT nope=1 | stats count()`, "5"},
		{"where", `source=logs | where status >= 500 and not like(msg, '%node%') | stats count()`, "1"},
		{"like at the start", `source=logs | where like(msg, 'disk%') | stats count()`, "2"},
		{"like in the given case", `source=logs | where like(msg, 'disk%', true) | stats count()`, "1"},
		{"like of one character", `source=logs | where like(host, '_') and like(msg, 'not_found') | stats count()`, "1"},
		{"regular expression", `source=logs | where regexp_match(msg, '[Dd]isk f(ull|ailure)') | stats count()`, "2"},
		{"replace", `source=logs | eval m = replace(msg, 'disk', 'tape') | fields m, status | sort - status | head 2`, "Disk failure 503; tape full on node 500"},
		{"position", `source=logs | where locate('full', msg) > 0 | stats count()`, "1"},
		{"in", `source=logs | where status in (200, 404) | stats count()`, "3"},
		{"not in", `source=logs | where status not in (200, 404) | stats count()`, "2"},
		{"null tests", `source=logs | where isnull(nope) and isnotnull(host) | stats count()`, "5"},
		{"cast", `source=logs | where cast(status as string) = '200' | stats count()`, "2"},
		{"eval", `source=logs | eval slow = dur > 1.0 | fields status, slow | sort - status | head 2`, "503 true; 500 true"},
		{"fields", `source=logs | fields level, status | sort - status, level | head 1`, "error 503"},
		{"fields without one", `source=logs | fields level, status, host | fields - host | sort status, level | head 1`, "info 200"},
		{"rename", `source=logs | fields level, status | rename status as code | sort - code | head 1`, "error 503"},
		{"stats", `source=logs | stats count() as n, avg(dur) as mean, max(status) as top by level | sort level`,
			"2 2 503 error; 2 0.75 200 info; 1 0.25 404 warn"},
		{"picked rows", `source=logs | stats latest(msg) as last, earliest(status) as first by host | sort host`, "disk full on node 200 a; Disk failure 404 b"},
		{"where and sort by a picked value", `source=logs | stats latest(status) as last, count() as n by host | where last >= 500 | sort - last`, "503 2 b; 500 3 a"},
		{"function of a picked value", `source=logs | stats latest(msg) as last by host | eval up = upper(last) | fields host, up | sort host`,
			"a DISK FULL ON NODE; b DISK FAILURE"},
		{"dedup", `source=logs | dedup host | fields host, status, _time | sort host`,
			fmt.Sprintf("a 500 %d; b 503 %d", testNow.Add(-30*time.Minute).UnixNano(), testNow.Add(-150*time.Minute).UnixNano())},
		{"dedup of filtered rows", `source=logs | where status < 500 | dedup level, host | eval code = status + 1 | fields level, code | sort level`, "info 201; warn 405"},
		{"distinct counts", `source=logs | stats distinct_count(msg) as texts, distinct_count_approx(status) as codes by level | sort level`, "2 2 error; 1 1 info; 1 1 warn"},
		{"conditional aggregates", `source=logs | stats count(eval(status >= 500)) as errors, count() as n by host | sort host`, "1 3 a; 1 2 b"},
		{"share of the total", `source=logs | stats count() as n by level | eventstats sum(n) as all | eval share = round(n * 100.0 / all, 1) | sort level`,
			"2 error 5 40; 2 info 5 40; 1 warn 5 20"},
		{"top", `source=logs | top 2 host`, "a 3; b 2"},
		{"rare", `source=logs | rare 1 level`, "warn 1"},
		{"case", `source=logs | eval k = case(status >= 500, 'bad', status >= 400, 'meh' else 'ok') | stats count() as n by k | sort k`, "2 bad; 1 meh; 2 ok"},
		{"functions of strings", `source=logs | eval k = if(status >= 500, 'bad', 'ok'), u = upper(host), l = length(msg), s = substring(msg, 1, 4) | fields k, u, l, s, status | sort - status | head 2`,
			"bad B 12 Disk 503; bad A 17 disk 500"},
		{"math", `source=logs | eval r = round(dur * 3, 1), f = floor(dur), c = ceil(dur), p = pow(status - 500, 2), q = sqrt(abs(status - 504)) | fields r, f, c, p, q | sort - r | head 1`,
			"7.5 2 3 9 1"},
		{"where after stats", `source=logs | stats count() as n by host | where n > 2 | sort host`, "3 a"},
		{"eval after stats", `source=logs | stats sum(status) as total, count() as n | eval mean = total / n`, "1807 5 361"},
		{"buckets of a number", `source=logs | stats count() as n by span(status, 100) as code | sort code`, "2 200; 1 400; 2 500"},
		{"buckets of time", `source=logs | stats count() as n by span(_time, 2h) as at | sort at | head 1`,
			fmt.Sprintf("1 %d", testNow.Add(-400*time.Minute).Truncate(2*time.Hour).UnixNano())},
		{"sort and head", `source=logs | fields status, host | sort host, - status | head 3`, "500 a; 200 a; 200 a"},
		{"fields above a sort and a head", `source=logs | sort - status | head 2 | fields status`, "503; 500"},
		{"eval above a sort", `source=logs | stats count() as n by host | sort - n | head 1 | eval twice = n * 2`, "3 a 6"},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := answer(t, snap, c.text); got != c.want {
				t.Fatalf("%s\n got  %s\n want %s", c.text, got, c.want)
			}
		})
	}
}

// TestPercentiles requires the percentiles of the durations, which a sketch
// gives within one percent.
func TestPercentiles(t *testing.T) {
	snap := testSnapshot(t)
	plan, err := compile(`source=logs | stats percentile(dur, 50) as mid, percentile(dur, 100) as top, p99(nope) as none by host | sort host`)
	if err != nil {
		t.Fatal(err)
	}
	res, err := exec.Run(context.Background(), snap, plan, exec.Options{Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	want := [][]float64{{1, 1.5}, {0.25, 2.5}}
	if res.Rows != len(want) {
		t.Fatalf("got %d rows, want %d", res.Rows, len(want))
	}
	for r, row := range want {
		for c, exact := range row {
			if v := res.Value(r, c); v.Kind != vec.KindFloat || v.Float < 0.99*exact || v.Float > 1.01*exact {
				t.Errorf("row %d, column %d: got %v, want %g within one percent", r, c, v, exact)
			}
		}
		if v := res.Value(r, 2); !v.IsNull() {
			t.Errorf("row %d: a percentile of no values is %v", r, v)
		}
	}
}

func TestTimeRangesCountFromNow(t *testing.T) {
	snap := testSnapshot(t)
	for text, want := range map[string]string{
		`source=logs earliest=-2h | stats count()`:             "2",
		`source=logs earliest=-4h latest=-1h | stats count()`:  "3",
		`source=logs earliest=-10h latest=-5h | stats count()`: "1",
		fmt.Sprintf(`source=logs | where _time < '%s' | stats count()`, testNow.Add(-3*time.Hour).UTC().Format("2006-01-02 15:04:05")): "2",
	} {
		if got := answer(t, snap, text); got != want {
			t.Errorf("%s: %s, want %s", text, got, want)
		}
	}
}

func TestRefusedPlans(t *testing.T) {
	for text, want := range map[string]string{
		`source=logs | stats count() as n by host | eventstats avg(n) as m`:                   "the window function avg(n)",
		`source=logs | stats count() as n by host | eventstats sum(n) as all by host`:         "a window gives the sum of a column of an aggregate over all of its groups",
		`source=logs | stats count() as n by host | eventstats sum(n) as all | where all > 1`: "a filter above the first rows of keys or above a total",
		`source=logs | where status > 400`:                                                    "rows without named columns",
		`source=logs | stats count() by host | stats count()`:                                 "above an aggregate",
		`source=logs | dedup host | stats count()`:                                            "above the first rows of keys",
		`source=logs | dedup 2 host | fields host`:                                            "Dedup(host, count=2)",
		`source=logs | dedup host | where status > 1 | fields host`:                           "a filter above the first rows of keys",
		`source=logs | where md5(msg) = 'is' | stats count()`:                                 "the function md5",
		`source=logs | stats count() as n by host | sort - n | fields host`:                   "above a sort or a limit",
		`source=logs | fields - host | stats count()`:                                         "before the columns are named",
		`source=logs | fields h* | head 1`:                                                    "the column pattern h*",
	} {
		_, err := compile(text)
		if !errors.Is(err, ErrUnsupported) || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: got %v, want an unsupported error naming %q", text, err, want)
		}
	}
}
