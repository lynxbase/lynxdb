package flow

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

// answer runs a query and renders its rows, values joined by spaces and rows
// by semicolons.
func answer(t *testing.T, snap *colstore.Snapshot, text string) string {
	t.Helper()
	plan, err := Compile(text, Options{Now: testNow})
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

func TestStages(t *testing.T) {
	snap := testSnapshot(t)
	for _, c := range []struct{ name, text, want string }{
		{"search terms", `from logs disk | count`, "2"},
		{"field term", `from logs level=info NOT host=b | count`, "2"},
		{"where", `from logs | where status >= 500 and not has(msg, "node") | count`, "1"},
		{"where with a regular expression", `from logs | where matches(msg, r"[Dd]isk f(ull|ailure)") | count`, "2"},
		{"where with one in any case", `from logs | where matches(msg, r"(?i)^DISK\s") | count`, "2"},
		{"a regular expression on any field", `from logs | where matches(_raw, r"^(warn|not found)$") | count`, "1"},
		{"extend", `from logs | extend slow = dur > 1.0 | keep status, slow | sort -status | head 2`, "503 true; 500 true"},
		{"keep", `from logs | keep level, status | sort -status, level | head 1`, "error 503"},
		{"drop", `from logs | keep level, status, host | drop host | sort status, level | head 1`, "info 200"},
		{"rename", `from logs | keep level, status | rename status as code | sort -code | head 1`, "error 503"},
		{"stats", `from logs | stats count() as n, avg(dur) as mean, max(status) as top by level | sort level`, "error 2 2 503; info 2 0.75 200; warn 1 0.25 404"},
		{"picked rows", `from logs | stats latest(msg) as last, earliest(status) as first by host | sort host`, "a disk full on node 200; b Disk failure 404"},
		{"picked by a number", `from logs | stats arg_max(msg, dur) as slowest, arg_min(host, status) as ok, any_value(level) as some by level | sort level`,
			"error Disk failure a error; info request served a info; warn not found b warn"},
		{"where and sort by a picked value", `from logs | stats latest(status) as last, count() as n by host | where last >= 500 | sort -last`, "b 503 2; a 500 3"},
		{"function of a picked value", `from logs | stats latest(msg) as last by host | extend up = upper(last) | keep host, up | sort host`,
			"a DISK FULL ON NODE; b DISK FAILURE"},
		{"dedup", `from logs | dedup host | keep host, status, _time | sort host`,
			fmt.Sprintf("a 500 %d; b 503 %d", testNow.Add(-30*time.Minute).UnixNano(), testNow.Add(-150*time.Minute).UnixNano())},
		{"dedup of filtered rows", `from logs | where status < 500 | dedup level, host | extend code = status + 1 | keep level, code | sort level`, "info 201; warn 405"},
		{"estimated distinct count", `from logs | stats estdc(status) as codes, estdc(host) as hosts by level | sort level`, "error 2 2; info 1 1; warn 1 1"},
		{"conditional aggregates", `from logs | stats count(where status >= 500) as errors, sum(dur, where level == "info") as calm, count() as n, dc(msg, where status < 500) as texts by host | sort host`,
			"a 1 1.5 3 1; b 1 null 2 1"},
		{"share of the total", `from logs | stats count() as n by level | eventstats sum(n) as all | extend share = round(n * 100.0 / all, 1) | sort level`,
			"error 2 5 40; info 2 5 40; warn 1 5 20"},
		{"top", `from logs | top 2 host`, "a 3 60; b 2 40"},
		{"rare", `from logs | rare 1 level`, "warn 1 20"},
		{"math", `from logs | extend r = round(dur * 3, 1), f = floor(dur), c = ceil(dur), p = pow(status - 500, 2), q = sqrt(abs(status - 504)) | keep r, f, c, p, q | sort -r | head 1`,
			"7.5 2 3 9 1"},
		{"trimmed", `from logs | extend t = trim(msg, "dn"), at = index_of(msg, " ") | keep t, at | sort t | head 2`, "Disk failure 4; isk full on node 4"},
		{"where after stats", `from logs | stats count() as n by host | where n > 2 | sort host`, "a 3"},
		{"extend after stats", `from logs | stats sum(status) as total, count() as n | extend mean = total / n`, "1807 5 361"},
		{"count by", `from logs | count by level | sort level`, "error 2; info 2; warn 1"},
		{"sort and head", `from logs | keep status, host | sort host, -status | head 3`, "500 a; 200 a; 200 a"},
		{"limit", `from logs | keep status | sort status | limit 1`, "200"},
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
	plan, err := Compile(`from logs | stats p50(dur) as mid, perc(dur, 100) as top, p95(dur, where status < 500) as calm, p99(nope) as none by host | sort host`, Options{Now: testNow})
	if err != nil {
		t.Fatal(err)
	}
	res, err := exec.Run(context.Background(), snap, plan, exec.Options{Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	want := [][]float64{{1, 1.5, 0.5}, {0.25, 2.5, 0.25}}
	if res.Rows != len(want) {
		t.Fatalf("got %d rows, want %d", res.Rows, len(want))
	}
	for r, row := range want {
		for c, exact := range row {
			if v := res.Value(r, c+1); v.Kind != vec.KindFloat || v.Float < 0.99*exact || v.Float > 1.01*exact {
				t.Errorf("row %d, column %d: got %v, want %g within one percent", r, c+1, v, exact)
			}
		}
		if v := res.Value(r, 4); !v.IsNull() {
			t.Errorf("row %d: a percentile of no values is %v", r, v)
		}
	}
}

func TestTimeRangesCountFromNow(t *testing.T) {
	snap := testSnapshot(t)
	for text, want := range map[string]string{
		`from logs[-2h] | count`:                        "2",
		`from logs[-4h..-1h] | count`:                   "3",
		`from logs[-10h..-5h] | count`:                  "1",
		`from logs | where _time >= now() - 1h | count`: "1",
		fmt.Sprintf(`from logs | where _time < timestamp(%q) | count`, testNow.Add(-3*time.Hour).UTC().Format(time.RFC3339)): "2",
	} {
		if got := answer(t, snap, text); got != want {
			t.Errorf("%s: %s, want %s", text, got, want)
		}
	}
}

func TestRefusedQueries(t *testing.T) {
	for text, want := range map[string]string{
		`from logs | stats count() as n by host | eventstats avg(n) as m`:                   "eventstats avg(n)",
		`from logs | stats count() as n by host | eventstats sum(n) as all by host`:         "eventstats before stats or by a field",
		`from logs | stats count() as n by host | eventstats sum(n) as all | where all > 1`: "where after dedup or eventstats",
		`from logs | keep host | tail 2`:                                                    "stage tail",
		`from logs | stats perc(dur) by host`:                                               "aggregate perc(dur)",
		`from logs | stats perc(dur, status) by host`:                                       "aggregate perc(dur, status)",
		`from logs | stats count(status, host)`:                                             "aggregate count(status, host)",
		`from logs | stats mode(status) by host`:                                            "aggregate mode(status)",
		`from logs | where status > 400`:                                                    "events without keep",
		`from logs | stats count() by host | stats count()`:                                 "stats after stats",
		`from logs | dedup host | count`:                                                    "stats after dedup",
		`from logs | dedup 2 host | keep host`:                                              "dedup of more than one row per key",
		`from logs | dedup host | where status > 1 | keep host`:                             "where after dedup or eventstats",
		`from logs | dedup host`:                                                            "events without keep",
		`from logs | stats arg_max(msg) by host`:                                            "aggregate arg_max(msg)",
		`from logs | where extract(msg, "d(.*)k") == "is" | count`:                          "function extract",
		`from logs | stats count() as n by host | sort -n | keep host`:                      "keep after sort or head",
		`from logs | keep host | sort len(host)`:                                            "sort by len(host)",
		`from logs | drop host | count`:                                                     "drop before keep or stats",
		`from logs | keep h* | head 1`:                                                      "field pattern h*",
		`from logs[@d] | count`:                                                             "time range snap",
		`let $a = from logs; from $a | count`:                                               "let",
		`from logs | stats count() as n by host | where status > 1`:                         "field status is not a column of the aggregate",
	} {
		_, err := Compile(text, Options{Now: testNow})
		if !errors.Is(err, ErrUnsupported) || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: got %v, want an unsupported error naming %q", text, err, want)
		}
	}
	if _, err := Compile(`from logs | where (`, Options{}); err == nil || errors.Is(err, ErrUnsupported) {
		t.Errorf("a query that does not parse: got %v", err)
	}
}
