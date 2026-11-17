package exec_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
)

// The JSONBench queries and searches over the Bluesky event stream, stated
// as plans.

const (
	benchTimeField = "time_us"
	benchBlockRows = 16384
)

var benchSortKey = []string{"kind", "commit.operation", "commit.collection", "did"}

// The regular expressions of the benchmark: one that every match of holds a
// literal, one made of character classes alone, and one without regard to
// letter case.
const (
	benchRegexLiteral = `[Gg]ood (morning|night)`
	benchRegexClasses = `\d{5}`
	benchRegexAnyCase = `(?i)crypto\w*`
)

func regex(expr string) tokenfilter.Pattern {
	p, err := tokenfilter.Regexp(expr)
	if err != nil {
		panic(err)
	}

	return p
}

type benchPlan struct {
	name string
	plan exec.Node
	// ordered is set when the plan's sort makes the row order unique.
	ordered bool
	// text is the query in PPL, when the language can state it.
	text string
	// estimated is set when the answer may differ between layouts.
	estimated bool
}

// benchTexts states the benchmark plans in PPL, by plan name. The language
// searches words without regard to letter case, so only the plans that do
// the same have a text. A text names its columns in the order of its plan.
func benchTexts(win benchWindows) map[string]string {
	const created = `source=bluesky | where kind = 'commit' and commit.operation = 'create'`
	const text = "commit.record.text"
	stamp := func(ns int64) string { return time.Unix(0, ns).UTC().Format(time.RFC3339Nano) }
	within := func(lo, hi int64) string {
		return fmt.Sprintf(`_time >= '%s' and _time <= '%s'`, stamp(lo), stamp(hi))
	}
	byCount := ` | stats count() as count by commit.collection | fields commit.collection, count | sort - count, commit.collection`
	posts := created + ` and commit.collection = 'app.bsky.feed.post'`

	return map[string]string{
		"Q1": `source=bluesky` + byCount,
		"Q2": created + ` | stats count() as count, distinct_count(did) as users by commit.collection` +
			` | fields commit.collection, count, users | sort - count, commit.collection`,
		"Q2 estimated users": created + ` | stats count() as count, distinct_count_approx(did) as users by commit.collection` +
			` | fields commit.collection, count, users | sort - count, commit.collection`,
		"Q3": created + ` and commit.collection in ('app.bsky.feed.like', 'app.bsky.feed.post', 'app.bsky.feed.repost')` +
			` | eval hour = hour(_time) | stats count() as count by commit.collection, hour | fields commit.collection, hour, count | sort hour, commit.collection`,
		"Q4":                        posts + ` | stats min(_time) as first by did | fields did, first | sort first, did | head 3`,
		"S1 rare token":             `source=bluesky | where match(commit.record.subject.uri, 'sc7btuzqdaepksx43mr74pma') | stats count()`,
		"S2 any case":               `source=bluesky | where match(` + text + `, 'love') | stats count()`,
		"S3 any case":               `source=bluesky | where match_phrase(` + text + `, 'good morning') | stats count()`,
		"S4 any case":               `source=bluesky | where match_phrase_prefix(` + text + `, 'crypto') | stats count()`,
		"S5 substring":              `source=bluesky | where like(` + text + `, '%ware%', true) | stats count()`,
		"S5 any case":               `source=bluesky | where like(` + text + `, '%ware%') | stats count()`,
		"S6 field equals":           `source=bluesky | where did = 'did:plc:yj3sjq3blzpynh27cumnp5ks' | stats count()`,
		"S7 unique id":              `source=bluesky | where commit.cid = 'bafyreicsj6qpl6dreic6ekdjxvdjltncngl52x3jddf6dxhg2gwo35iqhi' | stats count()`,
		"R1 regex with a literal":   `source=bluesky | where regexp_match(` + text + `, '` + benchRegexLiteral + `') | stats count()`,
		"R2 regex of classes":       `source=bluesky | where regexp_match(` + text + `, '` + benchRegexClasses + `') | stats count()`,
		"R3 regex, any case":        `source=bluesky | where regexp_match(` + text + `, '` + benchRegexAnyCase + `') | stats count()`,
		"A1 rare token, any field":  `source=bluesky sc7btuzqdaepksx43mr74pma | stats count()`,
		"A3 any field, any case":    `source=bluesky love | stats count()`,
		"T1 count in 10 minutes":    `source=bluesky | where ` + within(win.tenLo, win.tenHi) + ` | stats count()`,
		"T3 groups in 1 hour":       `source=bluesky | where ` + within(win.hourLo, win.hourHi) + byCount,
		"L1 latest post per user":   posts + ` | stats latest(` + text + `) as text, max(_time) as at by did | fields did, text, at | sort - at, did | head 5`,
		"K1 top 10 users by events": `source=bluesky | stats count() as count by did | fields did, count | sort - count, did | head 10`,
		"K2 top 10 users and their share": `source=bluesky | stats count() as count by did | eventstats sum(count) as total` +
			` | eval percent = round(count * 100.0 / total, 2) | fields did, count, percent | sort - count, did | head 10`,
		"L2 latest post of every user": posts + ` | stats latest(` + text + `) as text by did | fields did, text`,
		"T4 latest 20 of a user": `source=bluesky | where did = 'did:plc:yj3sjq3blzpynh27cumnp5ks' | eval event = commit.collection` +
			` | fields _time, event | sort - _time, event | head 20`,
		"N1 latest 100 events":             `source=bluesky | sort - _time | head 100`,
		"N2 latest 100 events with a word": `source=bluesky love | sort - _time | head 100`,
		"N3 latest 100 posts": `source=bluesky | where commit.collection = 'app.bsky.feed.post' | sort - _time | head 100` +
			` | fields _time, did, commit.record.text`,
	}
}

// benchWindows holds the time windows of the time-bounded queries, inclusive
// ranges of row timestamps in nanoseconds.
type benchWindows struct {
	tenLo, tenHi, hourLo, hourHi int64
}

// benchWindow returns the window named by env as "lo,hi" in microseconds, lo
// inclusive and hi exclusive, or else a window of the given width around the
// time that half of the snapshot's rows precede.
func benchWindow(t testing.TB, snap *colstore.Snapshot, env string, width time.Duration) (lo, hi int64) {
	t.Helper()
	if v := os.Getenv(env); v != "" {
		a, b, ok := strings.Cut(v, ",")
		from, err1 := strconv.ParseInt(a, 10, 64)
		to, err2 := strconv.ParseInt(b, 10, 64)
		if !ok || err1 != nil || err2 != nil || to <= from {
			t.Fatalf("%s=%q is not lo,hi in microseconds", env, v)
		}

		return from * 1000, to*1000 - 1
	}
	type span struct {
		at   int64
		rows int
	}
	var spans []span
	total := 0
	for _, h := range snap.Parts() {
		for i := 0; i < h.NumBlocks(); i++ {
			rows, first, last := h.BlockStats(i)
			spans = append(spans, span{at: first/2 + last/2, rows: rows})
			total += rows
		}
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].at < spans[j].at })
	mid, seen := int64(0), 0
	for _, s := range spans {
		if seen += s.rows; seen >= total/2 {
			mid = s.at

			break
		}
	}
	// Whole minutes, so that the same window can be asked of other systems.
	mid -= mid % int64(time.Minute)

	return mid - int64(width)/2, mid + int64(width)/2 - 1
}

func windowsOf(t testing.TB, snap *colstore.Snapshot) benchWindows {
	t.Helper()
	var w benchWindows
	w.tenLo, w.tenHi = benchWindow(t, snap, "LYNXDB_BENCH_T10_US", 10*time.Minute)
	w.hourLo, w.hourHi = benchWindow(t, snap, "LYNXDB_BENCH_T60_US", time.Hour)

	return w
}

func benchPlans(win benchWindows) []benchPlan {
	collection := named("event", str("commit.collection"))
	created := []*exec.Expr{exec.Eq(str("kind"), exec.Str("commit")), exec.Eq(str("commit.operation"), exec.Str("create"))}
	isCollection := func(name string) *exec.Expr { return exec.Eq(str("commit.collection"), exec.Str(name)) }
	count := exec.Agg{Name: "count", Func: exec.Count}
	byCount := []exec.Order{{Column: "count", Desc: true}, {Column: "event"}}
	posts := &exec.Scan{Filter: exec.And(append(created[:2:2], isCollection("app.bsky.feed.post"))...)}
	search := func(filter *exec.Expr) exec.Node {
		return &exec.Aggregate{Input: &exec.Scan{Filter: filter}, Aggs: []exec.Agg{count}}
	}
	text := str("commit.record.text")
	within := func(lo, hi int64) []*exec.Expr {
		return []*exec.Expr{exec.Ge(exec.Time(), exec.Int(lo)), exec.Le(exec.Time(), exec.Int(hi))}
	}

	plans := []benchPlan{
		{name: "Q1", plan: &exec.Sort{Input: &exec.Aggregate{Input: &exec.Scan{}, Keys: []exec.Named{collection}, Aggs: []exec.Agg{count}}, By: byCount}, ordered: true},
		{name: "Q2", plan: &exec.Sort{Input: &exec.Aggregate{
			Input: &exec.Scan{Filter: exec.And(created...)},
			Keys:  []exec.Named{collection},
			Aggs:  []exec.Agg{count, {Name: "users", Func: exec.CountDistinct, Arg: str("did")}},
		}, By: byCount}, ordered: true},
		{name: "Q2 estimated users", plan: &exec.Sort{Input: &exec.Aggregate{
			Input: &exec.Scan{Filter: exec.And(created...)},
			Keys:  []exec.Named{collection},
			Aggs:  []exec.Agg{count, {Name: "users", Func: exec.ApproxDistinct, Arg: str("did")}},
		}, By: byCount}, ordered: true, estimated: true},
		{name: "Q3", plan: &exec.Sort{Input: &exec.Aggregate{
			Input: &exec.Scan{Filter: exec.And(append(created[:2:2], exec.Or(isCollection("app.bsky.feed.like"),
				isCollection("app.bsky.feed.post"), isCollection("app.bsky.feed.repost")))...)},
			Keys: []exec.Named{collection, named("hour", exec.HourOfDay(exec.Time()))},
			Aggs: []exec.Agg{count},
		}, By: []exec.Order{{Column: "hour"}, {Column: "event"}}}, ordered: true},
		{name: "Q4", plan: &exec.Sort{Input: &exec.Aggregate{
			Input: posts,
			Keys:  []exec.Named{named("did", str("did"))},
			Aggs:  []exec.Agg{{Name: "first", Func: exec.Min, Arg: exec.Time()}},
		}, By: []exec.Order{{Column: "first"}, {Column: "did"}}, Limit: 3}, ordered: true},
		{name: "Q5", plan: &exec.Sort{Input: &exec.Project{
			Input: &exec.Aggregate{
				Input: posts,
				Keys:  []exec.Named{named("did", str("did"))},
				Aggs:  []exec.Agg{{Name: "first", Func: exec.Min, Arg: exec.Time()}, {Name: "last", Func: exec.Max, Arg: exec.Time()}},
			},
			Exprs: []exec.Named{named("did", anyOf("did")), named("span_ms", exec.Sub(
				exec.Div(anyOf("last"), exec.Int(1_000_000)), exec.Div(anyOf("first"), exec.Int(1_000_000))))},
		}, By: []exec.Order{{Column: "span_ms", Desc: true}, {Column: "did"}}, Limit: 3}, ordered: true},
		{name: "S1 rare token", plan: search(exec.Match(str("commit.record.subject.uri"), tokenfilter.Word("sc7btuzqdaepksx43mr74pma"))), ordered: true},
		{name: "S2 common token", plan: search(exec.Match(text, tokenfilter.Word("love"))), ordered: true},
		{name: "S3 phrase", plan: search(exec.Match(text, tokenfilter.Word("Good morning"))), ordered: true},
		{name: "S4 prefix", plan: search(exec.Match(text, tokenfilter.Prefix("crypto"))), ordered: true},
		{name: "S5 substring", plan: search(exec.Match(text, tokenfilter.Substring("ware"))), ordered: true},
		{name: "S6 field equals", plan: search(exec.Eq(str("did"), exec.Str("did:plc:yj3sjq3blzpynh27cumnp5ks"))), ordered: true},
		{name: "S7 unique id", plan: search(exec.Eq(str("commit.cid"), exec.Str("bafyreicsj6qpl6dreic6ekdjxvdjltncngl52x3jddf6dxhg2gwo35iqhi"))), ordered: true},
		{name: "R1 regex with a literal", plan: search(exec.Match(text, regex(benchRegexLiteral))), ordered: true},
		{name: "R2 regex of classes", plan: search(exec.Match(text, regex(benchRegexClasses))), ordered: true},
		{name: "R3 regex, any case", plan: search(exec.Match(text, regex(benchRegexAnyCase))), ordered: true},
		{name: "A1 rare token, any field", plan: search(exec.MatchAny(tokenfilter.Word("sc7btuzqdaepksx43mr74pma"))), ordered: true},
		{name: "A2 common token, any field", plan: search(exec.MatchAny(tokenfilter.Word("love"))), ordered: true},
		{name: "A3 any field, any case", plan: search(exec.MatchAny(tokenfilter.Word("love").IgnoreCase())), ordered: true},
		{name: "S2 any case", plan: search(exec.Match(text, tokenfilter.Word("love").IgnoreCase())), ordered: true},
		{name: "S3 any case", plan: search(exec.Match(text, tokenfilter.Word("good morning").IgnoreCase())), ordered: true},
		{name: "S4 any case", plan: search(exec.Match(text, tokenfilter.Prefix("crypto").IgnoreCase())), ordered: true},
		{name: "S5 any case", plan: search(exec.Match(text, tokenfilter.Substring("ware").IgnoreCase())), ordered: true},
		{name: "T1 count in 10 minutes", plan: search(exec.And(within(win.tenLo, win.tenHi)...)), ordered: true},
		{name: "T2 token in 10 minutes", plan: search(exec.And(append(within(win.tenLo, win.tenHi), exec.Match(text, tokenfilter.Word("love")))...)), ordered: true},
		{name: "T3 groups in 1 hour", plan: &exec.Sort{Input: &exec.Aggregate{Input: &exec.Scan{Filter: exec.And(within(win.hourLo, win.hourHi)...)},
			Keys: []exec.Named{collection}, Aggs: []exec.Agg{count}}, By: byCount}, ordered: true},
		{name: "T4 latest 20 of a user", plan: &exec.Sort{Input: &exec.Project{
			Input: &exec.Scan{Filter: exec.Eq(str("did"), exec.Str("did:plc:yj3sjq3blzpynh27cumnp5ks"))},
			Exprs: []exec.Named{named("time", exec.Time()), collection},
		}, By: []exec.Order{{Column: "time", Desc: true}, {Column: "event"}}, Limit: 20}, ordered: true},
	}
	latestText := exec.Agg{Name: "text", Func: exec.Latest, Arg: text}
	byUser := &exec.Aggregate{Input: &exec.Scan{}, Keys: []exec.Named{named("did", str("did"))}, Aggs: []exec.Agg{count}}
	topUsers := []exec.Order{{Column: "count", Desc: true}, {Column: "did"}}
	// The milliseconds of a row's time within its minute.
	withinMinute := exec.Div(exec.Mod(exec.Time(), exec.Int(60_000_000_000)), exec.Int(1_000_000))
	plans = append(plans,
		benchPlan{name: "L1 latest post per user", plan: &exec.Sort{Input: &exec.Aggregate{Input: posts, Keys: []exec.Named{named("did", str("did"))},
			Aggs: []exec.Agg{latestText, {Name: "at", Func: exec.Max, Arg: exec.Time()}}},
			By: []exec.Order{{Column: "at", Desc: true}, {Column: "did"}}, Limit: 5}, ordered: true},
		benchPlan{name: "P1 percentiles by collection", plan: &exec.Sort{Input: &exec.Aggregate{Input: &exec.Scan{}, Keys: []exec.Named{collection},
			Aggs: []exec.Agg{{Name: "p50", Func: exec.Percentile, Arg: withinMinute, P: 50}, {Name: "p95", Func: exec.Percentile, Arg: withinMinute, P: 95},
				{Name: "p99", Func: exec.Percentile, Arg: withinMinute, P: 99}}},
			By: []exec.Order{{Column: "event"}}}, ordered: true},
		benchPlan{name: "K1 top 10 users by events", plan: &exec.Sort{Input: byUser, By: topUsers, Limit: 10}, ordered: true},
		benchPlan{name: "K2 top 10 users and their share", plan: &exec.Sort{Input: &exec.Project{Input: byUser, Exprs: []exec.Named{
			named("did", exec.Col("did", exec.KindAny)), named("count", exec.Col("count", exec.KindAny)),
			named("percent", exec.Round(exec.Div(exec.Mul(exec.Col("count", exec.KindAny), exec.Float(100)), exec.Total(exec.Col("count", exec.KindAny))), exec.Int(2))),
		}}, By: topUsers, Limit: 10}, ordered: true},
		benchPlan{name: "L2 latest post of every user", plan: &exec.Aggregate{Input: posts, Keys: []exec.Named{named("did", str("did"))},
			Aggs: []exec.Agg{latestText}}},
	)
	// The latest events, as a log viewer asks for them: whole events, and
	// three fields of the latest posts.
	latest := func(filter *exec.Expr) exec.Node {
		return &exec.Sort{Input: &exec.Project{Input: &exec.Scan{Filter: filter}, Exprs: []exec.Named{named("_time", exec.Time())}, Rest: true},
			By: []exec.Order{{Column: "_time", Desc: true}}, Limit: 100}
	}
	plans = append(plans,
		benchPlan{name: "N1 latest 100 events", plan: latest(nil)},
		benchPlan{name: "N2 latest 100 events with a word", plan: latest(exec.MatchAny(tokenfilter.Word("love").IgnoreCase()))},
		benchPlan{name: "N3 latest 100 posts", plan: &exec.Sort{Input: &exec.Project{Input: &exec.Scan{Filter: isCollection("app.bsky.feed.post")},
			Exprs: []exec.Named{named("_time", exec.Time()), named("did", str("did")), named("text", text)}},
			By: []exec.Order{{Column: "_time", Desc: true}}, Limit: 100}},
	)
	texts := benchTexts(win)
	for i := range plans {
		plans[i].text = texts[plans[i].name]
	}

	return plans
}

// loadArrival shreds data on every core, one part per worker, in the order
// the lines arrive.
func loadArrival(t testing.TB, data []byte, workers int) *colstore.Table {
	t.Helper()
	chunks := splitLines(data, workers)
	handles := make([]*colstore.Handle, len(chunks))
	var wg sync.WaitGroup
	for i, chunk := range chunks {
		wg.Add(1)
		go func(i int, chunk []byte) {
			defer wg.Done()
			var buf bytes.Buffer
			buf.Grow(len(chunk) / 3)
			w := part.NewWriter(&buf, part.Options{})
			batch := shred.Get(shred.Options{TimeField: benchTimeField})
			defer shred.Put(batch)
			for len(chunk) > 0 {
				var lines []byte
				lines, chunk = shred.SplitLines(chunk, benchBlockRows)
				batch.AddNDJSON(lines, 0, nil)
				if err := w.WriteBatch(batch); err != nil {
					t.Error(err)
				}
				batch.Reset()
			}
			if err := w.Close(); err != nil {
				t.Error(err)
			}
			p, err := part.Open(buf.Bytes())
			if err != nil {
				t.Error(err)

				return
			}
			handles[i] = colstore.NewHandle(p, nil)
		}(i, chunk)
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}
	tbl := colstore.NewTable()
	if err := tbl.Apply(handles, nil); err != nil {
		t.Fatal(err)
	}

	return tbl
}

// splitLines cuts data into at most n pieces of similar size that end at
// line ends.
func splitLines(data []byte, n int) [][]byte {
	var chunks [][]byte
	for rest := data; len(rest) > 0; {
		size := min(len(data)/n+1, len(rest))
		if size < len(rest) {
			if nl := bytes.IndexByte(rest[size:], '\n'); nl >= 0 {
				size += nl + 1
			} else {
				size = len(rest)
			}
		}
		chunks = append(chunks, rest[:size])
		rest = rest[size:]
	}

	return chunks
}

// loadOrdered shreds data into runs sorted by key and merges them into
// parts with disjoint key ranges, so that the table is one ordered run.
func loadOrdered(t testing.TB, data []byte, parts int, key []string) *colstore.Table {
	t.Helper()

	return mergedTable(t, data, parts, key, benchTimeField, benchBlockRows)
}

// mergedTable is loadOrdered for documents with the given time field, in
// blocks of blockRows rows.
func mergedTable(t testing.TB, data []byte, parts int, key []string, timeField string, blockRows int) *colstore.Table {
	t.Helper()
	opts := part.Options{Ordered: true, SortKey: key}
	chunks := splitLines(data, parts)
	runs := make([]*part.Part, len(chunks))
	var wg sync.WaitGroup
	for i, chunk := range chunks {
		wg.Add(1)
		go func(i int, chunk []byte) {
			defer wg.Done()
			all := shred.NewBatch(shred.Options{TimeField: timeField})
			all.AddNDJSON(chunk, 0, nil)
			all.SortBy(key)
			var buf bytes.Buffer
			w := part.NewWriter(&buf, opts)
			block := shred.Get(shred.Options{})
			defer shred.Put(block)
			for from := 0; from < all.Rows; from += blockRows {
				block.Reset()
				all.CopyRows(block, from, min(from+blockRows, all.Rows))
				if err := w.WriteBatch(block); err != nil {
					t.Error(err)

					return
				}
			}
			if err := w.Close(); err != nil {
				t.Error(err)

				return
			}
			p, err := part.Open(buf.Bytes())
			if err != nil {
				t.Error(err)

				return
			}
			runs[i] = p
		}(i, chunk)
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}
	cuts := part.SplitKeys(runs, parts)
	handles := make([]*colstore.Handle, len(cuts)+1)
	for i := range handles {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var lo, hi []byte
			if i > 0 {
				lo = cuts[i-1]
			}
			if i < len(cuts) {
				hi = cuts[i]
			}
			var buf bytes.Buffer
			w := part.NewWriter(&buf, opts)
			if err := part.MergeRange(w, runs, blockRows, lo, hi); err != nil {
				t.Error(err)

				return
			}
			if err := w.Close(); err != nil {
				t.Error(err)

				return
			}
			p, err := part.Open(buf.Bytes())
			if err != nil {
				t.Error(err)

				return
			}
			handles[i] = colstore.NewHandle(p, nil)
		}(i)
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}
	tbl := colstore.NewTable()
	if err := tbl.Apply(handles, nil); err != nil {
		t.Fatal(err)
	}

	return tbl
}

func benchData(t *testing.T) []byte {
	t.Helper()
	path := os.Getenv("LYNXDB_BENCH_NDJSON")
	if path == "" {
		t.Skip("set LYNXDB_BENCH_NDJSON to a Bluesky NDJSON file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	return data
}

// TestJSONBenchReference runs the benchmark plans over the first documents
// of the file named by LYNXDB_BENCH_NDJSON, 100000 or LYNXDB_BENCH_SAMPLE of
// them, in arrival order and ordered by the sort key, and requires the
// answers of the reference evaluator.
func TestJSONBenchReference(t *testing.T) {
	data := benchData(t)
	sample := 100_000
	if s := os.Getenv("LYNXDB_BENCH_SAMPLE"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 {
			t.Fatalf("LYNXDB_BENCH_SAMPLE=%q", s)
		}
		sample = n
	}
	end := 0
	var rows []refRow
	for len(rows) < sample && end < len(data) {
		nl := bytes.IndexByte(data[end:], '\n')
		if nl < 0 {
			nl = len(data) - end
		}
		if line := bytes.TrimSpace(data[end : end+nl]); len(line) > 0 {
			rows = append(rows, flatten(t, line, benchTimeField))
		}
		end = min(end+nl+1, len(data))
	}
	data = data[:end]
	cores := runtime.GOMAXPROCS(0)
	tables := map[string]*colstore.Table{
		"arrival order": loadArrival(t, data, cores),
		"ordered":       loadOrdered(t, data, cores, benchSortKey),
	}
	for name, tbl := range tables {
		snap := tbl.Snapshot()
		for _, q := range benchPlans(windowsOf(t, snap)) {
			for _, workers := range []int{cores, 1} {
				t.Run(fmt.Sprintf("%s/%s/%d", name, q.name, workers), func(t *testing.T) {
					if q.estimated && name == "ordered" {
						t.Skip("in key order an estimate is the exact count, which the reference does not give")
					}
					res := check(t, snap, rows, q.plan, q.ordered, workers)
					if q.text == "" {
						return
					}
					plan, err := pplPlan(q.text, time.Time{})
					if err != nil {
						t.Fatalf("%s: %v", q.text, err)
					}
					stated := check(t, snap, rows, plan, q.ordered, workers)
					if got, want := fmt.Sprint(resultRows(stated)), fmt.Sprint(resultRows(res)); q.ordered && got != want {
						t.Fatalf("%s\n gives %s\n the plan %s", q.text, got, want)
					}
				})
			}
		}
		snap.Release()
		tbl.Close()
	}
}

// answer renders a result the way the hand-written benchmark prints its own.
func answer(name string, res *exec.Result) string {
	var b strings.Builder
	if strings.HasPrefix(name, "L2") {
		// Too many rows to print: their number and the bytes of their texts.
		total := 0
		for i := 0; i < res.Rows; i++ {
			total += len(res.Value(i, 1).Bytes)
		}

		return fmt.Sprintf("%d rows, %d bytes", res.Rows, total)
	}
	if strings.HasPrefix(name, "N") {
		// Events with the same time come in any order: the number of rows
		// and of values, and the times of the first and the last.
		values := 0
		for i := 0; i < res.Rows; i++ {
			for c := range res.Columns {
				if !res.Value(i, c).IsNull() {
					values++
				}
			}
		}
		if res.Rows == 0 {
			return "0 rows"
		}

		return fmt.Sprintf("%d rows, %d values, from %d to %d", res.Rows, values, res.Value(0, 0).Int/1000, res.Value(res.Rows-1, 0).Int/1000)
	}
	for i := 0; i < res.Rows; i++ {
		if i > 0 {
			b.WriteByte(' ')
		}
		row := res.Row(i, nil)
		switch {
		case strings.HasPrefix(name, "T3"):
			fmt.Fprintf(&b, "%s=%d", row[0].Bytes, row[1].Int)

			continue
		case strings.HasPrefix(name, "T4"):
			fmt.Fprintf(&b, "%d:%s", row[0].Int/1000, row[1].Bytes)

			continue
		case strings.HasPrefix(name, "K1"):
			fmt.Fprintf(&b, "%s=%d", row[0].Bytes, row[1].Int)

			continue
		case strings.HasPrefix(name, "K2"):
			fmt.Fprintf(&b, "%s=%d/%.2f%%", row[0].Bytes, row[1].Int, row[2].Float)

			continue
		case strings.HasPrefix(name, "P1"):
			fmt.Fprintf(&b, "%s=%.5g/%.5g/%.5g", row[0].Bytes, row[1].Float, row[2].Float, row[3].Float)

			continue
		case strings.HasPrefix(name, "L1"):
			fmt.Fprintf(&b, "%s@%d:%d", row[0].Bytes, row[2].Int/1000, len(row[1].Bytes))

			continue
		}
		switch name {
		case "Q1":
			fmt.Fprintf(&b, "%s=%d", row[0].Bytes, row[1].Int)
		case "Q2", "Q2 estimated users":
			fmt.Fprintf(&b, "%s=%d/%d", row[0].Bytes, row[1].Int, row[2].Int)
		case "Q3":
			fmt.Fprintf(&b, "%s@%d=%d", row[0].Bytes, row[1].Int, row[2].Int)
		case "Q4":
			fmt.Fprintf(&b, "%s@%d", row[0].Bytes, row[1].Int/1000)
		case "Q5":
			fmt.Fprintf(&b, "%s=%d", row[0].Bytes, row[1].Int)
		default:
			fmt.Fprintf(&b, "%d", row[0].Int)
		}
	}

	return b.String()
}

// The answers ClickHouse gives for the first benchmark file of 1,000,000
// rows.
var benchAnswers = map[string]string{
	"Q1": "app.bsky.feed.like=448944 app.bsky.graph.follow=360374 app.bsky.feed.post=90816 app.bsky.feed.repost=58540 " +
		"app.bsky.graph.block=14040 app.bsky.actor.profile=11762 app.bsky.graph.listitem=8103 =5328 " +
		"app.bsky.graph.listblock=895 app.bsky.graph.starterpack=405 app.bsky.graph.list=356 app.bsky.feed.threadgate=255 " +
		"app.bsky.feed.postgate=104 app.bsky.feed.generator=74 app.bsky.labeler.service=4",
	"Q2": "app.bsky.feed.like=444523/117617 app.bsky.graph.follow=337978/63957 app.bsky.feed.post=86812/50464 " +
		"app.bsky.feed.repost=56993/26581 app.bsky.graph.block=13838/5785 app.bsky.graph.listitem=7568/1078 " +
		"app.bsky.actor.profile=5337/5337 app.bsky.graph.listblock=860/449 app.bsky.graph.list=259/218 " +
		"app.bsky.feed.threadgate=228/196 app.bsky.graph.starterpack=104/101 app.bsky.feed.postgate=101/82 app.bsky.feed.generator=10/9",
	"Q3": "app.bsky.feed.like@16=444523 app.bsky.feed.post@16=86812 app.bsky.feed.repost@16=56993",
	"Q4": "did:plc:yj3sjq3blzpynh27cumnp5ks@1732206349000167 did:plc:l5o3qjrmfztir54cpwlv2eme@1732206349001905 did:plc:s4bwqchfzm6gjqfeb6mexgbu@1732206349003907",
	"Q5": "did:plc:tsyymlun4eqjuw7hqrhmwagd=813007 did:plc:3ug235sfy2pz7cawmpsftb65=811602 did:plc:doxhhgtxqiv47tmcovpbcqai=811404",
}

// timeRuns runs fn n times and returns the durations in ascending order and
// the process CPU time of the shortest run.
func timeRuns(n int, fn func() error) (took []time.Duration, cpu time.Duration, err error) {
	took = make([]time.Duration, n)
	best := time.Duration(-1)
	for i := range took {
		cpuBefore, begin := cpuTime(), time.Now()
		if err := fn(); err != nil {
			return nil, 0, err
		}
		took[i] = time.Since(begin)
		if used := cpuTime() - cpuBefore; best < 0 || took[i] < best {
			best, cpu = took[i], used
		}
	}
	sort.Slice(took, func(i, j int) bool { return took[i] < took[j] })

	return took, cpu, nil
}

func millis(d time.Duration) float64 { return float64(d.Microseconds()) / 1e3 }

// TestJSONBenchPlans times the benchmark plans on every core over the file
// named by LYNXDB_BENCH_NDJSON, loaded in arrival order and ordered by the
// sort key, and over the table in LYNXDB_BENCH_DIR when that is set; with
// the directory alone it needs no file. It prints the best of
// LYNXDB_BENCH_RUNS runs (7 by default) and the answers. LYNXDB_BENCH_T10_US
// and LYNXDB_BENCH_T60_US set the windows of the time-bounded plans as
// "lo,hi" in microseconds; by default they lie around the middle of the data.
// LYNXDB_BENCH_QUERY keeps the plans whose name contains it,
// LYNXDB_BENCH_LAYOUT the layouts whose name does, and LYNXDB_BENCH_PROFILE
// names a directory that receives one CPU profile per plan and layout.
func TestJSONBenchPlans(t *testing.T) {
	var data []byte
	if os.Getenv("LYNXDB_BENCH_DIR") == "" || os.Getenv("LYNXDB_BENCH_NDJSON") != "" {
		data = benchData(t)
	}
	cores := runtime.GOMAXPROCS(0)
	runs := 7
	if s := os.Getenv("LYNXDB_BENCH_RUNS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 {
			t.Fatalf("LYNXDB_BENCH_RUNS=%q", s)
		}
		runs = n
	}
	only, onlyLayout, profileDir := os.Getenv("LYNXDB_BENCH_QUERY"), os.Getenv("LYNXDB_BENCH_LAYOUT"), os.Getenv("LYNXDB_BENCH_PROFILE")

	type table struct {
		name string
		snap *colstore.Snapshot
	}
	var tables []table
	add := func(name string, load func() *colstore.Table) {
		if !strings.Contains(name, onlyLayout) {
			return
		}
		tbl := load()
		t.Cleanup(tbl.Close)
		snap := tbl.Snapshot()
		t.Cleanup(snap.Release)
		tables = append(tables, table{name, snap})
	}
	if data != nil {
		add("arrival", func() *colstore.Table { return loadArrival(t, data, cores) })
		add("ordered", func() *colstore.Table { return loadOrdered(t, data, cores, benchSortKey) })
	}
	if dir := os.Getenv("LYNXDB_BENCH_DIR"); dir != "" && strings.Contains("disk", onlyLayout) {
		d, err := colstore.OpenDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = d.Close() })
		snap := d.Table().Snapshot()
		t.Cleanup(snap.Release)
		tables = append(tables, table{"disk", snap})
	}

	if len(tables) == 0 {
		t.Skip("no layout selected")
	}
	win := windowsOf(t, tables[0].snap)
	t.Logf("10-minute window %d..%d us, 1-hour window %d..%d us", win.tenLo/1000, (win.tenHi+1)/1000, win.hourLo/1000, (win.hourHi+1)/1000)
	answers := map[string]string{}
	for _, tbl := range tables {
		var rows uint64
		blocks := 0
		for _, h := range tbl.snap.Parts() {
			rows += h.Rows
			blocks += h.NumBlocks()
		}
		t.Logf("%s: %d rows in %d parts / %d blocks", tbl.name, rows, len(tbl.snap.Parts()), blocks)
		for _, q := range benchPlans(win) {
			if !strings.Contains(q.name, only) {
				continue
			}
			var res *exec.Result
			// Timed as a query met for the first time: what earlier runs
			// learned about the blocks is reported apart, below.
			run := func() (err error) {
				res, err = exec.Run(context.Background(), tbl.snap, q.plan, exec.Options{Workers: cores, Forgetful: true})

				return err
			}
			if profileDir != "" {
				f, err := os.Create(fmt.Sprintf("%s/%s_%s.prof", profileDir, strings.Fields(q.name)[0], tbl.name))
				if err != nil {
					t.Fatal(err)
				}
				if err := pprof.StartCPUProfile(f); err != nil {
					t.Fatal(err)
				}
			}
			took, cpu, err := timeRuns(runs, run)
			if profileDir != "" {
				pprof.StopCPUProfile()
			}
			if err != nil {
				t.Fatalf("%s: %v", q.name, err)
			}
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			if err := run(); err != nil {
				t.Fatal(err)
			}
			runtime.ReadMemStats(&after)
			s := res.Stats
			how := "hashed"
			if s.Ordered {
				how = "in key order"
			}
			t.Logf("%-8s %-22s %8.2f ms on %d cores (median %.2f, cpu %.1f, holds %.1f MB), %d heap allocations (%.2f MB); %s; %d blocks opened, %d counted from the index, skipped: %d by key, %d by tokens, %d by time, %d by statistics; %d blocks pass the filter with %d rows",
				tbl.name, q.name, millis(took[0]), cores, millis(took[runs/2]), millis(cpu), float64(s.HeldBytes)/1e6,
				after.Mallocs-before.Mallocs, float64(after.TotalAlloc-before.TotalAlloc)/1e6, how,
				s.Blocks, s.CountedByIndex, s.SkippedByKey, s.SkippedByToken, s.SkippedByTime, s.SkippedByStats, s.BlocksMatched, s.RowsMatched)
			got := answer(q.name, res)
			if want, ok := benchAnswers[q.name]; ok && rows == 1_000_000 && got != want {
				t.Errorf("%s %s\n got  %s\n want %s", tbl.name, q.name, got, want)
			}
			again := func() (err error) {
				res, err = exec.Run(context.Background(), tbl.snap, q.plan, exec.Options{Workers: cores})

				return err
			}
			if err := again(); err != nil {
				t.Fatal(err)
			}
			if took, cpu, err := timeRuns(runs, again); err != nil {
				t.Fatal(err)
			} else if res.Stats.SkippedAsEmpty > 0 {
				t.Logf("%-8s %-22s %8.2f ms on %d cores (median %.2f, cpu %.1f) run again: %d blocks opened, %d left out as found empty",
					tbl.name, q.name, millis(took[0]), cores, millis(took[runs/2]), millis(cpu), res.Stats.Blocks, res.Stats.SkippedAsEmpty)
			}
			if repeated := answer(q.name, res); repeated != got {
				t.Errorf("%s %s run again answers %s, at first %s", tbl.name, q.name, repeated, got)
			}
			if prev, ok := answers[q.name]; ok && prev != got && !q.estimated {
				t.Errorf("%s %s answers %s, another layout %s", tbl.name, q.name, got, prev)
			}
			if _, ok := answers[q.name]; !ok || q.estimated {
				t.Logf("%-8s %-22s answer: %s", "", q.name, got)
			}
			answers[q.name] = got
			if q.text == "" {
				continue
			}
			var stated exec.Node
			compiled, _, err := timeRuns(20, func() (err error) {
				stated, err = pplPlan(q.text, time.Time{})

				return err
			})
			if err != nil {
				t.Fatalf("%s: %v", q.text, err)
			}
			took, cpu, err = timeRuns(runs, func() (err error) {
				res, err = exec.Run(context.Background(), tbl.snap, stated, exec.Options{Workers: cores})

				return err
			})
			if err != nil {
				t.Fatalf("%s: %v", q.text, err)
			}
			how = "hashed"
			if res.Stats.Ordered {
				how = "in key order"
			}
			t.Logf("%-8s %-22s %8.2f ms on %d cores (median %.2f, cpu %.1f) as text, %s, after %.3f ms to parse and compile (median %.3f)",
				tbl.name, q.name, millis(took[0]), cores, millis(took[runs/2]), millis(cpu), how,
				float64(compiled[0].Nanoseconds())/1e6, float64(compiled[10].Nanoseconds())/1e6)
			if stated := answer(q.name, res); stated != got {
				t.Errorf("%s %s as text\n got  %s\n want %s", tbl.name, q.name, stated, got)
			}
		}
	}
}
