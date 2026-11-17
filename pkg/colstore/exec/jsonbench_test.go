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

type benchPlan struct {
	name string
	plan exec.Node
	// ordered is set when the plan's sort makes the row order unique.
	ordered bool
}

func benchPlans() []benchPlan {
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

	return []benchPlan{
		{"Q1", &exec.Sort{Input: &exec.Aggregate{Input: &exec.Scan{}, Keys: []exec.Named{collection}, Aggs: []exec.Agg{count}}, By: byCount}, true},
		{"Q2", &exec.Sort{Input: &exec.Aggregate{
			Input: &exec.Scan{Filter: exec.And(created...)},
			Keys:  []exec.Named{collection},
			Aggs:  []exec.Agg{count, {Name: "users", Func: exec.CountDistinct, Arg: str("did")}},
		}, By: byCount}, true},
		{"Q3", &exec.Sort{Input: &exec.Aggregate{
			Input: &exec.Scan{Filter: exec.And(append(created[:2:2], exec.Or(isCollection("app.bsky.feed.like"),
				isCollection("app.bsky.feed.post"), isCollection("app.bsky.feed.repost")))...)},
			Keys: []exec.Named{collection, named("hour", exec.HourOfDay(exec.Time()))},
			Aggs: []exec.Agg{count},
		}, By: []exec.Order{{Column: "hour"}, {Column: "event"}}}, true},
		{"Q4", &exec.Sort{Input: &exec.Aggregate{
			Input: posts,
			Keys:  []exec.Named{named("did", str("did"))},
			Aggs:  []exec.Agg{{Name: "first", Func: exec.Min, Arg: exec.Time()}},
		}, By: []exec.Order{{Column: "first"}, {Column: "did"}}, Limit: 3}, true},
		{"Q5", &exec.Sort{Input: &exec.Project{
			Input: &exec.Aggregate{
				Input: posts,
				Keys:  []exec.Named{named("did", str("did"))},
				Aggs:  []exec.Agg{{Name: "first", Func: exec.Min, Arg: exec.Time()}, {Name: "last", Func: exec.Max, Arg: exec.Time()}},
			},
			Exprs: []exec.Named{named("did", anyOf("did")), named("span_ms", exec.Sub(
				exec.Div(anyOf("last"), exec.Int(1_000_000)), exec.Div(anyOf("first"), exec.Int(1_000_000))))},
		}, By: []exec.Order{{Column: "span_ms", Desc: true}, {Column: "did"}}, Limit: 3}, true},
		{"S1 rare token", search(exec.Match(str("commit.record.subject.uri"), tokenfilter.Word("sc7btuzqdaepksx43mr74pma"))), true},
		{"S2 common token", search(exec.Match(text, tokenfilter.Word("love"))), true},
		{"S3 phrase", search(exec.Match(text, tokenfilter.Word("Good morning"))), true},
		{"S4 prefix", search(exec.Match(text, tokenfilter.Prefix("crypto"))), true},
		{"S5 substring", search(exec.Match(text, tokenfilter.Substring("ware"))), true},
		{"S6 field equals", search(exec.Eq(str("did"), exec.Str("did:plc:yj3sjq3blzpynh27cumnp5ks"))), true},
		{"S7 unique id", search(exec.Eq(str("commit.cid"), exec.Str("bafyreicsj6qpl6dreic6ekdjxvdjltncngl52x3jddf6dxhg2gwo35iqhi"))), true},
	}
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
	opts := part.Options{Ordered: true, SortKey: key}
	chunks := splitLines(data, parts)
	runs := make([]*part.Part, len(chunks))
	var wg sync.WaitGroup
	for i, chunk := range chunks {
		wg.Add(1)
		go func(i int, chunk []byte) {
			defer wg.Done()
			all := shred.NewBatch(shred.Options{TimeField: benchTimeField})
			all.AddNDJSON(chunk, 0, nil)
			all.SortBy(key)
			var buf bytes.Buffer
			w := part.NewWriter(&buf, opts)
			block := shred.Get(shred.Options{})
			defer shred.Put(block)
			for from := 0; from < all.Rows; from += benchBlockRows {
				block.Reset()
				all.CopyRows(block, from, min(from+benchBlockRows, all.Rows))
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
			if err := part.MergeRange(w, runs, benchBlockRows, lo, hi); err != nil {
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
		for _, q := range benchPlans() {
			for _, workers := range []int{cores, 1} {
				t.Run(fmt.Sprintf("%s/%s/%d", name, q.name, workers), func(t *testing.T) {
					check(t, snap, rows, q.plan, q.ordered, workers)
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
	for i := 0; i < res.Rows; i++ {
		if i > 0 {
			b.WriteByte(' ')
		}
		row := res.Row(i, nil)
		switch name {
		case "Q1":
			fmt.Fprintf(&b, "%s=%d", row[0].Bytes, row[1].Int)
		case "Q2":
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
// sort key, and over the table in LYNXDB_BENCH_DIR when that is set. It
// prints the best of LYNXDB_BENCH_RUNS runs (7 by default) and the answers.
// LYNXDB_BENCH_QUERY keeps the plans whose name contains it,
// LYNXDB_BENCH_LAYOUT the layouts whose name does, and LYNXDB_BENCH_PROFILE
// names a directory that receives one CPU profile per plan and layout.
func TestJSONBenchPlans(t *testing.T) {
	data := benchData(t)
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
	add("arrival", func() *colstore.Table { return loadArrival(t, data, cores) })
	add("ordered", func() *colstore.Table { return loadOrdered(t, data, cores, benchSortKey) })
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

	answers := map[string]string{}
	for _, tbl := range tables {
		var rows uint64
		blocks := 0
		for _, h := range tbl.snap.Parts() {
			rows += h.Rows
			blocks += h.NumBlocks()
		}
		t.Logf("%s: %d rows in %d parts / %d blocks", tbl.name, rows, len(tbl.snap.Parts()), blocks)
		for _, q := range benchPlans() {
			if !strings.Contains(q.name, only) {
				continue
			}
			var res *exec.Result
			run := func() (err error) {
				res, err = exec.Run(context.Background(), tbl.snap, q.plan, exec.Options{Workers: cores})

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
			t.Logf("%-8s %-16s %8.2f ms on %d cores (median %.2f, cpu %.1f, holds %.1f MB), %d heap allocations (%.2f MB); %d blocks opened, skipped: %d by key, %d by tokens, %d by time, %d by statistics; %d blocks pass the filter with %d rows",
				tbl.name, q.name, millis(took[0]), cores, millis(took[runs/2]), millis(cpu), float64(s.HeldBytes)/1e6,
				after.Mallocs-before.Mallocs, float64(after.TotalAlloc-before.TotalAlloc)/1e6,
				s.Blocks, s.SkippedByKey, s.SkippedByToken, s.SkippedByTime, s.SkippedByStats, s.BlocksMatched, s.RowsMatched)
			got := answer(q.name, res)
			if want, ok := benchAnswers[q.name]; ok && rows == 1_000_000 && got != want {
				t.Errorf("%s %s\n got  %s\n want %s", tbl.name, q.name, got, want)
			}
			if prev, ok := answers[q.name]; ok && prev != got {
				t.Errorf("%s %s answers %s, another layout %s", tbl.name, q.name, got, prev)
			}
			if _, ok := answers[q.name]; !ok {
				t.Logf("%-8s %-16s answer: %s", "", q.name, got)
			}
			answers[q.name] = got
		}
	}
}
