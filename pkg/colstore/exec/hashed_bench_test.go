package exec_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/internal/benchdata"
	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
)

// The queries of BenchmarkHashed: each groups or counts by account over a
// store whose rows lie in time order, so that the accounts are hashed.
const (
	hashedCreated = `source=bluesky | where kind = 'commit' and commit.operation = 'create'`
	hashedUsers   = hashedCreated + ` | stats count() as count, distinct_count(did) as users by commit.collection | sort - count, commit.collection`
	hashedTop     = `source=bluesky | stats count() as n by did | sort - n | head 10`
	hashedAll     = `source=bluesky | stats distinct_count(did) as users`
	hashedSpans   = hashedCreated + ` and commit.collection = 'app.bsky.feed.post' | stats min(_time) as tmin, max(_time) as tmax by did` +
		` | eval span = timestampdiff(MILLISECOND, tmin, tmax) | fields did, span | sort - span | head 3`
)

// hashedAccount is what the events of one account add up to.
type hashedAccount struct {
	events      int64
	collections uint16
	posts       bool
	first, last int64
}

// hashedTruth computes the answers of the queries from the generated lines.
type hashedTruth struct {
	accounts    map[[32]byte]*hashedAccount
	collections []string
	created     []int64
}

func (t *hashedTruth) collection(name []byte) int {
	for i, c := range t.collections {
		if c == string(name) {
			return i
		}
	}
	t.collections = append(t.collections, string(name))
	t.created = append(t.created, 0)

	return len(t.collections) - 1
}

func between(line []byte, after string) []byte {
	i := bytes.Index(line, []byte(after))
	if i < 0 {
		return nil
	}
	rest := line[i+len(after):]

	return rest[:bytes.IndexByte(rest, '"')]
}

func (t *hashedTruth) add(b *testing.B, lines []byte) {
	for len(lines) > 0 {
		end := bytes.IndexByte(lines, '\n')
		line := lines[:end]
		lines = lines[end+1:]
		const head = `{"did":"`
		var did [32]byte
		copy(did[:], line[len(head):])
		rest := line[len(head)+len(did)+len(`","time_us":`):]
		us, err := strconv.ParseInt(string(rest[:bytes.IndexByte(rest, ',')]), 10, 64)
		if err != nil {
			b.Fatal(err)
		}
		a := t.accounts[did]
		if a == nil {
			a = &hashedAccount{}
			t.accounts[did] = a
		}
		a.events++
		if string(between(line, `"operation":"`)) != "create" {
			continue
		}
		name := between(line, `"collection":"`)
		c := t.collection(name)
		t.created[c]++
		a.collections |= 1 << c
		if string(name) != "app.bsky.feed.post" {
			continue
		}
		if !a.posts {
			a.posts, a.first = true, us
		}
		a.first, a.last = min(a.first, us), max(a.last, us)
	}
}

// answers renders the answer of every query: the rows of the first and the
// third whole, and of the two that keep the first rows of an order that ties
// leave open, the numbers that order them.
func (t *hashedTruth) answers() map[string]string {
	users := make([]int64, len(t.collections))
	var events, spans []int64
	for _, a := range t.accounts {
		events = append(events, a.events)
		if a.posts {
			spans = append(spans, (a.last-a.first)/1000)
		}
		for c := range users {
			users[c] += int64(a.collections >> c & 1)
		}
	}
	order := make([]int, len(t.collections))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool {
		a, b := order[i], order[j]
		if t.created[a] != t.created[b] {
			return t.created[a] > t.created[b]
		}

		return t.collections[a] < t.collections[b]
	})
	var byCollection []string
	for _, c := range order {
		byCollection = append(byCollection, fmt.Sprintf("%s=%d/%d", t.collections[c], t.created[c], users[c]))
	}
	top := func(v []int64, n int) string {
		sort.Slice(v, func(i, j int) bool { return v[i] > v[j] })
		var out []string
		for _, x := range v[:min(n, len(v))] {
			out = append(out, strconv.FormatInt(x, 10))
		}

		return strings.Join(out, " ")
	}

	return map[string]string{
		"users by collection": strings.Join(byCollection, " "),
		"top accounts":        top(events, 10),
		"distinct accounts":   strconv.Itoa(len(t.accounts)),
		"longest spans":       top(spans, 3),
	}
}

// hashedAnswer renders the rows of a result as answers does; of a result
// that keeps the first rows of an order it checks every row against the
// account it names when t is set.
func hashedAnswer(b *testing.B, name string, res *exec.Result, t *hashedTruth) string {
	col := func(name string) int {
		for i := range res.Columns {
			if res.Columns[i].Name == name {
				return i
			}
		}
		b.Fatalf("no column %q in the result", name)

		return 0
	}
	var out []string
	for i := 0; i < res.Rows; i++ {
		switch name {
		case "users by collection":
			out = append(out, fmt.Sprintf("%s=%d/%d", res.Value(i, col("commit.collection")).Bytes,
				res.Value(i, col("count")).Int, res.Value(i, col("users")).Int))
		case "distinct accounts":
			out = append(out, strconv.FormatInt(res.Value(i, 0).Int, 10))
		default:
			ordered := "n"
			if name == "longest spans" {
				ordered = "span"
			}
			v := res.Value(i, col(ordered)).Int
			out = append(out, strconv.FormatInt(v, 10))
			if t == nil {
				continue
			}
			var did [32]byte
			copy(did[:], res.Value(i, col("did")).Bytes)
			a := t.accounts[did]
			if a == nil || (name == "top accounts" && a.events != v) || (name == "longest spans" && (a.last-a.first)/1000 != v) {
				b.Fatalf("%s: row %d is %s with %d, the account has %+v", name, i, did[:], v, a)
			}
		}
	}

	return strings.Join(out, " ")
}

// hashedStore opens the store of the benchmark in dir, writing it first
// unless it is there, and returns it with the answers of the queries: rows
// events of users accounts over days days, in daily partitions and with no
// sort key, as an index is laid out unless told otherwise.
func hashedStore(b *testing.B, dir string, rows, users, days int) (*colstore.Store, *hashedTruth) {
	b.Helper()
	opts := colstore.StoreOptions{TimeFields: []string{"time_us"}, Partition: 24 * time.Hour, FlushLevel: part.LevelFastest}
	_, statErr := os.Stat(dir)
	s, err := colstore.OpenStore(dir, opts)
	if err != nil {
		b.Fatal(err)
	}
	truth := &hashedTruth{accounts: map[[32]byte]*hashedAccount{}}
	step := int64(days) * int64(24*time.Hour/time.Microsecond) / int64(max(rows, 1))
	g := benchdata.New(1, users).Spread(step)
	const chunk = 100_000
	var lines []byte
	for left := rows; left > 0; left -= chunk {
		lines = g.Append(lines[:0], min(chunk, left))
		truth.add(b, lines)
		if statErr == nil {
			continue
		}
		if _, err := s.Append(lines); err != nil {
			b.Fatal(err)
		}
	}
	if statErr != nil {
		if err := s.Sync(); err != nil {
			b.Fatal(err)
		}
		if err := s.Compact(); err != nil {
			b.Fatal(err)
		}
	}

	return s, truth
}

// peakHeap runs fn and returns by how much the heap grew at most while it
// ran, in bytes, read every millisecond.
func peakHeap(fn func()) uint64 {
	const name = "/memory/classes/heap/objects:bytes"
	read := func() uint64 {
		s := []metrics.Sample{{Name: name}}
		metrics.Read(s)

		return s[0].Value.Uint64()
	}
	runtime.GC()
	before, peak := read(), uint64(0)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				peak = max(peak, read())
			}
		}
	}()
	fn()
	close(stop)
	wg.Wait()
	peak = max(peak, read())
	if peak < before {
		return 0
	}

	return peak - before
}

// BenchmarkHashed times the aggregations that hash a key of many values:
// events and distinct accounts by collection, the accounts with the most
// events, the distinct accounts, and the accounts whose posts span the
// longest time, over a store of daily partitions with no sort key, whose
// rows lie in time order. Every answer is checked against what the
// generated events add up to. Each query runs on the goroutines that
// LYNXDB_BENCH_HASHED_WORKERS sets (one per CPU by default) and on one.
// LYNXDB_BENCH_HASHED_ROWS sets the events (16Mi by default),
// LYNXDB_BENCH_HASHED_USERS the accounts that write them (one per 8 events),
// LYNXDB_BENCH_HASHED_DAYS the days they cover (4),
// LYNXDB_BENCH_HASHED_DIR a directory that keeps the store between runs and
// LYNXDB_BENCH_HASHED_PROFILE one that receives a CPU profile of the timed
// runs of every query. LYNXDB_BENCH_HASHED_STAGE sets the megabytes of
// groups that the workers of a query hold before they merge them while they
// scan, which a query over many more events than these does by itself.
// Beside the time it reports the CPU time per aggregated row, the memory
// the query held at its end and the most that the heap grew by during a
// run.
func BenchmarkHashed(b *testing.B) {
	rows := envOr(b, "LYNXDB_BENCH_HASHED_ROWS", 1<<24)
	users := envOr(b, "LYNXDB_BENCH_HASHED_USERS", rows/8)
	days := envOr(b, "LYNXDB_BENCH_HASHED_DAYS", 4)
	workers := 0
	if os.Getenv("LYNXDB_BENCH_HASHED_WORKERS") != "" {
		workers = envOr(b, "LYNXDB_BENCH_HASHED_WORKERS", 0)
	}
	if os.Getenv("LYNXDB_BENCH_HASHED_STAGE") != "" {
		defer exec.SetStageBudget(envOr(b, "LYNXDB_BENCH_HASHED_STAGE", 0) << 20)()
	}
	dir := filepath.Join(b.TempDir(), "store")
	if kept := os.Getenv("LYNXDB_BENCH_HASHED_DIR"); kept != "" {
		dir = filepath.Join(kept, fmt.Sprintf("hashed-%d-%d-%d", rows, users, days))
	}
	s, truth := hashedStore(b, dir, rows, users, days)
	defer s.Close()
	snap := s.Table().Snapshot()
	defer snap.Release()
	want := truth.answers()
	profiles := os.Getenv("LYNXDB_BENCH_HASHED_PROFILE")
	b.Logf("%d events of %d accounts: %s", rows, len(truth.accounts), describeStore(snap))
	// What the writes left to collect would count as the time of the queries.
	runtime.GC()
	for _, q := range []struct{ name, text string }{
		{"users by collection", hashedUsers},
		{"top accounts", hashedTop},
		{"distinct accounts", hashedAll},
		{"longest spans", hashedSpans},
	} {
		plan, err := pplPlan(q.text, time.Now())
		if err != nil {
			b.Fatal(err)
		}
		for _, n := range []int{workers, 1} {
			name := q.name
			if n == 1 {
				name += ", one goroutine"
			}
			opts := exec.Options{Workers: n}
			b.Run(name, func(b *testing.B) {
				var res *exec.Result
				peak := peakHeap(func() {
					if res, err = exec.Run(context.Background(), snap, plan, opts); err != nil {
						b.Fatal(err)
					}
				})
				if res.Stats.Ordered {
					b.Fatal("the query ran in key order")
				}
				if got := hashedAnswer(b, q.name, res, truth); got != want[q.name] {
					b.Fatalf("answer\n %s\nwant\n %s", got, want[q.name])
				}
				held, matched := res.Stats.HeldBytes, int64(0)
				if profiles != "" && b.N > 1 {
					f, err := os.Create(filepath.Join(profiles, strings.NewReplacer(" ", "_", ",", "").Replace(name)+".cpu"))
					if err != nil {
						b.Fatal(err)
					}
					defer f.Close()
					if err := pprof.StartCPUProfile(f); err != nil {
						b.Fatal(err)
					}
					defer pprof.StopCPUProfile()
				}
				b.ReportAllocs()
				b.ResetTimer()
				before := cpuTime()
				for i := 0; i < b.N; i++ {
					res, err := exec.Run(context.Background(), snap, plan, opts)
					if err != nil {
						b.Fatal(err)
					}
					if got := hashedAnswer(b, q.name, res, nil); got != want[q.name] {
						b.Fatalf("answer\n %s\nwant\n %s", got, want[q.name])
					}
					matched += res.Stats.RowsMatched
				}
				cpu := cpuTime() - before
				b.StopTimer()
				b.ReportMetric(float64(cpu.Nanoseconds())/float64(matched), "cpu-ns/row")
				b.ReportMetric(float64(cpu.Nanoseconds())/float64(b.N)/float64(rows), "cpu-ns/scanned-row")
				b.ReportMetric(cpu.Seconds()/b.Elapsed().Seconds(), "cores")
				b.ReportMetric(float64(held)/1e6, "held-MB")
				b.ReportMetric(float64(peak)/1e6, "peak-heap-MB")
			})
		}
	}
}
