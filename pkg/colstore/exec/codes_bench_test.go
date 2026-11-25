package exec_test

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
)

// codedQuery is a query of BenchmarkCodedColumns; whole is set when its
// answer is the same rows whatever the goroutines, so that the benchmark
// can tell one run of it from another.
type codedQuery struct {
	name, text string
	whole      bool
}

// codedQueries returns the queries of BenchmarkCodedColumns for a store
// whose most active account is did.
func codedQueries(did string) []codedQuery {
	word := did[len("did:plc:"):]

	return []codedQuery{
		{"one account by collection", `source=bluesky did="` + did + `" | stats count() as n by commit.collection | sort commit.collection`, true},
		{"accounts by their start", `source=bluesky | where like(did, 'did:plc:ab%') | stats count() as n`, true},
		{"accounts by an expression", `source=bluesky | where regexp_match(did, 'plc:[a-c].*7$') | stats count() as n`, true},
		{"accounts from one on", `source=bluesky | where did >= 'did:plc:x' | stats count() as n`, true},
		{"an account in any field", `source=bluesky ` + word + ` | stats count() as n`, true},
		{"a word in any field", `source=bluesky moderation | stats count() as n`, true},
		{"events by collection and account", `source=bluesky | stats count() as n by commit.collection, did | sort - n, did | head 10`, true},
		{"the latest events", `source=bluesky | sort - _time | head 100`, false},
		{"the latest posts", `source=bluesky | where commit.collection = 'app.bsky.feed.post' | sort - _time | head 100`, false},
		{"any events", `source=bluesky | head 100`, false},
	}
}

// digest names the rows of a result, whatever their order: their number and
// a hash of them.
func digest(res *exec.Result) string {
	rows := make([]string, res.Rows)
	for i := range rows {
		var cells []string
		for c, v := range res.Row(i, nil) {
			cells = append(cells, fmt.Sprintf("%s=%d:%d:%g:%v:%s", res.Columns[c].Name, v.Kind, v.Int, v.Float, v.Bool, v.Bytes))
		}
		sort.Strings(cells)
		rows[i] = strings.Join(cells, "/")
	}
	sort.Strings(rows)
	h := fnv.New64a()
	for _, row := range rows {
		h.Write([]byte(row))
		h.Write([]byte{0})
	}

	return fmt.Sprintf("%d rows, %016x", res.Rows, h.Sum64())
}

// BenchmarkCodedColumns times queries that read the account of an event
// other than to group by it, and queries that fetch whole events, over the
// store of BenchmarkHashed: a filter by one account, by the start of an
// account, by an expression and by a range of accounts; a search of every
// field for an account and for a word of the texts; the events of the
// accounts by collection; and the latest and the first events whole. Where
// parts store the accounts as codes of part dictionaries, these are the
// queries that read the dictionaries beside the blocks. Beside the time it
// reports the CPU time per scanned row and, for a first run after the
// process let go of the pages of the parts, its time and the page faults it
// took, which stand for what a cold run reads. The variables of
// BenchmarkHashed set the store.
func BenchmarkCodedColumns(b *testing.B) {
	rows := envOr(b, "LYNXDB_BENCH_HASHED_ROWS", 1<<24)
	users := envOr(b, "LYNXDB_BENCH_HASHED_USERS", rows/8)
	days := envOr(b, "LYNXDB_BENCH_HASHED_DAYS", 4)
	workers := 0
	if os.Getenv("LYNXDB_BENCH_HASHED_WORKERS") != "" {
		workers = envOr(b, "LYNXDB_BENCH_HASHED_WORKERS", 0)
	}
	dir := filepath.Join(b.TempDir(), "store")
	if kept := os.Getenv("LYNXDB_BENCH_HASHED_DIR"); kept != "" {
		dir = filepath.Join(kept, fmt.Sprintf("hashed-%d-%d-%d", rows, users, days))
	}
	s, truth := hashedStore(b, dir, rows, users, days)
	defer s.Close()
	snap := s.Table().Snapshot()
	defer snap.Release()
	// The account with the most events, the first of those by its bytes.
	var did [32]byte
	most := int64(0)
	for id, a := range truth.accounts {
		if a.events > most || (a.events == most && bytes.Compare(id[:], did[:]) < 0) {
			did, most = id, a.events
		}
	}
	b.Logf("%d events of %d accounts, %d of them of %s: %s", rows, len(truth.accounts), most, did[:], describeStore(snap))
	runtime.GC()
	for _, q := range codedQueries(string(did[:])) {
		plan, err := pplPlan(q.text, time.Now())
		if err != nil {
			b.Fatal(err)
		}
		opts := exec.Options{Workers: workers, Forgetful: true}
		b.Run(q.name, func(b *testing.B) {
			s.ReleasePages()
			faults, begin := pageFaults(), time.Now()
			res, err := exec.Run(context.Background(), snap, plan, opts)
			if err != nil {
				b.Fatal(err)
			}
			first, faults := time.Since(begin), pageFaults()-faults
			want := digest(res)
			b.Logf("%s; %d of %d blocks read, %d rows passed", want, res.Stats.Blocks,
				res.Stats.Blocks+res.Stats.SkippedByTime+res.Stats.SkippedByToken+res.Stats.SkippedByKey, res.Stats.RowsMatched)
			b.ReportAllocs()
			b.ResetTimer()
			before := cpuTime()
			for i := 0; i < b.N; i++ {
				res, err := exec.Run(context.Background(), snap, plan, opts)
				if err != nil {
					b.Fatal(err)
				}
				if got := digest(res); q.whole && got != want {
					b.Fatalf("answer %s, want %s", got, want)
				}
			}
			cpu := cpuTime() - before
			b.StopTimer()
			b.ReportMetric(float64(cpu.Nanoseconds())/float64(b.N)/float64(rows), "cpu-ns/scanned-row")
			b.ReportMetric(float64(first.Microseconds())/1e3, "first-ms")
			b.ReportMetric(float64(faults), "first-faults")
		})
	}
}
