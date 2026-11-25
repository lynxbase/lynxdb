package exec_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/internal/benchdata"
	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
)

// rareWord is the word that a few posts of a latestStore end with.
const rareWord = "earthquake"

// latestStore writes rows events of benchdata spread over days days into a
// store of daily partitions in dir, in blocks of blockRows rows ordered by
// sortKey, or by time alone without one, and compacts it into one run per
// day. One post in oneIn ends with rareWord.
func latestStore(b testing.TB, dir string, rows, days, blockRows, oneIn int, sortKey []string) *colstore.Store {
	b.Helper()
	s, err := colstore.OpenStore(dir, colstore.StoreOptions{
		SortKey:    sortKey,
		TimeFields: []string{benchTimeField},
		BlockRows:  blockRows,
		Partition:  24 * time.Hour,
		FlushLevel: part.LevelFastest,
	})
	if err != nil {
		b.Fatal(err)
	}
	step := int64(days) * int64(24*time.Hour/time.Microsecond) / int64(max(rows, 1))
	g := benchdata.New(1, max(rows/64, 1)).Spread(step).Rare(rareWord, oneIn)
	const chunk = 100_000
	var lines []byte
	for left := rows; left > 0; left -= chunk {
		lines = g.Append(lines[:0], min(chunk, left))
		if _, err := s.Append(lines); err != nil {
			b.Fatal(err)
		}
	}
	if err := s.Sync(); err != nil {
		b.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		b.Fatal(err)
	}

	return s
}

// latestQueries are plans that keep the first rows or groups by time, as
// PPL: of a word that few rows of a block hold, of every row, of one
// collection, of the account with the most events, of a window of time, and
// the accounts with the earliest posts. Those of the time alone read no
// other field of the rows they keep, which takes a block for each row.
func latestQueries(b testing.TB, snap *colstore.Snapshot) []struct{ name, text string } {
	b.Helper()
	const posts = `kind = 'commit' and commit.operation = 'create' and commit.collection = 'app.bsky.feed.post'`
	plan, err := pplPlan(`source=bluesky | stats count() as count by did | sort - count, did | head 1 | fields did`, time.Time{})
	if err != nil {
		b.Fatal(err)
	}
	res, err := exec.Run(context.Background(), snap, plan, exec.Options{})
	if err != nil || res.Rows != 1 {
		b.Fatalf("the account with the most events: %d rows, %v", res.Rows, err)
	}
	did := string(res.Row(0, nil)[0].Bytes)
	lo, hi := int64(0), int64(0)
	for i, h := range snap.Parts() {
		if i == 0 || h.MinTime < lo {
			lo = h.MinTime
		}
		hi = max(hi, h.MaxTime)
	}
	stamp := func(ns int64) string { return time.Unix(0, ns).UTC().Format(time.RFC3339Nano) }
	window := fmt.Sprintf(`_time >= '%s' and _time < '%s'`, stamp(lo+(hi-lo)/2), stamp(lo+(hi-lo)/2+int64(6*time.Hour)))
	word := `match(commit.record.text, '` + rareWord + `')`

	return []struct{ name, text string }{
		{"latest of a word", `source=bluesky | where ` + word + ` | sort - _time | head 100`},
		{"earliest of a word", `source=bluesky | where ` + word + ` | sort _time | head 100`},
		{"latest of a word, ten", `source=bluesky | where ` + word + ` | sort - _time | head 10`},
		{"latest of a word, the time alone", `source=bluesky | where ` + word + ` | fields _time | sort - _time | head 100`},
		{"latest", `source=bluesky | sort - _time | head 100`},
		{"latest, the time alone", `source=bluesky | fields _time | sort - _time | head 100`},
		{"latest of a collection", `source=bluesky | where commit.collection = 'app.bsky.feed.post' | sort - _time | head 100`},
		{"latest of an account", `source=bluesky | where did = '` + did + `' | sort - _time | head 100`},
		{"latest of a window", `source=bluesky | where ` + window + ` | sort - _time | head 100`},
		{"latest of a word in a window", `source=bluesky | where ` + window + ` and ` + word + ` | sort - _time | head 100`},
		{"earliest posts by account", `source=bluesky | where ` + posts + ` | stats min(_time) as first_post_ts by did | sort first_post_ts | head 3`},
		{"earliest posts with a word by account", `source=bluesky | where ` + posts + ` and ` + word +
			` | stats min(_time) as first_post_ts by did | sort first_post_ts | head 3`},
	}
}

// renderedSet renders the rows of a result in an order that does not depend
// on the order of the rows that a sort leaves equal.
func renderedSet(res *exec.Result) string {
	rows := strings.Split(rendered(res), " ")
	sort.Strings(rows)

	return strings.Join(rows, " ")
}

// BenchmarkLatestRows times plans that keep the first rows by time over a
// store of daily partitions in the order of the Bluesky benchmark's sort key
// and over one in the order of time, and reports the blocks they open: the
// mean, the fewest and the most of the runs, and the blocks that one
// goroutine opens, which takes them one after another and so opens the
// fewest that the data allows. LYNXDB_BENCH_LATEST_ROWS sets the events (4Mi
// by default), LYNXDB_BENCH_LATEST_DAYS the days they cover (8),
// LYNXDB_BENCH_LATEST_BLOCK_ROWS the rows of a block (2048),
// LYNXDB_BENCH_LATEST_ONE_IN the posts for each that holds the word (700),
// and LYNXDB_BENCH_LATEST_WORKERS the goroutines of the query (one per CPU).
func BenchmarkLatestRows(b *testing.B) {
	rows := envOr(b, "LYNXDB_BENCH_LATEST_ROWS", 1<<22)
	days := envOr(b, "LYNXDB_BENCH_LATEST_DAYS", 8)
	blockRows := envOr(b, "LYNXDB_BENCH_LATEST_BLOCK_ROWS", 2048)
	oneIn := envOr(b, "LYNXDB_BENCH_LATEST_ONE_IN", 700)
	opts := exec.Options{}
	if os.Getenv("LYNXDB_BENCH_LATEST_WORKERS") != "" {
		opts.Workers = envOr(b, "LYNXDB_BENCH_LATEST_WORKERS", 0)
	}
	for _, store := range []struct {
		name    string
		sortKey []string
	}{{"by key", benchSortKey}, {"by time", nil}} {
		dir := filepath.Join(b.TempDir(), store.name)
		s := latestStore(b, dir, rows, days, blockRows, oneIn, store.sortKey)
		snap := s.Table().Snapshot()
		b.Logf("%s: %s", store.name, describeStore(snap))
		runtime.GC()
		for _, q := range latestQueries(b, snap) {
			plan, err := pplPlan(q.text, time.Time{})
			if err != nil {
				b.Fatal(err)
			}
			b.Run(store.name+"/"+q.name, func(b *testing.B) { latestRuns(b, snap, plan, opts) })
		}
		snap.Release()
		if err := s.Close(); err != nil {
			b.Fatal(err)
		}
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
	}
}

// latestRuns times plan over snap and reports the blocks its runs open.
func latestRuns(b *testing.B, snap *colstore.Snapshot, plan exec.Node, opts exec.Options) {
	// The first run leaves the blocks that hold no row of the filter for
	// the others to skip, as the first query of a server does.
	one, err := exec.Run(context.Background(), snap, plan, exec.Options{Workers: 1})
	if err != nil {
		b.Fatal(err)
	}
	want := renderedSet(one)
	least, fewest, most, sum, rows := one.Stats.Blocks, int64(0), int64(0), int64(0), int64(0)
	counts := map[int64]int{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := exec.Run(context.Background(), snap, plan, opts)
		if err != nil {
			b.Fatal(err)
		}
		if got := renderedSet(res); got != want {
			b.Fatalf("answer\n %s\nwant\n %s", got, want)
		}
		n := res.Stats.Blocks
		if i == 0 || n < fewest {
			fewest = n
		}
		most = max(most, n)
		sum += n
		rows += res.Stats.RowsMatched
		counts[n]++
	}
	b.StopTimer()
	b.ReportMetric(float64(sum)/float64(b.N), "blocks/op")
	b.ReportMetric(float64(fewest), "blocks-fewest")
	b.ReportMetric(float64(most), "blocks-most")
	b.ReportMetric(float64(least), "blocks-one-goroutine")
	b.ReportMetric(float64(rows)/float64(b.N), "rows-matched/op")
	b.ReportMetric(float64(len(counts)), "counts-seen")
}
