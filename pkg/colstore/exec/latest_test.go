package exec_test

import (
	"context"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
)

// TestFirstRowsOfAWordOpenTheSameBlocks requires of plans that keep the
// first rows or groups by time of a word that few rows of a block hold that
// they give the rows of a scan of every block, and open the same blocks run
// after run, few more than one goroutine opens: that takes the blocks one
// after another and opens none that the rows before it rule out. The store
// is in the order of a sort key, a day to a partition, or in the order of
// time. Each plan keeps more rows than a block holds of the word: a scan
// whose first block fills the result goes on as its goroutines fall.
func TestFirstRowsOfAWordOpenTheSameBlocks(t *testing.T) {
	const rows, days, blockRows, oneIn, workers = 300_000, 4, 512, 120, 8
	const posts = `kind = 'commit' and commit.operation = 'create' and commit.collection = 'app.bsky.feed.post'`
	word := `match(commit.record.text, '` + rareWord + `')`
	for _, store := range []struct {
		name    string
		sortKey []string
	}{{"by key", benchSortKey}, {"by time", nil}} {
		s := latestStore(t, filepath.Join(t.TempDir(), store.name), rows, days, blockRows, oneIn, store.sortKey)
		snap := s.Table().Snapshot()
		t.Logf("%s: %s", store.name, describeStore(snap))
		for _, c := range []struct {
			name, text string
			limit      int
		}{
			{"the latest", `source=bluesky | where ` + word + ` | sort - _time`, 20},
			{"the earliest", `source=bluesky | where ` + word + ` | sort _time`, 20},
			{"the latest hundred", `source=bluesky | where ` + word + ` | sort - _time`, 100},
			{"the accounts that wrote it first", `source=bluesky | where ` + posts + ` and ` + word +
				` | stats min(_time) as first by did | sort first, did`, 30},
		} {
			t.Run(store.name+"/"+c.name, func(t *testing.T) {
				run := func(text string, opts exec.Options) *exec.Result {
					t.Helper()
					plan, err := pplPlan(text, time.Time{})
					if err != nil {
						t.Fatal(err)
					}
					res, err := exec.Run(context.Background(), snap, plan, opts)
					if err != nil {
						t.Fatal(err)
					}

					return res
				}
				// Without a limit every block that may hold the word is
				// opened and its rows sorted.
				all := resultRows(run(c.text, exec.Options{Forgetful: true}))
				if len(all) <= c.limit {
					t.Fatalf("%d rows hold the word, want more than the %d kept", len(all), c.limit)
				}
				// Rows of one time come in any order.
				want := all[:c.limit]
				sort.Strings(want)
				limited := c.text + ` | head ` + strconv.Itoa(c.limit)
				least := run(limited, exec.Options{Workers: 1, Forgetful: true}).Stats.Blocks
				var first exec.Stats
				for i := 0; i < 12; i++ {
					res := run(limited, exec.Options{Workers: workers, Forgetful: true})
					got := resultRows(res)
					sort.Strings(got)
					if strings.Join(got, "\n") != strings.Join(want, "\n") {
						t.Fatalf("run %d: rows\n%s\nwant\n%s", i, strings.Join(got, "\n"), strings.Join(want, "\n"))
					}
					st := res.Stats
					st.HeldBytes = 0
					if i == 0 {
						first = st
						t.Logf("%d blocks opened, %d by one goroutine, of %d that may hold the word", st.Blocks, least, st.Blocks+st.SkippedByTime)
					}
					if st != first {
						t.Fatalf("run %d: %+v, the first run %+v", i, st, first)
					}
				}
				if most := 2*least + workers; first.Blocks > most {
					t.Fatalf("%d blocks opened, one goroutine opens %d, want at most %d", first.Blocks, least, most)
				}
				if first.SkippedByTime == 0 {
					t.Fatalf("no block left out by time: %+v", first)
				}
			})
		}
		snap.Release()
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
