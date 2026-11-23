package exec_test

import (
	"context"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/internal/benchdata"
	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
)

// timeCountStore holds generated events over four days in daily partitions,
// in small blocks ordered as the server orders the Bluesky benchmark, and
// events without a time of their own, which take the time they arrived at;
// some of it is merged, the rest left as flushed.
func timeCountStore(t *testing.T) *colstore.Store {
	t.Helper()
	s, err := colstore.OpenStore(t.TempDir(), colstore.StoreOptions{
		SortKey:       []string{"kind", "commit.operation", "commit.collection", "did"},
		TimeFields:    []string{"time_us"},
		Partition:     24 * time.Hour,
		BlockRows:     1024,
		MergeFanIn:    -1,
		FlushInterval: -1,
		FlushLevel:    part.LevelFastest,
	})
	if err != nil {
		t.Fatal(err)
	}
	const rows = 120_000
	g := benchdata.New(7, 3000).Spread(4 * int64(24*time.Hour/time.Microsecond) / rows)
	var lines []byte
	for left := rows; left > 0; left -= 20_000 {
		lines = g.Append(lines[:0], 20_000)
		if _, err := s.Append(lines); err != nil {
			t.Fatal(err)
		}
		untimed := []byte(`{"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.like"},"did":"did:plc:untimed"}` + "\n")
		if _, err := s.AppendAt(untimed, time.Date(2024, 11, 22, 7, 59, 59, 0, time.UTC).UnixNano()+int64(left)); err != nil {
			t.Fatal(err)
		}
		if err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		if left == rows/2 {
			if err := s.Compact(); err != nil {
				t.Fatal(err)
			}
		}
	}

	return s
}

// within returns plan with the times of its rows limited to [from, to), as
// the time range of a request limits them.
func within(n exec.Node, from, to time.Time) exec.Node {
	switch n := n.(type) {
	case *exec.Sort:
		c := *n
		c.Input = within(n.Input, from, to)

		return &c
	case *exec.Project:
		c := *n
		c.Input = within(n.Input, from, to)

		return &c
	case *exec.Aggregate:
		c := *n
		c.Input = within(n.Input, from, to)

		return &c
	case *exec.Scan:
		c := *n
		c.Filter = exec.And(exec.Ge(exec.Time(), exec.Int(from.UnixNano())), exec.Lt(exec.Time(), exec.Int(to.UnixNano())))
		if n.Filter != nil {
			c.Filter = exec.And(n.Filter, c.Filter)
		}

		return &c
	}

	return n
}

func TestCountsByTimeAnswerAsTheTimesOfTheRowsDo(t *testing.T) {
	s := timeCountStore(t)
	defer s.Close()
	snap := s.Table().Snapshot()
	defer snap.Release()
	created := `where kind = 'commit' and commit.operation = 'create' and commit.collection in ('app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like')`
	const (
		some = iota // blocks are counted from their rows by time
		none        // no block can be
		any         // blocks whose rows pass and hold one key may be
	)
	day := func(d, h, m, sec int) time.Time { return time.Date(2024, 11, d, h, m, sec, 0, time.UTC) }
	cases := []struct {
		name, text string
		byTime     int
		// from and to, when set, limit the rows to a range of time.
		from, to time.Time
	}{
		{"the third query over whole days", q3Text, some, day(22, 0, 0, 0), day(24, 0, 0, 0)},
		{"the third query over a day and a half", q3Text, some, day(22, 0, 0, 0), day(23, 12, 0, 0)},
		{"the third query over a range off the minutes", q3Text, some, day(22, 3, 17, 31), day(24, 0, 0, 1)},
		{"the third query within an hour", q3Text, some, day(23, 5, 10, 0), day(23, 5, 40, 0)},
		{"a histogram of the last hour", `source=x | stats count() as count by span(_time, 1m) as _time | sort _time`, some, day(23, 9, 12, 7), day(23, 10, 12, 7)},
		{"a histogram of days", `source=x | stats count() as count by span(_time, 1h) as _time | sort _time`, some, day(21, 23, 59, 59), day(24, 13, 0, 0)},
		{"a histogram in seconds", `source=x | stats count() as count by span(_time, 30s) as _time | sort _time`, none, day(23, 9, 12, 7), day(23, 9, 42, 7)},
		{"the benchmark's third query", q3Text, some, time.Time{}, time.Time{}},
		{"hours", `source=x | eval h = hour(_time) | stats count() as count by h | sort h`, some, time.Time{}, time.Time{}},
		{"hours by kind", `source=x | eval h = hour(_time) | stats count() as count by kind, h | sort kind, h`, some, time.Time{}, time.Time{}},
		{"hourly spans", `source=x | ` + created + ` | stats count() as count by span(_time, 1h) as t, commit.collection | sort t, commit.collection`, some, time.Time{}, time.Time{}},
		{"spans of five minutes", `source=x | stats count() as count by span(_time, 5m) as t | sort t`, some, time.Time{}, time.Time{}},
		{"daily spans", `source=x | stats count() as count by kind, span(_time, 1d) as t | sort kind, t`, some, time.Time{}, time.Time{}},
		{"two counts", `source=x | eval h = hour(_time) | stats count() as a, count() as b by commit.operation, h | sort commit.operation, h`, some, time.Time{}, time.Time{}},
		{"spans of 90 seconds", `source=x | stats count() as count by span(_time, 90s) as t | sort t`, none, time.Time{}, time.Time{}},
		{"hours and a sum", `source=x | eval h = hour(_time) | stats count() as count, sum(time_us) as s by h | sort h`, none, time.Time{}, time.Time{}},
		{"hours by user", `source=x | eval h = hour(_time) | stats count() as count by did, h | sort did, h`, any, time.Time{}, time.Time{}},
		{"hours of rows a filter reads", `source=x | where length(commit.rkey) > 12 | eval h = hour(_time) | stats count() as count by h | sort h`, any, time.Time{}, time.Time{}},
	}
	// why names, for a case whose blocks are counted row by row, the count
	// of the reason they are.
	why := map[string]func(exec.Stats) int64{
		"spans of 90 seconds":          func(s exec.Stats) int64 { return s.TimeMissedRecord },
		"a histogram in seconds":       func(s exec.Stats) int64 { return s.TimeMissedRecord },
		"hours by user":                func(s exec.Stats) int64 { return s.TimeMissedKeys },
		"hours of rows a filter reads": func(s exec.Stats) int64 { return s.TimeMissedFilter },
	}
	missed := func(s exec.Stats) int64 {
		return s.TimeMissedFilter + s.TimeMissedRecord + s.TimeMissedKeys + s.TimeMissedNulls
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			plan, err := pplPlan(c.text, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if !c.from.IsZero() {
				plan = within(plan, c.from, c.to)
			}
			want, err := exec.Run(context.Background(), snap, rowTimes(plan), exec.Options{})
			if err != nil {
				t.Fatal(err)
			}
			got, err := exec.Run(context.Background(), snap, plan, exec.Options{})
			if err != nil {
				t.Fatal(err)
			}
			if g, w := rendered(got), rendered(want); g != w {
				t.Fatalf("answer\n %s\nwant\n %s", g, w)
			}
			if counted := got.Stats.CountedByTime; want.Stats.CountedByTime != 0 || (c.byTime == some && counted == 0) || (c.byTime == none && counted != 0) {
				t.Fatalf("%d of %d blocks counted by time, of the reference %d", counted, got.Stats.BlocksMatched, want.Stats.CountedByTime)
			}
			// A block whose rows match is counted by time or counted row by
			// row for a reason; a block that a range cuts can be counted by
			// time and match no row.
			st, by := got.Stats, got.Stats.CountedByTime+missed(got.Stats)
			if missed(want.Stats) != 0 || (by != 0 && by != st.BlocksMatched && (c.from.IsZero() || by < st.BlocksMatched)) {
				t.Fatalf("of %d blocks matched, %d were counted by time and %d row by row", st.BlocksMatched, st.CountedByTime, missed(st))
			}
			if reason := why[c.name]; reason != nil && reason(st) == 0 {
				t.Fatalf("blocks counted row by row for no reason given: %+v", st)
			}
			if want.Rows == 0 {
				t.Fatal("the query has no rows")
			}
		})
	}
}
