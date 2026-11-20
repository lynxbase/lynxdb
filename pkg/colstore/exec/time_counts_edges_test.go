package exec_test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
)

// edgeTimeStore holds rows of two kinds at the given times, in small blocks
// of daily partitions, half of them merged; rows of a third kind lack a
// time and take the one they arrive at.
func edgeTimeStore(t *testing.T, r *rand.Rand, times []int64) *colstore.Store {
	t.Helper()
	s, err := colstore.OpenStore(t.TempDir(), colstore.StoreOptions{
		SortKey:       []string{"kind"},
		TimeFields:    []string{"t"},
		Partition:     24 * time.Hour,
		BlockRows:     64,
		MergeFanIn:    -1,
		FlushInterval: -1,
		FlushLevel:    part.LevelFastest,
	})
	if err != nil {
		t.Fatal(err)
	}
	var lines []byte
	for i, ts := range times {
		// Epochs this far from 1970 read as nanoseconds whatever their unit.
		stamp := strconv.Quote(time.Unix(0, ts).UTC().Format(time.RFC3339Nano))
		if ts >= 1e17 || ts <= -1e17 {
			stamp = strconv.FormatInt(ts, 10)
		}
		lines = fmt.Appendf(lines, `{"kind":"%c","t":%s,"n":%d}`+"\n", 'a'+r.Intn(2), stamp, i)
		if i%997 == 0 || i == len(times)-1 {
			if _, err := s.Append(lines); err != nil {
				t.Fatal(err)
			}
			lines = lines[:0]
			if _, err := s.AppendAt([]byte(`{"kind":"c"}`+"\n"+`{"kind":"c"}`+"\n"), ts); err != nil {
				t.Fatal(err)
			}
			if err := s.Sync(); err != nil {
				t.Fatal(err)
			}
		}
		if i == len(times)/2 {
			if err := s.Compact(); err != nil {
				t.Fatal(err)
			}
		}
	}

	return s
}

// edgeTimes returns times around the given instants: on the edges of
// minutes, hours and days, a nanosecond to either side of them, and spread
// over the days around.
func edgeTimes(r *rand.Rand, around ...int64) []int64 {
	var out []int64
	for _, at := range around {
		for _, width := range []int64{60e9, 3600e9, 86400e9} {
			for k := int64(-3); k <= 3; k++ {
				edge := at - at%width + k*width
				out = append(out, edge-1, edge, edge+1)
			}
		}
		for n := 0; n < 3000; n++ {
			out = append(out, at+r.Int63n(4*86400e9)-2*86400e9)
		}
	}
	r.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })

	return out
}

// Counts by buckets of time answer as the times of the rows do for times
// before and after the epoch, at the edges of buckets, for ranges that every
// comparison bounds and that cut blocks and buckets anywhere.
func TestCountsByTimeAnswerAsRowTimesAtTheEdges(t *testing.T) {
	r := rand.New(rand.NewSource(31))
	epoch, recent := int64(0), time.Date(2024, 2, 29, 12, 0, 0, 0, time.UTC).UnixNano()
	countsAsRowTimes(t, r, edgeTimes(r, epoch, recent), []int64{
		epoch, epoch - 1, epoch + 1, epoch - 3600e9, epoch - 3600e9 + 1, epoch - 86400e9 - 1, epoch + 61e9 + 7,
		recent, recent - 1, recent + 1, recent + 3600e9, recent - 86400e9 + 59e9, recent + 7*60e9 - 1,
	}, append(hourQueries,
		`source=x | stats count() as count by span(_time, 7m) as t | sort t`,
		`source=x | stats count() as count by span(_time, 1h) as t, kind | sort t, kind`,
		`source=x | stats count() as count by span(_time, 1d) as t | sort t`,
		`source=x | stats count() as count by span(_time, 2d) as t | sort t`,
		`source=x | stats count() as count by kind, span(_time, 7d) as t | sort kind, t`,
	))
}

// hourQueries count rows by functions of their time that take a value for
// any time.
var hourQueries = []string{
	`source=x | eval h = hour(_time) | stats count() as count by h | sort h`,
	`source=x | eval h = hour(_time) | stats count() as count by kind, h | sort kind, h`,
	`source=x | stats count() as count by span(_time, 1m) as t | sort t`,
}

// The same holds for times in the first and the last hours that a time can
// take, where the start or the end of a bucket of an hour or a day lies
// outside them. Spans of an hour or more are left out: the start of the
// span of such a time lies outside them too.
func TestCountsByTimeAnswerAsRowTimesAtTheEndsOfTime(t *testing.T) {
	r := rand.New(rand.NewSource(32))
	var times []int64
	for n := 0; n < 2000; n++ {
		times = append(times, math.MinInt64+r.Int63n(2*3600e9), math.MaxInt64-r.Int63n(2*3600e9))
	}
	first, last := int64(math.MinInt64+3600e9), int64(math.MaxInt64-3600e9)
	countsAsRowTimes(t, r, times, []int64{first, first + 1, last, last - 1, -9223372020000000000, -9223369200000000000}, hourQueries)
}

// countsAsRowTimes requires queries that count rows by functions of their
// time, over rows at times, to answer as they do from the time of every
// row, for ranges that the bounds start and end.
func countsAsRowTimes(t *testing.T, r *rand.Rand, times, bounds []int64, queries []string) {
	t.Helper()
	s := edgeTimeStore(t, r, times)
	defer s.Close()
	snap := s.Table().Snapshot()
	defer snap.Release()

	ops := map[string]func(a, b *exec.Expr) *exec.Expr{"<": exec.Lt, "<=": exec.Le, ">": exec.Gt, ">=": exec.Ge, "=": exec.Eq}
	type cut struct {
		name   string
		filter *exec.Expr
	}
	cuts := []cut{{"no range", nil}}
	for name, op := range ops {
		for _, b := range bounds[:3] {
			cuts = append(cuts, cut{fmt.Sprintf("_time %s %d", name, b), op(exec.Time(), exec.Int(b))})
		}
	}
	for n := 0; n < 30; n++ {
		lo, hi := bounds[r.Intn(len(bounds))], bounds[r.Intn(len(bounds))]
		if lo > hi {
			lo, hi = hi, lo
		}
		lop, hop := []string{">", ">="}[r.Intn(2)], []string{"<", "<="}[r.Intn(2)]
		cuts = append(cuts, cut{fmt.Sprintf("_time %s %d and _time %s %d", lop, lo, hop, hi),
			exec.And(ops[lop](exec.Time(), exec.Int(lo)), ops[hop](exec.Time(), exec.Int(hi)))})
	}
	counted := 0
	for _, text := range queries {
		for _, c := range cuts {
			plan, err := pplPlan(text, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if c.filter != nil {
				plan = filtered(plan, c.filter)
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
				t.Fatalf("%s where %s: answer\n %s\nwant\n %s", text, c.name, g, w)
			}
			counted += int(got.Stats.CountedByTime)
		}
	}
	if counted == 0 {
		t.Fatal("no block was counted by time")
	}
}

// filtered returns plan with its scan also filtered by f.
func filtered(n exec.Node, f *exec.Expr) exec.Node {
	switch n := n.(type) {
	case *exec.Sort:
		c := *n
		c.Input = filtered(n.Input, f)

		return &c
	case *exec.Project:
		c := *n
		c.Input = filtered(n.Input, f)

		return &c
	case *exec.Aggregate:
		c := *n
		c.Input = filtered(n.Input, f)

		return &c
	case *exec.Scan:
		c := *n
		c.Filter = f
		if n.Filter != nil {
			c.Filter = exec.And(n.Filter, f)
		}

		return &c
	}

	return n
}
