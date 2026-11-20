package exec_test

import (
	"context"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
)

// asked records the bytes that queries ask for ahead of reading them, which
// a store whose pages are not in memory reads from its device.
type asked struct {
	mu     sync.Mutex
	ranges map[*part.Part][][2]int
	// starts holds the offset of every block of a part, in order.
	starts map[*part.Part][]int
}

// askedOf records what queries ask of the parts of snap.
func askedOf(t testing.TB, snap *colstore.Snapshot) *asked {
	t.Helper()
	a := &asked{ranges: map[*part.Part][][2]int{}, starts: map[*part.Part][]int{}}
	for _, h := range snap.Parts() {
		p := h.Part
		ids := make([]uint32, len(p.Paths()))
		for i := range ids {
			ids[i] = uint32(i)
		}
		var b part.Block
		for i := 0; i < p.NumBlocks(); i++ {
			start := -1
			p.SetReadAhead(func(off, _ int) {
				if start < 0 || off < start {
					start = off
				}
			})
			p.ForgetReadAhead()
			if err := p.OpenBlock(i, &b); err != nil {
				t.Fatal(err)
			}
			b.Ahead(ids)
			if start < 0 {
				t.Fatalf("block %d asks for no bytes", i)
			}
			a.starts[p] = append(a.starts[p], start)
		}
		if !slices.IsSorted(a.starts[p]) {
			t.Fatal("the blocks of a part do not follow one another")
		}
		p.SetReadAhead(func(off, n int) {
			a.mu.Lock()
			a.ranges[p] = append(a.ranges[p], [2]int{off, n})
			a.mu.Unlock()
		})
	}
	a.cold(snap)

	return a
}

// cold makes the parts of snap ask again for every byte they read, as after
// their pages were let go of.
func (a *asked) cold(snap *colstore.Snapshot) {
	for _, h := range snap.Parts() {
		h.Part.ForgetReadAhead()
		h.Part.Recheck()
	}
	clear(a.ranges)
}

// total returns the requests asked for and their bytes.
func (a *asked) total() (requests, bytes int) {
	for _, rs := range a.ranges {
		for _, r := range rs {
			requests++
			bytes += r[1]
		}
	}

	return requests, bytes
}

// blocks returns the number of blocks that were asked for bytes.
func (a *asked) blocks() int {
	n := 0
	for p, rs := range a.ranges {
		seen := map[int]bool{}
		for _, r := range rs {
			i, found := slices.BinarySearch(a.starts[p], r[0])
			if !found {
				i--
			}
			seen[i] = true
		}
		n += len(seen)
	}

	return n
}

func TestCountsByTimeReadNoColumnOfTheBlocksTheyCount(t *testing.T) {
	const rows, days = 1 << 20, 2
	step := int64(days) * int64(24*time.Hour/time.Microsecond) / rows
	s := blueskyPartitions(t, filepath.Join(t.TempDir(), "store"), rows, rows/64, step, 366*24*time.Hour)
	defer s.Close()
	snap := s.Table().Snapshot()
	defer snap.Release()
	a := askedOf(t, snap)
	for _, text := range []string{
		q3Text,
		`source=x | eval h = hour(_time) | stats count() as count by h | sort h`,
		`source=x | stats count() as count by kind, span(_time, 1h) as t | sort kind, t`,
	} {
		plan, err := pplPlan(text, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		a.cold(snap)
		want, err := exec.Run(context.Background(), snap, rowTimes(plan), exec.Options{Forgetful: true})
		if err != nil {
			t.Fatal(err)
		}
		wantRequests, wantBytes := a.total()
		a.cold(snap)
		got, err := exec.Run(context.Background(), snap, plan, exec.Options{Forgetful: true})
		if err != nil {
			t.Fatal(err)
		}
		requests, bytes := a.total()
		st := got.Stats
		t.Logf("%s\n%d requests for %d bytes in %d blocks, row by row %d for %d; %+v", text, requests, bytes, a.blocks(), wantRequests, wantBytes, st)
		if rendered(got) != rendered(want) {
			t.Fatal("the answer is not that of the rows")
		}
		// An opened block is counted by time, rejected from the column
		// directory, or read.
		if st.CountedByTime == 0 || int64(a.blocks()) > st.Blocks-st.CountedByTime-st.SkippedByStats {
			t.Fatalf("bytes asked of %d blocks; of %d opened, %d were counted by time and %d rejected from the directory",
				a.blocks(), st.Blocks, st.CountedByTime, st.SkippedByStats)
		}
	}
}
