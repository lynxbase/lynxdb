package colstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// firstKept keeps the first want times by time of the rows that a scan
// hands it, as a caller of a scan by time does, and moves the bound of the
// scan to the last of them.
type firstKept struct {
	mu     sync.Mutex
	latest bool
	want   int
	// times holds the times kept, the first by time first.
	times []int64
	bound atomic.Int64
}

func newFirstKept(want int, latest bool) *firstKept {
	k := &firstKept{latest: latest, want: want}
	k.bound.Store(math.MaxInt64)
	if latest {
		k.bound.Store(math.MinInt64)
	}

	return k
}

func (k *firstKept) offer(times []int64) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.times = append(k.times, times...)
	slices.Sort(k.times)
	if k.latest {
		slices.Reverse(k.times)
	}
	if len(k.times) >= k.want {
		k.times = k.times[:k.want]
		k.bound.Store(k.times[k.want-1])
	}
}

func (k *firstKept) kept() (have, want int) { return len(k.times), k.want }

// scanFirst scans snap by time for the first want rows of those that keep
// admits by their part and their n, which is their time, and returns their
// times and what the scan counted. Each goroutine waits after a block for a
// time that seed and the block decide, so that the goroutines fall
// differently from seed to seed; seed zero waits for none.
func scanFirst(t *testing.T, snap *Snapshot, opts ScanOptions, want int, keep func(p *part.Part, n int64) bool, seed uint64) ([]int64, ScanStats) {
	t.Helper()
	k := newFirstKept(want, opts.ByTime < 0)
	opts.Bound, opts.Kept = &k.bound, k.kept
	cols := make([]vec.Column, opts.Workers)
	found := make([][]int64, opts.Workers)
	stats, err := snap.Scan(context.Background(), opts, func(w int, p *part.Part, b *part.Block) error {
		id, _ := p.PathID("n")
		ref, ok := b.Column(id, vec.KindInt)
		if !ok {
			return errors.New("no n column")
		}
		if err := b.Decode(ref, &cols[w]); err != nil {
			return err
		}
		times := found[w][:0]
		for i := 0; i < b.Rows; i++ {
			if n := cols[w].Int(i); keep(p, n) {
				times = append(times, n)
			}
		}
		found[w] = times
		if seed != 0 {
			h := (seed + uint64(b.Index())*0x9e3779b97f4a7c15 + uint64(cols[w].Int(0))) * 0xbf58476d1ce4e5b9
			time.Sleep(time.Duration(h>>32%200) * time.Microsecond)
		}
		k.offer(times)

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	return k.times, stats
}

// TestScanByTimeOpensTheSameBlocksHoweverItsGoroutinesFall requires of a
// scan by time whose caller keeps a few rows of each block that the blocks
// it opens do not depend on how long its goroutines take over theirs, that
// they are few more than one goroutine opens, which opens none that the
// rows before it rule out, and that the rows kept are the first of all.
func TestScanByTimeOpensTheSameBlocksHoweverItsGoroutinesFall(t *testing.T) {
	const partRows, blockRows, workers, want = 4000, 50, 8, 60
	tbl := NewTable()
	defer tbl.Close()
	// Parts in the order of time that overlap their neighbours by half, of
	// two tags by turns.
	var handles []*Handle
	tags := map[*part.Part]string{}
	firsts := map[*part.Part]int64{}
	for i := 0; i < 8; i++ {
		tag := "alpha"
		if i%2 == 1 {
			tag = "beta"
		}
		p := makePart(t, i*partRows/2, partRows, blockRows, tag)
		tags[p], firsts[p] = tag, int64(i*partRows/2)
		handles = append(handles, NewHandle(p, nil))
	}
	blocks := int64(len(handles) * partRows / blockRows)
	if err := tbl.Apply(handles, nil); err != nil {
		t.Fatal(err)
	}
	snap := tbl.Snapshot()
	defer snap.Release()
	for _, c := range []struct {
		name   string
		byTime int
		// every is one in how many rows is kept.
		every int64
		tag   string
		// lo and hi bound the times of the scan unless both are zero.
		lo, hi int64
	}{
		{name: "the latest of a few", byTime: -1, every: 13},
		{name: "the earliest of a few", byTime: 1, every: 13},
		{name: "the latest of one in a block", byTime: -1, every: 61},
		{name: "the latest of a few with a token", byTime: -1, every: 13, tag: "beta"},
		{name: "the earliest of a few with a token", byTime: 1, every: 7, tag: "beta"},
		{name: "the latest of a few in a window", byTime: -1, every: 13, lo: 2500, hi: 13_333},
		{name: "the earliest of a few in a window", byTime: 1, every: 13, lo: 2500, hi: 13_333},
		{name: "the latest of fewer than are wanted", byTime: -1, every: 13, lo: 9000, hi: 9200},
		{name: "the latest of all", byTime: -1, every: 1},
		{name: "the earliest of all", byTime: 1, every: 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			keep := func(p *part.Part, n int64) bool {
				return n%c.every == 0 && (c.tag == "" || tags[p] == c.tag) && (c.hi == 0 || (n >= c.lo && n <= c.hi))
			}
			var wanted []int64
			for p, first := range firsts {
				for n := first; n < first+partRows; n++ {
					if keep(p, n) {
						wanted = append(wanted, n)
					}
				}
			}
			slices.Sort(wanted)
			if c.byTime < 0 {
				slices.Reverse(wanted)
			}
			wanted = wanted[:min(want, len(wanted))]
			opts := ScanOptions{ByTime: c.byTime, MinTime: c.lo, MaxTime: c.hi, Workers: 1}
			if c.tag != "" {
				opts.Tokens = tokenfilter.AppendHashes(nil, []byte(c.tag))
			}
			got, least := scanFirst(t, snap, opts, want, keep, 0)
			if !slices.Equal(got, wanted) {
				t.Fatalf("one goroutine keeps %v, want %v", got, wanted)
			}
			opts.Workers = workers
			var first ScanStats
			for seed := uint64(0); seed < 24; seed++ {
				got, stats := scanFirst(t, snap, opts, want, keep, seed)
				if !slices.Equal(got, wanted) {
					t.Fatalf("seed %d: kept %v, want %v", seed, got, wanted)
				}
				if stats.Blocks+stats.SkippedByTime+stats.SkippedByToken != blocks {
					t.Fatalf("seed %d: %+v, which do not add up to the %d blocks", seed, stats, blocks)
				}
				if seed == 0 {
					first = stats
					t.Logf("%d blocks opened, %d by one goroutine", stats.Blocks, least.Blocks)
				}
				// A scan whose first block fills the result goes on as its
				// goroutines fall, within a block for each of them.
				if c.every == 1 {
					if stats.Blocks > least.Blocks+workers {
						t.Fatalf("seed %d: %d blocks opened, one goroutine opens %d", seed, stats.Blocks, least.Blocks)
					}

					continue
				}
				if stats != first {
					t.Fatalf("seed %d: %+v, without waits %+v", seed, stats, first)
				}
			}
			// No round opens more blocks than all before it together or
			// than there are goroutines, and those before the last open
			// only blocks that one goroutine opens as well.
			if most := 2*least.Blocks + workers; first.Blocks > most {
				t.Fatalf("%d blocks opened, one goroutine opens %d, want at most %d", first.Blocks, least.Blocks, most)
			}
		})
	}
}

// TestScanByTimeTakesEveryBlockThatNoBoundRulesOut requires a scan by time
// whose blocks all hold the same times to open each of them.
func TestScanByTimeTakesEveryBlockThatNoBoundRulesOut(t *testing.T) {
	tbl := NewTable()
	defer tbl.Close()
	var handles []*Handle
	for i := 0; i < 12; i++ {
		handles = append(handles, NewHandle(makePart(t, 0, 2, 2, "alpha"), nil))
	}
	if err := tbl.Apply(handles, nil); err != nil {
		t.Fatal(err)
	}
	snap := tbl.Snapshot()
	defer snap.Release()
	all := func(*part.Part, int64) bool { return true }
	for byTime, want := range map[int]string{-1: "[1 1 1]", 1: "[0 0 0]"} {
		got, stats := scanFirst(t, snap, ScanOptions{ByTime: byTime, Workers: 4}, 3, all, 1)
		if fmt.Sprint(got) != want || stats.Blocks != 12 || stats.SkippedByTime != 0 {
			t.Fatalf("by time %d: kept %v from %+v, want %s from 12 blocks", byTime, got, stats, want)
		}
	}
}
