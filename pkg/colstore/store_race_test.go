package colstore

import (
	"errors"
	"maps"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStoreKeepsItsRowsWhileEverythingRunsAtOnce(t *testing.T) {
	defer func(n int) { manifestRewriteAfter = n }(manifestRewriteAfter)
	manifestRewriteAfter = 4
	// Without syncs far more flushes, merges and drops cross each other.
	skipSyncs(t)
	path := filepath.Join(t.TempDir(), "store")
	s, err := OpenStore(path, killStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	// taken holds, per writer, the last number the store took, and acked the
	// last one it reported durable. A drop is noted in dropBegun before it
	// runs and in dropped once it has returned.
	var taken, acked [killWriters]atomic.Int64
	var dropBegun, dropped atomic.Int64
	for w := range taken {
		taken[w].Store(-1)
		acked[w].Store(-1)
	}
	closed := func(what string, err error) bool {
		if err != nil && !errors.Is(err, ErrClosed) {
			t.Errorf("%s: %v", what, err)
		}

		return err != nil
	}
	var writers sync.WaitGroup
	var seed int64
	start := func(fn func(rng *rand.Rand)) {
		seed++
		seed := seed
		writers.Add(1)
		go func() {
			defer writers.Done()
			fn(rand.New(rand.NewSource(seed)))
		}()
	}
	for w := int64(0); w < killAppenders; w++ {
		start(func(rng *rand.Rand) {
			var buf []byte
			for n := int64(0); ; {
				buf = buf[:0]
				for i := 1 + rng.Intn(30); i > 0; i-- {
					buf = killDocID{w, n}.appendJSON(buf)
					n++
				}
				if _, err := s.Append(buf); closed("Append", err) {
					return
				}
				taken[w].Store(n - 1)
				if rng.Intn(4) == 0 {
					if closed("Sync", s.Sync()) {
						return
					}
					acked[w].Store(n - 1)
				}
				time.Sleep(time.Duration(rng.Intn(300)) * time.Microsecond)
			}
		})
	}
	// The last writer syncs every request.
	start(func(rng *rand.Rand) {
		const w = killAppenders
		var buf []byte
		for n := int64(0); ; {
			buf = buf[:0]
			for i := 5 + rng.Intn(20); i > 0; i-- {
				buf = killDocID{w, n}.appendJSON(buf)
				n++
			}
			if _, err := s.Append(buf); closed("Append", err) {
				return
			}
			taken[w].Store(n - 1)
			if closed("Sync", s.Sync()) {
				return
			}
			acked[w].Store(n - 1)
			time.Sleep(time.Duration(rng.Intn(300)) * time.Microsecond)
		}
	})
	for i := 0; i < 2; i++ {
		start(func(rng *rand.Rand) {
			for !closed("Sync", s.Sync()) {
				time.Sleep(time.Duration(rng.Intn(500)) * time.Microsecond)
			}
		})
	}
	closing := make(chan struct{})
	start(func(rng *rand.Rand) {
		for done := int64(0); ; {
			select {
			case <-closing:
				return
			case <-time.After(time.Duration(1+rng.Intn(5)) * time.Millisecond):
			}
			hour := int64(math.MaxInt64)
			for w := range acked {
				hour = min(hour, firstTimedHour+(acked[w].Load()+1)/killPerHour)
			}
			if hour -= killKept; hour <= done {
				continue
			}
			dropBegun.Store(hour)
			if _, err := s.DropBefore(hour * int64(time.Hour)); closed("DropBefore", err) {
				return
			}
			dropped.Store(hour)
			done = hour
		}
	})
	start(func(rng *rand.Rand) {
		for !closed("Compact", s.Compact()) {
			time.Sleep(time.Duration(1+rng.Intn(8)) * time.Millisecond)
		}
	})
	start(func(rng *rand.Rand) {
		for !closed("Settle", s.Settle()) {
			time.Sleep(time.Duration(rng.Intn(500)) * time.Microsecond)
		}
	})

	stop := make(chan struct{})
	var readers sync.WaitGroup
	var scans atomic.Int64
	for i := 0; i < 3; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				var durable [killWriters]int64
				for w := range durable {
					durable[w] = acked[w].Load()
				}
				gone := dropped.Load()
				snap := s.Table().Snapshot()
				for _, h := range snap.Parts() {
					if h.Partition < gone {
						t.Errorf("a snapshot holds a part of partition %d, taken after the drop below %d", h.Partition, gone)
					}
				}
				// A drop that this snapshot misses parts of began before.
				floor := dropBegun.Load()
				counts, err := scanKillDocs(snap)
				if err != nil {
					t.Error(err)
					snap.Release()

					return
				}
				for id, count := range counts {
					if count != 1 {
						t.Errorf("a snapshot holds document %v %d times", id, count)
					}
				}
				for w := range durable {
					for n := int64(0); n <= durable[w]; n++ {
						if id := (killDocID{int64(w), n}); id.hour() >= floor && counts[id] != 1 {
							t.Errorf("a snapshot misses document %v, which was durable before it", id)

							break
						}
					}
				}
				// Merges and drops go on under a snapshot that is held.
				time.Sleep(2 * time.Millisecond)
				if again, err := scanKillDocs(snap); err != nil || !maps.Equal(counts, again) {
					t.Errorf("a held snapshot read %d documents and then %d: %v", len(counts), len(again), err)
				}
				snap.Release()
				scans.Add(1)
			}
		}()
	}

	// Long enough for a drop, a merge and some scans; a soak gives it more.
	// A loaded machine may finish no merge in the least time, and a drop may
	// take the merged run away again, so the merge is looked for as it runs.
	least := 500 * time.Millisecond
	if os.Getenv("LYNXDB_STORE_SOAK") != "" {
		least = 10 * time.Second
	}
	mergedRun := func() bool {
		snap := s.Table().Snapshot()
		defer snap.Release()
		for _, h := range snap.Parts() {
			if h.Tier > 0 {
				return true
			}
		}

		return false
	}
	sawMerge := false
	for begun := time.Now(); time.Since(begun) < 20*time.Second; time.Sleep(10 * time.Millisecond) {
		sawMerge = sawMerge || mergedRun()
		if time.Since(begun) >= least && dropped.Load() > 0 && scans.Load() > 2 && sawMerge {
			break
		}
	}
	close(stop)
	readers.Wait()
	// Appends, flushes, merges and drops are still arriving at the Close,
	// and a snapshot outlives it.
	held := s.Table().Snapshot()
	before := readKillDocs(t, held)
	close(closing)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	within(t, "the writers of a closed store", writers.Wait)
	if after := readKillDocs(t, held); !maps.Equal(before, after) {
		t.Fatalf("a snapshot read %d documents before the Close and %d after", len(before), len(after))
	}
	held.Release()

	opts := killStoreOptions()
	opts.MergeFanIn, opts.FlushInterval = -1, -1
	if s, err = OpenStore(path, opts); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	checkDirMatchesManifest(t, s.dir, "after reopen")
	snap := s.Table().Snapshot()
	defer snap.Release()
	counts := readKillDocs(t, snap)
	merged := 0
	for _, shape := range runShapes(t, snap) {
		if shape.tier > 0 {
			merged++
		}
	}
	// What the store took is there once, but for the partitions dropped.
	floor, want := dropped.Load(), 0
	for w := range taken {
		for n := int64(0); n <= taken[w].Load(); n++ {
			id := killDocID{int64(w), n}
			switch count := counts[id]; {
			case id.hour() < floor && count != 0:
				t.Fatalf("document %v is in a partition below the dropped hour %d", id, floor)
			case id.hour() >= floor && count != 1:
				t.Fatalf("document %v, which the store took, is there %d times", id, count)
			case count == 1:
				want++
			}
		}
	}
	if len(counts) != want {
		t.Fatalf("%d documents, %d of them are what the store took", len(counts), want)
	}
	if scans.Load() == 0 || floor == 0 || (merged == 0 && !sawMerge) || want == 0 {
		t.Fatalf("%d scans, drops up to hour %d, %d merged runs, %d documents: not everything ran", scans.Load(), floor, merged, want)
	}
}
