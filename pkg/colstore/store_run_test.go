package colstore

import (
	"bytes"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
)

// runDocs returns count documents numbered from first with a key of two
// columns, the second of which takes many values, and a timestamp in
// microseconds that spreads them over five hours.
func runDocs(first, count int) []byte {
	var buf bytes.Buffer
	for i := first; i < first+count; i++ {
		fmt.Fprintf(&buf, `{"n":%d,"svc":"s%02d","user":"u%05d","ts":%d}`+"\n", i, (i*7)%11, (i*7919)%4001, 1_700_000_000_000_000+int64(i%5)*3_600_000_000+int64(i))
	}

	return buf.Bytes()
}

// checkRuns requires every run of the snapshot to hold its rows in key
// order across parts whose key ranges do not meet, and returns the number of
// runs and of parts.
func checkRuns(t testing.TB, snap *Snapshot, key []string) (runs, parts int) {
	t.Helper()
	byRun := map[uint64][]*Handle{}
	for _, h := range snap.Parts() {
		byRun[h.Run] = append(byRun[h.Run], h)
	}
	var b part.Block
	var keys part.BlockKeys
	for run, hs := range byRun {
		slices.SortFunc(hs, func(x, y *Handle) int {
			a, _ := x.KeyRange()
			c, _ := y.KeyRange()

			return bytes.Compare(a, c)
		})
		var prev, at []byte
		for i, h := range hs {
			if len(h.SortKey()) != len(key) {
				t.Fatalf("run %d: a part with the key %v", run, h.SortKey())
			}
			first, last := h.KeyRange()
			if i > 0 && bytes.Compare(first, prev) <= 0 {
				t.Fatalf("run %d: part %d starts at a key that part %d holds", run, i, i-1)
			}
			if h.Partition != hs[0].Partition {
				t.Fatalf("run %d spans partitions %d and %d", run, hs[0].Partition, h.Partition)
			}
			for j := 0; j < h.NumBlocks(); j++ {
				if err := h.OpenBlock(j, &b); err != nil {
					t.Fatal(err)
				}
				if err := keys.Load(&b); err != nil {
					t.Fatal(err)
				}
				for row := 0; row < b.Rows; row++ {
					at = keys.AppendKey(at[:0], row)
					if bytes.Compare(at, prev) < 0 {
						t.Fatalf("run %d: part %d block %d row %d goes back in key order", run, i, j, row)
					}
					prev = append(prev[:0], at...)
				}
			}
			if !bytes.Equal(prev, last) {
				t.Fatalf("run %d: part %d ends at another key than it says", run, i)
			}
		}
		parts += len(hs)
	}

	return len(byRun), parts
}

func TestAppendWritesItsBuffersAsOneRunOfPartsByKeyRange(t *testing.T) {
	key := []string{"svc", "user"}
	for _, c := range []struct {
		name string
		opts StoreOptions
	}{
		{"one partition", StoreOptions{SortKey: key, BlockRows: 256, BufferBytes: 1 << 20, MergeFanIn: -1, FlushInterval: -1}},
		{"hourly partitions", StoreOptions{SortKey: key, TimeField: "ts", BlockRows: 256, BufferBytes: 1 << 20, MergeFanIn: -1, FlushInterval: -1, Partition: time.Hour}},
	} {
		t.Run(c.name, func(t *testing.T) {
			store, err := OpenStore(t.TempDir(), c.opts)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			const writers, requests, docs = 6, 12, 1500
			var wg sync.WaitGroup
			for w := range writers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for r := range requests {
						if _, err := store.Append(runDocs((w*requests+r)*docs, docs)); err != nil {
							t.Error(err)

							return
						}
					}
				}()
			}
			wg.Wait()
			if err := store.Sync(); err != nil {
				t.Fatal(err)
			}
			const total = writers * requests * docs
			if rows, sum := rowsAndSumOf(t, store.Table()); rows != total || sum != (total-1)*total/2 {
				t.Fatalf("%d rows, sum %d; want %d rows", rows, sum, total)
			}
			snap := store.Table().Snapshot()
			defer snap.Release()
			runs, parts := checkRuns(t, snap, key)
			flushed, _ := store.dir.Written()
			// About 6 MB of input in buffers of 1 MB: a handful of flushes,
			// each one run per partition, and far fewer runs than requests.
			partitions := 1
			if c.opts.Partition > 0 {
				partitions = 5
			}
			if runs < 2*partitions || runs > 12*partitions || parts <= runs {
				t.Fatalf("%d runs of %d parts for %d requests (%d bytes flushed)", runs, parts, writers*requests, flushed)
			}
		})
	}
}

func TestAppendHoldsNoMoreThanItsBuffersAndARunBeingWritten(t *testing.T) {
	// A slow disk, so that buffers fill while a run is written.
	replaceSyncFile(t, func(*os.File) error {
		time.Sleep(2 * time.Millisecond)

		return nil
	})
	const budget = 64 << 10
	store, err := OpenStore(t.TempDir(), StoreOptions{SortKey: []string{"svc"}, BufferBytes: budget, MergeFanIn: -1, FlushInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	request := runDocs(0, 100)
	stop := make(chan struct{})
	var most int64
	var watching sync.WaitGroup
	watching.Add(1)
	go func() {
		defer watching.Done()
		for {
			select {
			case <-stop:
				return
			default:
				most = max(most, store.buffered.Load())
				time.Sleep(50 * time.Microsecond)
			}
		}
	}()
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := range 150 {
				if _, err := store.Append(runDocs((w*150+r)*100, 100)); err != nil {
					t.Error(err)

					return
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	watching.Wait()
	// Every caller may add one request beyond the budget before it waits.
	if limit := int64(budget + 8*len(request)); most > limit || most < budget/2 {
		t.Fatalf("the buffers held %d bytes of input at most, with a budget of %d", most, budget)
	}
	if err := store.Sync(); err != nil {
		t.Fatal(err)
	}
	const total = 8 * 150 * 100
	if rows, sum := rowsAndSumOf(t, store.Table()); rows != total || sum != (total-1)*total/2 {
		t.Fatalf("%d rows, sum %d; want %d rows", rows, sum, total)
	}
}

func TestAppendWithoutASortKeyCutsARunByTime(t *testing.T) {
	store, err := OpenStore(t.TempDir(), StoreOptions{TimeField: "ts", BufferBytes: 256 << 10, BlockRows: 64, MergeFanIn: -1, FlushInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := range 10 {
				if _, err := store.Append(runDocs((w*10+r)*500, 500)); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	if err := store.Sync(); err != nil {
		t.Fatal(err)
	}
	const total = 4 * 10 * 500
	if rows, sum := rowsAndSumOf(t, store.Table()); rows != total || sum != (total-1)*total/2 {
		t.Fatalf("%d rows, sum %d; want %d rows", rows, sum, total)
	}
	snap := store.Table().Snapshot()
	defer snap.Release()
	checkTimeOrder(t, snap)
	runs := map[uint64]int{}
	most := 0
	for _, h := range snap.Parts() {
		runs[h.Run]++
		most = max(most, runs[h.Run])
	}
	if most < 2 {
		t.Fatalf("no run of %d is cut into parts", len(runs))
	}
}

func TestMergesGiveWayToFlushesBelowTheBacklog(t *testing.T) {
	runsOf := func(s *Store) int {
		snap := s.Table().Snapshot()
		defer snap.Release()
		runs := map[uint64]struct{}{}
		for _, h := range snap.Parts() {
			runs[h.Run] = struct{}{}
		}

		return len(runs)
	}
	for _, c := range []struct {
		name    string
		backlog int
		waits   bool
	}{
		{"fewer runs than the backlog", 100, true},
		{"as many runs as the backlog", 2, false},
		{"no backlog set", 0, false},
	} {
		for press, hold := range map[string]func(s *Store, by int){
			"a flush writing":      func(s *Store, by int) { s.flushWrites.Add(int32(by)) },
			"rows waiting for one": func(s *Store, by int) { s.buffered.Add(int64(by) * pressingBytes) },
		} {
			t.Run(c.name+", "+press, func(t *testing.T) {
				opts := StoreOptions{SortKey: []string{"svc"}, MergeFanIn: 2, MergeBacklog: c.backlog, FlushInterval: -1}
				s, err := OpenStore(t.TempDir(), opts)
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				// The flush or the rows are there all along.
				hold(s, 1)
				for i := range 2 {
					w := s.NewWriter()
					if err := w.AddNDJSON(storeDocs(i*500, 500)); err != nil {
						t.Fatal(err)
					}
					if err := w.Close(); err != nil {
						t.Fatal(err)
					}
				}
				deadline := time.Now().Add(2 * time.Second)
				for runsOf(s) != 1 && time.Now().Before(deadline) && !c.waits {
					time.Sleep(time.Millisecond)
				}
				if c.waits {
					time.Sleep(100 * time.Millisecond)
				}
				if got := runsOf(s); (got == 2) != c.waits {
					t.Fatalf("%d runs while it lasts", got)
				}
				hold(s, -1)
				if err := s.Settle(); err != nil {
					t.Fatal(err)
				}
				if got := runsOf(s); got != 1 {
					t.Fatalf("%d runs once it has ended", got)
				}
				if rows, sum := rowsAndSumOf(t, s.Table()); rows != 1000 || sum != 999*1000/2 {
					t.Fatalf("%d rows, sum %d", rows, sum)
				}
			})
		}
	}
}

// TestMergesDoNotWaitForEachOther requires that the runs of rows that
// arrive while a merge is under way are merged without waiting for it, and
// that the store settles on one run once it ends.
func TestMergesDoNotWaitForEachOther(t *testing.T) {
	skipSyncs(t)
	held, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	beforeMergeCommit = func(PartMeta) {
		// The first merge to finish waits where it is about to commit.
		wait := false
		once.Do(func() { wait = true })
		if wait {
			close(held)
			<-release
		}
	}
	defer func() { beforeMergeCommit = nil }()
	s, err := OpenStore(t.TempDir(), StoreOptions{SortKey: []string{"svc"}, MergeFanIn: 2, FlushInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	write := func(first int) {
		t.Helper()
		w := s.NewWriter()
		if err := w.AddNDJSON(storeDocs(first, 500)); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	runs := func() map[int]int {
		snap := s.Table().Snapshot()
		defer snap.Release()
		tiers, seen := map[int]int{}, map[uint64]bool{}
		for _, h := range snap.Parts() {
			if !seen[h.Run] {
				seen[h.Run] = true
				tiers[h.Tier]++
			}
		}

		return tiers
	}
	write(0)
	write(500)
	<-held
	// Two more runs: their merge commits while the first one waits.
	write(1000)
	write(1500)
	deadline := time.Now().Add(5 * time.Second)
	for runs()[1] != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := runs(); got[0] != 2 || got[1] != 1 {
		t.Fatalf("runs by tier %v while a merge waits, want two unmerged and one merged", got)
	}
	settled := make(chan error, 1)
	go func() { settled <- s.Settle() }()
	select {
	case err := <-settled:
		t.Fatalf("the store settled while a merge was under way: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-settled; err != nil {
		t.Fatal(err)
	}
	if got := runs(); len(got) != 1 || got[2] != 1 {
		t.Fatalf("runs by tier %v once settled, want one of the third tier", got)
	}
	if rows, sum := rowsAndSumOf(t, s.Table()); rows != 2000 || sum != 1999*2000/2 {
		t.Fatalf("%d rows, sum %d", rows, sum)
	}
}
