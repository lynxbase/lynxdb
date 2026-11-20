package colstore

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
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
		{"the backlog of a store that sets none", 0, true},
		{"no backlog", -1, false},
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

// TestClosedPartitionsAreMergedIntoOneRun requires that a partition whose
// span ended an hour ago is merged into one run once rows stop arriving in
// it, that the ones still open keep their runs, and that rows which come
// later are merged in once they make up an eighth of the partition.
func TestClosedPartitionsAreMergedIntoOneRun(t *testing.T) {
	skipSyncs(t)
	hour := int64(time.Hour)
	var now atomic.Int64
	timeNow = func() time.Time { return time.Unix(0, now.Load()) }
	defer func() { timeNow = time.Now }()
	// Half an hour into the third hour of the rows.
	now.Store((firstTimedHour+2)*hour + hour/2)
	s, err := OpenStore(t.TempDir(), StoreOptions{SortKey: []string{"svc"}, TimeField: "ts", Partition: time.Hour, FlushInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	write := func(docs []byte) {
		t.Helper()
		w := s.NewWriter()
		if err := w.AddNDJSON(docs); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		now.Add(int64(closedQuiet))
		s.kick()
		if err := s.Settle(); err != nil {
			t.Fatal(err)
		}
	}
	// late returns count documents numbered from first in the first hour.
	late := func(first, count int) []byte {
		var buf bytes.Buffer
		for i := first; i < first+count; i++ {
			fmt.Fprintf(&buf, `{"n":%d,"svc":"s00","ts":%d}`+"\n", i, int64(firstTimedHour)*3600e6+int64(i))
		}

		return buf.Bytes()
	}
	runs := func() [3]int {
		snap := s.Table().Snapshot()
		defer snap.Release()
		var out [3]int
		seen := map[uint64]bool{}
		for _, h := range snap.Parts() {
			if !seen[h.Run] {
				seen[h.Run] = true
				out[h.Partition-firstTimedHour]++
			}
		}

		return out
	}
	for i := range 3 {
		write(timedDocs(i*300, 300, 3))
	}
	if got := runs(); got != [3]int{1, 3, 3} {
		t.Fatalf("runs by hour %v, want the first hour in one run and three in the others", got)
	}
	// Few rows that come late stay a run of their own.
	write(late(900, 30))
	if got := runs(); got != [3]int{2, 3, 3} {
		t.Fatalf("runs by hour %v after a few late rows, want them apart", got)
	}
	// With more of them the partition is one run again.
	write(late(930, 30))
	if got := runs(); got != [3]int{1, 3, 3} {
		t.Fatalf("runs by hour %v after more late rows, want the first hour in one run", got)
	}
	// An hour later the second hour has closed as well.
	now.Add(hour)
	s.kick()
	if err := s.Settle(); err != nil {
		t.Fatal(err)
	}
	if got := runs(); got != [3]int{1, 1, 3} {
		t.Fatalf("runs by hour %v an hour later, want the second hour in one run", got)
	}
	if rows, sum := rowsAndSumOf(t, s.Table()); rows != 960 || sum != 959*960/2 {
		t.Fatalf("%d rows, sum %d", rows, sum)
	}
}

// TestAClosedPartitionWaitsForQuietAndSkipsItsTiers requires that a closed
// partition keeps its runs while rows arrive in it, without the merges by
// tier that its whole merge would repeat, that it is merged by tier once it
// holds as many runs as two tiers take and its whole merge can take on every
// core, and that it is merged whole when rows have stopped arriving.
func TestAClosedPartitionWaitsForQuietAndSkipsItsTiers(t *testing.T) {
	skipSyncs(t)
	hour := int64(time.Hour)
	var now atomic.Int64
	timeNow = func() time.Time { return time.Unix(0, now.Load()) }
	defer func() { timeNow = time.Now }()
	now.Store((firstTimedHour+2)*hour + hour/2)
	s, err := OpenStore(t.TempDir(), StoreOptions{
		TimeField: "ts", Partition: time.Hour, MergeFanIn: 3, FlushInterval: -1, MergeBlocks: 9 * runtime.GOMAXPROCS(0),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// tiers returns the tiers of the runs of the first hour, lowest first.
	tiers := func() []int {
		snap := s.Table().Snapshot()
		defer snap.Release()
		seen := map[uint64]bool{}
		var out []int
		for _, h := range snap.Parts() {
			if h.Partition == firstTimedHour && !seen[h.Run] {
				seen[h.Run] = true
				out = append(out, h.Tier)
			}
		}
		slices.Sort(out)

		return out
	}
	rows := 0
	write := func() {
		t.Helper()
		var buf bytes.Buffer
		for i := rows; i < rows+50; i++ {
			fmt.Fprintf(&buf, `{"n":%d,"ts":%d}`+"\n", i, int64(firstTimedHour)*3600e6+int64(i))
		}
		rows += 50
		w := s.NewWriter()
		if err := w.AddNDJSON(buf.Bytes()); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		// Rows keep arriving: less than the quiet time passes.
		now.Add(int64(closedQuiet) / 4)
		s.kick()
		if err := s.Settle(); err != nil {
			t.Fatal(err)
		}
	}
	for range 8 {
		write()
	}
	if got := tiers(); !slices.Equal(got, []int{0, 0, 0, 0, 0, 0, 0, 0}) {
		t.Fatalf("tiers of the runs %v while rows arrive, want eight flushed runs", got)
	}
	write()
	if got := tiers(); !slices.Equal(got, []int{0, 0, 0, 0, 0, 0, 1}) {
		t.Fatalf("tiers of the runs %v at nine runs, want three of them merged", got)
	}
	now.Add(int64(closedQuiet))
	s.kick()
	if err := s.Settle(); err != nil {
		t.Fatal(err)
	}
	if got := tiers(); len(got) != 1 {
		t.Fatalf("tiers of the runs %v once quiet, want one run", got)
	}
	if got, sum := rowsAndSumOf(t, s.Table()); got != int64(rows) || sum != int64(rows-1)*int64(rows)/2 {
		t.Fatalf("%d rows, sum %d, want %d rows", got, sum, rows)
	}
}

// TestADropWaitsOnlyForMergesOfItsPartitions requires that dropping a
// partition waits for a merge that reads it, whose run would bring its rows
// back, and for no merge of another partition.
func TestADropWaitsOnlyForMergesOfItsPartitions(t *testing.T) {
	skipSyncs(t)
	hour := int64(time.Hour)
	// No partition has closed by this clock, so that only the runs of one
	// tier are merged.
	timeNow = func() time.Time { return time.Unix(0, firstTimedHour*hour+hour/2) }
	defer func() { timeNow = time.Now }()
	for _, c := range []struct {
		name  string
		held  int64
		waits bool
	}{
		{"a merge of the partition", firstTimedHour, true},
		{"a merge of another partition", firstTimedHour + 2, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			reached, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			beforeMergeCommit = func(meta PartMeta) {
				if meta.Partition == c.held {
					once.Do(func() { close(reached) })
					<-release
				}
			}
			defer func() { beforeMergeCommit = nil }()
			s, err := OpenStore(t.TempDir(), StoreOptions{SortKey: []string{"svc"}, TimeField: "ts", Partition: time.Hour, MergeFanIn: 2, FlushInterval: -1})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			for i := range 2 {
				w := s.NewWriter()
				if err := w.AddNDJSON(timedDocs(i*300, 300, 3)); err != nil {
					t.Fatal(err)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
			}
			<-reached
			type result struct {
				rows uint64
				err  error
			}
			dropped := make(chan result, 1)
			go func() {
				rows, err := s.DropBefore((firstTimedHour + 1) * hour)
				dropped <- result{rows, err}
			}()
			var drop result
			select {
			case drop = <-dropped:
				if c.waits {
					t.Fatal("the drop did not wait for the merge of its partition")
				}
				close(release)
			case <-time.After(time.Second):
				if !c.waits {
					t.Fatal("the drop waited for a merge of another partition")
				}
				close(release)
				drop = <-dropped
			}
			if drop.err != nil || drop.rows != 200 {
				t.Fatalf("dropped %d rows: %v, want the 200 of the first hour", drop.rows, drop.err)
			}
			if err := s.Settle(); err != nil {
				t.Fatal(err)
			}
			snap := s.Table().Snapshot()
			defer snap.Release()
			for _, h := range snap.Parts() {
				if h.Partition == firstTimedHour {
					t.Fatal("the dropped partition holds a part again")
				}
			}
			if rows, _ := rowsAndSumOf(t, s.Table()); rows != 400 {
				t.Fatalf("%d rows left, want 400", rows)
			}
		})
	}
}

// TestMergeStateCountsTheRunsOfAPartitionThatWaitsForQuiet requires that the
// runs of a closed partition that rows still reach count as due to be
// merged, and that a compaction leaves nothing due and nothing running.
func TestMergeStateCountsTheRunsOfAPartitionThatWaitsForQuiet(t *testing.T) {
	skipSyncs(t)
	hour := int64(time.Hour)
	var now atomic.Int64
	timeNow = func() time.Time { return time.Unix(0, now.Load()) }
	defer func() { timeNow = time.Now }()
	now.Store((firstTimedHour+2)*hour + hour/2)
	s, err := OpenStore(t.TempDir(), StoreOptions{TimeField: "ts", Partition: time.Hour, FlushInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := range 3 {
		w := s.NewWriter()
		if err := w.AddNDJSON(timedDocs(i*50, 50, 1)); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Settle(); err != nil {
		t.Fatal(err)
	}
	if running, due := s.MergeState(); running != 0 || due != 3 {
		t.Fatalf("%d merges running and %d runs due while rows arrive, want none and three", running, due)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if running, due := s.MergeState(); running != 0 || due != 0 {
		t.Fatalf("%d merges running and %d runs due after Compact, want none", running, due)
	}
}
