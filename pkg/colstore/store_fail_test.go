package colstore

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// failSyncs makes the syncs that match fail while the returned switch is
// set, and leaves out all others.
func failSyncs(t testing.TB, match func(name string) bool) *atomic.Bool {
	t.Helper()
	var failing atomic.Bool
	replaceSyncFile(t, func(f *os.File) error {
		if failing.Load() && match(f.Name()) {
			return errInjected
		}

		return nil
	})

	return &failing
}

func isPartFile(name string) bool { return strings.HasSuffix(name, partSuffix+tmpSuffix) }

func isManifest(name string) bool { return filepath.Base(name) == manifestName }

func TestFlushThatFailsKeepsItsRowsForTheNextTry(t *testing.T) {
	const hours = 3
	cases := []struct {
		name string
		// arrange makes flushes fail until the function it returns is called.
		arrange func(path string) (repair func())
	}{
		{"the second part file cannot be made durable", func(string) func() {
			var parts atomic.Int64
			failing := failSyncs(t, func(name string) bool { return isPartFile(name) && parts.Add(1)%hours == 2 })
			failing.Store(true)

			return func() { failing.Store(false) }
		}},
		{"the directory cannot be made durable", func(path string) func() {
			failing := failSyncs(t, func(name string) bool { return name == path })
			failing.Store(true)

			return func() { failing.Store(false) }
		}},
		{"the manifest record cannot be made durable", func(string) func() {
			// Every other sync of the manifest fails: the one that follows a
			// record, not the one that follows cutting it off again.
			var syncs atomic.Int64
			failing := failSyncs(t, func(name string) bool { return isManifest(name) && syncs.Add(1)%2 == 1 })
			failing.Store(true)

			return func() { failing.Store(false) }
		}},
		{"no part file can be created", func(path string) func() {
			if os.Geteuid() == 0 || runtime.GOOS == "windows" {
				t.Skip("the directory cannot be made read-only for this user")
			}
			if err := os.Chmod(path, 0o555); err != nil {
				t.Fatal(err)
			}

			return func() {
				if err := os.Chmod(path, 0o755); err != nil {
					t.Fatal(err)
				}
			}
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "store")
			opts := StoreOptions{
				SortKey: []string{"svc"}, TimeField: "ts", Partition: time.Hour, MergeFanIn: -1, FlushInterval: -1,
			}
			s, err := OpenStore(path, opts)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Append(timedDocs(0, 300, hours)); err != nil {
				t.Fatal(err)
			}
			if err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			repair := c.arrange(path)
			// Buffered rows that fall into three partitions, from two calls.
			for first := 300; first < 900; first += 300 {
				if _, err := s.Append(timedDocs(first, 300, hours)); err != nil {
					t.Fatal(err)
				}
			}
			for try := 0; try < 2; try++ {
				if err := s.Sync(); err == nil {
					t.Fatal("a Sync that could not be written returned no error")
				}
				if rows, sum := rowsAndSumOf(t, s.Table()); rows != 300 || sum != 299*300/2 {
					t.Fatalf("after a failed flush: %d rows, sum %d", rows, sum)
				}
			}
			repair()
			checkDirMatchesManifest(t, s.dir, "after the failed flushes")
			if err := s.Sync(); err != nil {
				t.Fatalf("the Sync after the failed ones: %v", err)
			}
			check := func(when string) {
				t.Helper()
				counts := countN(t, s.Table())
				for n := int64(0); n < 900; n++ {
					if counts[n] != 1 {
						t.Fatalf("%s: document %d is there %d times", when, n, counts[n])
					}
				}
				if len(counts) != 900 {
					t.Fatalf("%s: %d documents, want 900", when, len(counts))
				}
				checkDirMatchesManifest(t, s.dir, when)
			}
			check("after the flush that worked")
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if s, err = OpenStore(path, opts); err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			check("after reopen")
		})
	}
}

func TestStoreReportsAFailedTimedFlush(t *testing.T) {
	failing := failSyncs(t, isPartFile)
	path := filepath.Join(t.TempDir(), "store")
	opts := StoreOptions{SortKey: []string{"svc"}, MergeFanIn: -1, FlushInterval: 2 * time.Millisecond}
	s, err := OpenStore(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	failing.Store(true)
	if _, err := s.Append(storeDocs(0, 200)); err != nil {
		t.Fatal(err)
	}
	// Nothing is merged, so Settle returns as soon as the flush has failed.
	deadline := time.Now().Add(10 * time.Second)
	for s.Settle() == nil {
		if time.Now().After(deadline) {
			t.Fatal("Settle does not report the timed flush that failed")
		}
		time.Sleep(time.Millisecond)
	}
	if err := s.Settle(); !errors.Is(err, errInjected) {
		t.Fatalf("Settle: %v", err)
	}
	if err := s.Sync(); !errors.Is(err, errInjected) {
		t.Fatalf("Sync while flushes fail: %v", err)
	}
	if rows, _ := rowsAndSumOf(t, s.Table()); rows != 0 {
		t.Fatalf("%d rows are visible though no flush worked", rows)
	}
	failing.Store(false)
	// The rows waited in their buffer and are written once.
	if err := s.Sync(); err != nil {
		t.Fatalf("Sync after the flushes work again: %v", err)
	}
	if rows, sum := rowsAndSumOf(t, s.Table()); rows != 200 || sum != 199*200/2 {
		t.Fatalf("%d rows, sum %d", rows, sum)
	}
	checkDirMatchesManifest(t, s.dir, "after the flush that worked")
	if err := s.Close(); !errors.Is(err, errInjected) {
		t.Fatalf("Close of a store whose timed flush failed: %v", err)
	}
	if s, err = OpenStore(path, opts); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if rows, sum := rowsAndSumOf(t, s.Table()); rows != 200 || sum != 199*200/2 {
		t.Fatalf("after reopen: %d rows, sum %d", rows, sum)
	}
}

func TestStoreReportsAFailedMergeAndMergesAgain(t *testing.T) {
	cases := []struct {
		name string
		// match picks the syncs that fail, counting those of its kind.
		match func(name string, seen int64) bool
	}{
		{"the merged part cannot be made durable", func(name string, _ int64) bool { return isPartFile(name) }},
		// The sync behind a record fails, the one behind cutting it off
		// again does not.
		{"the manifest record of the merge cannot be made durable", func(name string, seen int64) bool { return isManifest(name) && seen%2 == 1 }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "store")
			opts := StoreOptions{SortKey: []string{"svc"}, MergeFanIn: -1, FlushInterval: -1}
			s, err := OpenStore(path, opts)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 4; i++ {
				if _, err := s.Append(storeDocs(i*100, 100)); err != nil {
					t.Fatal(err)
				}
				if err := s.Sync(); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			// The merger finds four runs to merge and cannot write.
			var manifests atomic.Int64
			failing := failSyncs(t, func(name string) bool {
				seen := manifests.Load()
				if isManifest(name) {
					seen = manifests.Add(1)
				}

				return c.match(name, seen)
			})
			failing.Store(true)
			opts.MergeFanIn = 2
			if s, err = OpenStore(path, opts); err != nil {
				t.Fatal(err)
			}
			// The store stops before the syncs are put back.
			t.Cleanup(func() { s.Close() })
			if err := s.Settle(); !errors.Is(err, errInjected) {
				t.Fatalf("Settle after a merge that failed: %v", err)
			}
			if err := s.Compact(); !errors.Is(err, errInjected) {
				t.Fatalf("Compact after a merge that failed: %v", err)
			}
			tiers := func() (top int) {
				snap := s.Table().Snapshot()
				defer snap.Release()
				for _, h := range snap.Parts() {
					top = max(top, h.Tier)
				}

				return top
			}
			if rows, sum := rowsAndSumOf(t, s.Table()); rows != 400 || sum != 399*400/2 || tiers() != 0 {
				t.Fatalf("after the failed merges: %d rows, sum %d, tier %d", rows, sum, tiers())
			}
			failing.Store(false)
			// The error stays, and the merger goes on with the next flush.
			if _, err := s.Append(storeDocs(400, 100)); err != nil {
				t.Fatal(err)
			}
			if err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			for deadline := time.Now().Add(10 * time.Second); tiers() == 0; time.Sleep(time.Millisecond) {
				if time.Now().After(deadline) {
					t.Fatal("no merge after the one that failed")
				}
			}
			if err := s.Close(); !errors.Is(err, errInjected) {
				t.Fatalf("Close of a store whose merge failed: %v", err)
			}
			opts.MergeFanIn = -1
			if s, err = OpenStore(path, opts); err != nil {
				t.Fatal(err)
			}
			counts := countN(t, s.Table())
			for n := int64(0); n < 500; n++ {
				if counts[n] != 1 {
					t.Fatalf("after reopen: document %d is there %d times", n, counts[n])
				}
			}
			if len(counts) != 500 {
				t.Fatalf("after reopen: %d documents, want 500", len(counts))
			}
			checkDirMatchesManifest(t, s.dir, "after reopen")
		})
	}
}

func TestAppendTakesNoLinesWhileAFullBufferCannotBeWritten(t *testing.T) {
	failing := failSyncs(t, isPartFile)
	path := filepath.Join(t.TempDir(), "store")
	opts := StoreOptions{SortKey: []string{"svc"}, BufferBytes: 4 << 10, MergeFanIn: -1, FlushInterval: -1}
	s, err := OpenStore(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	// The store stops before the syncs are put back.
	t.Cleanup(func() { s.Close() })
	failing.Store(true)
	// The first request fills the buffer, whose flush fails. It is told
	// only if it still has lines to buffer when the flush has failed.
	_, first := s.Append(storeDocs(0, 200))
	if first != nil && !errors.Is(first, errInjected) {
		t.Fatalf("an Append whose flush failed: %v", first)
	}
	within(t, "the flush of the full buffer to fail", func() {
		for failed := false; !failed; time.Sleep(time.Millisecond) {
			s.roomMu.Lock()
			failed = s.flushErr != nil
			s.roomMu.Unlock()
		}
	})
	// A client that is told of a failure sends the request again later;
	// none of its lines may be kept in the meantime.
	const requests, lines = 5, 50
	for i := 1; i <= requests; i++ {
		if _, err := s.Append(storeDocs(i*1000, lines)); !errors.Is(err, errInjected) {
			t.Fatalf("request %d against a full buffer: %v", i, err)
		}
	}
	failing.Store(false)
	// A flush that began while syncs failed may end in that failure after
	// they work again, and a Sync that waited for it reports it; the next
	// Sync writes the rows it left.
	within(t, "a sync to write the rows of the failed flushes", func() {
		for s.Sync() != nil {
			time.Sleep(time.Millisecond)
		}
	})
	counts := countN(t, s.Table())
	for n, count := range counts {
		if n >= 1000 || count != 1 {
			t.Fatalf("document %d of a request that failed is there %d times", n, count)
		}
	}
	// Of the request that filled the buffer the lines it buffered stay:
	// all of them when it was not told of the failure.
	if kept := int64(len(counts)); kept == 0 || counts[kept-1] != 1 || (first == nil) != (kept == 200) {
		t.Fatalf("%d documents of the first request are there, which was answered %v", kept, first)
	}
	for i := 1; i <= requests; i++ {
		if _, err := s.Append(storeDocs(i*1000, lines)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	again := countN(t, s.Table())
	for i := 1; i <= requests; i++ {
		for n := int64(i * 1000); n < int64(i*1000+lines); n++ {
			if again[n] != 1 {
				t.Fatalf("document %d of a request sent again is there %d times", n, again[n])
			}
		}
	}
	if len(again) != len(counts)+requests*lines {
		t.Fatalf("%d documents, want %d", len(again), len(counts)+requests*lines)
	}
}
