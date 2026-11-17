package colstore

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// within fails the test when fn has not returned after a few seconds.
func within(t testing.TB, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatalf("%s did not return", what)
	}
}

func TestClosedStoreTakesNothing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store")
	opts := StoreOptions{SortKey: []string{"svc"}, FlushBytes: 1 << 10, MergeFanIn: 2, FlushInterval: -1}
	s, err := OpenStore(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(storeDocs(0, 100)); err != nil {
		t.Fatal(err)
	}
	w := s.NewWriter()
	if err := w.AddNDJSON(storeDocs(100, 5)); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	files := partFiles(t, path)
	if _, err := s.Append(storeDocs(200, 100)); !errors.Is(err, ErrClosed) {
		t.Fatalf("Append after Close: %v", err)
	}
	if err := s.Sync(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Sync after Close: %v", err)
	}
	// A writer that was left open cannot reach the directory either.
	if err := w.Flush(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Flush of a writer after Close: %v", err)
	}
	if err := s.NewWriter().AddNDJSON(storeDocs(300, 100)); !errors.Is(err, ErrClosed) {
		t.Fatalf("a writer made after Close: %v", err)
	}
	if _, err := s.DropBefore(1 << 62); !errors.Is(err, ErrClosed) {
		t.Fatalf("DropBefore after Close: %v", err)
	}
	within(t, "Settle after Close", func() {
		if err := s.Settle(); !errors.Is(err, ErrClosed) {
			t.Errorf("Settle after Close: %v", err)
		}
	})
	within(t, "Compact after Close", func() {
		if err := s.Compact(); !errors.Is(err, ErrClosed) {
			t.Errorf("Compact after Close: %v", err)
		}
	})
	if err := s.Close(); err != nil {
		t.Fatalf("closing twice: %v", err)
	}
	if got := partFiles(t, path); len(got) != len(files) {
		t.Fatalf("files %v after Close, were %v", got, files)
	}
	if s, err = OpenStore(path, opts); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if rows, sum := rowsAndSumOf(t, s.Table()); rows != 100 || sum != 99*100/2 {
		t.Fatalf("after reopen: %d rows, sum %d", rows, sum)
	}
}

func TestSettleAndCompactReturnWhenTheStoreCloses(t *testing.T) {
	// Without syncs the rounds are short enough to run many.
	skipSyncs(t)
	for round := 0; round < 40; round++ {
		s, err := OpenStore(filepath.Join(t.TempDir(), "store"), StoreOptions{
			SortKey: []string{"svc"}, FlushBytes: 1 << 10, MergeFanIn: 2, FlushInterval: -1,
		})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 6; i++ {
			if _, err := s.Append(storeDocs(i*40, 40)); err != nil {
				t.Fatal(err)
			}
		}
		var wg sync.WaitGroup
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if i%2 == 0 {
					_ = s.Settle()
				} else {
					_ = s.Compact()
				}
			}()
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		within(t, "Settle or Compact during Close", wg.Wait)
	}
}

// countN returns how often each value of n occurs in the table.
func countN(t testing.TB, tbl *Table) map[int64]int {
	t.Helper()
	snap := tbl.Snapshot()
	defer snap.Release()
	const workers = 4
	seen := make([][]int64, workers)
	cols := make([]vec.Column, workers)
	_, err := snap.Scan(context.Background(), ScanOptions{Workers: workers}, func(w int, p *part.Part, b *part.Block) error {
		id, _ := p.PathID("n")
		ref, ok := b.Column(id, vec.KindInt)
		if !ok || ref.Count != b.Rows {
			return errors.New("a block without a full n column")
		}
		if err := b.Decode(ref, &cols[w]); err != nil {
			return err
		}
		for i := 0; i < b.Rows; i++ {
			seen[w] = append(seen[w], cols[w].Int(i))
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	counts := map[int64]int{}
	for _, values := range seen {
		for _, n := range values {
			counts[n]++
		}
	}

	return counts
}

func TestCloseWritesTheRowsOfEveryAppendThatReturnedNil(t *testing.T) {
	// Without syncs many more flushes and merges meet the Close.
	skipSyncs(t)
	for round := 0; round < 12; round++ {
		path := filepath.Join(t.TempDir(), "store")
		opts := StoreOptions{SortKey: []string{"svc"}, FlushBytes: 2 << 10, MergeFanIn: 2, FlushInterval: time.Millisecond}
		s, err := OpenStore(path, opts)
		if err != nil {
			t.Fatal(err)
		}
		const appenders, batch = 6, 7
		taken := make([][]int, appenders)
		refused := make([][]int, appenders)
		var wg sync.WaitGroup
		for a := 0; a < appenders; a++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for first := a * 1000000; ; first += batch {
					_, err := s.Append(storeDocs(first, batch))
					if errors.Is(err, ErrClosed) {
						refused[a] = append(refused[a], first)

						return
					}
					if err != nil {
						t.Error(err)

						return
					}
					taken[a] = append(taken[a], first)
					if len(taken[a])%64 == 0 {
						if err := s.Sync(); err != nil && !errors.Is(err, ErrClosed) {
							t.Error(err)
						}
					}
				}
			}()
		}
		time.Sleep(time.Duration(1+round*2) * time.Millisecond)
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		within(t, "appenders after Close", wg.Wait)
		if s, err = OpenStore(path, opts); err != nil {
			t.Fatal(err)
		}
		counts := countN(t, s.Table())
		want := 0
		for a := range taken {
			for _, first := range taken[a] {
				for n := first; n < first+batch; n++ {
					if counts[int64(n)] != 1 {
						t.Fatalf("round %d: document %d of an Append that returned nil is there %d times", round, n, counts[int64(n)])
					}
				}
				want += batch
			}
			for _, first := range refused[a] {
				if counts[int64(first)] != 0 {
					t.Fatalf("round %d: document %d of a refused Append was written", round, first)
				}
			}
		}
		if len(counts) != want {
			t.Fatalf("round %d: %d documents, want %d", round, len(counts), want)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDropBeforeThatMeetsACloseDropsOrReturnsErrClosed(t *testing.T) {
	const hours, total = 3, 600
	path := filepath.Join(t.TempDir(), "store")
	opts := StoreOptions{
		SortKey: []string{"svc"}, TimeField: "ts", Partition: time.Hour, MergeFanIn: -1, FlushInterval: -1,
	}
	s, err := OpenStore(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	w := s.NewWriter()
	if err := w.AddNDJSON(timedDocs(0, total, hours)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// The drop is held where it waits for its turn, and the store is closed
	// in the meantime.
	s.dropping.Lock()
	type result struct {
		rows uint64
		err  error
	}
	dropped := make(chan result, 1)
	go func() {
		rows, err := s.DropBefore((firstTimedHour + 2) * int64(time.Hour))
		dropped <- result{rows, err}
	}()
	time.Sleep(20 * time.Millisecond)
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	time.Sleep(20 * time.Millisecond)
	s.dropping.Unlock()
	drop := <-dropped
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if s, err = OpenStore(path, opts); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	rows, _ := rowsAndSumOf(t, s.Table())
	switch {
	case errors.Is(drop.err, ErrClosed):
		if rows != total {
			t.Fatalf("a drop that returned ErrClosed left %d of %d rows", rows, total)
		}
	case drop.err != nil:
		t.Fatal(drop.err)
	case drop.rows == 0 || rows != total-int64(drop.rows):
		t.Fatalf("a drop that returned nil reports %d rows dropped and left %d of %d", drop.rows, rows, total)
	}
}
