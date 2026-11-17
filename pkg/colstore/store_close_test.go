package colstore

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
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

func TestSettleAndCompactReturnWhenTheStoreCloses(t *testing.T) {
	// Without syncs the rounds are short enough to run many.
	replaceSyncFile(t, func(*os.File) error { return nil })
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
