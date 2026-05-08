package segment

import (
	"fmt"
	"sync"
	"testing"
)

func TestConcurrent_Reader_LoadRangeBSI_MultipleGoroutines_ReturnsCachedIndexes(t *testing.T) {
	events := makeRangeBSIEvents(t, 4096)
	data := writeRangeBSISegment(t, events, nil)

	r, err := OpenSegment(data)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	columns := rangeBSICatalogColumnsForReaderTest(t, r)
	if len(columns) == 0 {
		t.Fatal("no range BSI columns found")
	}

	const goroutines = 16
	const iterations = 128
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines*iterations)
	for worker := 0; worker < goroutines; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				rgIdx := (worker + i) % r.RowGroupCount()
				col := columns[(worker*7+i)%len(columns)]
				idx, err := r.LoadRangeBSI(rgIdx, col)
				if err != nil {
					errCh <- err
					return
				}
				if idx == nil {
					errCh <- fmt.Errorf("nil range BSI for row group %d column %q", rgIdx, col)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent LoadRangeBSI: %v", err)
	}

	for rgIdx := 0; rgIdx < r.RowGroupCount(); rgIdx++ {
		for _, col := range columns {
			first, err := r.LoadRangeBSI(rgIdx, col)
			if err != nil {
				t.Fatalf("LoadRangeBSI(%d, %q): %v", rgIdx, col, err)
			}
			second, err := r.LoadRangeBSI(rgIdx, col)
			if err != nil {
				t.Fatalf("LoadRangeBSI(%d, %q) second call: %v", rgIdx, col, err)
			}
			if first == nil || second == nil {
				t.Fatalf("LoadRangeBSI(%d, %q) = (%p,%p), want non-nil", rgIdx, col, first, second)
			}
			if first != second {
				t.Fatalf("LoadRangeBSI(%d, %q) did not reuse cached pointer", rgIdx, col)
			}
		}
	}
}
