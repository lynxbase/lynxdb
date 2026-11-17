package pipeline

import (
	"errors"
	"io"
	"os"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/memgov"
)

func TestSortIteratorCloseReturnsErrorsAndReleasesResources(t *testing.T) {
	mergerErr := errors.New("merger close failed")
	childErr := errors.New("child close failed")

	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	f, err := mgr.NewSpillFile("sort-close")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	if _, err := f.WriteString("spill data"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	acct := memgov.NewTestBudget("sort", 1024).NewAccount("sort")
	if err := acct.Grow(512); err != nil {
		t.Fatal(err)
	}

	iter := &SortIterator{
		child:      &closeErrorIterator{closeErr: childErr},
		acct:       acct,
		spillMgr:   mgr,
		spillFiles: []string{path},
		merger:     closeErrorSpillMerger{err: mergerErr},
	}

	err = iter.Close()
	if !errors.Is(err, mergerErr) {
		t.Fatalf("Close() error = %v, want merger close error", err)
	}
	if !errors.Is(err, childErr) {
		t.Fatalf("Close() error = %v, want child close error", err)
	}
	if got := acct.Used(); got != 0 {
		t.Fatalf("acct.Used() = %d, want 0", got)
	}
	if count, _ := mgr.Stats(); count != 0 {
		t.Fatalf("spill file count = %d, want 0", count)
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("spill file stat error = %v, want not exist", statErr)
	}
}

func TestSpillMergerReturnsCorruptErrors(t *testing.T) {
	fields := []SortField{{Name: "key"}}

	t.Run("priming", func(t *testing.T) {
		path := writeCorruptFile(t, []byte{0xc1})

		_, err := NewSpillMerger([]string{path}, fields)
		if err == nil {
			t.Fatal("NewSpillMerger() error = nil, want corrupt spill error")
		}
		if errors.Is(err, io.EOF) {
			t.Fatalf("NewSpillMerger() error = %v, want non-EOF corrupt spill error", err)
		}
	})

	t.Run("next refill", func(t *testing.T) {
		path := writeRowSpillFile(t, []map[string]event.Value{
			{"key": event.IntValue(1)},
		}, []byte{0xc1})

		merger, err := NewSpillMerger([]string{path}, fields)
		if err != nil {
			t.Fatal(err)
		}
		defer merger.Close()

		row, err := merger.Next()
		if err == nil {
			t.Fatalf("Next() row = %v, error = nil, want corrupt spill error", row)
		}
		if errors.Is(err, io.EOF) {
			t.Fatalf("Next() error = %v, want non-EOF corrupt spill error", err)
		}
	})

	t.Run("next batch refill", func(t *testing.T) {
		path := writeRowSpillFile(t, []map[string]event.Value{
			{"key": event.IntValue(1)},
		}, []byte{0xc1})

		merger, err := NewSpillMerger([]string{path}, fields)
		if err != nil {
			t.Fatal(err)
		}
		defer merger.Close()

		batch, err := merger.NextBatch(2)
		if err == nil {
			t.Fatalf("NextBatch() batch = %v, error = nil, want corrupt spill error", batch)
		}
		if errors.Is(err, io.EOF) {
			t.Fatalf("NextBatch() error = %v, want non-EOF corrupt spill error", err)
		}
	})
}

func TestColumnarSpillMergerReturnsCorruptErrors(t *testing.T) {
	fields := []SortField{{Name: "key"}}

	t.Run("priming", func(t *testing.T) {
		path := writeCorruptFile(t, []byte("bad!"))

		_, err := NewColumnarSpillMerger([]string{path}, fields)
		if err == nil {
			t.Fatal("NewColumnarSpillMerger() error = nil, want corrupt spill error")
		}
		if errors.Is(err, io.EOF) {
			t.Fatalf("NewColumnarSpillMerger() error = %v, want non-EOF corrupt spill error", err)
		}
	})

	t.Run("next refill", func(t *testing.T) {
		path := writeColumnarSpillFile(t, []map[string]event.Value{
			{"key": event.IntValue(1)},
		}, []byte("bad!"))

		merger, err := NewColumnarSpillMerger([]string{path}, fields)
		if err != nil {
			t.Fatal(err)
		}
		defer merger.Close()

		row, err := merger.Next()
		if err == nil {
			t.Fatalf("Next() row = %v, error = nil, want corrupt spill error", row)
		}
		if errors.Is(err, io.EOF) {
			t.Fatalf("Next() error = %v, want non-EOF corrupt spill error", err)
		}
	})

	t.Run("next batch refill", func(t *testing.T) {
		path := writeColumnarSpillFile(t, []map[string]event.Value{
			{"key": event.IntValue(1)},
		}, []byte("bad!"))

		merger, err := NewColumnarSpillMerger([]string{path}, fields)
		if err != nil {
			t.Fatal(err)
		}
		defer merger.Close()

		batch, err := merger.NextBatch(2)
		if err == nil {
			t.Fatalf("NextBatch() batch = %v, error = nil, want corrupt spill error", batch)
		}
		if errors.Is(err, io.EOF) {
			t.Fatalf("NextBatch() error = %v, want non-EOF corrupt spill error", err)
		}
	})
}

type closeErrorSpillMerger struct {
	err error
}

func (c closeErrorSpillMerger) Next() (map[string]event.Value, error) { return nil, nil }

func (c closeErrorSpillMerger) NextBatch(int) (*Batch, error) { return nil, nil }

func (c closeErrorSpillMerger) Close() error { return c.err }

func writeCorruptFile(t *testing.T, data []byte) string {
	t.Helper()

	path := t.TempDir() + "/corrupt-spill"
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	return path
}

func writeRowSpillFile(t *testing.T, rows []map[string]event.Value, trailer []byte) string {
	t.Helper()

	writer, err := NewSpillWriter()
	if err != nil {
		t.Fatal(err)
	}
	path := writer.Path()
	t.Cleanup(func() { _ = os.Remove(path) })

	for _, row := range rows {
		if err := writer.WriteRow(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.CloseFile(); err != nil {
		t.Fatal(err)
	}
	appendToFile(t, path, trailer)

	return path
}

func writeColumnarSpillFile(t *testing.T, rows []map[string]event.Value, trailer []byte) string {
	t.Helper()

	writer, err := NewColumnarSpillWriter(nil, "test")
	if err != nil {
		t.Fatal(err)
	}
	path := writer.Path()
	t.Cleanup(func() { _ = os.Remove(path) })

	for _, row := range rows {
		if err := writer.WriteRow(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.CloseFile(); err != nil {
		t.Fatal(err)
	}
	appendToFile(t, path, trailer)

	return path
}

func appendToFile(t *testing.T, path string, data []byte) {
	t.Helper()

	if len(data) == 0 {
		return
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}
