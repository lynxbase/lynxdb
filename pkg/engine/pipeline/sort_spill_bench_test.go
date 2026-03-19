package pipeline

import (
	"context"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/memgov"
)

// makeSortBenchRows creates n rows with a "key" field set to descending integers
// and a "data" field of the given size. Descending order ensures sort does real work.
func makeSortBenchRows(n, dataSize int) []map[string]event.Value {
	data := make([]byte, dataSize)
	for i := range data {
		data[i] = 'x'
	}
	dataStr := string(data)

	rows := make([]map[string]event.Value, n)
	for i := 0; i < n; i++ {
		rows[i] = map[string]event.Value{
			"key":  event.IntValue(int64(n - 1 - i)),
			"data": event.StringValue(dataStr),
		}
	}

	return rows
}

// BenchmarkSortInMemory measures the baseline: sort 100K rows entirely in memory.
func BenchmarkSortInMemory(b *testing.B) {
	const nRows = 100_000
	rows := makeSortBenchRows(nRows, 16)
	ctx := context.Background()
	fields := []SortField{{Name: "key", Desc: false}}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		child := NewRowScanIterator(rows, DefaultBatchSize)
		// Large budget — no spill expected.
		acct := memgov.NewTestBudget("bench", 1<<30).NewAccount("sort")
		mgr, err := NewSpillManager(b.TempDir(), nil)
		if err != nil {
			b.Fatal(err)
		}

		iter := NewSortIteratorWithSpill(child, fields, DefaultBatchSize, acct, mgr)
		if err := iter.Init(ctx); err != nil {
			b.Fatal(err)
		}

		for {
			batch, err := iter.Next(ctx)
			if err != nil {
				b.Fatal(err)
			}
			if batch == nil {
				break
			}
		}

		iter.Close()
		mgr.CleanupAll()
	}
}

// BenchmarkSortWithSpill measures sort 100K rows with a budget that forces ~4 spill runs.
func BenchmarkSortWithSpill(b *testing.B) {
	const nRows = 100_000
	rows := makeSortBenchRows(nRows, 16)
	ctx := context.Background()
	fields := []SortField{{Name: "key", Desc: false}}

	// Budget: ~25K rows × 256 bytes = ~6.4MB. 100K rows / 25K = 4 spill runs.
	const budget = 25_000 * 256

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		child := NewRowScanIterator(rows, 1024)
		acct := memgov.NewTestBudget("bench", budget).NewAccount("sort")
		mgr, err := NewSpillManager(b.TempDir(), nil)
		if err != nil {
			b.Fatal(err)
		}

		iter := NewSortIteratorWithSpill(child, fields, 1024, acct, mgr)
		if err := iter.Init(ctx); err != nil {
			b.Fatal(err)
		}

		for {
			batch, err := iter.Next(ctx)
			if err != nil {
				b.Fatal(err)
			}
			if batch == nil {
				break
			}
		}

		iter.Close()
		mgr.CleanupAll()
	}
}

// BenchmarkSpillMergerKWay measures k-way merge throughput for k=2, 4, 8, 16.
func BenchmarkSpillMergerKWay(b *testing.B) {
	const rowsPerRun = 10_000
	fields := []SortField{{Name: "key", Desc: false}}

	for _, k := range []int{2, 4, 8, 16} {
		b.Run("k="+itoa(k), func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()

				mgr, err := NewSpillManager(b.TempDir(), nil)
				if err != nil {
					b.Fatal(err)
				}

				// Write k sorted runs with interleaved keys.
				paths := make([]string, k)
				for run := 0; run < k; run++ {
					sw, err := NewManagedSpillWriter(mgr, "bench")
					if err != nil {
						b.Fatal(err)
					}
					for j := 0; j < rowsPerRun; j++ {
						val := int64(run + j*k)
						if err := sw.WriteRow(map[string]event.Value{
							"key": event.IntValue(val),
						}); err != nil {
							b.Fatal(err)
						}
					}
					paths[run] = sw.Path()
					if err := sw.CloseFile(); err != nil {
						b.Fatal(err)
					}
				}

				b.StartTimer()

				merger, err := NewSpillMerger(paths, fields)
				if err != nil {
					b.Fatal(err)
				}

				for {
					row, err := merger.Next()
					if err != nil {
						b.Fatal(err)
					}
					if row == nil {
						break
					}
				}

				merger.Close()
				mgr.CleanupAll()
			}
		})
	}
}

// itoa converts int to string without importing strconv.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}

	return string(buf[pos:])
}
