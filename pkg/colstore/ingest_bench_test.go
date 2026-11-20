package colstore

import (
	"fmt"
	"os"
	"runtime"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/internal/benchdata"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
)

// BenchmarkAppendBluesky loads generated events into a store with the sort
// key and the daily partitions of JSONBench from 8, 16 and 32 streams at
// once, in pieces of 1 MiB as the events endpoint appends a body, until they
// are durable. The buffers hold a quarter of the input and blocks a quarter
// of the rows of a store's, so that callers meet full buffers and flushes
// cut runs into parts as a large load does. Files are not synced: a sync
// here costs tens of milliseconds, on the volumes of a server a few. cores
// is CPU time over wall time.
func BenchmarkAppendBluesky(b *testing.B) {
	defer func(old func(*os.File) error) { syncFile = old }(syncFile)
	syncFile = func(*os.File) error { return nil }
	data := benchdata.New(1, 1<<20).Append(nil, 600000)
	opts := StoreOptions{
		TimeField:  "time_us",
		SortKey:    []string{"kind", "commit.operation", "commit.collection", "did"},
		Partition:  24 * time.Hour,
		BlockRows:  4096,
		FlushBytes: len(data) / 32,
		FlushLevel: part.LevelFastest,
	}
	const piece = 1 << 20
	var peak atomic.Uint64
	done := make(chan struct{})
	defer close(done)
	go func() {
		sample := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
		for {
			select {
			case <-done:
				return
			case <-time.After(5 * time.Millisecond):
				metrics.Read(sample)
				if v := sample[0].Value.Uint64(); v > peak.Load() {
					peak.Store(v)
				}
			}
		}
	}()
	for _, streams := range []int{8, 16, 32} {
		b.Run(fmt.Sprintf("streams=%d", streams), func(b *testing.B) {
			peak.Store(0)
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			b.StopTimer()
			var cpu, wall time.Duration
			for i := 0; i < b.N; i++ {
				dir, err := os.MkdirTemp(b.TempDir(), "store")
				if err != nil {
					b.Fatal(err)
				}
				s, err := OpenStore(dir, opts)
				if err != nil {
					b.Fatal(err)
				}
				runtime.GC()
				began, start := cpuTime(), time.Now()
				b.StartTimer()
				var wg sync.WaitGroup
				for _, region := range splitAtLines(data, streams) {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for len(region) > 0 {
							var lines []byte
							lines, region = shred.SplitLines(region, piece/443)
							if _, err := s.Append(lines); err != nil {
								b.Error(err)

								return
							}
						}
					}()
				}
				wg.Wait()
				if err := s.Sync(); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				cpu, wall = cpu+cpuTime()-began, wall+time.Since(start)
				if err := s.Close(); err != nil {
					b.Fatal(err)
				}
				_ = os.RemoveAll(dir)
			}
			b.ReportMetric(cpu.Seconds()/float64(b.N), "cpu-s/op")
			b.ReportMetric(cpu.Seconds()/wall.Seconds(), "cores")
			b.ReportMetric(float64(peak.Load())/1e6, "peak-live-MB")
		})
	}
}
