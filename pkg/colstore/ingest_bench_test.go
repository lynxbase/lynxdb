package colstore

import (
	"os"
	"runtime"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/internal/benchdata"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
)

// BenchmarkAppendBluesky loads generated events into a store with the sort
// key and the daily partitions of JSONBench from eight streams at once, in
// requests of 2 MiB, until they are durable. The buffers hold a quarter of
// the input, so that callers meet full buffers as a large load does.
func BenchmarkAppendBluesky(b *testing.B) {
	data := benchdata.New(1, 1<<20).Append(nil, 600000)
	opts := StoreOptions{
		TimeField:  "time_us",
		SortKey:    []string{"kind", "commit.operation", "commit.collection", "did"},
		Partition:  24 * time.Hour,
		FlushBytes: len(data) / 32,
	}
	const streams, request = 8, 2 << 20
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
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	b.StopTimer()
	var cpu time.Duration
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
		began := cpuTime()
		b.StartTimer()
		var wg sync.WaitGroup
		for _, region := range splitAtLines(data, streams) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for len(region) > 0 {
					var piece []byte
					piece, region = shred.SplitLines(region, request/450)
					if _, err := s.Append(piece); err != nil {
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
		cpu += cpuTime() - began
		if err := s.Close(); err != nil {
			b.Fatal(err)
		}
		_ = os.RemoveAll(dir)
	}
	b.ReportMetric(cpu.Seconds()/float64(b.N), "cpu-s/op")
	b.ReportMetric(float64(peak.Load())/1e6, "peak-live-MB")
}
