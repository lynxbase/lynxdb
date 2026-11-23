package colstore

import (
	"os"
	"runtime/metrics"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/internal/benchdata"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/mem"
)

// BenchmarkLoadMemory loads LYNXDB_BENCH_LOAD_MB (6144 by default) of
// generated events into a store set up as a server sets it up for JSONBench,
// with its buffers at their defaults (LYNXDB_BENCH_BUFFER_MB sets them) and
// merges off, from LYNXDB_BENCH_STREAMS (16) streams that make their events
// as they go and append them in pieces of 1 MiB, as the events endpoint
// appends a body. Runs are dropped as they are written, so that the disk
// holds little, and files are not synced. It reports the peak resident
// memory of the process and the runtime's account of its memory where that
// peaked; LYNXDB_BENCH_HEAP_PROFILE names a file for the heap profile there.
// Run it alone, with -cpu 32 for the buffers of a 32-core machine.
func BenchmarkLoadMemory(b *testing.B) {
	defer func(old func(*os.File) error) { syncFile = old }(syncFile)
	syncFile = func(*os.File) error { return nil }
	defer mem.BoundGarbage(512 << 20)()
	total := int64(envInt(b, "LYNXDB_BENCH_LOAD_MB", 6144)) << 20
	streams := envInt(b, "LYNXDB_BENCH_STREAMS", 16)
	opts := StoreOptions{
		TimeFields: []string{"time_us"},
		SortKey:    []string{"kind", "commit.operation", "commit.collection", "did"},
		Partition:  24 * time.Hour,
		MergeFanIn: -1,
		FlushLevel: part.LevelFastest,
	}
	if os.Getenv("LYNXDB_BENCH_BUFFER_MB") != "" {
		opts.BufferBytes = envInt(b, "LYNXDB_BENCH_BUFFER_MB", 0) << 20
	}
	const piece = 1 << 20
	var peak memoryAccount
	var wall, cpu time.Duration
	b.SetBytes(total)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		dir, err := os.MkdirTemp(b.TempDir(), "store")
		if err != nil {
			b.Fatal(err)
		}
		s, err := OpenStore(dir, opts)
		if err != nil {
			b.Fatal(err)
		}
		dropped := dropRuns(b, s)
		watch := watchMemory(os.Getenv("LYNXDB_BENCH_HEAP_PROFILE"))
		began, start := cpuTime(), time.Now()
		var fed atomic.Int64
		var wg sync.WaitGroup
		for j := range streams {
			wg.Add(1)
			go func() {
				defer wg.Done()
				g := benchdata.New(uint64(j+1), 1<<22)
				var lines []byte
				for fed.Add(piece) <= total {
					lines = g.Append(lines[:0], piece/443)
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
		wall, cpu = wall+time.Since(start), cpu+cpuTime()-began
		if got := watch(); got.held() > peak.held() {
			peak = got
		}
		dropped()
		if err := s.Close(); err != nil {
			b.Fatal(err)
		}
		_ = os.RemoveAll(dir)
	}
	b.ReportMetric(cpu.Seconds()/wall.Seconds(), "cores")
	b.ReportMetric(float64(peakRSS())/(1<<20), "MB-peak-rss")
	b.ReportMetric(float64(peak.held())/(1<<20), "MB-runtime-held")
	b.ReportMetric(float64(peak.objects)/(1<<20), "MB-objects")
	b.ReportMetric(float64(peak.live)/(1<<20), "MB-live")
	b.ReportMetric(float64(peak.free+peak.unused)/(1<<20), "MB-heap-free")
	b.ReportMetric(float64(peak.stacks)/(1<<20), "MB-stacks")
	b.ReportMetric(float64(peak.other)/(1<<20), "MB-other")
}

// dropRuns removes the parts that the store commits as they come, and
// returns the function that stops it.
func dropRuns(b *testing.B, s *Store) (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			case <-time.After(20 * time.Millisecond):
			}
			snap := s.Table().Snapshot()
			if parts := snap.Parts(); len(parts) > 0 {
				if _, err := s.dir.Commit(nil, parts); err != nil {
					b.Error(err)
				}
			}
			snap.Release()
		}
	}()

	return func() {
		close(done)
		wg.Wait()
	}
}

// memoryAccount is the runtime's account of the memory it holds: objects on
// the heap, the part of those that the last collection found live, heap
// spans free or not yet used, goroutine stacks and the rest of what it has
// from the system and not returned.
type memoryAccount struct {
	total, released                     uint64
	objects, live, free, unused, stacks uint64
	other                               uint64
}

func (m memoryAccount) held() uint64 { return m.total - m.released }

// watchMemory samples the runtime's memory until the function it returns is
// called, which returns the sample where the memory held peaked. With a
// profile path it writes the heap profile whenever the objects on the heap
// grew by a sixteenth over the last one written.
func watchMemory(profile string) (stop func() memoryAccount) {
	names := []string{
		"/memory/classes/total:bytes",
		"/memory/classes/heap/released:bytes",
		"/memory/classes/heap/objects:bytes",
		"/gc/heap/live:bytes",
		"/memory/classes/heap/free:bytes",
		"/memory/classes/heap/unused:bytes",
		"/memory/classes/heap/stacks:bytes",
		"/memory/classes/os-stacks:bytes",
	}
	samples := make([]metrics.Sample, len(names))
	for i, name := range names {
		samples[i].Name = name
	}
	read := func() memoryAccount {
		metrics.Read(samples)
		v := func(i int) uint64 { return samples[i].Value.Uint64() }
		m := memoryAccount{
			total: v(0), released: v(1), objects: v(2), live: v(3), free: v(4), unused: v(5), stacks: v(6) + v(7),
		}
		m.other = m.held() - m.objects - m.free - m.unused - m.stacks

		return m
	}
	var peak memoryAccount
	var profiled uint64
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			case <-time.After(2 * time.Millisecond):
			}
			m := read()
			if m.held() > peak.held() {
				peak = m
			}
			if profile != "" && m.objects > profiled+profiled/16 {
				profiled = m.objects
				if f, err := os.Create(profile); err == nil {
					_ = pprof.WriteHeapProfile(f)
					f.Close()
				}
			}
		}
	}()

	return func() memoryAccount {
		close(done)
		wg.Wait()

		return peak
	}
}
