package colstore

import (
	"bytes"
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestJSONBenchSmallWrites sends the lines of LYNXDB_BENCH_NDJSON, when
// LYNXDB_BENCH_SMALL_WRITES is set, from LYNXDB_BENCH_CLIENTS clients in
// requests of LYNXDB_BENCH_REQUEST_DOCS lines for LYNXDB_BENCH_SECONDS, each
// request waiting until its rows are durable, and reports the requests and
// rows per second, the latency of a request, the parts written and CPU time.
func TestJSONBenchSmallWrites(t *testing.T) {
	path := os.Getenv("LYNXDB_BENCH_NDJSON")
	if path == "" || os.Getenv("LYNXDB_BENCH_SMALL_WRITES") == "" {
		t.Skip("set LYNXDB_BENCH_NDJSON and LYNXDB_BENCH_SMALL_WRITES")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	docs := envInt(t, "LYNXDB_BENCH_REQUEST_DOCS", 100)
	var requests [][]byte
	for len(data) > 0 && len(requests) < 20000 {
		end := 0
		for n := 0; n < docs && end < len(data); n++ {
			i := bytes.IndexByte(data[end:], '\n')
			if i < 0 {
				end = len(data)

				break
			}
			end += i + 1
		}
		requests = append(requests, data[:end])
		data = data[end:]
	}
	seconds := time.Duration(envInt(t, "LYNXDB_BENCH_SECONDS", 3)) * time.Second
	t.Logf("%d CPUs; requests of %d lines; every request waits until it is durable", runtime.GOMAXPROCS(0), docs)
	t.Logf("%7s %9s %10s %9s %9s %9s %7s %7s", "clients", "req/s", "rows/s", "p50 ms", "p99 ms", "CPU ms", "parts", "left")
	for _, clients := range []int{1, 8, 64, 256} {
		opts := ingestOptions(t)
		if opts.SortKey == nil {
			opts.SortKey = []string{"kind", "commit.operation", "commit.collection", "did"}
		}
		dir := t.TempDir()
		store, err := OpenStore(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		var next atomic.Int64
		var failed sync.Once
		var wg sync.WaitGroup
		taken := make([][]time.Duration, clients)
		cpuBefore, begin := cpuTime(), time.Now()
		deadline := begin.Add(seconds)
		for c := range clients {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for time.Now().Before(deadline) {
					request := requests[int(next.Add(1))%len(requests)]
					start := time.Now()
					_, err := store.Append(request)
					if err == nil {
						err = store.Sync()
					}
					if err != nil {
						failed.Do(func() { t.Error(err) })

						return
					}
					taken[c] = append(taken[c], time.Since(start))
				}
			}()
		}
		wg.Wait()
		wall, cpu := time.Since(begin), cpuTime()-cpuBefore
		all := slices.Concat(taken...)
		if len(all) == 0 {
			t.Fatal("no request finished")
		}
		slices.Sort(all)
		ms := func(d time.Duration) float64 { return float64(d) / 1e6 }
		written := store.dir.nextID.Load()
		if err := store.Settle(); err != nil {
			t.Fatal(err)
		}
		snap := store.Table().Snapshot()
		left := len(snap.Parts())
		snap.Release()
		t.Logf("%7d %9.0f %10.0f %9.2f %9.2f %9.2f %7d %7d", clients, float64(len(all))/wall.Seconds(), float64(len(all)*docs)/wall.Seconds(),
			ms(all[len(all)/2]), ms(all[len(all)*99/100]), ms(cpu)/float64(len(all)), written, left)
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
