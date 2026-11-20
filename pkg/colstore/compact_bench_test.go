package colstore

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/internal/benchdata"
)

// BenchmarkCompact merges the sorted runs that ingest leaves into one run per
// partition, as a load that ends with a compaction does, and reports rows per
// second, CPU seconds, the cores that were busy on average and the bytes the
// merges wrote. The store is set up as a server sets it up for the Bluesky
// benchmark. LYNXDB_BENCH_MERGE_ROWS sets the events (1Mi by default),
// LYNXDB_BENCH_MERGE_RUNS the runs they are written in (16 by default),
// LYNXDB_BENCH_PARTITION the span of a partition (24h by default; the events
// cover about 200 s per million) and LYNXDB_BENCH_MERGE_BLOCKS the merge
// budget of the store.
func BenchmarkCompact(b *testing.B) {
	rows := envInt(b, "LYNXDB_BENCH_MERGE_ROWS", 1<<20)
	runs := envInt(b, "LYNXDB_BENCH_MERGE_RUNS", 16)
	opts := StoreOptions{
		SortKey:     []string{"kind", "commit.operation", "commit.collection", "did"},
		TimeFields:  []string{"time_us"},
		KeepText:    true,
		Partition:   24 * time.Hour,
		MergeBlocks: envInt(b, "LYNXDB_BENCH_MERGE_BLOCKS", 0),
	}
	if v := os.Getenv("LYNXDB_BENCH_PARTITION"); v != "" {
		width, err := time.ParseDuration(v)
		if err != nil || width <= 0 {
			b.Fatalf("LYNXDB_BENCH_PARTITION=%q is not a positive duration", v)
		}
		opts.Partition = width
	}
	template := filepath.Join(b.TempDir(), "runs")
	size := writeRunsOnly(b, template, opts, rows, runs)

	var wall, cpu time.Duration
	var written int64
	work := b.TempDir()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := filepath.Join(work, "store")
		linkStore(b, template, dir)
		b.StartTimer()
		cpuBefore, begin := cpuTime(), time.Now()
		s, err := OpenStore(dir, opts)
		if err != nil {
			b.Fatal(err)
		}
		if err := s.Compact(); err != nil {
			b.Fatal(err)
		}
		wall, cpu = wall+time.Since(begin), cpu+cpuTime()-cpuBefore
		b.StopTimer()
		_, merged := s.Written()
		written += merged
		if left := runsOf(s); left != partitionsOf(s) {
			b.Fatalf("%d runs in %d partitions after compaction", left, partitionsOf(s))
		}
		if err := s.Close(); err != nil {
			b.Fatal(err)
		}
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	n := float64(b.N)
	b.ReportMetric(float64(rows)*n/wall.Seconds(), "rows/s")
	b.ReportMetric(cpu.Seconds()/n, "cpu-s/op")
	b.ReportMetric(cpu.Seconds()/wall.Seconds(), "cores")
	b.ReportMetric(float64(written)/n/1e6, "MB-written/op")
	b.ReportMetric(float64(size)/1e6, "MB-runs")
}

// writeRunsOnly writes rows events of benchdata into a store in path as runs
// sorted runs, with merges turned off, and returns the bytes of the parts.
func writeRunsOnly(b *testing.B, path string, opts StoreOptions, rows, runs int) int64 {
	b.Helper()
	opts.MergeFanIn = -1
	s, err := OpenStore(path, opts)
	if err != nil {
		b.Fatal(err)
	}
	g := benchdata.New(1, max(rows/8, 1))
	var lines []byte
	for i := range runs {
		n := rows / runs
		if i == runs-1 {
			n = rows - i*n
		}
		lines = g.Append(lines[:0], n)
		if _, err := s.Append(lines); err != nil {
			b.Fatal(err)
		}
		if err := s.Sync(); err != nil {
			b.Fatal(err)
		}
	}
	snap := s.Table().Snapshot()
	var size int64
	for _, h := range snap.Parts() {
		size += int64(h.Size())
	}
	snap.Release()
	if err := s.Close(); err != nil {
		b.Fatal(err)
	}

	return size
}

// linkStore makes dst a store holding the parts of src: part files, which no
// one writes once they are sealed, are linked, and the rest is copied.
func linkStore(b *testing.B, src, dst string) {
	b.Helper()
	if err := os.MkdirAll(dst, 0o755); err != nil {
		b.Fatal(err)
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		b.Fatal(err)
	}
	for _, e := range entries {
		from, to := filepath.Join(src, e.Name()), filepath.Join(dst, e.Name())
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), partSuffix) {
			if err := os.Link(from, to); err != nil {
				b.Fatal(err)
			}

			continue
		}
		in, err := os.Open(from)
		if err != nil {
			b.Fatal(err)
		}
		out, err := os.Create(to)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(out, in); err != nil {
			b.Fatal(err)
		}
		in.Close()
		if err := out.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// runsOf counts the runs of a store's table.
func runsOf(s *Store) int {
	snap := s.Table().Snapshot()
	defer snap.Release()
	seen := map[uint64]bool{}
	for _, h := range snap.Parts() {
		seen[h.Run] = true
	}

	return len(seen)
}

// partitionsOf counts the partitions of a store's table.
func partitionsOf(s *Store) int {
	snap := s.Table().Snapshot()
	defer snap.Release()
	seen := map[int64]bool{}
	for _, h := range snap.Parts() {
		seen[h.Partition] = true
	}

	return len(seen)
}
