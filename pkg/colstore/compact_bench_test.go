package colstore

import (
	"bytes"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"

	"github.com/lynxbase/lynxdb/internal/benchdata"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// BenchmarkCompact merges the sorted runs that ingest leaves into one run per
// partition, as a load that ends with a compaction does, and reports rows per
// second, CPU seconds, the cores that were busy on average, the bytes the
// merges wrote and the peak of the heap. The store is set up as a server sets
// it up for the Bluesky benchmark. LYNXDB_BENCH_MERGE_ROWS sets the events
// (1Mi by default), LYNXDB_BENCH_MERGE_RUNS the runs they are written in (16
// by default), LYNXDB_BENCH_PARTITION the span of a partition (24h by
// default; the events cover about 200 s per million) and
// LYNXDB_BENCH_MERGE_BLOCKS the merge budget of the store;
// LYNXDB_BENCH_BACKGROUND leaves the merge to the background merger of the
// reopened store, whose closed partitions count as quiet, instead of calling
// Compact.
func BenchmarkCompact(b *testing.B) {
	background := os.Getenv("LYNXDB_BENCH_BACKGROUND") != ""
	rows := envInt(b, "LYNXDB_BENCH_MERGE_ROWS", 1<<20)
	runs := envInt(b, "LYNXDB_BENCH_MERGE_RUNS", 16)
	opts := blueskyStoreOptions(b)
	template := filepath.Join(b.TempDir(), "runs")
	size := writeRunsOnly(b, template, opts, rows, runs)
	want := storeSum(b, template, opts)

	var wall, cpu time.Duration
	var written int64
	var peak uint64
	work := b.TempDir()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := filepath.Join(work, "store")
		linkStore(b, template, dir)
		b.StartTimer()
		watch := heapWatch()
		cpuBefore, begin := cpuTime(), time.Now()
		s, err := OpenStore(dir, opts)
		if err != nil {
			b.Fatal(err)
		}
		settle := s.Compact
		if background {
			settle = s.Settle
		}
		if err := settle(); err != nil {
			b.Fatal(err)
		}
		wall, cpu = wall+time.Since(begin), cpu+cpuTime()-cpuBefore
		peak = max(peak, watch())
		b.StopTimer()
		_, merged := s.Written()
		written += merged
		if left := runsOf(s); left != partitionsOf(s) {
			b.Fatalf("%d runs in %d partitions after compaction", left, partitionsOf(s))
		}
		if i == 0 {
			checkMerged(b, s, want)
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
	b.ReportMetric(float64(peak)/1e6, "MB-heap-peak")
}

// blueskyStoreOptions returns the options of a store as a server sets it up
// for the Bluesky benchmark, with LYNXDB_BENCH_PARTITION and
// LYNXDB_BENCH_MERGE_BLOCKS applied.
func blueskyStoreOptions(b *testing.B) StoreOptions {
	b.Helper()
	opts := StoreOptions{
		SortKey:     []string{"kind", "commit.operation", "commit.collection", "did"},
		TimeFields:  []string{"time_us"},
		KeepText:    true,
		Partition:   24 * time.Hour,
		FlushLevel:  part.LevelFastest,
		MergeBlocks: envInt(b, "LYNXDB_BENCH_MERGE_BLOCKS", 0),
	}
	if v := os.Getenv("LYNXDB_BENCH_PARTITION"); v != "" {
		width, err := time.ParseDuration(v)
		if err != nil || width <= 0 {
			b.Fatalf("LYNXDB_BENCH_PARTITION=%q is not a positive duration", v)
		}
		opts.Partition = width
	}

	return opts
}

// BenchmarkLoadAndCompact writes events into a store as a bulk load does, one
// run per flush while the background merger works, and then compacts it. It
// reports what flushes and merges wrote in multiples of the size of the
// compacted store, the share of it that merges wrote before the compaction,
// and the seconds of the load and of the compaction. LYNXDB_BENCH_MERGE_ROWS
// and LYNXDB_BENCH_MERGE_RUNS set the events and the flushes as for
// BenchmarkCompact.
func BenchmarkLoadAndCompact(b *testing.B) {
	rows := envInt(b, "LYNXDB_BENCH_MERGE_ROWS", 1<<21)
	runs := envInt(b, "LYNXDB_BENCH_MERGE_RUNS", 100)
	opts := blueskyStoreOptions(b)
	g := benchdata.New(1, max(rows/8, 1))
	chunks := make([][]byte, runs)
	for i := range chunks {
		n := rows / runs
		if i == runs-1 {
			n = rows - i*n
		}
		chunks[i] = g.Append(nil, n)
	}
	var load, compact time.Duration
	var flushed, merged, before, final int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := filepath.Join(b.TempDir(), "store")
		b.StartTimer()
		begin := time.Now()
		s, err := OpenStore(dir, opts)
		if err != nil {
			b.Fatal(err)
		}
		for _, chunk := range chunks {
			if _, err := s.Append(chunk); err != nil {
				b.Fatal(err)
			}
			if err := s.Sync(); err != nil {
				b.Fatal(err)
			}
		}
		loaded := time.Now()
		load += loaded.Sub(begin)
		_, early := s.Written()
		if err := s.Compact(); err != nil {
			b.Fatal(err)
		}
		compact += time.Since(loaded)
		b.StopTimer()
		f, m := s.Written()
		flushed, merged, before = flushed+f, merged+m, before+early
		final += storeBytes(s)
		if err := s.Close(); err != nil {
			b.Fatal(err)
		}
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	n := float64(b.N)
	b.ReportMetric(float64(flushed+merged)/float64(final), "x-written")
	b.ReportMetric(float64(before)/float64(final), "x-merged-in-load")
	b.ReportMetric(load.Seconds()/n, "load-s")
	b.ReportMetric(compact.Seconds()/n, "compact-s")
}

// storeBytes returns the bytes of the parts of a store's table.
func storeBytes(s *Store) int64 {
	snap := s.Table().Snapshot()
	defer snap.Release()
	var size int64
	for _, h := range snap.Parts() {
		size += int64(h.Size())
	}

	return size
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

// rowSum is a sum over the rows of parts of a hash of each row's values,
// which does not depend on the order of the rows.
type rowSum struct {
	sum  uint64
	rows int
}

func sumRows(b *testing.B, parts []*part.Part) rowSum {
	b.Helper()
	var out rowSum
	var blk part.Block
	var col vec.Column
	var rowHash []uint64
	for _, p := range parts {
		for i := 0; i < p.NumBlocks(); i++ {
			if err := p.OpenBlock(i, &blk); err != nil {
				b.Fatal(err)
			}
			rowHash = append(rowHash[:0], make([]uint64, blk.Rows)...)
			refs := blk.Columns()
			for j := range refs {
				ref := &refs[j]
				if err := blk.Decode(ref, &col); err != nil {
					b.Fatal(err)
				}
				seed := xxhash.Sum64String(p.Paths()[ref.PathID]) + uint64(ref.Kind)*0x9e3779b97f4a7c15
				for r := 0; r < blk.Rows; r++ {
					if !col.IsValid(r) {
						continue
					}
					var v uint64
					switch ref.Kind {
					case vec.KindBool:
						if col.Bool(r) {
							v = 1
						}
					case vec.KindInt:
						v = uint64(col.Int(r))
					case vec.KindFloat:
						v = math.Float64bits(col.Float(r))
					case vec.KindString, vec.KindVariant:
						v = xxhash.Sum64(col.Bytes(r))
					}
					rowHash[r] += (seed ^ v) * 0xff51afd7ed558ccd
				}
			}
			for _, h := range rowHash {
				out.sum += h ^ h>>29
			}
			out.rows += blk.Rows
		}
	}

	return out
}

// storeSum returns the rowSum of the store in path.
func storeSum(b *testing.B, path string, opts StoreOptions) rowSum {
	b.Helper()
	opts.MergeFanIn = -1
	s, err := OpenStore(path, opts)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	snap := s.Table().Snapshot()
	defer snap.Release()
	var parts []*part.Part
	for _, h := range snap.Parts() {
		parts = append(parts, h.Part)
	}

	return sumRows(b, parts)
}

// checkMerged fails unless the store holds the rows of want and every
// partition holds parts whose key ranges follow one another.
func checkMerged(b *testing.B, s *Store, want rowSum) {
	b.Helper()
	snap := s.Table().Snapshot()
	defer snap.Release()
	byPartition := map[int64][]*part.Part{}
	var parts []*part.Part
	for _, h := range snap.Parts() {
		byPartition[h.Partition] = append(byPartition[h.Partition], h.Part)
		parts = append(parts, h.Part)
	}
	if got := sumRows(b, parts); got != want {
		b.Fatalf("compacted store holds %d rows of sum %x, the runs %d of sum %x", got.rows, got.sum, want.rows, want.sum)
	}
	for p, ps := range byPartition {
		slices.SortFunc(ps, func(x, y *part.Part) int {
			a, _ := x.KeyRange()
			c, _ := y.KeyRange()

			return bytes.Compare(a, c)
		})
		for i := 1; i < len(ps); i++ {
			_, last := ps[i-1].KeyRange()
			if first, _ := ps[i].KeyRange(); bytes.Compare(last, first) >= 0 {
				b.Fatalf("partition %d: parts %d and %d of its run overlap", p, i-1, i)
			}
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
