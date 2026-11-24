package colstore

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/internal/benchdata"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// repeatedColumns are the string columns of the generated events whose
// values repeat across the blocks of a part.
var repeatedColumns = []string{
	"did",
	"commit.record.subject",
	"commit.record.subject.cid",
	"commit.record.subject.uri",
	"commit.record.reply.root.cid",
	"commit.record.reply.parent.cid",
	"commit.cid",
}

// BenchmarkRepeatedColumns loads generated events into a store in two
// halves, each written as runs and then compacted, so that the second
// compaction merges a compacted run with flushed ones, in the design of
// rows in time order and in the one ordered by the key of the Bluesky
// benchmark. It reports the rows a second of the flushes and of the two
// compactions, the peak of the heap while compacting, the bytes of the
// store, and for the columns whose values repeat across blocks the bytes a
// value, their part dictionaries among them, and the nanoseconds a value of
// decoding their strings and, where blocks store codes of a part dictionary,
// the codes alone, with the pages of the parts in memory. LYNXDB_BENCH_DICT_ROWS sets the events (4Mi by default),
// LYNXDB_BENCH_DICT_RUNS the runs of each half (8), LYNXDB_BENCH_DICT_USERS
// the accounts (a sixteenth of the events), LYNXDB_BENCH_DICT_FAVOUR the
// power that favours the active ones (2) and LYNXDB_BENCH_DICT_POSTS the
// latest posts that likes and replies name (an eighth of the events).
func BenchmarkRepeatedColumns(b *testing.B) {
	rows := envInt(b, "LYNXDB_BENCH_DICT_ROWS", 1<<22)
	runs := envInt(b, "LYNXDB_BENCH_DICT_RUNS", 8)
	users := envInt(b, "LYNXDB_BENCH_DICT_USERS", max(rows/16, 1))
	favour := envInt(b, "LYNXDB_BENCH_DICT_FAVOUR", 2)
	posts := envInt(b, "LYNXDB_BENCH_DICT_POSTS", max(rows/8, 1))
	designs := []struct {
		name string
		opts StoreOptions
	}{
		{"time", StoreOptions{TimeFields: []string{"time_us"}, Partition: 24 * time.Hour, FlushLevel: part.LevelFastest}},
		{"keyed", StoreOptions{
			SortKey:    []string{"kind", "commit.operation", "commit.collection", "did"},
			TimeFields: []string{"time_us"}, Partition: 24 * time.Hour, FlushLevel: part.LevelFastest,
		}},
	}
	loaded := rows / (2 * runs) * (2 * runs)
	for _, d := range designs {
		b.Run(d.name, func(b *testing.B) {
			var flush, first, second time.Duration
			var peak uint64
			var report string
			var chunk []byte
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dir := filepath.Join(b.TempDir(), "store")
				opts := d.opts
				opts.MergeFanIn = -1
				g := benchdata.New(1, users).Favour(favour).Posts(posts)
				b.StartTimer()
				s, err := OpenStore(dir, opts)
				if err != nil {
					b.Fatal(err)
				}
				// The events of a run are made as it is loaded, so that the
				// heap holds one run of them and what the store holds.
				load := func() {
					for range runs {
						chunk = g.Append(chunk[:0], loaded/(2*runs))
						begin := time.Now()
						if _, err := s.Append(chunk); err != nil {
							b.Fatal(err)
						}
						if err := s.Sync(); err != nil {
							b.Fatal(err)
						}
						flush += time.Since(begin)
					}
				}
				compact := func() time.Duration {
					watch := heapWatch()
					begin := time.Now()
					if err := s.Compact(); err != nil {
						b.Fatal(err)
					}
					took := time.Since(begin)
					peak = max(peak, watch())

					return took
				}
				load()
				first += compact()
				load()
				second += compact()
				b.StopTimer()
				if i == 0 {
					report = repeatedReport(b, s)
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
			b.ReportMetric(float64(loaded)*n/flush.Seconds(), "flush-rows/s")
			b.ReportMetric(float64(loaded/2)*n/first.Seconds(), "merge1-rows/s")
			b.ReportMetric(float64(loaded)*n/second.Seconds(), "merge2-rows/s")
			b.ReportMetric(float64(peak)/1e6, "MB-heap-peak")
			// The log of a benchmark keeps ten lines.
			fmt.Printf("%s\n%s", b.Name(), report)
		})
	}
}

// repeatedReport returns a table of the store's size and of what the
// columns whose values repeat take and cost to decode.
func repeatedReport(b *testing.B, s *Store) string {
	b.Helper()
	snap := s.Table().Snapshot()
	defer snap.Release()
	var u part.Usage
	for _, h := range snap.Parts() {
		if err := h.AddUsage(&u, nil); err != nil {
			b.Fatal(err)
		}
	}
	var out strings.Builder
	fmt.Fprintf(&out, "parts %d, rows %d, bytes %d, bytes/row %.2f\n", u.Parts, u.Rows, u.Bytes, float64(u.Bytes)/float64(u.Rows))
	var parts []*part.Part
	for _, h := range snap.Parts() {
		parts = append(parts, h.Part)
	}
	// A sum over the values of every row, which a change of how they are
	// stored leaves as it is.
	fmt.Fprintf(&out, "sum of rows %x\n", sumRows(b, parts).sum)
	fmt.Fprintf(&out, "%-34s %10s %12s %8s %10s %12s %8s %8s  %s\n",
		"column", "values", "bytes", "B/value", "entries", "dict-bytes", "ns/value", "ns/code", "encodings")
	columns := repeatedColumns
	if os.Getenv("LYNXDB_BENCH_DICT_ALL") != "" {
		// Every string column, for looking into a difference.
		columns = nil
		for key := range u.Columns {
			if key.Kind == vec.KindString {
				columns = append(columns, key.Path)
			}
		}
		slices.Sort(columns)
		for name, n := range u.Regions {
			fmt.Fprintf(&out, "region %s %d\n", name, n)
		}
		for key, c := range u.Columns {
			if key.Kind != vec.KindString {
				fmt.Fprintf(&out, "other %s/%s %d\n", key.Path, key.Kind, columnBytes(c)+c.FilterBytes)
			}
		}
	}
	for _, path := range columns {
		c := u.Columns[part.ColumnKey{Path: path, Kind: vec.KindString}]
		if c == nil {
			continue
		}
		size := columnBytes(c)
		var encodings []string
		for name, a := range c.Encodings {
			encodings = append(encodings, fmt.Sprintf("%s:%d", name, a.Blocks))
		}
		slices.Sort(encodings)
		var entries uint64
		var dict int64
		if c.Dictionary != nil {
			entries, dict = c.Dictionary.Entries, c.Dictionary.Bytes
		}
		fmt.Fprintf(&out, "%-34s %10d %12d %8.2f %10d %12d %8.1f %8.1f  %s\n", path, c.Values, size, float64(size)/float64(c.Values),
			entries, dict, decodeCost(b, snap.Parts(), path, false), decodeCost(b, snap.Parts(), path, true), strings.Join(encodings, " "))
	}

	return out.String()
}

// columnBytes returns the bytes that a column takes in its parts, its part
// dictionaries among them and the token filters of its own left out.
func columnBytes(c *part.ColumnUsage) int64 {
	size := c.ChunkBytes + c.DirectoryBytes
	if c.Dictionary != nil {
		size += c.Dictionary.Bytes
	}

	return size
}

// decodeCost returns the nanoseconds a value that decoding the string
// column at path takes over all blocks of parts, the lower of two passes;
// with codes set it reads the codes of the blocks that store codes of a
// part dictionary and no other block, and returns zero when there is none.
func decodeCost(b *testing.B, parts []*Handle, path string, codes bool) float64 {
	b.Helper()
	var blk part.Block
	var col vec.Column
	var buf []uint32
	best := time.Duration(1<<63 - 1)
	values := 0
	for range 2 {
		values = 0
		begin := time.Now()
		for _, h := range parts {
			id, ok := h.PathID(path)
			if !ok {
				continue
			}
			for i := 0; i < h.NumBlocks(); i++ {
				if err := h.OpenBlock(i, &blk); err != nil {
					b.Fatal(err)
				}
				ref, ok := blk.Column(id, vec.KindString)
				if !ok || ref.Count == 0 || (codes && !ref.Shared()) {
					continue
				}
				var err error
				if codes {
					buf, err = blk.DecodeShared(ref, &col, buf)
				} else {
					err = blk.Decode(ref, &col)
				}
				if err != nil {
					b.Fatal(err)
				}
				values += ref.Count
			}
		}
		best = min(best, time.Since(begin))
	}
	if values == 0 {
		return 0
	}

	return float64(best.Nanoseconds()) / float64(values)
}
