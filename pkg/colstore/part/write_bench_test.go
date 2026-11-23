package part

import (
	"testing"

	"github.com/lynxbase/lynxdb/internal/benchdata"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
)

// countingWriter counts what is written to it.
type countingWriter struct{ n int64 }

func (w *countingWriter) Write(p []byte) (int, error) {
	w.n += int64(len(p))

	return len(p), nil
}

// BenchmarkWriteBluesky encodes generated events sorted by the sort key of
// JSONBench into one part of blocks of 16384 rows at each level, as a flush
// writes them, and at the fastest level without packed identifiers; part-MB
// is the size of the part.
func BenchmarkWriteBluesky(b *testing.B) {
	data := benchdata.New(1, 1<<20).Append(nil, 300000)
	key := []string{"kind", "commit.operation", "commit.collection", "did"}
	batch := shred.NewBatch(shred.Options{TimeField: "time_us"})
	if errs, _ := batch.AddNDJSON(data, 0, shred.Intake{}, nil); len(errs) != 0 {
		b.Fatal(errs[0].Err)
	}
	batch.SortBy(key)
	var blocks []*shred.Batch
	for from := 0; from < batch.Rows; from += 16384 {
		block := shred.NewBatch(shred.Options{})
		batch.CopyRows(block, from, min(from+16384, batch.Rows))
		blocks = append(blocks, block)
	}
	for _, level := range []struct {
		name string
		opts Options
	}{
		{"fast", Options{Level: LevelFast}},
		{"fastest", Options{Level: LevelFastest}},
		{"fastest-nopack", Options{Level: LevelFastest, NoPacking: true}},
		{"compact", Options{Level: LevelCompact}},
	} {
		b.Run(level.name, func(b *testing.B) {
			opts := level.opts
			opts.Ordered, opts.SortKey = true, key
			var out countingWriter
			w := NewWriter(&out, opts)
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out.n = 0
				w.Reset(&out, opts)
				for _, block := range blocks {
					if err := w.WriteBatch(block); err != nil {
						b.Fatal(err)
					}
				}
				if err := w.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(out.n)/1e6, "part-MB")
		})
	}
}
