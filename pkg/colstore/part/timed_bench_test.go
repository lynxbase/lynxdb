package part

import (
	"bytes"
	"testing"

	"github.com/lynxbase/lynxdb/internal/benchdata"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// sortedBlocks returns the generator's events sorted by the benchmark's key
// in blocks of blockRows rows.
func sortedBlocks(t testing.TB, events, blockRows int) []*shred.Batch {
	t.Helper()
	key := []string{"kind", "commit.operation", "commit.collection", "did"}
	all := shred.NewBatch(shred.Options{TimeField: "time_us"})
	if errs, _ := all.AddNDJSON(benchdata.New(3, 300000).Append(nil, events), 0, shred.Intake{}, nil); len(errs) != 0 {
		t.Fatal(errs[0].Err)
	}
	all.SortBy(key)
	var out []*shred.Batch
	for from := 0; from < all.Rows; from += blockRows {
		b := shred.NewBatch(shred.Options{})
		all.CopyRows(b, from, min(from+blockRows, all.Rows))
		out = append(out, b)
	}

	return out
}

// BenchmarkTimedColumn writes and reads the revisions of the generator's
// events as timed chunks, per value.
func BenchmarkTimedColumn(b *testing.B) {
	blocks := sortedBlocks(b, 65536, 16384)
	values := 0
	for _, bl := range blocks {
		values += bl.Lookup("commit.rev", vec.KindString).Count
	}
	b.Run("write", func(b *testing.B) {
		var buf bytes.Buffer
		w := NewWriter(&buf, Options{})
		for b.Loop() {
			for _, bl := range blocks {
				w.block, w.timedRefs = w.block[:0], w.timedRefs[:0]
				if !w.appendTimed(bl.Lookup("commit.rev", vec.KindString), bl.Times) {
					b.Fatal("the revisions are not timed")
				}
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*values), "ns/value")
	})
	b.Run("read", func(b *testing.B) {
		var out bytes.Buffer
		w := NewWriter(&out, Options{})
		for _, bl := range blocks {
			if err := w.WriteBatch(bl); err != nil {
				b.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
		p, err := Open(out.Bytes())
		if err != nil {
			b.Fatal(err)
		}
		id, _ := p.PathID("commit.rev")
		var blk Block
		var col vec.Column
		for b.Loop() {
			for i := 0; i < p.NumBlocks(); i++ {
				if err := p.OpenBlock(i, &blk); err != nil {
					b.Fatal(err)
				}
				ref, _ := blk.Column(id, vec.KindString)
				if err := blk.DecodeValues(ref, &col); err != nil {
					b.Fatal(err)
				}
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*values), "ns/value")
	})
}
